"""Driver-side Spark batches: declarative operations dispatch directly.

The CLI loads this module only when executing a job. Steps build lazy DataFrames; outputs each trigger one write.
Pinned reference rows, file plans and persisted frames retain their replay and cleanup semantics.
"""

import json
import logging
import math
from types import MappingProxyType

from pyspark.storagelevel import StorageLevel

from streamwright.core.jobs import ExecutionResult, WriteReceipt, calls
from streamwright.core.jobs.policy import SKIPPED, ExecutionPolicy, classify_transient, guard
from streamwright.core.jobs.refs import step_columns_used, walk_strings
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.engine.session import resolve_driver_host
from streamwright.spark.plan import build_plan, role_of
from streamwright.spark.sdk import SparkContext

__all__ = ["SparkExecutor"]

LOG = logging.getLogger("streamwright.spark")

_PERSIST_SHORTHANDS = MappingProxyType(
    {
        "memory": "MEMORY_ONLY",
        "memory_and_disk": "MEMORY_AND_DISK",
        "disk": "DISK_ONLY",
    }
)


def _resolve_storage_level(level):
    canonical = _PERSIST_SHORTHANDS.get(str(level).strip().lower(), str(level).strip().upper())
    if hasattr(StorageLevel, canonical):
        return getattr(StorageLevel, canonical)
    raise ValueError("Unknown Spark StorageLevel %r" % (level,))


def _retrying(surface, name):
    """A `guard` on_retry callback that logs a retry without the (possibly sensitive) error value."""
    def log(exc, delay, number):
        LOG.warning("%s %s: attempt %d failed (%s), retrying in %.1fs", surface, name, number,
                    type(exc).__name__, delay)
    return log


def _tolerated(surface, name):
    """A `guard` on_tolerated callback: log the skip/warn (the error type only)."""
    def note(exc, mode):
        (LOG.warning if mode == "warn" else LOG.error)("%s %s %s on %s: continuing", surface, name, mode,
                                                       type(exc).__name__)
    return note


class SparkExecutor:
    def __init__(self, spark, job, connections, adapters, *, config, limits, state=None, declared_connections=None,
                 secrets=None, allowed_connectors=None):
        """`connections`: resolved by name; `adapters`: definition adapters by provider.

        `declared_connections` (as written, secret references kept), `secrets` and `allowed_connectors` are what
        source-connector call steps hand to the source engine on the driver (contract v1.2 §6a engine reach).
        """
        self.spark, self.job, self.connections, self.adapters = spark, job, connections, adapters
        self.config, self.limits = config, limits
        self.state = state
        self.declared_connections = dict(declared_connections or {})
        self.secrets = dict(secrets or {})
        self.allowed_connectors = allowed_connectors
        self.carry = {}
        self.plan = build_plan(job, adapters)
        self.pinned_steps = set(self.plan.pinned_steps)
        self.accepted_signatures = []
        self.schema_reports = []

    # --- entry point used by Runner --------------------------------------------------------------------------

    def execute(self, ctx, manifest, done, hooks):
        rt = StepRuntime(
            self.spark, ctx, manifest, self.connections, self.adapters, limits=self.limits, state=self.state
        )
        driver_host = ctx.engine_metadata.get("driver_host")
        if not driver_host and self.spark is not None:
            try:
                driver_host = resolve_driver_host(self.spark)
            except Exception:
                driver_host = "unknown"
        if not driver_host:
            driver_host = "unknown"
        self.params = dict(self.config, batch_id=ctx.batch_id, batch_ts=ctx.batch_ts, driver_host=driver_host)
        self.carry = manifest.setdefault("carry", {})
        self.messages = self.spark.createDataFrame(
            [(m["message_id"], m["key"], m["body"], m["headers"], m["timestamp"]) for m in manifest["messages"]],
            "message_id STRING, key STRING, body STRING, headers MAP<STRING,STRING>, timestamp STRING",
        )
        self.messages.createOrReplaceTempView("input")
        persisted = []
        try:
            self._run_steps(rt, persisted)
            receipts = dict(done)
            writes, publishes = self._outputs(rt)
            hooks.save_manifest(manifest)
            self._runtime_tables(rt, receipts)  # _file_errors is complete once the steps are built
            self._run_writes(rt, writes, done=done, receipts=receipts, hooks=hooks, manifest=manifest)
            return self._result(rt, self._publish(rt, publishes))
        finally:
            for frame in persisted:
                frame.unpersist()

    def _run_steps(self, runtime, persisted):
        steps = {step["name"]: step for step in self.job.get("steps") or []}
        for step in (steps[name] for name in self.plan.order):  # dependency order, as DuckDBJob runs them
            policy = ExecutionPolicy.of(step)
            outcome = guard(policy, lambda step=step, rt=runtime: self._step(rt, step),
                            classify=classify_transient(policy), on_retry=_retrying("step", step["name"]),
                            on_tolerated=_tolerated("step", step["name"]))
            frame = self._empty_frame() if outcome is SKIPPED else outcome
            if step.get("persist"):
                frame = frame.persist(_resolve_storage_level(step["persist"]))
                persisted.append(frame)
            elif step.get("checkpoint"):
                frame = frame.checkpoint(eager=False)
            if str(step.get("view", "")).strip().lower() == "global":
                frame.createOrReplaceGlobalTempView(step["name"])
            frame.createOrReplaceTempView(step["name"])
            if step["name"] in self.pinned_steps:
                # Whole-step references pin rows once so later outputs replay the same input.
                frame = self.spark.createDataFrame(runtime.rows(step["name"]), frame.schema)
                if str(step.get("view", "")).strip().lower() == "global":
                    frame.createOrReplaceGlobalTempView(step["name"])
                frame.createOrReplaceTempView(step["name"])

    def _outputs(self, runtime):
        writes, publishes = [], []
        for output in self.job.get("outputs") or []:
            target = publishes if role_of(self.job, output, self.adapters) == "publish" else writes
            target.append(output)
        for output in writes + publishes:
            for text in walk_strings(output.get("arguments") or {}):
                for step, column in step_columns_used(text):
                    runtime.values(step, column)
        return writes, publishes

    def _run_writes(self, runtime, writes, *, done, receipts, hooks, manifest):
        for output in writes:
            if output["name"] in done:
                continue

            def write(output=output, rt=runtime):
                hooks.fence()
                return self._write(rt, output)

            policy = ExecutionPolicy.of(output)
            receipt = guard(policy, write, classify=classify_transient(policy),
                            on_retry=_retrying("output", output["name"]),
                            on_tolerated=_tolerated("output", output["name"]))
            if receipt is SKIPPED:
                receipt = WriteReceipt(output["name"], "skipped (on_error: %s)" % policy.on_error, 0)
            hooks.record_receipt(receipt, manifest)
            receipts[output["name"]] = receipt.to_dict()
            self._runtime_tables(runtime, receipts)  # later outputs may read _commits

    def _publish(self, runtime, outputs):
        events = {}
        for output in outputs:
            rows = self.spark.sql(output["sql"], args=self.params).limit(runtime.cap + 1).collect()
            if len(rows) > runtime.cap:
                raise ValueError("event output %s has more than %d rows" % (output["name"], runtime.cap))
            events[output["name"]] = [row.asDict(recursive=True) for row in rows]
        return events

    def _result(self, runtime, events):
        self.accepted_signatures = [
            (report.prefix, report.header_hash, json.dumps(report.mapped))
            for report in runtime.schema_reports
            if report.status in ("ok", "drift") and report.prefix and report.header_hash
        ]
        self.schema_reports = list(runtime.schema_reports)
        return ExecutionResult(events, self.accepted_signatures, self.schema_reports)

    # --- steps and outputs -----------------------------------------------------------------------------------

    def _step(self, rt, step):
        if not step.get("connection"):
            return self.spark.sql(step["sql"], args=self.params)
        conn = self.connections.get(step["connection"])
        if conn is None:
            frame = self._source_call(rt, step)            # a source connector, run on the driver as a call step
        else:
            conn, adapter, method = self._resolve(step)
            frame = (
                self.messages
                if method.kind == "consume"
                else adapter.adapter.read(SparkContext(rt, step["name"], conn.auth), method,
                                          step.get("arguments") or {})
            )
        if step.get("sql"):
            frame.createOrReplaceTempView("input")  # a call's own result is `input` for its sql
            frame = self.spark.sql(step["sql"], args=self.params)
            self.messages.createOrReplaceTempView("input")  # existing DataFrames keep their resolved plan
        return frame

    def _source_call(self, runtime, step):
        """
        Run a source connector on the driver (SourceRunner.collect), once per partition scope (calls.partition_scopes:
        the pinned distinct values of an earlier step's column), and load its records into a DataFrame.
        """
        declared = self.declared_connections[step["connection"]]
        carry_key = "bookmarks:%s" % step["name"]
        state, records = self.carry.get(carry_key), []

        def values(index, part):
            return [value for value in runtime.values(part["from"], part["field"]) if value is not None]

        for scope in calls.partition_scopes(step, values):
            found, collected = calls.call_records(step, declared, self.config, self.secrets,
                                                  allowed_connectors=self.allowed_connectors, state=state, scope=scope)
            records.extend(found)
            if collected.partial:
                raise ValueError("step %s: the source connector loaded partially" % step["name"])
            state = collected.state
        self.carry[carry_key] = state
        return self._frame_from_records(records, step)

    def _frame_from_records(self, records, step):
        """Records (dicts, as the connector returned them) into a DataFrame, in bounded batches (`page_size`)."""
        lines = [json.dumps(record, default=str) for record in records]
        if not lines:
            return self.spark.createDataFrame([], "record STRING")
        batch = step.get("page_size") or 5000
        slices = max(1, math.ceil(len(lines) / int(batch)))
        return self.spark.read.json(self.spark.sparkContext.parallelize(lines, slices))

    def _empty_frame(self):
        """A placeholder frame for a step whose on_error policy tolerated a failure."""
        return self.spark.createDataFrame([], "record STRING")

    def _write(self, rt, out):
        conn, adapter, method = self._resolve(out)
        frame = self.spark.sql(out["sql"], args=self.params)
        return adapter.adapter.write(
            SparkContext(rt, out["name"], conn.auth), frame, method, out.get("arguments") or {}
        )

    def _resolve(self, item):
        conn = self.connections[item["connection"]]
        adapter = self.adapters[conn.provider]
        operation = adapter.operation(item["service"], item["method"])
        if operation is None:
            raise ValueError("unavailable Spark operation %s.%s" % (item["service"], item["method"]))
        return conn, adapter, operation

    # --- runtime tables --------------------------------------------------------------------------------------

    def _runtime_tables(self, rt, receipts):
        rows = [
            (name, r["destination"], r.get("version"), r.get("rows"), rt.ctx.batch_id)
            for name, r in sorted(receipts.items())
        ]
        self.spark.createDataFrame(
            rows, "output STRING, table_name STRING, version BIGINT, rows BIGINT, batch_id STRING"
        ).createOrReplaceTempView("_commits")
        self.spark.createDataFrame(
            list(rt.file_errors), "step STRING, path STRING, reason STRING"
        ).createOrReplaceTempView("_file_errors")
        schema_rows = [r.as_tuple() if hasattr(r, "as_tuple") else r for r in getattr(rt, "schema_reports", [])]
        schema_ddl = (
            "step STRING, path STRING, prefix STRING, status STRING, header_hash STRING, "
            "mapped ARRAY<STRING>, unmapped ARRAY<STRING>, missing_optional ARRAY<STRING>"
        )
        if schema_rows:
            self.spark.createDataFrame(schema_rows, schema_ddl).createOrReplaceTempView("_schema_report")
        else:
            self.spark.createDataFrame([], schema_ddl).createOrReplaceTempView("_schema_report")
