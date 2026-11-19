#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""Driver-side Spark batches: declarative operations dispatch directly.

The CLI loads this module only when executing a job. Steps build lazy DataFrames; outputs each trigger one write.
Pinned reference rows, file plans and persisted frames retain their replay and cleanup semantics.
"""

import itertools
import json
import logging

from pyspark.storagelevel import StorageLevel

from streamwright.core.jobs import ExecutionResult, WriteReceipt, calls
from streamwright.core.jobs.policy import SKIPPED, ExecutionPolicy, classify_transient, guard
from streamwright.core.jobs.refs import step_columns_used, walk_strings
from streamwright.core.jobs.step_log import StepLog, log_step_rows
from streamwright.core.runtime.logs.context import log_context
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.engine.session import resolve_driver_host
from streamwright.spark.job.vocabulary import RECORDS_VIEW, limit, storage_level_name
from streamwright.spark.plan import build_plan, role_of
from streamwright.spark.sdk import SparkContext

__all__ = ["SparkExecutor"]

LOG = logging.getLogger("streamwright.spark")

_RECORDS_SCHEMA = "record STRING"
_LOAD_BATCH = 5000


def _resolve_storage_level(level):
    name = storage_level_name(level)
    if name is None:
        raise ValueError("Unknown Spark StorageLevel %r" % (level,))
    return getattr(StorageLevel, name)


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
                 secrets=None, allowed_connectors=None, apply_outputs=(), redact=None):
        """`connections`: resolved by name; `adapters`: definition adapters by provider; `apply_outputs`: the outputs
        that apply connector operations (their rows go to the outbox, like publish outputs').

        `declared_connections` (as written, secret references kept), `secrets` and `allowed_connectors` are what
        source-connector call steps hand to the source engine on the driver (contract v1.2 §6a engine reach).
        `redact` scrubs the lines of a step's `log:`.
        """
        self.spark, self.job, self.connections, self.adapters = spark, job, connections, adapters
        self.config, self.limits = config, limits
        self.state = state
        self.declared_connections = dict(declared_connections or {})
        self.secrets = dict(secrets or {})
        self.allowed_connectors = allowed_connectors
        self.apply_outputs = frozenset(apply_outputs)
        self.redact = redact
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
            with log_context(step=step["name"]):
                outcome = guard(policy, lambda step=step, rt=runtime: self._step(rt, step),
                                classify=classify_transient(policy), on_retry=_retrying("step", step["name"]),
                                on_tolerated=_tolerated("step", step["name"]))
            frame = self._empty_frame() if outcome is SKIPPED else outcome
            if step.get("persist"):
                frame = frame.persist(_resolve_storage_level(step["persist"]))
                persisted.append(frame)
            if step.get("checkpoint"):
                frame = frame.checkpoint(eager=False)       # after persist: the checkpoint job reads the cache
            if str(step.get("view", "")).strip().lower() == "global":
                frame.createOrReplaceGlobalTempView(step["name"])
            frame.createOrReplaceTempView(step["name"])
            if step["name"] in self.pinned_steps:
                # Whole-step references pin rows once so later outputs replay the same input.
                frame = self.spark.createDataFrame(runtime.rows(step["name"]), frame.schema)
                if str(step.get("view", "")).strip().lower() == "global":
                    frame.createOrReplaceGlobalTempView(step["name"])
                frame.createOrReplaceTempView(step["name"])
            if outcome is not SKIPPED and step.get("log"):
                with log_context(step=step["name"]):
                    self._log_rows(runtime, step, frame)

    def _log_rows(self, runtime, step, frame):
        """
        A step's `log:`: one Spark action collects `limit(max_rows + 1)` rows on the driver; a second one, `count()`,
        runs only when there are more (persist the step to keep both from computing it again).
        """
        log_step_rows(StepLog.of(step), frame.columns, lambda n: [tuple(row) for row in frame.limit(n).collect()],
                      frame.count, constants=runtime.builtins(), redact=self.redact)

    def _outputs(self, runtime):
        writes, publishes = [], []
        for output in self.job.get("outputs") or []:
            outboxed = output["name"] in self.apply_outputs or role_of(self.job, output, self.adapters) == "publish"
            target = publishes if outboxed else writes
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
            with log_context(output=output["name"]):
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
            applies = output["name"] in self.apply_outputs
            key = "apply_rows" if applies else "events"
            cap = limit(self.limits, key)
            with log_context(output=output["name"]):
                rows = self.spark.sql(output["sql"], args=self.params).limit(cap + 1).collect()
                if len(rows) > cap:
                    raise ValueError("%s output %s has more than %d rows (runtime.limits.%s)" % (
                        "apply" if applies else "event", output["name"], cap, key))
                events[output["name"]] = [row.asDict(recursive=True) for row in rows]
                LOG.info("output %s: %d %s to the outbox", output["name"], len(rows),
                         "operations" if applies else "events")
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
        if self.connections.get(step["connection"]) is None:
            return self._source_call(rt, step)            # a source connector, run on the driver as a call step
        conn, adapter, method = self._resolve(step)
        frame = (
            self.messages
            if method.kind == "consume"
            else adapter.adapter.read(SparkContext(rt, step["name"], conn.auth), method, step.get("arguments") or {})
        )
        if step.get("sql"):
            frame.createOrReplaceTempView("input")  # a native read's own result is `input` for its sql
            frame = self.spark.sql(step["sql"], args=self.params)
            self.messages.createOrReplaceTempView("input")  # existing DataFrames keep their resolved plan
        return frame

    def _source_call(self, runtime, step):
        """
        A call step: its records (a source connector run on the driver), as DuckDBJob's call step has them. With `sql`,
        the step is that query over `records` (one `record` column, the JSON text of each record: a stable schema
        however many records came); without, the records' own columns (inferred, `record` when there are none).
        """
        lines = self._record_lines(self._capped(self._call_records(runtime, step), step), step)
        if not step.get("sql"):
            return self._empty_frame() if lines is None else self.spark.read.json(lines)
        records = (self._empty_frame() if lines is None
                   else self.spark.createDataFrame(lines, "string").toDF("record"))
        records.createOrReplaceTempView(RECORDS_VIEW)
        try:
            return self.spark.sql(step["sql"], args=self.params)
        finally:
            self.spark.catalog.dropTempView(RECORDS_VIEW)   # the step's DataFrame keeps its resolved plan

    def _call_records(self, runtime, step):
        """
        The records of a call step's connector, lazily: once per partition scope (calls.partition_scopes: the pinned
        distinct values of an earlier step's column); its bookmarks are carried once every scope has been read.
        """
        declared = self.declared_connections[step["connection"]]
        carry_key = "bookmarks:%s" % step["name"]
        state = self.carry.get(carry_key)

        def values(index, part):
            return [value for value in runtime.values(part["from"], part["field"]) if value is not None]

        for scope in calls.partition_scopes(step, values):
            found, collected = calls.call_records(step, declared, self.config, self.secrets,
                                                  allowed_connectors=self.allowed_connectors, state=state, scope=scope)
            yield from found
            if collected.partial:
                raise ValueError("step %s: the source connector loaded partially" % step["name"])
            state = collected.state
        self.carry[carry_key] = state

    def _capped(self, records, step):
        """
        `records`, failing as soon as there are more than `runtime.limits.call_records`: the connector's stream is
        closed then (its reader stops after the pages it had already buffered) rather than read to its end.
        """
        cap = limit(self.limits, "call_records")
        try:
            for count, record in enumerate(records, 1):
                if count > cap:
                    raise ValueError("call step %s reached %d records, more than runtime.limits.call_records (%d)"
                                     % (step["name"], count, cap))
                yield record
        finally:
            records.close()

    def _record_lines(self, records, step):
        """
        `records` (dicts, as the connector returned them) as an RDD of their JSON texts (None: no records), loaded in
        chunks (`page_size`, default 5000): Python holds one chunk at a time; the parallelized chunks are kept by the
        driver JVM until Spark reads them, so a call step's result has to fit the driver's memory
        (runtime.limits.call_records caps it).
        """
        size, chunks, iterator = int(step.get("page_size") or _LOAD_BATCH), [], iter(records)
        context = self.spark.sparkContext
        while chunk := [json.dumps(record, default=str) for record in itertools.islice(iterator, size)]:
            chunks.append(context.parallelize(chunk, 1))
        return context.union(chunks) if chunks else None

    def _empty_frame(self):
        """A frame of no records: a call step that read none, a step whose on_error policy tolerated a failure."""
        return self.spark.createDataFrame([], _RECORDS_SCHEMA)

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
