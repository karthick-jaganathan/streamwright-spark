"""
The Spark side of a batch: build the steps lazily, run each output with one action, produce the event rows. PROPOSED.

NOT RUN YET: needs pyspark, delta-spark, a Hadoop file system and the brokers, none of which exist in the authoring
sandbox. Covered by the Docker acceptance jobs, not by the unit tests.

This module never names a provider. A call is run by the adapter method its table entry names:
    handler = getattr(provider, method.handler);  handler(rt, connection, step_name, arguments)
Event methods (a consume step, a publish output) are handled by role, through the adapter's broker.

Plan rules (see plan/graph.py):
  * steps create DataFrames and temp views only; no count/collect/show on a step except the reference reads
  * each output performs one action: its write
  * persist only where the job says so; every persisted step is unpersisted at the end of the batch
  * values collected for references are pinned in the manifest, so a replay uses the same files, mappings and predicates
"""

import dataclasses

from streamwright.spark.contracts import Role
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.plan import build_plan, role_of


__all__ = ["SparkExecutor"]

_PERSIST_SHORTHANDS = {"memory": "MEMORY_ONLY", "memory_and_disk": "MEMORY_AND_DISK", "disk": "DISK_ONLY"}


def _resolve_storage_level(level):
    from pyspark.storagelevel import StorageLevel
    canonical = _PERSIST_SHORTHANDS.get(str(level).strip().lower(), str(level).strip().upper())
    if hasattr(StorageLevel, canonical):
        return getattr(StorageLevel, canonical)
    raise ValueError("Unknown Spark StorageLevel %r" % (level,))


class SparkExecutor(object):
    def __init__(self, spark, job, connections, adapters, config, limits):
        """`connections`: {name: Connection} (resolved); `adapters`: {provider: Provider instance}."""
        self.spark, self.job, self.connections, self.adapters = spark, job, connections, adapters
        self.config, self.limits = config, limits
        self.plan = build_plan(job, adapters)
        self.pinned_steps = set(self.plan.pinned_steps)

    # --- entry point used by Runner --------------------------------------------------------------------------

    def execute(self, ctx, manifest, done, on_receipt):
        from pyspark.storagelevel import StorageLevel

        spark, job = self.spark, self.job
        ctx = dataclasses.replace(ctx, spark=spark)
        rt = StepRuntime(spark, ctx, manifest, self.connections, self.adapters, self.limits)
        self.params = dict(self.config, batch_id=ctx.batch_id, batch_ts=ctx.batch_ts)
        self.messages = spark.createDataFrame(
            [(m["message_id"], m["key"], m["body"], m["headers"], m["timestamp"]) for m in manifest["messages"]],
            "message_id STRING, key STRING, body STRING, headers MAP<STRING,STRING>, timestamp STRING")
        self.messages.createOrReplaceTempView("input")
        persisted = []
        try:
            for step in job.get("steps") or []:
                frame = self._step(rt, step)
                if step.get("persist"):
                    frame = frame.persist(_resolve_storage_level(step["persist"]))
                    persisted.append(frame)
                elif step.get("checkpoint"):
                    frame = frame.checkpoint(eager=False)
                frame.createOrReplaceTempView(step["name"])
                if step["name"] in self.pinned_steps:
                    # used whole by {{ step }}: collect once (pinned in the manifest) and rebuild from those rows, so
                    # every output sees the same mapping and the source is read once per batch
                    frame = spark.createDataFrame(rt.rows(step["name"]), frame.schema)
                    frame.createOrReplaceTempView(step["name"])
            receipts, events = dict(done), {}
            writes, publishes = [], []
            for out in job.get("outputs") or []:
                (publishes if role_of(job, out, self.adapters) == Role.PUBLISH else writes).append(out)
            self._runtime_tables(rt, receipts)             # _file_errors is complete once the steps are built
            for out in writes:
                if out["name"] in done:
                    continue
                receipt = self._write(rt, out)
                on_receipt(receipt)
                receipts[out["name"]] = {"target": receipt.target, "rows": receipt.rows, "version": receipt.version}
                self._runtime_tables(rt, receipts)         # later outputs may read _commits
            for out in publishes:
                rows = self.spark.sql(out["sql"], args=self.params).limit(rt.cap + 1).collect()
                if len(rows) > rt.cap:
                    raise ValueError("event output %s has more than %d rows" % (out["name"], rt.cap))
                events[out["name"]] = [r.asDict(recursive=True) for r in rows]
            return events
        finally:
            for frame in persisted:
                frame.unpersist()

    # --- steps and outputs -----------------------------------------------------------------------------------

    def _step(self, rt, step):
        if not step.get("connection"):
            return self.spark.sql(step["sql"], args=self.params)
        conn, adapter, method = self._resolve(step)
        if method.role == Role.CONSUME:
            frame = self.messages
        else:
            frame = getattr(adapter, method.handler)(rt, conn, step["name"], step.get("arguments") or {})
        if step.get("sql"):
            frame.createOrReplaceTempView("input")             # a call's own result is `input` for its sql
            frame = self.spark.sql(step["sql"], args=self.params)
            self.messages.createOrReplaceTempView("input")     # existing DataFrames keep their resolved plan
        return frame

    def _write(self, rt, out):
        conn, adapter, method = self._resolve(out)
        frame = self.spark.sql(out["sql"], args=self.params)
        return getattr(adapter, method.handler)(rt, conn, out["name"], method, out.get("arguments") or {}, frame)

    def _resolve(self, item):
        conn = self.connections[item["connection"]]
        adapter = self.adapters[conn.provider]
        return conn, adapter, adapter.method(item["service"], item["method"])

    # --- runtime tables --------------------------------------------------------------------------------------

    def _runtime_tables(self, rt, receipts):
        rows = [(name, r["target"], r.get("version"), r.get("rows"), rt.ctx.batch_id) for name, r in sorted(receipts.items())]
        self.spark.createDataFrame(rows, "output STRING, table_name STRING, version BIGINT, rows BIGINT, batch_id STRING"
                                   ).createOrReplaceTempView("_commits")
        self.spark.createDataFrame(list(rt.file_errors), "step STRING, path STRING, reason STRING"
                                   ).createOrReplaceTempView("_file_errors")
