"""
The batch loop and delivery guarantees. PROPOSED. No Spark import: the Spark work is behind `executor.execute`.

One batch:
  1. poll messages, pin them (bodies and receipts) with the batch time in the manifest        status: received
  2. execute (writes, one action each); each write's receipt is recorded as it completes      status: processing
  3. all outputs done                                                                          status: committed
  4. event rows go to the outbox (durable), THEN the messages are acknowledged                 status: acked
  5. the outbox is relayed to the broker (at least once, stable dedupe key)                    status: completed
A crash anywhere is repaired by the next start: unfinished batches are resumed from their manifest (outputs that
already have a receipt are not rerun; the rest rerun idempotently), then the outbox is relayed, then new work starts.
A failed batch stops the job until `state retry` or `state abandon` (so batches are never silently skipped).
Every state write carries the lease; a worker that lost the lease stops at its next write (StaleLease).
"""

import datetime
import time

from streamwright.spark.contracts import BatchContext, Message
from streamwright.spark.state import StaleLease


__all__ = ["Runner", "RunnerError", "redact"]


class RunnerError(Exception):
    pass


def redact(text, secrets):
    for value in secrets:
        if value and len(str(value)) >= 4:
            text = text.replace(str(value), "***")
    return text


def _now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Runner(object):
    def __init__(self, job, state, executor, consumer=None, consume=None, publishers=None, secrets=(), owner="local",
                 lease_seconds=300, log=None, clock=_now_iso, sleep=time.sleep, app_id=None, driver_host=None):
        """
        consume:    {"arguments": {...}} for the job's consume step, or None (no broker); `consumer` is its Broker
        publishers: {output name: (broker, arguments)}
        executor:   .execute(ctx, manifest, done, on_receipt) -> {output: [event rows]}
        """
        self.job, self.state, self.executor, self.consumer = job, state, executor, consumer
        self.consume, self.publishers, self.secrets = consume, publishers or {}, list(secrets)
        self.owner, self.lease_seconds, self.clock, self.sleep = owner, lease_seconds, clock, sleep
        self.log = log or (lambda event, **fields: None)
        self.app_id = app_id
        self.driver_host = driver_host
        if (self.app_id is None or self.driver_host is None) and hasattr(executor, "spark") and executor.spark is not None:
            try:
                from streamwright.spark.engine.session import resolve_spark_metadata
                a_id, d_host = resolve_spark_metadata(executor.spark)
                if self.app_id is None:
                    self.app_id = a_id
                if self.driver_host is None:
                    self.driver_host = d_host
            except Exception:
                pass
        self.lease = None

    # --- public ---------------------------------------------------------------------------------------------

    def run(self, mode="once", interval=10, max_messages=500, run_key=None, max_wait=30):
        """mode: once | drain | follow. max_messages 0 = recover and relay only. Returns an exit code."""
        self.lease = self.state.acquire_lease(self.job, self.owner, self.lease_seconds)
        try:
            self._recover()
            self._relay()
            if max_messages == 0:
                return 0
            while True:
                self.lease = self.state.renew_lease(self.lease, self.lease_seconds)
                processed = self._next_batch(max_messages, run_key, max_wait)
                if mode == "once" or (mode == "drain" and not processed):
                    return 0
                if not processed:
                    self.sleep(interval)
        except StaleLease as exc:
            self.log("lease_lost", error=str(exc))
            return 1
        except RunnerError as exc:
            self.log("stopped", error=str(exc))
            return 1
        except KeyboardInterrupt:
            self.log("interrupted")
            return 130
        finally:
            try:
                if self.lease is not None:
                    self.state.release_lease(self.lease)
            except Exception:
                pass

    # --- recovery -------------------------------------------------------------------------------------------

    def _recover(self):
        for batch in self.state.incomplete(self.job):
            if batch["status"] == "failed":
                raise RunnerError("batch %s failed earlier (%s); run `state retry` or `state abandon`" % (
                    batch["batch_id"], batch.get("error") or "no detail"))
            self.log("resume", batch=batch["batch_id"], status=batch["status"])
            self._process(batch["batch_id"], batch["seq"], batch["manifest"], batch["status"])

    # --- one new batch --------------------------------------------------------------------------------------

    def _next_batch(self, max_messages, run_key, max_wait):
        if self.consume is not None:
            c_args = self.consume["arguments"]
            batch_limit = c_args.get("max.poll.records") or c_args.get("MaxNumberOfMessages") or c_args.get("max_messages") or max_messages
            wait_time = max_wait
            if "fetch.max.wait.ms" in c_args:
                wait_time = int(c_args["fetch.max.wait.ms"]) / 1000.0
            elif "WaitTimeSeconds" in c_args:
                wait_time = int(c_args["WaitTimeSeconds"])
            messages = self.consumer.poll(c_args, int(batch_limit), wait_time)
            if not messages:
                return False
            run_key = None
        else:
            if run_key is not None and self.state.batch_by_run_key(self.job, run_key) is not None:
                self.log("skip", run_key=run_key, reason="already ran")
                return False
            messages = []
        seq = self.state.next_seq(self.job)
        batch_id = "%s-%08d" % (self.job, seq)
        manifest = {"batch_ts": self.clock(), "pinned": {}, "messages": [_message_dict(m) for m in messages]}
        if self.app_id:
            manifest["app_id"] = self.app_id
        if self.driver_host:
            manifest["driver_host"] = self.driver_host
        self.state.record_batch(self.lease, batch_id, "received", manifest, seq=seq, run_key=run_key,
                                app_id=self.app_id, driver_host=self.driver_host)
        self._process(batch_id, seq, manifest, "received")
        return True

    def _process(self, batch_id, seq, manifest, status):
        messages = [Message(**m) for m in manifest["messages"]]
        ctx = BatchContext(batch_id=batch_id, batch_seq=seq, batch_ts=manifest["batch_ts"], job=self.job, spark=None,
                           epoch=self.lease.epoch)
        done = self.state.receipts(self.job, batch_id)
        events = {}
        try:
            if status in ("received", "processing"):
                self.state.record_batch(self.lease, batch_id, "processing", manifest)

            def on_receipt(receipt):
                self.state.record_receipt(self.lease, batch_id, receipt)

            if status in ("received", "processing", "committed"):
                events = self.executor.execute(ctx, manifest, done, on_receipt)
                if status != "committed":
                    self.state.record_batch(self.lease, batch_id, "committed", manifest)
            if status in ("received", "processing", "committed"):
                for output, rows in events.items():
                    topic = self.publishers[output][1].get("topic") or self.publishers[output][1].get("queue_url")
                    self.state.outbox_add(self.lease, batch_id, topic, rows, output=output)
                if messages:
                    try:
                        self.consumer.ack(messages, arguments=self.consume.get("arguments") if self.consume else None)
                    except TypeError:
                        self.consumer.ack(messages)      # only after the outbox is durable
                self.state.record_batch(self.lease, batch_id, "acked", manifest)
            self._relay()
            self.state.record_batch(self.lease, batch_id, "completed", manifest)
        except (StaleLease, KeyboardInterrupt):
            raise
        except Exception as exc:
            text = redact("%s: %s" % (type(exc).__name__, exc), self.secrets)[:2000]
            self.log("batch_failed", batch=batch_id, error=text)
            try:
                self.state.record_batch(self.lease, batch_id, "failed", manifest, error=text)
            except StaleLease:
                raise
            raise RunnerError("batch %s failed: %s" % (batch_id, text))

    # --- outbox ---------------------------------------------------------------------------------------------

    def _relay(self):
        groups = {}
        for row in self.state.outbox_pending(self.job):
            groups.setdefault((row["batch_id"], row["output"]), []).append(row)
        for (batch_id, output), rows in sorted(groups.items()):
            if output not in self.publishers:
                raise RunnerError("outbox holds events for unknown output %r" % output)
            publisher, arguments = self.publishers[output]
            publisher.publish(arguments, [r["payload"] for r in rows],
                              "%s:%s:%s" % (self.job, batch_id, output))
            self.state.outbox_mark_sent(self.lease, batch_id, output, [r["seq"] for r in rows])
            self.log("relayed", batch=batch_id, output=output, events=len(rows))


def _message_dict(message):
    return {"message_id": message.message_id, "key": message.key, "body": message.body,
            "headers": dict(message.headers or {}), "timestamp": message.timestamp, "receipt": message.receipt}
