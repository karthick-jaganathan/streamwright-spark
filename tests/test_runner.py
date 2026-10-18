import os
import tempfile
import unittest

from streamwright.spark.contracts import Message, WriteReceipt
from streamwright.spark.runtime.runner import Runner, redact
from streamwright.spark.state import open_sqlite


class Crash(BaseException):
    """Simulates a process that dies (not an ordinary error, so the runner cannot record 'failed')."""


class FakeBroker(object):
    def __init__(self, bodies):
        self.queue = [Message("m%d" % i, None, b, {}, None, receipt={"n": i}) for i, b in enumerate(bodies)]
        self.acked, self.published, self.crash_on_ack, self.crash_on_publish = [], [], False, False

    def poll(self, arguments, limit, wait):
        return [m for m in self.queue if m.message_id not in self.acked][:limit]

    def ack(self, messages):
        if self.crash_on_ack:
            self.crash_on_ack = False
            raise Crash()
        self.acked += [m.message_id for m in messages]

    def publish(self, arguments, events, dedupe_key):
        if self.crash_on_publish:
            self.crash_on_publish = False
            raise Crash()
        self.published.append((dedupe_key, list(events)))


class FakeExecutor(object):
    def __init__(self):
        self.calls, self.crash_after_first_output, self.fail = [], False, False

    def execute(self, ctx, manifest, done, on_receipt):
        self.calls.append((ctx.batch_id, sorted(done)))
        if self.fail:
            raise ValueError("boom hunter2")
        for name in ("bronze", "audit"):
            if name not in done:
                on_receipt(WriteReceipt(name, "t/" + name, len(manifest["messages"]), version=1))
                if self.crash_after_first_output and name == "bronze":
                    self.crash_after_first_output = False
                    raise Crash()
        return {"announce": [{"batch": ctx.batch_id, "n": len(manifest["messages"])}]}


class Loop(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.state = open_sqlite(os.path.join(self.dir.name, "s.db"))
        self.state.init()
        self.broker = FakeBroker(["a", "b", "c"])
        self.executor = FakeExecutor()

    def tearDown(self):
        self.dir.cleanup()

    def runner(self, consume=True):
        return Runner("job", self.state, self.executor, self.broker if consume else None,
                      {"arguments": {"topic": "t"}} if consume else None,
                      {"announce": (self.broker, {"topic": "out"})}, secrets=["hunter2"],
                      clock=lambda: "2026-10-17T00:00:00Z")

    def test_happy_path(self):
        self.assertEqual(self.runner().run("drain"), 0)
        self.assertEqual(self.broker.acked, ["m0", "m1", "m2"])
        self.assertEqual(len(self.broker.published), 1)
        self.assertEqual(self.state.batch("job", "job-00000001")["status"], "completed")
        self.assertEqual(self.state.outbox_pending("job"), [])

    def test_crash_before_commit_reruns_only_missing_outputs(self):
        self.executor.crash_after_first_output = True
        with self.assertRaises(Crash):
            self.runner().run("once")
        self.assertEqual(self.broker.acked, [])                       # never acked before the outputs were done
        self.runner().run("once")
        self.assertEqual(self.executor.calls[1], ("job-00000001", ["bronze"]))   # bronze had a receipt; not rerun
        self.assertEqual(self.broker.acked, ["m0", "m1", "m2"])
        self.assertEqual(len(self.broker.published), 1)

    def test_crash_after_outbox_before_ack_does_not_lose_or_duplicate(self):
        self.broker.crash_on_ack = True
        with self.assertRaises(Crash):
            self.runner().run("once")
        self.assertEqual(len(self.state.outbox_pending("job")), 1)    # the event is durable
        self.assertEqual(self.broker.published, [])
        self.runner().run("once", max_messages=0)                     # recover and relay only
        self.assertEqual(self.broker.acked, ["m0", "m1", "m2"])
        self.assertEqual(len(self.broker.published), 1)
        self.assertEqual(self.broker.published[0][0], "job:job-00000001:announce")

    def test_crash_during_publish_republishes_with_same_dedupe_key(self):
        self.broker.crash_on_publish = True
        with self.assertRaises(Crash):
            self.runner().run("once")
        self.runner().run("once", max_messages=0)
        self.assertEqual([k for k, _ in self.broker.published], ["job:job-00000001:announce"])

    def test_failed_batch_stops_the_job_and_redacts(self):
        self.executor.fail = True
        self.assertEqual(self.runner().run("once"), 1)
        batch = self.state.batch("job", "job-00000001")
        self.assertEqual(batch["status"], "failed")
        self.assertNotIn("hunter2", batch["error"])
        self.executor.fail = False
        self.assertEqual(self.runner().run("once"), 1)                # still stopped
        self.assertEqual(self.broker.acked, [])

    def test_no_broker_run_key_is_idempotent(self):
        runner = self.runner(consume=False)
        runner.consume = None
        self.assertEqual(runner.run("once", run_key="2026-10-17"), 0)
        self.assertEqual(self.runner(consume=False).run("once", run_key="2026-10-17"), 0)
        self.assertEqual(len(self.executor.calls), 1)

    def test_once_with_nothing_to_do(self):
        self.broker.queue = []
        self.assertEqual(self.runner().run("once"), 0)
        self.assertEqual(self.executor.calls, [])

    def test_redact(self):
        self.assertEqual(redact("pw=hunter2!", ["hunter2"]), "pw=***!")
        self.assertEqual(redact("abc", ["a"]), "abc")                 # too short to be a secret worth masking


if __name__ == "__main__":
    unittest.main()
