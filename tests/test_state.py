import os
import tempfile
import unittest

from streamwright.spark.contracts import WriteReceipt
from streamwright.spark.state import LeaseHeld, StaleLease, StateError, open_sqlite


class Store(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.store = open_sqlite(os.path.join(self.dir.name, "state.db"))
        self.store.init()
        self.store.init()                                   # idempotent

    def tearDown(self):
        self.dir.cleanup()

    def test_lease_exclusion_and_takeover(self):
        first = self.store.acquire_lease("j", "a", 60)
        with self.assertRaises(LeaseHeld):
            self.store.acquire_lease("j", "b", 60)
        self.store.release_lease(first)
        second = self.store.acquire_lease("j", "b", 60)
        self.assertEqual(second.epoch, first.epoch + 1)

    def test_fencing_rejects_the_old_owner(self):
        old = self.store.acquire_lease("j", "a", 0)         # expires at once
        new = self.store.acquire_lease("j", "b", 60)
        with self.assertRaises(StaleLease):
            self.store.record_batch(old, "b1", "received", {}, seq=1)
        self.store.record_batch(new, "b1", "received", {}, seq=1)

    def test_status_machine(self):
        lease = self.store.acquire_lease("j", "a", 60)
        self.store.record_batch(lease, "b1", "received", {"messages": [1]}, seq=1, run_key="r1")
        for status in ("processing", "committed", "acked", "completed"):
            self.store.record_batch(lease, "b1", status, {"messages": [1]})
        with self.assertRaises(StateError):
            self.store.record_batch(lease, "b1", "processing", {})
        self.assertEqual(self.store.batch("j", "b1")["status"], "completed")
        self.assertEqual(self.store.incomplete("j"), [])
        self.assertEqual(self.store.batch_by_run_key("j", "r1")["batch_id"], "b1")
        self.assertEqual(self.store.next_seq("j"), 2)

    def test_incomplete_and_failed_retry(self):
        lease = self.store.acquire_lease("j", "a", 60)
        self.store.record_batch(lease, "b1", "received", {}, seq=1)
        self.store.record_batch(lease, "b1", "failed", {}, error="boom")
        self.store.record_batch(lease, "b1", "processing", {})
        self.assertEqual([b["batch_id"] for b in self.store.incomplete("j")], ["b1"])

    def test_receipts_are_upserts(self):
        lease = self.store.acquire_lease("j", "a", 60)
        self.store.record_batch(lease, "b1", "received", {}, seq=1)
        self.store.record_receipt(lease, "b1", WriteReceipt("o", "s3a://x", 10, version=3))
        self.store.record_receipt(lease, "b1", WriteReceipt("o", "s3a://x", 10, version=4))
        self.assertEqual(self.store.receipts("j", "b1")["o"]["version"], 4)

    def test_outbox_is_idempotent_and_marks_sent(self):
        lease = self.store.acquire_lease("j", "a", 60)
        rows = [{"v": 1}, {"v": 2}]
        self.store.outbox_add(lease, "b1", "t", rows, output="announce")
        self.store.outbox_add(lease, "b1", "t", rows, output="announce")      # a replay adds nothing
        self.assertEqual(len(self.store.outbox_pending("j")), 2)
        self.store.outbox_mark_sent(lease, "b1", "announce", [0])
        self.assertEqual([r["seq"] for r in self.store.outbox_pending("j")], [1])

    def test_purge_only_finished(self):
        lease = self.store.acquire_lease("j", "a", 60)
        self.store.record_batch(lease, "b1", "received", {}, seq=1)
        self.store.record_batch(lease, "b2", "received", {}, seq=2)
        for status in ("processing", "committed", "acked", "completed"):
            self.store.record_batch(lease, "b2", status, {})
        self.assertEqual(self.store.purge("j", "9999"), 1)
        self.assertIsNotNone(self.store.batch("j", "b1"))
        self.assertIsNone(self.store.batch("j", "b2"))

    def test_schema_name_is_validated(self):
        from streamwright.spark.state import SqlStateStore
        with self.assertRaises(StateError):
            SqlStateStore(lambda: None, schema="x; DROP TABLE y", dialect="postgres")


if __name__ == "__main__":
    unittest.main()


class InitIsIdempotent(unittest.TestCase):
    """`sw spark run` calls store.init() on every start, so it must be safe to repeat and must not touch data."""

    def test_init_twice_keeps_leases_and_batches(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = open_sqlite(os.path.join(tmp, "s.db"))
            store.init()
            lease = store.acquire_lease("job", "me", 60)
            store.record_batch(lease, "job-1", "processing", {"messages": []}, seq=1)
            store.init()
            self.assertEqual(store.batch("job", "job-1")["status"], "processing")

    def test_record_batch_records_app_id_and_driver_host(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = open_sqlite(os.path.join(tmp, "s.db"))
            store.init()
            lease = store.acquire_lease("job", "worker-1", 60)
            # 1. passed explicitly
            store.record_batch(lease, "b1", "received", {}, seq=1, app_id="app-123", driver_host="10.0.0.1")
            batch1 = store.batch("job", "b1")
            self.assertEqual(batch1["app_id"], "app-123")
            self.assertEqual(batch1["driver_host"], "10.0.0.1")

            # 2. extracted from manifest
            manifest = {"app_id": "app-456", "driver_host": "ip-10-0-0-2.internal", "messages": []}
            store.record_batch(lease, "b2", "received", manifest, seq=2)
            batch2 = store.batch("job", "b2")
            self.assertEqual(batch2["app_id"], "app-456")
            self.assertEqual(batch2["driver_host"], "ip-10-0-0-2.internal")

            # 3. lease lookup
            l = store.lease("job")
            self.assertEqual(l["owner"], "worker-1")
            self.assertEqual(l["epoch"], 1)
