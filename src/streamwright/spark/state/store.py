"""
Bookkeeping store (runtime.state): leases with epoch fencing, batches with a status machine, manifests, write
receipts and the transactional outbox. PROPOSED.

One implementation over a DB-API connection and a Dialect (state.dialects): postgres is the real target, sqlite is used
for tests and `platform: local` without Docker. Nothing here is ever a business write target.

Status machine (a batch's row):  received -> processing -> committed -> acked -> completed
                                 any (not completed) -> failed | abandoned;  failed -> processing (retry)
Every write by a worker carries its Lease; a write whose epoch is no longer current raises StaleLease (fencing), so a
slow or partitioned worker cannot overwrite the state of the worker that took over.
"""

import datetime
import json
import re


__all__ = ["StateError", "StaleLease", "LeaseHeld", "SqlStateStore", "STATUSES"]

STATUSES = ("received", "processing", "committed", "acked", "completed", "failed", "abandoned")
_NEXT = {"received": ("processing", "failed", "abandoned"),
         "processing": ("processing", "committed", "failed", "abandoned"),
         "committed": ("acked", "failed", "abandoned"),
         "acked": ("completed", "failed", "abandoned"),
         "failed": ("processing", "abandoned"),
         "completed": (), "abandoned": ()}
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class StateError(Exception):
    pass


class StaleLease(StateError):
    pass


class LeaseHeld(StateError):
    pass


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _iso(when):
    return when.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class SqlStateStore(object):
    def __init__(self, connect, dialect, schema=None):
        if schema is not None and not _IDENT.match(schema):
            raise StateError("invalid schema name")
        self._connect, self._dialect = connect, dialect
        self._p = (schema + ".") if schema else ""
        self._schema = schema

    # --- plumbing -------------------------------------------------------------------------------------------

    def _sql(self, text):
        text = text.replace("{p}", self._p)
        return self._dialect.placeholders(text)

    def _run(self, work):
        conn = self._connect()
        try:
            cur = conn.cursor()
            result = work(cur)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _x(self, cur, text, args=()):
        cur.execute(self._sql(text), tuple(args))
        return cur

    def _check(self, cur, lease):
        suffix = self._dialect.share_lock                                  # blocks a takeover until we commit
        row = self._x(cur, "SELECT epoch FROM {p}sw_leases WHERE job = ?" + suffix, (lease.job,)).fetchone()
        if row is None or int(row[0]) != lease.epoch:
            raise StaleLease("lease for %s is no longer held by %s (epoch %s)" % (lease.job, lease.owner, lease.epoch))

    # --- contract -------------------------------------------------------------------------------------------

    def init(self):
        def work(cur):
            if self._schema and self._dialect.has_schemas:
                self._x(cur, "CREATE SCHEMA IF NOT EXISTS %s" % self._schema)
            for ddl in (
                "CREATE TABLE IF NOT EXISTS {p}sw_leases (job TEXT PRIMARY KEY, owner TEXT NOT NULL, "
                "epoch BIGINT NOT NULL, expires_at TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS {p}sw_batches (job TEXT NOT NULL, batch_id TEXT NOT NULL, "
                "seq BIGINT NOT NULL, run_key TEXT, status TEXT NOT NULL, manifest TEXT NOT NULL, error TEXT, "
                "app_id TEXT, driver_host TEXT, "
                "created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (job, batch_id), "
                "UNIQUE (job, seq), UNIQUE (job, run_key))",
                "CREATE TABLE IF NOT EXISTS {p}sw_receipts (job TEXT NOT NULL, batch_id TEXT NOT NULL, "
                "output TEXT NOT NULL, receipt TEXT NOT NULL, PRIMARY KEY (job, batch_id, output))",
                "CREATE TABLE IF NOT EXISTS {p}sw_outbox (job TEXT NOT NULL, batch_id TEXT NOT NULL, "
                "output TEXT NOT NULL, seq BIGINT NOT NULL, topic TEXT NOT NULL, payload TEXT NOT NULL, "
                "status TEXT NOT NULL, PRIMARY KEY (job, batch_id, output, seq))"):
                self._x(cur, ddl)
            if self._dialect.name == "postgres":
                self._x(cur, "ALTER TABLE {p}sw_batches ADD COLUMN IF NOT EXISTS app_id TEXT")
                self._x(cur, "ALTER TABLE {p}sw_batches ADD COLUMN IF NOT EXISTS driver_host TEXT")
            elif self._dialect.name == "sqlite":
                columns = [row[1] for row in cur.execute("PRAGMA table_info(sw_batches)").fetchall()]
                if "app_id" not in columns:
                    cur.execute("ALTER TABLE sw_batches ADD COLUMN app_id TEXT")
                if "driver_host" not in columns:
                    cur.execute("ALTER TABLE sw_batches ADD COLUMN driver_host TEXT")
        self._run(work)

    def acquire_lease(self, job, owner, ttl_seconds):
        from streamwright.spark.contracts import Lease

        def work(cur):
            self._x(cur, "INSERT INTO {p}sw_leases (job, owner, epoch, expires_at) VALUES (?, ?, 0, '') "
                         "ON CONFLICT (job) DO NOTHING", (job, owner))
            suffix = self._dialect.update_lock
            row = self._x(cur, "SELECT owner, epoch, expires_at FROM {p}sw_leases WHERE job = ?" + suffix, (job,)).fetchone()
            current, epoch, expires = row[0], int(row[1]), row[2]
            now = _now()
            if expires and expires > _iso(now) and current != owner:
                raise LeaseHeld("job %s is held by %s until %s" % (job, current, expires))
            epoch += 1
            until = _iso(now + datetime.timedelta(seconds=ttl_seconds))
            self._x(cur, "UPDATE {p}sw_leases SET owner = ?, epoch = ?, expires_at = ? WHERE job = ?",
                    (owner, epoch, until, job))
            return Lease(job, owner, epoch, until)
        return self._run(work)

    def renew_lease(self, lease, ttl_seconds):
        from streamwright.spark.contracts import Lease

        def work(cur):
            self._check(cur, lease)
            until = _iso(_now() + datetime.timedelta(seconds=ttl_seconds))
            self._x(cur, "UPDATE {p}sw_leases SET expires_at = ? WHERE job = ?", (until, lease.job))
            return Lease(lease.job, lease.owner, lease.epoch, until)
        return self._run(work)

    def release_lease(self, lease):
        def work(cur):
            self._x(cur, "UPDATE {p}sw_leases SET expires_at = '' WHERE job = ? AND epoch = ?", (lease.job, lease.epoch))
        self._run(work)

    def lease(self, job):
        def work(cur):
            row = self._x(cur, "SELECT job, owner, epoch, expires_at FROM {p}sw_leases WHERE job = ?", (job,)).fetchone()
            return None if row is None else {"job": row[0], "owner": row[1], "epoch": int(row[2]), "expires_at": row[3]}
        return self._run(work)

    def next_seq(self, job):
        def work(cur):
            row = self._x(cur, "SELECT COALESCE(MAX(seq), 0) FROM {p}sw_batches WHERE job = ?", (job,)).fetchone()
            return int(row[0]) + 1
        return self._run(work)

    def batch_by_run_key(self, job, run_key):
        def work(cur):
            row = self._x(cur, "SELECT batch_id FROM {p}sw_batches WHERE job = ? AND run_key = ?", (job, run_key)).fetchone()
            return self._batch(cur, job, row[0]) if row else None
        return self._run(work)

    def record_batch(self, lease, batch_id, status, manifest, seq=None, run_key=None, error=None, app_id=None, driver_host=None):
        if status not in STATUSES:
            raise StateError("unknown status %r" % status)
        if app_id is None and isinstance(manifest, dict):
            app_id = manifest.get("app_id")
        if driver_host is None and isinstance(manifest, dict):
            driver_host = manifest.get("driver_host")
        text = json.dumps(manifest, sort_keys=True, default=str)

        def work(cur):
            self._check(cur, lease)
            row = self._x(cur, "SELECT status FROM {p}sw_batches WHERE job = ? AND batch_id = ?",
                          (lease.job, batch_id)).fetchone()
            now = _iso(_now())
            if row is None:
                if seq is None:
                    raise StateError("a new batch needs a seq")
                self._x(cur, "INSERT INTO {p}sw_batches (job, batch_id, seq, run_key, status, manifest, error, "
                             "app_id, driver_host, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (lease.job, batch_id, seq, run_key, status, text, error, app_id, driver_host, now, now))
                return
            if row[0] != status and status not in _NEXT[row[0]]:
                raise StateError("batch %s cannot go from %s to %s" % (batch_id, row[0], status))
            if app_id is not None or driver_host is not None:
                self._x(cur, "UPDATE {p}sw_batches SET status = ?, manifest = ?, error = ?, "
                             "app_id = COALESCE(?, app_id), driver_host = COALESCE(?, driver_host), updated_at = ? "
                             "WHERE job = ? AND batch_id = ?",
                        (status, text, error, app_id, driver_host, now, lease.job, batch_id))
            else:
                self._x(cur, "UPDATE {p}sw_batches SET status = ?, manifest = ?, error = ?, updated_at = ? "
                             "WHERE job = ? AND batch_id = ?", (status, text, error, now, lease.job, batch_id))
        self._run(work)

    def record_receipt(self, lease, batch_id, receipt):
        data = json.dumps(_receipt_dict(receipt), sort_keys=True, default=str)

        def work(cur):
            self._check(cur, lease)
            self._x(cur, "INSERT INTO {p}sw_receipts (job, batch_id, output, receipt) VALUES (?, ?, ?, ?) "
                         "ON CONFLICT (job, batch_id, output) DO UPDATE SET receipt = excluded.receipt",
                    (lease.job, batch_id, receipt.output, data))
        self._run(work)

    def receipts(self, job, batch_id):
        def work(cur):
            rows = self._x(cur, "SELECT output, receipt FROM {p}sw_receipts WHERE job = ? AND batch_id = ? "
                                "ORDER BY output", (job, batch_id)).fetchall()
            return dict((r[0], json.loads(r[1])) for r in rows)
        return self._run(work)

    def outbox_add(self, lease, batch_id, topic, rows, output="", provider=None):
        def work(cur):
            self._check(cur, lease)
            for number, row in enumerate(rows):
                self._x(cur, "INSERT INTO {p}sw_outbox (job, batch_id, output, seq, topic, payload, status) "
                             "VALUES (?, ?, ?, ?, ?, ?, 'pending') ON CONFLICT (job, batch_id, output, seq) DO NOTHING",
                        (lease.job, batch_id, output, number, topic, json.dumps(row, sort_keys=True, default=str)))
        self._run(work)

    def outbox_pending(self, job):
        def work(cur):
            rows = self._x(cur, "SELECT batch_id, output, seq, topic, payload FROM {p}sw_outbox WHERE job = ? "
                                "AND status = 'pending' ORDER BY batch_id, output, seq", (job,)).fetchall()
            return [{"batch_id": r[0], "output": r[1], "seq": int(r[2]), "topic": r[3], "payload": json.loads(r[4])}
                    for r in rows]
        return self._run(work)

    def outbox_mark_sent(self, lease, batch_id, output, seqs):
        def work(cur):
            self._check(cur, lease)
            for seq in seqs:
                self._x(cur, "UPDATE {p}sw_outbox SET status = 'sent' WHERE job = ? AND batch_id = ? AND output = ? "
                             "AND seq = ?", (lease.job, batch_id, output, seq))
        self._run(work)

    def batch(self, job, batch_id):
        return self._run(lambda cur: self._batch(cur, job, batch_id))

    def _batch(self, cur, job, batch_id):
        row = self._x(cur, "SELECT batch_id, seq, run_key, status, manifest, error, created_at, updated_at, "
                           "app_id, driver_host FROM {p}sw_batches WHERE job = ? AND batch_id = ?", (job, batch_id)).fetchone()
        return None if row is None else {"batch_id": row[0], "seq": int(row[1]), "run_key": row[2], "status": row[3],
                                         "manifest": json.loads(row[4]), "error": row[5], "created_at": row[6],
                                         "updated_at": row[7], "app_id": row[8], "driver_host": row[9]}

    def incomplete(self, job):
        def work(cur):
            ids = self._x(cur, "SELECT batch_id FROM {p}sw_batches WHERE job = ? AND status NOT IN "
                               "('completed', 'abandoned') ORDER BY seq", (job,)).fetchall()
            return [self._batch(cur, job, r[0]) for r in ids]
        return self._run(work)

    def list_batches(self, job, limit=20):
        def work(cur):
            ids = self._x(cur, "SELECT batch_id FROM {p}sw_batches WHERE job = ? ORDER BY seq DESC LIMIT ?",
                          (job, int(limit))).fetchall()
            return [self._batch(cur, job, r[0]) for r in ids]
        return self._run(work)

    def purge(self, job, older_than_iso):
        """Deletes completed/abandoned batches older than the cutoff (and their receipts and sent outbox rows)."""
        def work(cur):
            ids = [r[0] for r in self._x(cur, "SELECT batch_id FROM {p}sw_batches WHERE job = ? AND status IN "
                                              "('completed', 'abandoned') AND updated_at < ?",
                                         (job, older_than_iso)).fetchall()]
            for batch_id in ids:
                for table in ("sw_receipts", "sw_batches"):
                    self._x(cur, "DELETE FROM {p}%s WHERE job = ? AND batch_id = ?" % table, (job, batch_id))
                self._x(cur, "DELETE FROM {p}sw_outbox WHERE job = ? AND batch_id = ? AND status = 'sent'", (job, batch_id))
            return len(ids)
        return self._run(work)


def _receipt_dict(receipt):
    return {"output": receipt.output, "target": receipt.target, "rows": receipt.rows, "version": receipt.version,
            "files": list(receipt.files), "predicate": receipt.predicate}
