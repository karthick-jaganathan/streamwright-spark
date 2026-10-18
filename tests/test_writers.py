import unittest

from streamwright.spark.kinds import merge_condition
from streamwright.spark.kinds import identifier
from streamwright.connectors.postgres._spark import upsert_sql


class Pure(unittest.TestCase):
    def test_identifier(self):
        self.assertEqual(identifier("reporting.ingest_audit"), '"reporting"."ingest_audit"')
        for bad in ("a;b", "a.b.c", "a b", 'a"b', "", "1abc", "a--b"):
            with self.assertRaises(ValueError, msg=bad):
                identifier(bad)

    def test_merge_condition(self):
        self.assertEqual(merge_condition("target.id = source.id", None), "target.id = source.id")
        self.assertEqual(merge_condition("target.id = source.id", "target.d IN (DATE '2026-10-17')"),
                         "(target.d IN (DATE '2026-10-17')) AND (target.id = source.id)")

    def test_upsert_sql(self):
        sql = upsert_sql("reporting.ingest_audit", "reporting.ingest_audit_stg", ["batch_id", "client_id"],
                         ["row_count", "revenue"])
        self.assertEqual(sql, 'INSERT INTO "reporting"."ingest_audit" ("batch_id", "client_id", "row_count", "revenue") '
                              'SELECT "batch_id", "client_id", "row_count", "revenue" FROM "reporting"."ingest_audit_stg" '
                              'ON CONFLICT ("batch_id", "client_id") DO UPDATE SET "row_count" = EXCLUDED."row_count", '
                              '"revenue" = EXCLUDED."revenue"')
        self.assertTrue(upsert_sql("t", "s", ["k"], None).endswith("DO NOTHING"))

    def test_upsert_rejects_injection(self):
        with self.assertRaises(ValueError):
            upsert_sql("t", "s", ["k; DROP TABLE x"], ["v"])


class DeltaRegistration(unittest.TestCase):
    def setUp(self):
        from streamwright.spark.kinds.delta import Delta
        self.delta = Delta()

    def test_ensure_registered_ddl_and_caching(self):
        class MockSpark:
            def __init__(self):
                self.executed = []

            def sql(self, statement):
                self.executed.append(statement)

        class MockAdapter:
            def roots(self, auth):
                return ["s3://lake/"]

            def to_hadoop(self, path, auth):
                return path.replace("s3://", "s3a://")

        class MockStorage:
            auth = {}

        class MockConnection:
            auth = {
                "storage": "lake",
                "schema": "analytics",
                "schema_location": "s3://lake/analytics/",
                "schema_comment": "Enterprise analytics schema",
            }

        class MockRuntime:
            def __init__(self):
                self.spark = MockSpark()
                self._registered_tables = set()

            def text(self, val):
                return val

            def storage_of(self, conn):
                return MockAdapter(), MockStorage()

        rt = MockRuntime()
        conn = MockConnection()
        args = {
            "table_path": "s3://lake/delta/orders",
            "table_name": "orders",
            "table_properties": {"delta.autoOptimize.optimizeWrite": True},
            "comment": "Orders table",
        }

        # First run: should execute CREATE SCHEMA, CREATE TABLE, and ALTER TABLE SET TBLPROPERTIES
        self.delta._ensure_registered(rt, conn, args, None, "s3a://lake/delta/orders")
        self.assertEqual(len(rt.spark.executed), 3)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS `analytics` COMMENT 'Enterprise analytics schema' LOCATION 's3a://lake/analytics/'", rt.spark.executed[0])
        self.assertIn("CREATE TABLE IF NOT EXISTS `analytics`.`orders` USING DELTA LOCATION 's3a://lake/delta/orders'", rt.spark.executed[1])
        self.assertIn("ALTER TABLE `analytics`.`orders` SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'comment' = 'Orders table')", rt.spark.executed[2])
        self.assertIn(("analytics", "orders"), rt._registered_tables)

        # Second run: should be cached in memory (0 SQL statements executed, 0ms overhead)
        self.delta._ensure_registered(rt, conn, args, None, "s3a://lake/delta/orders")
        self.assertEqual(len(rt.spark.executed), 3)

    def test_ensure_registered_derives_table_name_from_path(self):
        class MockSpark:
            def __init__(self):
                self.executed = []

            def sql(self, statement):
                self.executed.append(statement)

        class MockRuntime:
            def __init__(self):
                self.spark = MockSpark()
                self._registered_tables = set()

            def text(self, val):
                return val

            def storage_of(self, conn):
                return None, None

        rt = MockRuntime()
        conn = type("C", (), {"auth": {"schema": "analytics"}})()
        args = {"table_path": "s3://lake/delta/orders"}

        self.delta._ensure_registered(rt, conn, args, None, "s3a://lake/delta/orders")
        self.assertIn("CREATE TABLE IF NOT EXISTS `analytics`.`orders` USING DELTA LOCATION 's3a://lake/delta/orders'", rt.spark.executed[1])
        self.assertIn(("analytics", "orders"), rt._registered_tables)

    def test_ensure_registered_rejects_sql_injection_in_identifiers(self):
        class MockRuntime:
            def __init__(self):
                self.spark = None

        rt = MockRuntime()
        conn = type("C", (), {"auth": {}})()

        for bad in ("bad;schema", "bad schema", "bad-schema", "bad'schema"):
            with self.assertRaises(ValueError):
                self.delta._ensure_registered(rt, conn, {"schema": bad, "table_name": "orders"}, None, "s3a://lake/orders")

    def test_matched_clauses(self):
        class MockBuilder:
            def __init__(self):
                self.calls = []

            def whenMatchedUpdate(self, condition=None, set=None):
                self.calls.append(("update", condition, set))
                return self

            def whenMatchedUpdateAll(self, condition=None):
                self.calls.append(("update_all", condition))
                return self

            def whenMatchedDelete(self, condition=None):
                self.calls.append(("delete", condition))
                return self

        # 1. whenMatchedUpdate with explicit set mapping
        b = MockBuilder()
        self.delta._matched(b, {"action": "update", "set": {"amount": "source.amount", "tax": "source.tax"}})
        self.assertEqual(b.calls, [("update", None, {"amount": "source.amount", "tax": "source.tax"})])

        # 2. whenMatchedUpdate with condition and set
        b = MockBuilder()
        self.delta._matched(b, {"action": "update", "condition": "source.amount > 0", "set": {"amount": "source.amount"}})
        self.assertEqual(b.calls, [("update", "source.amount > 0", {"amount": "source.amount"})])

        # 3. whenMatchedUpdateAll (no set)
        b = MockBuilder()
        self.delta._matched(b, {"action": "update"})
        self.assertEqual(b.calls, [("update_all", None)])

        # 4. whenMatchedDelete
        b = MockBuilder()
        self.delta._matched(b, {"action": "delete", "condition": "source.amount <= 0"})
        self.assertEqual(b.calls, [("delete", "source.amount <= 0")])


if __name__ == "__main__":
    unittest.main()
