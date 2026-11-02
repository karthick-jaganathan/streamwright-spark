"""The Spark io helpers of the database and table families: JDBC targets and SQL, Delta writes and registration."""

from types import SimpleNamespace

import pytest

from streamwright.core.jobs import BatchContext
from streamwright.core.sdk.contract import ConnectorDefinition, Field, check_arguments
from streamwright.core.sdk.operations import databases, tables
from streamwright.spark.engine.runtime import StepRuntime
from streamwright.spark.io import delta, jdbc
from streamwright.spark.io.capabilities import CAPABILITIES
from streamwright.spark.io.sql import quote_identifier
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.runtime.definition import DefinedAdapter
from streamwright.spark.sdk import SparkContext

DIALECT = jdbc.JdbcDialect(
    name="database",
    url_template="jdbc:database://{host}:{port}/{database}",
    driver="org.Driver",
    default_port=5432,
    staging_statement=lambda target, staging: "CREATE TABLE %s (LIKE %s)" % (staging, target),
    merge_statement=lambda target, staging, columns, key, update: "MERGE %s %s %s" % (target, staging, update),
    delete_statement=lambda target, staging, key: "DELETE %s %s %s" % (target, staging, key),
)
DEFINITION = ConnectorDefinition(
    "database",
    "Database",
    "databases",
    connection=(Field("host", "string"),),
    operations=(databases.QUERY, databases.WRITE, tables.WRITE),
    adapters={"spark": "x:Y"},
)


def test_jdbc_urls_refuse_driver_properties_in_host_database_or_port():
    values = {"host": "db.example", "database": "orders", "user": "u", "password": "p"}
    for field, value in (
        ("database", "orders?socketFactory=evil"),
        ("database", "orders;ssl=false"),
        ("database", "orders/path"),
        ("host", "host:5432/evil"),
        ("host", "999.999.999.999"),
        ("port", "5432?ssl=false"),
        ("port", 70000),
    ):
        with pytest.raises(ValueError):
            jdbc.jdbc_target(DIALECT, **dict(values, **{field: value}))
    target = jdbc.jdbc_target(DIALECT, **dict(values, host="[::1]"))
    assert target.url == "jdbc:database://[::1]:5432/orders" and target.properties["password"] == "p"
    assert "'p'" not in repr(target)
    assert target.with_options({"fetchsize": 10})["fetchsize"] == "10"


def test_jdbc_driver_options_forward_undeclared_operation_arguments_to_the_properties():
    """A `database.query` vendor argument (fetchsize, sessionInitStatement, ...) reaches the JDBC properties."""
    operation = DEFINITION.operation("database", "query")
    options = jdbc.driver_options(operation, {"query": "SELECT 1", "fetchsize": 500,
                                              "sessionInitStatement": "SET work_mem='64MB'"})
    assert options == {"fetchsize": 500, "sessionInitStatement": "SET work_mem='64MB'"}
    properties = jdbc.JdbcTarget("jdbc:database://h:5432/d", {"user": "u"}).with_options(options)
    assert properties["fetchsize"] == "500" and properties["sessionInitStatement"] == "SET work_mem='64MB'"


def test_delta_vendor_options_forward_reader_options_and_reject_adapter_owned_ones():
    """A `table.read` vendor option (readChangeFeed, ...) reaches `.option()`; owned keys are rejected."""
    operation = tables.READ
    options = delta._vendor_options(operation, {"readChangeFeed": "true", "startingVersion": 3}, delta._READ_OWNED)
    assert options == {"readChangeFeed": "true", "startingVersion": 3}
    with pytest.raises(ValueError, match="versionAsOf set by the adapter"):
        delta._vendor_options(operation, {"versionAsOf": 2}, delta._READ_OWNED)


def test_identifiers_are_validated_not_escaped():
    assert quote_identifier("reporting.ingest_audit") == '"reporting"."ingest_audit"'
    for bad in ("a;b", "a.b.c", "a b", 'a"b', "", "1abc", "a--b"):
        with pytest.raises(ValueError):
            quote_identifier(bad)


def test_query_parameters_are_literals_and_table_reads_use_the_default_schema():
    assert jdbc.bind_params("SELECT * FROM t WHERE a = $a AND s = '$a' -- $b", {"a": "x'y"}) == (
        "SELECT * FROM t WHERE a = 'x''y' AND s = '$a' -- $b"
    )
    for params in ({}, {"a": 1, "unused": 2}):
        with pytest.raises(ValueError):
            jdbc.bind_params("SELECT $a", params)
    assert (
        jdbc.table_sql(
            DIALECT, {"table": "orders", "columns": ["id"], "where": [{"column": "id", "op": "IN", "value": [1, 2]}]}
        )
        == 'SELECT "id" FROM "public"."orders" WHERE "id" IN (1, 2)'
    )


class _Writer:
    def __init__(self, calls):
        self.calls = calls

    def option(self, *args):
        self.calls.append(("option", args))
        return self

    def jdbc(self, url, table, mode, properties):
        self.calls.append(("jdbc", table, mode))


class _Frame:
    columns = ("id", "v")

    def __init__(self, calls):
        self.write = _Writer(calls)

    def observe(self, *args):
        return self


class _Connection:
    def __init__(self, calls, fail=None):
        self.calls, self.fail = calls, fail

    def cursor(self):
        return self

    def execute(self, statement):
        self.calls.append(("sql", statement))
        if self.fail and statement.startswith(self.fail):
            raise RuntimeError("boom")

    def commit(self):
        self.calls.append(("commit",))

    def rollback(self):
        self.calls.append(("rollback",))

    def close(self):
        pass


def _context(store_identity="store-id"):
    runtime = StepRuntime(None, BatchContext("b-1", 7, "2026-10-05T00:00:00Z", "job", 1, store_identity, {}),
                          {}, {}, {}, limits={})
    return SparkContext(runtime, "out", {})


def test_a_staged_merge_loads_a_temporary_table_and_drops_it_even_when_the_merge_fails(monkeypatch):
    calls = []
    monkeypatch.setattr(jdbc, "_receipt", lambda name, table, metrics: (name, table))
    monkeypatch.setattr(jdbc, "_observed", lambda frame, name: (frame, None))
    target = jdbc.jdbc_target(DIALECT, host="db", database="d", user="u", password="p")
    arguments = {"table": "s.t", "mode": "merge", "primary_key": ["id"]}
    jdbc.write_table(
        _context(), _Frame(calls), arguments, dialect=DIALECT, target=target, connect=lambda: _Connection(calls)
    )
    staging = calls[0][1].split()[2]
    assert staging.startswith('"s"."_sw_stg_out_7_') and calls[1] == ("commit",)
    assert calls[2] == ("jdbc", staging.replace('"', ""), "append")
    assert calls[3] == ("sql", 'MERGE "s"."t" %s [\'"v"\']' % staging) and calls[4] == ("sql", "DROP TABLE " + staging)
    calls.clear()
    with pytest.raises(RuntimeError):
        jdbc.write_table(
            _context(),
            _Frame(calls),
            dict(arguments, mode="delete"),
            dialect=DIALECT,
            target=target,
            connect=lambda: _Connection(calls, fail="DELETE"),
        )
    assert ("rollback",) in calls and calls[-2][1].startswith("DROP TABLE IF EXISTS")


def test_a_named_staging_table_is_truncated_and_renders_its_tokens(monkeypatch):
    calls = []
    monkeypatch.setattr(jdbc, "_receipt", lambda name, table, metrics: (name, table))
    monkeypatch.setattr(jdbc, "_observed", lambda frame, name: (frame, None))
    target = jdbc.jdbc_target(DIALECT, host="db", database="d", user="u", password="p")
    jdbc.write_table(
        _context(),
        _Frame(calls),
        {
            "table": "t",
            "mode": "delete",
            "primary_key": ["id"],
            "staging_table": "s.stg_{{ output.name }}_{{ batch.seq }}",
        },
        dialect=DIALECT,
        target=target,
        connect=lambda: _Connection(calls),
    )
    assert calls[0] == ("option", ("truncate", "true")) and calls[1] == ("jdbc", "s.stg_out_7", "overwrite")
    assert calls[-2] == ("sql", 'TRUNCATE TABLE "s"."stg_out_7"')


def test_a_replayed_delta_append_returns_its_own_commit_without_writing(monkeypatch):
    monkeypatch.setattr(delta, "_find_commit", lambda *args: (17, {"numOutputRows": "9"}))
    monkeypatch.setattr(delta, "_writer", lambda *args: pytest.fail("a replay must not write"))
    monkeypatch.setattr(delta, "_register", lambda *args: None)
    receipt = delta.write_table(_context(), None, {}, path="file:///table", defaults={})
    assert (receipt.output_name, receipt.destination, receipt.version, receipt.rows) == ("out", "file:///table", 17, 9)


def test_a_delta_append_transaction_key_includes_the_store_identity(monkeypatch):
    captured = {}

    def find_commit(context, path, app_id, transaction):
        captured["app_id"], captured["transaction"] = app_id, transaction
        return (3, {"numOutputRows": "5"})           # pretend already committed so no real write happens

    monkeypatch.setattr(delta, "_find_commit", find_commit)
    monkeypatch.setattr(delta, "_register", lambda *args: None)
    delta.write_table(_context("store-xyz"), None, {}, path="file:///table", defaults={})
    assert captured == {"app_id": "job:out:store-xyz", "transaction": 7}


def test_registration_creates_what_the_catalog_lacks_with_validated_names():
    executed = []
    spark = SimpleNamespace(sql=executed.append, catalog=SimpleNamespace(tableExists=lambda name: False))
    context = SimpleNamespace(spark=spark)
    delta._register(context, {"table_properties": {"k": "it's"}}, {"schema": "analytics"}, "s3a://lake/orders", None)
    assert executed == [
        "CREATE SCHEMA IF NOT EXISTS `analytics`",
        "CREATE TABLE IF NOT EXISTS `analytics`.`orders` USING DELTA LOCATION 's3a://lake/orders'",
        "ALTER TABLE `analytics`.`orders` SET TBLPROPERTIES ('k' = 'it''s')",
    ]
    for bad in ("bad;schema", "bad schema", "bad'schema"):
        with pytest.raises(ValueError):
            delta._register(context, {"schema": bad, "table_name": "orders"}, {}, "s3a://lake/orders", None)


def test_merge_clauses_use_the_shared_predicate_key():
    calls = []
    builder = SimpleNamespace(
        whenMatchedUpdate=lambda condition, set: calls.append(("update", condition, set)) or builder,
        whenMatchedDelete=lambda condition: calls.append(("delete", condition)) or builder,
        whenNotMatchedInsertAll=lambda condition: calls.append(("insert", condition)) or builder,
    )
    delta._with_clauses(
        builder,
        {
            "when_matched": [
                {"action": "update", "predicate": "source.n > 0", "set": {"n": "source.n"}},
                {"action": "delete"},
            ],
            "when_not_matched": [{"action": "insert"}],
        },
    )
    assert calls == [("update", "source.n > 0", {"n": "source.n"}), ("delete", None), ("insert", None)]


def test_spark_rules_judge_replay_safety_and_merge_scope_of_defined_writes():
    rules = SparkRules({"database": DefinedAdapter(DEFINITION, SimpleNamespace(state_connection=print))})
    connection = {"provider": "database"}

    def check(service, method, arguments):
        return [
            (code, warning)
            for code, _, warning in rules._check_call(
                {"name": "o", "service": service, "method": method, "arguments": arguments}, connection, False
            )
        ]

    assert check("database", "write", {"table": "t"}) == [("not-idempotent", True)]
    assert check("database", "write", {"table": "t", "mode": "merge", "primary_key": ["id"]}) == []
    assert check(
        "table", "write", {"path": "t", "mode": "merge", "condition": "c", "when_not_matched": [{"action": "insert"}]}
    ) == [("merge-scope", True)]
    assert rules.adapters["database"].holds_state
    assert check_arguments(DEFINITION, databases.WRITE, {"table": "t", "mode": "overwrite"}, CAPABILITIES)
