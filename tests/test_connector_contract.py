"""Declarative Spark connector discovery, checking and file safety."""

from types import SimpleNamespace

import pytest

from streamwright.connectors.s3 import CONNECTOR
from streamwright.connectors.s3.engines.spark import S3SparkAdapter
from streamwright.spark import schema as spark_schema
from streamwright.spark.contracts import Connection
from streamwright.spark.engine.executor import SparkExecutor
from streamwright.spark.io import files
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.runtime import registry
from streamwright.spark.runtime.definition import DefinedAdapter
from streamwright.spark.runtime.registry import Registry, RegistryError
from streamwright.spark.sdk import SparkContext
from streamwright.spark.storage import ingest
from streamwright.spark.storage.paths import StorageError


def test_definition_is_loaded_from_the_only_connector_group(monkeypatch):
    calls = []

    def discover(group, *, entry_points=None):
        if group == "streamwright.connectors":
            return [{"name": "s3"}]
        return []

    def load_plugin(group, name, **kwargs):
        calls.append(group)
        return CONNECTOR

    monkeypatch.setattr(registry.plugins, "discover", discover)
    monkeypatch.setattr(registry.plugins, "load_plugin", load_plugin)
    adapter = Registry().load("s3")
    assert isinstance(adapter, DefinedAdapter)
    assert isinstance(adapter.adapter, S3SparkAdapter)
    assert calls == ["streamwright.connectors"]
    with pytest.raises(RegistryError, match="allowed"):
        Registry().load("s3", allow={"other"})
    assert calls == ["streamwright.connectors"]


def test_definition_checks_connection_and_operation_without_legacy_tables():
    rules = SparkRules({"s3": DefinedAdapter(CONNECTOR, S3SparkAdapter())})
    connection = {"provider": "s3", "roots": ["s3://landing/events/"]}
    assert rules.check_connection("landing", connection) == []
    assert rules.check_connection("landing", dict(connection, unexpected="value")) == []
    assert rules.check_connection("landing", dict(connection, regon="us-east-1")) == [
        ("bad-connection", "regon: unknown vendor argument (did you mean 'region'?)", False)
    ]
    assert any(
        "path" in message
        for _, message, _ in rules._check_call(
            {"service": "object", "method": "read", "arguments": {}}, connection, True
        )
    )
    assert any(
        "match" in message
        for _, message, _ in rules._check_call(
            {"service": "object", "method": "read", "arguments": {"path": "s3://landing/events/*.csv", "match": ".*"}},
            connection,
            True,
        )
    )
    assert any(
        "path" in message
        for _, message, _ in rules._check_call(
            {"service": "object", "method": "read", "arguments": {"path": ""}}, connection, True
        )
    )
    assert any(
        code == "wrong-role"
        for code, _, _ in rules._check_call(
            {"service": "object", "method": "write", "arguments": {"path": "s3://landing/out/", "format": "csv"}},
            connection,
            True,
        )
    )
    unsafe_output = {
        "service": "object",
        "method": "write",
        "arguments": {"path": "s3://landing/out/", "format": "csv", "mode": "error"},
    }
    assert any(code == "not-idempotent" for code, _, _ in rules._check_call(unsafe_output, connection, False))
    safe_output = {**unsafe_output, "arguments": {**unsafe_output["arguments"], "mode": "overwrite"}}
    assert not any(code == "not-idempotent" for code, _, _ in rules._check_call(safe_output, connection, False))


def test_spark_json_schema_uses_connector_field_schema(monkeypatch):
    adapter = DefinedAdapter(CONNECTOR, S3SparkAdapter())
    monkeypatch.setattr(spark_schema, "Registry", lambda entry_points=None: Registry(adapters={"s3": adapter}))
    schema = spark_schema.json_schema()
    connections = schema["properties"]["connections"]["additionalProperties"]["allOf"]
    s3 = next(branch["then"] for branch in connections if branch["if"]["properties"]["provider"]["const"] == "s3")
    assert s3["properties"]["key_id"] == CONNECTOR.field("key_id").json_schema()
    assert s3["properties"]["roots"] == CONNECTOR.field("roots").json_schema()


def test_executor_dispatches_declared_operations_without_method_tables(monkeypatch):
    seen = []
    adapter = S3SparkAdapter()
    monkeypatch.setattr(
        adapter,
        "read",
        lambda context, operation, arguments: (
            seen.append(("read", context.name, operation.name, context.connection)) or "read-frame"
        ),
    )
    monkeypatch.setattr(
        adapter,
        "write",
        lambda context, frame, operation, arguments: (
            seen.append(("write", context.name, operation.name, frame)) or "receipt"
        ),
    )
    spark = SimpleNamespace(sql=lambda query, args: "output-frame")
    connection = {"roots": ("s3://landing/",)}
    executor = SparkExecutor(
        spark,
        {"name": "sparkjob", "steps": [], "outputs": []},
        {"landing": Connection("landing", "s3", connection)},
        {"s3": DefinedAdapter(CONNECTOR, adapter)},
        config={},
        limits={},
    )
    executor.params = {}
    runtime = SimpleNamespace(spark=spark)
    step = {
        "name": "raw",
        "connection": "landing",
        "service": "object",
        "method": "read",
        "arguments": {"path": "s3://landing/input.csv"},
    }
    output = {
        "name": "sink",
        "connection": "landing",
        "service": "object",
        "method": "write",
        "sql": "SELECT 1",
        "arguments": {"path": "s3://landing/out/", "format": "csv"},
    }
    assert executor._step(runtime, step) == "read-frame"
    assert executor._write(runtime, output) == "receipt"
    assert seen == [
        ("read", "raw", "object.read", connection),
        ("write", "sink", "object.write", "output-frame"),
    ]


def test_read_files_rejects_outside_root_before_filesystem_access():
    runtime = SimpleNamespace(path_values=lambda path: ["s3://elsewhere/private.csv"])
    context = SparkContext(runtime, "raw", {"roots": ("s3://landing/",)})
    location = files.FileLocation(("s3://landing/",), "s3", "s3a")
    with pytest.raises(StorageError, match="outside"):
        files.read_files(context, {"path": "s3://elsewhere/private.csv"}, location=location)


def test_relative_keys_resolve_inside_first_root_without_escape():
    location = files.FileLocation(("s3://landing/events/", "s3://archive/"), "s3", "s3a")
    assert location.resolve_path("partners/a.csv") == "s3://landing/events/partners/a.csv"
    assert location.resolve_path("s3://archive/a.csv") == "s3://archive/a.csv"
    for path in ("../private.csv", "/private.csv", "s3://elsewhere/private.csv", "s3:/private.csv"):
        with pytest.raises(StorageError):
            location.resolve_path(path)


def test_pinned_read_plan_cannot_escape_roots(monkeypatch):
    monkeypatch.setattr(
        ingest, "deserialize_plan", lambda plan: ([SimpleNamespace(files=["s3://elsewhere/private.csv"])], [], [])
    )
    runtime = SimpleNamespace(
        path_values=lambda path: ["s3://landing/input.csv"],
        pinned={"ingest:raw": {}},
    )
    context = SparkContext(runtime, "raw", {"roots": ("s3://landing/",)})
    with pytest.raises(StorageError, match="outside"):
        files.read_files(
            context, {"path": "s3://landing/input.csv"}, location=files.FileLocation(("s3://landing/",), "s3", "s3a")
        )
