"""The generated Spark schema enforces the same structure as its JobRules."""

import copy

import jsonschema
import pytest

from streamwright.core.jobs.fields import check_field
from streamwright.core.sdk.contract import Field, Operation, OperationKind
from streamwright.spark import schema as spark_schema
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.runtime.registry import Registry
from streamwright.spark.schema import json_schema


def test_spark_schema_and_checker_agree_on_structure():
    schema = json_schema(entry_points=dict)
    field = SparkRules({}).manifest_field()
    job = {
        "kind": "SparkJob",
        "name": "sample",
        "connections": {"sink": {"provider": "uninstalled"}},
        "steps": [{"name": "rows", "sql": "SELECT 1"}],
        "outputs": [{"name": "result", "sql": "SELECT * FROM rows"}],
        "runtime": {"state": {"sqlite": ":memory:"}, "spark": {}, "platform": {"type": "local"}},
    }
    assert check_field(job, field) == []
    jsonschema.validate(job, schema)
    for section, key, value in (("outputs", "bad_key", True), ("steps", "name", 4)):
        invalid = copy.deepcopy(job)
        invalid[section][0][key] = value
        assert check_field(invalid, field)
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(invalid, schema)


def test_installed_adapter_declares_connection_fields_in_schema(definition_adapter, monkeypatch):
    adapter = definition_adapter(
        "installed",
        object(),
        operations=(Operation(service="database", method="query", kind=OperationKind.READ),),
        connection=(
            Field("host", "string", required=True),
            Field("database", "string"),
            Field("timeout", "integer"),
            Field("password", "string", secret=True),
        ),
    )
    monkeypatch.setattr(spark_schema, "Registry", lambda entry_points=None: Registry(adapters={"installed": adapter}))
    schema = spark_schema.json_schema()
    auth = {"provider": "installed", "host": "db", "database": "safe", "timeout": 12, "password": "{{ secrets.key }}"}
    variant = schema["properties"]["connections"]["additionalProperties"]
    installed = next(
        branch["then"] for branch in variant["allOf"] if branch["if"]["properties"]["provider"]["const"] == "installed"
    )
    assert installed["properties"]["timeout"] == adapter.definition.field("timeout").json_schema()
    jsonschema.validate(
        {
            "kind": "SparkJob",
            "name": "sample",
            "connections": {"db": auth},
            "outputs": [{"name": "result", "sql": "SELECT 1"}],
        },
        schema,
    )
    with pytest.raises(jsonschema.ValidationError):
        invalid = dict(auth, timeout="not an integer")
        jsonschema.validate(
            {
                "kind": "SparkJob",
                "name": "sample",
                "connections": {"db": invalid},
                "outputs": [{"name": "result", "sql": "SELECT 1"}],
            },
            schema,
        )
