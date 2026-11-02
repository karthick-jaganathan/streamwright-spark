"""Spark-specific definition discovery and adapter contract regressions."""

import importlib

import pytest

from streamwright.connectors.s3 import CONNECTOR
from streamwright.core.cli import main as core_main
from streamwright.core.jobs.check import check_job
from streamwright.core.runtime import plugins
from streamwright.core.sdk.contract import Field, Operation, OperationKind
from streamwright.spark.cli import main, register
from streamwright.spark.contracts import AdapterError
from streamwright.spark.docs import guide
from streamwright.spark.io.file_rules import check_write
from streamwright.spark.io.sql import where_clause
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.launchers.local import LocalLauncher
from streamwright.spark.plugin import JOB_ENGINE
from streamwright.spark.runtime.registry import Registry, RegistryError


def test_adapter_error_is_part_of_public_contract_api():
    assert issubclass(AdapterError, Exception)


@pytest.mark.parametrize("operator,expected", [("IN", "FALSE"), ("NOT IN", "TRUE")])
def test_empty_membership_filter_matches_core_semantics(operator, expected):
    assert where_clause([{"column": "id", "op": operator, "value": []}], "`") == " WHERE " + expected
    assert where_clause([{"column": "id", "op": operator, "value": []}], '"') == " WHERE " + expected


def test_spark_filter_renders_core_membership_and_safe_literals():
    assert (
        where_clause(
            [
                {"column": "id", "op": "IN", "value": ["O'Neil", "$p_1"]},
                {"column": "age", "op": ">=", "value": 21, "type": "INTEGER"},
            ],
            "`",
        )
        == " WHERE `id` IN ('O''Neil', '$p_1') AND `age` >= CAST(21 AS INTEGER)"
    )
    assert (
        where_clause(
            [
                {"column": "id", "op": "IN", "value": [1, 2], "type": "BIGINT"},
            ]
        )
        == ' WHERE "id" IN (CAST(1 AS BIGINT), CAST(2 AS BIGINT))'
    )


def test_append_name_with_batch_id_uses_core_template_metadata():
    assert check_write({"path": "output/", "mode": "append", "file_name": "{batch_id}.csv"}) == []
    assert any(
        "replay-safe" in issue
        for issue in check_write({"path": "output/", "mode": "append", "file_name": "{today}.csv"})
    )


def test_registry_loads_only_definitions_after_authorization():
    loaded = []

    class Entry:
        name = "s3"
        dist = None

        def load(self):
            loaded.append(True)
            return CONNECTOR

    registry = Registry(entry_points=lambda: {"streamwright.connectors": [Entry()]})
    assert registry.available() == ["s3"]
    assert loaded == []
    with pytest.raises(RegistryError, match="allowed"):
        registry.load("s3", allow={"other"})
    assert loaded == []
    assert registry.load("s3").definition is CONNECTOR
    assert loaded == [True]


def test_registry_rejects_non_definition_connector():
    class Entry:
        name = "old"
        dist = None

        def load(self):
            return object()

    registry = Registry(entry_points=lambda: {"streamwright.connectors": [Entry()]})
    with pytest.raises(RegistryError, match="old \\(unknown distribution\\) targets the pre-contract connector API"):
        registry.load("old")
    _, problems = registry.load_all()
    assert len(problems) == 1 and "old (unknown distribution) targets the pre-contract connector API" in problems[0]
    with pytest.raises(RegistryError, match="ConnectorDefinition"):
        registry.register(object())


def test_guide_lists_only_supported_platforms():
    assert "local" in guide("pipeline").lower() or "local" in guide().lower()
    with pytest.raises(KeyError):
        guide("unsupported")


def test_spark_command_is_registered_through_core_plugin_group(monkeypatch, capsys):
    class CommandEntry:
        name = "spark"
        dist = None

        def load(self):
            return register

    monkeypatch.setattr(plugins, "entry_point_source", lambda: {plugins.COMMANDS: [CommandEntry()]})
    with pytest.raises(SystemExit) as exited:
        core_main(["spark", "state", "--help"])
    assert exited.value.code == 0
    assert "init" in capsys.readouterr().out
    with pytest.raises(SystemExit) as exited:
        core_main(["spark", "--help"])
    assert exited.value.code == 0
    assert "validate" in capsys.readouterr().out


def test_spark_rules_restrict_secret_placement_and_warn_non_idempotent(definition_adapter):
    adapter = definition_adapter(
        "writing",
        object(),
        operations=(Operation(service="data", method="put", kind=OperationKind.WRITE, idempotent=False),),
        connection=(Field("password", "string", secret=True),),
    )
    job = {
        "kind": "SparkJob",
        "name": "sample",
        "spec": {"secrets": {"key": {"type": "string"}}},
        "connections": {"sink": {"provider": "writing", "password": "{{ secrets.key }}", "label": "{{ secrets.key }}"}},
        "steps": [{"name": "rows", "sql": "SELECT 1"}],
        "outputs": [
            {
                "name": "result",
                "sql": "SELECT * FROM rows",
                "connection": "sink",
                "service": "data",
                "method": "put",
                "arguments": {},
            }
        ],
        "runtime": {"state": {"sqlite": ":memory:"}, "spark": {}, "platform": {"type": "local"}},
    }
    found = check_job(job, SparkRules({"writing": adapter}))
    assert any(issue.code == "secret-placement" and issue.path.endswith("label") for issue in found)
    assert any(issue.code == "not-idempotent" and issue.severity == "warning" for issue in found)
    job["connections"]["sink"].pop("label")
    assert not any(issue.code == "secret-placement" for issue in check_job(job, SparkRules({"writing": adapter})))


def test_spark_cli_validates_job_through_public_report(monkeypatch, tmp_path, capsys, definition_adapter):
    adapter = definition_adapter(
        "fake", object(), operations=(Operation(service="data", method="put", kind=OperationKind.WRITE),)
    )
    job = tmp_path / "job.yaml"
    job.write_text(
        "kind: SparkJob\nname: sample\nconnections:\n  sink:\n    provider: fake\n"
        "steps:\n  - name: rows\n    sql: SELECT 1\noutputs:\n"
        "  - name: result\n    connection: sink\n    service: data\n    method: put\n"
        "    arguments: {}\n    sql: SELECT * FROM rows\nruntime:\n"
        "  state:\n    sqlite: ':memory:'\n  spark: {}\n  platform:\n    type: local\n",
        encoding="utf-8",
    )
    validation = importlib.import_module("streamwright.spark.job.validate")
    original = validation.registry.Registry
    monkeypatch.setattr(validation.registry, "Registry", lambda: original(adapters={"fake": adapter}))
    assert main(["validate", str(job)]) == 0
    assert "ok" in capsys.readouterr().out
    assert main(["plan", str(job)]) == 0
    assert "Spark actions per batch" in capsys.readouterr().out

    class SparkJobEntry:
        name = "SparkJob"
        dist = None

        def load(self):
            return JOB_ENGINE

    class LocalEntry:
        name = "local"
        dist = None

        def load(self):
            return LocalLauncher

    monkeypatch.setattr(
        plugins,
        "entry_point_source",
        lambda: {plugins.JOB_KINDS: [SparkJobEntry()], plugins.SPARK_LAUNCHERS: [LocalEntry()]},
    )
    assert core_main(["plan", str(job)]) == 0
    assert "Spark actions per batch" in capsys.readouterr().out
