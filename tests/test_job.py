"""Spark-specific rules use the shared jobs checker, loader and dependency plan."""

import io
import sys
from types import SimpleNamespace

import pytest

from streamwright.core.jobs.check import check_job as check_shared
from streamwright.core.jobs.manifest import MAX_MANIFEST_BYTES, JobError, load_job_text
from streamwright.core.sdk.contract import Field, Operation, OperationKind
from streamwright.spark.job import load_job, resolve
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.launchers.common import _read_s3, resolve_job_location


def sample_job():
    return {
        "kind": "SparkJob",
        "name": "sample",
        "spec": {"config": {"tenant": {"type": "string", "default": "acme"}}, "secrets": {"key": {"type": "string"}}},
        "connections": {"db": {"provider": "table", "password": "{{ secrets.key }}"}},
        "steps": [{"name": "rows", "sql": "SELECT :tenant AS id"}],
        "outputs": [
            {
                "name": "result",
                "sql": "SELECT * FROM rows",
                "connection": "db",
                "service": "database",
                "method": "put",
                "arguments": {},
            }
        ],
        "runtime": {"state": {"sqlite": ":memory:"}, "spark": {"conf": {}}, "platform": {"type": "local"}},
    }


def issues(job, definition_adapter):
    adapter = definition_adapter(
        "table",
        object(),
        operations=(Operation(service="database", method="put", kind=OperationKind.WRITE),),
        connection=(Field("password", "string", secret=True),),
    )
    return check_shared(job, SparkRules({"table": adapter}))


def test_spark_rules_accept_valid_job_and_reject_misplaced_secret(definition_adapter):
    job = sample_job()
    assert issues(job, definition_adapter) == []
    job["outputs"][0]["arguments"]["path"] = "{{ secrets.key }}"
    assert any(issue.code == "secret-placement" for issue in issues(job, definition_adapter))
    job["outputs"][0]["arguments"].clear()
    job["connections"]["db"]["password"] = "literal-password"
    assert any(issue.code == "literal-secret" for issue in issues(job, definition_adapter))


def test_shared_checker_flags_cycles_and_unknown_parameters(definition_adapter):
    job = sample_job()
    job["steps"] = [{"name": "first", "sql": "SELECT * FROM second"}, {"name": "second", "sql": "SELECT * FROM first"}]
    assert any(issue.code == "dependency" for issue in issues(job, definition_adapter))
    job["steps"][0]["sql"] = "SELECT :not_declared"
    assert any(issue.code == "unknown-parameter" for issue in issues(job, definition_adapter))


def test_spark_uses_core_manifest_limit_and_overlay(tmp_path):
    path, overlay = tmp_path / "job.yaml", tmp_path / "overlay.yml"
    path.write_text("kind: SparkJob\nname: sample\nruntime:\n  old: yes\n", encoding="utf-8")
    overlay.write_text("runtime:\n  old: null\n", encoding="utf-8")
    loaded = load_job(path, overlays=(overlay,))
    assert loaded["runtime"] == {}
    with pytest.raises(JobError, match="larger than"):
        load_job_text("a" * (MAX_MANIFEST_BYTES + 1))


def test_remote_manifest_folder_uses_bucket_root_job_file(monkeypatch):

    calls = []

    class Client:
        def get_object(self, **kwargs):
            calls.append(kwargs)
            return {"Body": io.BytesIO(b"kind: SparkJob\n")}

    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda *args, **kwargs: Client()))
    assert _read_s3("s3://bucket/", 100) == "kind: SparkJob\n"
    assert calls == [{"Bucket": "bucket", "Key": "job.yaml"}]
    assert resolve_job_location("s3://bucket/folder/") == "s3://bucket/folder/job.yaml"


def test_input_resolution_preserves_non_input_references():
    job = sample_job()
    resolved, missing = resolve(job, {"tenant": "acme"}, {"key": "secret-value"})
    assert missing == []
    assert resolved["steps"][0]["sql"] == "SELECT :tenant AS id"
    assert resolved["connections"]["db"]["password"] == "secret-value"
