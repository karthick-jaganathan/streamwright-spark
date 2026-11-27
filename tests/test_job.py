#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""Spark-specific rules use the shared jobs checker, loader and dependency plan."""

import io
import sys
from types import SimpleNamespace

import pytest

from streamwright.core.jobs.check import check_job as check_shared
from streamwright.core.jobs.duckdb.engine import validate_job as validate_duckdb_job
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
                "operation": {"service": "database", "method": "put", "arguments": {}},
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
    job["outputs"][0]["operation"]["arguments"]["path"] = "{{ secrets.key }}"
    assert any(issue.code == "secret-placement" for issue in issues(job, definition_adapter))
    job["outputs"][0]["operation"]["arguments"].clear()
    job["connections"]["db"]["password"] = "literal-password"
    assert any(issue.code == "literal-secret" for issue in issues(job, definition_adapter))


def test_a_spark_jobs_name_may_have_dotted_parts(definition_adapter):
    job = sample_job()
    job["name"] = "s3.orders_export"
    assert issues(job, definition_adapter) == []
    job["name"] = "s3..orders"
    assert [(issue.code, issue.path) for issue in issues(job, definition_adapter)] == [("bad-value", "name")]


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


def test_spark_only_sql_validates_and_orders_the_steps(definition_adapter):
    job = sample_job()
    job["steps"] = [
        {"name": "items", "sql": "SELECT id, item FROM rows LATERAL VIEW explode(array(1, 2)) t AS item "
                                 "DISTRIBUTE BY id"},
        {"name": "rows", "sql": "SELECT :tenant AS id"},
    ]
    assert issues(job, definition_adapter) == []
    assert SparkRules({}).dependencies_of(job["steps"][0]) == ["rows"]


def test_runtime_limits_are_typed_and_an_unknown_one_names_the_engine(definition_adapter):
    job = sample_job()
    job["runtime"]["limits"] = {"events": 5, "max_files_per_batch": 10}
    assert issues(job, definition_adapter) == []
    job["runtime"]["limits"] = {"events": "many", "bogus_key": 1}
    found = {(issue.code, issue.path): issue.message for issue in issues(job, definition_adapter)}
    assert ("bad-type", "runtime.limits.events") in found
    assert "Spark engine has no runtime.limits.bogus_key" in found[("unsupported-key", "runtime.limits.bogus_key")]


@pytest.mark.parametrize("value", [0, "many", 1.5])
def test_runtime_limits_call_records_is_an_integer_of_at_least_one(definition_adapter, value):
    job = sample_job()
    job["runtime"]["limits"] = {"call_records": 1}
    assert issues(job, definition_adapter) == []
    job["runtime"]["limits"] = {"call_records": value}
    assert [issue.path for issue in issues(job, definition_adapter)] == ["runtime.limits.call_records"]


def test_only_the_constants_the_engine_renders_are_accepted_and_only_where_it_renders_them(definition_adapter):
    job = sample_job()
    job["steps"][0]["sql"] = "SELECT '{{ batch.id }}' AS id"
    assert [issue.code for issue in issues(job, definition_adapter)] == ["unrendered-reference"]
    job["steps"][0]["sql"] = "SELECT :batch_id AS id"
    job["outputs"][0]["operation"]["arguments"]["path"] = "out/{{ batch_seq }}/"
    assert [issue.code for issue in issues(job, definition_adapter)] == ["unknown-reference"]


def test_an_adapters_own_tokens_are_accepted_only_in_its_arguments(definition_adapter):
    operations = (Operation(service="database", method="put", kind=OperationKind.WRITE),)
    staging = definition_adapter("staging", SimpleNamespace(template_tokens=("uuid",)), operations=operations)
    rules = SparkRules({"table": definition_adapter("table", object(), operations=operations), "staging": staging})
    job = sample_job()
    job["connections"]["db"] = {"provider": "staging"}
    job["connections"]["other"] = {"provider": "table"}
    job["outputs"][0]["operation"]["arguments"]["staging_table"] = "stg_{{ uuid }}"
    job["outputs"].append(dict(job["outputs"][0], name="copy", connection="other"))
    found = [(issue.code, issue.path) for issue in check_shared(job, rules)]
    assert found == [("unsupported-reference", "outputs[1].operation.arguments")]



@pytest.mark.parametrize(("log", "valid"), [
    ({"message": "row {id}", "level": "warning", "max_rows": 1000}, True),
    ({"message": "x", "level": "error"}, False), ({"message": "x", "max_rows": 0}, False),
    ({"message": "x", "max_rows": 1001}, False), ({"message": "x", "lines": 2}, False), ({"level": "info"}, False),
    ({"message": ""}, False), ({"message": "{a b}"}, False),
])
def test_a_steps_log_is_checked_as_duckdbjob_checks_it(definition_adapter, log, valid):
    job = sample_job()
    job["steps"][0]["log"] = log
    duckdb_job = {"kind": "DuckDBJob", "name": "sample",
                  "connections": {"wh": {"provider": "duckdb", "path": ":memory:"}},
                  "steps": [{"name": "rows", "sql": "SELECT 1 AS id", "log": log}],
                  "outputs": [{"name": "o", "connection": "wh", "step": "rows", "table": "t", "mode": "overwrite"}]}
    found = [(i.code, i.path, i.message) for i in issues(job, definition_adapter)]
    assert found == [(i.code, i.path, i.message) for i in validate_duckdb_job(duckdb_job).issues]
    assert (found == []) is valid
    del job["steps"][0]["log"]
    job["outputs"][0]["log"] = {"message": "x"}
    assert [i.path for i in issues(job, definition_adapter)] == ["outputs[0].log"]
