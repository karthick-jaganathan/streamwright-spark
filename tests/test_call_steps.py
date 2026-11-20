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

"""SparkJob source-connector call steps (contract v1.2 §6a engine reach) and execution policy, with a fake source."""

import dataclasses
import importlib
import json
import os
import sys
from types import SimpleNamespace
from typing import ClassVar

import pytest

from streamwright.core.jobs import Runner, WriteReceipt, calls
from streamwright.core.jobs.apply.delivery import Applier, apply_calls
from streamwright.core.jobs.grammar import to_internal
from streamwright.core.jobs.manifest import JobError
from streamwright.core.jobs.policy import TransientError
from streamwright.core.jobs.state import open_sqlite
from streamwright.core.runtime import components
from streamwright.core.runtime.redact import Redactor
from streamwright.core.sdk.apply import RowResult
from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind
from streamwright.spark.cli.common import connections_of
from streamwright.spark.cli.run import apply_connectors
from streamwright.spark.contracts import Connection
from streamwright.spark.engine import sources
from streamwright.spark.engine.executor import SparkExecutor
from streamwright.spark.job import validate
from streamwright.spark.plan import build_plan
from streamwright.spark.runtime import registry
from streamwright.spark.runtime.definition import bind

RECORDS = ({"id": 1, "name": "a"}, {"id": 2, "name": "b"})


class FakeSession:
    def read(self, operation, arguments, context):
        yield list(RECORDS)

    def close(self):
        pass


class FakeSourceAdapter:
    def open(self, connection, context):
        assert connection["token"] == "secret-value"      # the source engine rendered the declared secret
        return FakeSession()


CONNECTOR = ConnectorDefinition(
    name="fakesrc", title="Fake Source", family="test",
    connection=(Field("token", "string", required=True, secret=True),),
    operations=(Operation(service="data", method="list", kind=OperationKind.READ,
                          arguments=(Field("query", "string"),)),),
    adapters={"source": "tests.test_call_steps:FakeSourceAdapter"},
)


@pytest.fixture
def fakesrc():
    components.register(CONNECTOR)
    yield
    components.unregister("fakesrc")


class EchoSession(FakeSession):
    def read(self, operation, arguments, context):
        yield [{"campaign": arguments["query"]}]          # the rendered {{ partition.* }} value


class EchoSourceAdapter(FakeSourceAdapter):
    def open(self, connection, context):
        return EchoSession()


class FakeFrame:
    def __init__(self, lines=()):
        self.lines = list(lines)

    def createOrReplaceTempView(self, name):
        pass


class FakeReader:
    def json(self, rdd):
        return FakeFrame(rdd)


class FakeContext:
    def __init__(self):
        self.chunks = []

    def parallelize(self, lines, slices):
        self.chunks.append(list(lines))
        return list(lines)

    def union(self, chunks):
        return [line for chunk in chunks for line in chunk]


class FakeSpark:
    def __init__(self):
        self.read = FakeReader()
        self.sparkContext = FakeContext()

    def createDataFrame(self, rows, schema=None):
        return FakeFrame()

    def sql(self, text, args=None):
        return FakeFrame()


def _declared():
    return {"src": {"provider": "fakesrc", "token": "{{ secrets.tok }}"},
            "lake": {"provider": "s3", "roots": ["s3://b/pre"]}}


def _job():
    return {
        "kind": "SparkJob", "name": "ads",
        "connections": _declared(),
        "spec": {"secrets": {"tok": {"type": "string"}}},
        "steps": [{"name": "rows", "connection": "src",
                   "operation": {"service": "data", "method": "list", "arguments": {"query": "SELECT 1"}},
                   "policy": {"retry": {"max_attempts": 3}, "rate_limit": {"requests": 2, "per": "1s"},
                              "on_error": "skip", "timeout": "30s", "page_size": 1}}],
        "outputs": [{"name": "out", "connection": "lake",
                     "operation": {"service": "object", "method": "write",
                                   "arguments": {"path": "out/", "format": "parquet", "mode": "overwrite"}},
                     "sql": "SELECT * FROM rows"}],
        "runtime": {"state": {"sqlite": "state.db"}, "platform": {"type": "local"}},
    }


def test_a_source_connector_call_step_validates(fakesrc):
    report = validate(_job())
    assert report.valid, [str(i) for i in report.issues if i.severity == "error"]


def test_call_records_runs_the_connector_through_the_shared_path(fakesrc):
    step = {"name": "rows", "connection": "src", "service": "data", "method": "list",
            "arguments": {"query": "SELECT 1"}}
    records, collected = calls.call_records(step, {"provider": "fakesrc", "token": "{{ secrets.tok }}"},
                                            config={}, secrets={"tok": "secret-value"})
    assert list(records) == list(RECORDS) and not collected.partial


def test_a_source_call_step_loads_its_records_into_a_dataframe(fakesrc):
    # a provider without a Spark adapter is no native connection: the step runs as a call step
    connections = connections_of(_job(), adapters={}, secrets={"tok": "secret-value"})
    assert connections == {}
    spark = FakeSpark()
    executor = SparkExecutor(spark, {"name": "ads", "steps": [], "outputs": []}, connections, {},
                             config={}, limits={}, declared_connections=_declared(),
                             secrets={"tok": "secret-value"})
    frame = executor._step(None, to_internal(_job())["steps"][0])
    assert [json.loads(line) for line in frame.lines] == list(RECORDS)
    assert [len(chunk) for chunk in spark.sparkContext.chunks] == [1, 1]   # loaded page_size records at a time


class ReportSession(FakeSession):
    """A report job: `submit` gives its id, `poll` its state (running, then done), `rows` the records."""
    polls: ClassVar[list] = []

    def read(self, operation, arguments, context):
        if operation.method == "submit":
            yield {"report": "r1"}
        elif operation.method == "poll":
            ReportSession.polls.append(arguments["report"])
            yield {"state": "done" if len(ReportSession.polls) > 1 else "running"}
        else:
            yield [dict(record, report=arguments["report"]) for record in RECORDS]


class ReportSourceAdapter(FakeSourceAdapter):
    def open(self, connection, context):
        return ReportSession()


def test_a_call_step_runs_an_async_job_inline(monkeypatch):
    report_ops = tuple(Operation(service="reports", method=method, kind=OperationKind.READ,
                                 arguments=(Field("report", "string"),)) for method in ("submit", "poll", "rows"))
    components.register(dataclasses.replace(CONNECTOR, operations=report_ops,
                                            adapters={"source": "tests.test_call_steps:ReportSourceAdapter"}))
    session = importlib.import_module("tests.test_call_steps").ReportSession   # the class the connector loads
    session.polls.clear()
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    try:
        job = _job()
        job["steps"][0] = {"name": "rows", "connection": "src", "async_job": {
            "submit": {"operation": {"service": "reports", "method": "submit"}},
            "poll": {"operation": {"method": "poll", "arguments": {"report": "{{ submit.report }}"}},
                     "every": "1s", "timeout": "1m", "done_when": {"path": "state", "equals": "done"}},
            "results": {"operation": {"service": "reports", "method": "rows",
                                      "arguments": {"report": "{{ submit.report }}"}}}}}
        report = validate(job)
        assert report.valid, [str(i) for i in report.issues if i.severity == "error"]
        executor = SparkExecutor(FakeSpark(), {"name": "ads", "steps": [], "outputs": []}, {}, {}, config={},
                                 limits={}, declared_connections=_declared(), secrets={"tok": "secret-value"})
        frame = executor._step(None, to_internal(job)["steps"][0])
        assert [json.loads(line) for line in frame.lines] == [dict(record, report="r1") for record in RECORDS]
        assert session.polls == ["r1", "r1"]
    finally:
        components.unregister("fakesrc")


SOURCE_YAML = """\
kind: source
name: fake
spec: {secrets: {tok: {type: string}}, config: {label: {type: string, default: none}}}
auth: {provider: fakesrc, token: "{{ secrets.tok }}"}
streams:
  - name: items
    requests: [{name: raw_items, operation: {service: data, method: list}}]
    transform:
      mode: page
      steps:
        - {name: items, select: "SELECT (record->>'id')::BIGINT AS id, record->>'name' AS name,
                                         $label AS label FROM raw_items"}
    export: {items: {step: items}}
"""


def _source_job(tmp_path, **step):
    folder = tmp_path / "sources" / "fake"
    folder.mkdir(parents=True)
    (folder / "source.yaml").write_text(SOURCE_YAML)
    job = _job()
    job["steps"][0] = dict({"name": "rows", "source": "sources/fake", "stream": "items",
                            "config": {"label": "{{ partition.label }}"},
                            "partitions": [{"from": "labels", "field": "label"}]}, **step)
    job["steps"].insert(0, {"name": "labels", "sql": "SELECT 'x' AS label"})
    (tmp_path / "job.yaml").write_text(json.dumps(job))
    return job


def test_a_source_step_runs_its_stream_on_the_driver_once_per_partition(fakesrc, tmp_path):
    job = _source_job(tmp_path)
    report = validate(job)
    assert report.valid, [str(i) for i in report.issues if i.severity == "error"]
    executor = SparkExecutor(FakeSpark(), {"name": "ads", "steps": [], "outputs": []}, {}, {}, config={}, limits={},
                             declared_connections=_declared(), secrets={"tok": "secret-value"},
                             job_location=str(tmp_path / "job.yaml"))
    runtime = SimpleNamespace(values=lambda step, field: ["x"])
    frame = executor._step(runtime, to_internal(job)["steps"][1])
    assert [json.loads(line) for line in frame.lines] == [dict(record, label="x") for record in RECORDS]
    assert list(executor.carry) == ['bookmarks:rows:{"label": "x"}']      # each scope resumes from its own


def test_a_source_step_without_rows_keeps_its_export_columns(tmp_path, monkeypatch):
    components.register(dataclasses.replace(CONNECTOR, adapters={"source": "tests.test_call_steps:EmptySourceAdapter"}))
    try:
        job = _source_job(tmp_path)
        spark = FakeSpark()
        created = []
        monkeypatch.setattr(spark, "createDataFrame", lambda rows, schema=None: created.append(schema) or FakeFrame())
        executor = SparkExecutor(spark, {"name": "ads", "steps": [], "outputs": []}, {}, {}, config={}, limits={},
                                 declared_connections=_declared(), secrets={"tok": "secret-value"},
                                 job_location=str(tmp_path / "job.yaml"))
        executor._step(SimpleNamespace(values=lambda step, field: ["x"]), to_internal(job)["steps"][1])
        assert created == ["`id` BIGINT, `name` STRING, `label` STRING"]
    finally:
        components.unregister("fakesrc")


def test_a_relative_source_of_an_s3_manifest_is_downloaded_from_its_prefix(monkeypatch, tmp_path):
    downloads = []
    monkeypatch.setitem(sys.modules, "streamwright.spark.launchers.s3_jobs", SimpleNamespace(
        download_folder=lambda location, target, limit: downloads.append(location)))
    step = {"name": "rows", "source": "../sources/fake", "stream": "items"}
    folder = sources.source_folder(step, "s3://code-bucket/jobs/ads/job.yaml")
    assert downloads == ["s3://code-bucket/jobs/sources/fake"] and os.path.isdir(folder)
    sources.source_folder(dict(step, source="s3://other/sources/fake"), None)
    assert downloads[-1] == "s3://other/sources/fake"
    with pytest.raises(JobError, match="outside the bucket"):
        sources.source_folder(dict(step, source="../../../x"), "s3://code-bucket/jobs/job.yaml")
    with pytest.raises(JobError, match="is not a folder"):
        sources.source_folder(step, str(tmp_path / "job.yaml"))


def test_a_remote_source_folder_copies_only_its_yaml_files_and_never_outside_the_target(monkeypatch, tmp_path):
    keys = ["src/source.yaml", "src/streams/a.yaml", "src/streams/deep/b.yaml", "src/README.md", "src/../x.yaml",
            "src/streams/../../y.yaml", "src//z.yaml"]
    copied = []

    class Client:
        def get_paginator(self, name):
            return SimpleNamespace(paginate=lambda **kwargs: [{"Contents": [{"Key": key, "Size": 10} for key in keys]}])

        def download_file(self, bucket, key, path):
            copied.append((key, os.path.relpath(path, str(tmp_path))))

    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda *args, **kwargs: Client()))
    try:
        reader = importlib.import_module("streamwright.spark.launchers.s3_jobs")   # bound to the fake boto3
        reader.download_folder("s3://bucket/src", str(tmp_path), 1000)
    finally:
        sys.modules.pop("streamwright.spark.launchers.s3_jobs", None)       # other tests import it afresh
    assert copied == [("src/source.yaml", "source.yaml"), ("src/streams/a.yaml", os.path.join("streams", "a.yaml"))]


class EmptySourceAdapter(FakeSourceAdapter):
    def open(self, connection, context):
        return SimpleNamespace(read=lambda operation, arguments, context: iter([[]]), close=lambda: None)


class PagedSession(FakeSession):
    pages = 0

    def read(self, operation, arguments, context):
        for page in range(100):
            PagedSession.pages += 1
            yield [{"id": page * 100 + index} for index in range(100)]


class PagedSourceAdapter(FakeSourceAdapter):
    def open(self, connection, context):
        return PagedSession()


@pytest.mark.parametrize(("cap", "pages"), [(150, range(2, 5)), (10_000, [100])], ids=["over", "under"])
def test_a_call_step_stops_collecting_once_past_runtime_limits_call_records(cap, pages):
    components.register(dataclasses.replace(CONNECTOR, adapters={"source": "tests.test_call_steps:PagedSourceAdapter"}))
    session = importlib.import_module("tests.test_call_steps").PagedSession   # the class the connector loads
    session.pages = 0
    try:
        executor = SparkExecutor(FakeSpark(), {"name": "ads", "steps": [], "outputs": []}, {}, {}, config={},
                                 limits={"call_records": cap}, declared_connections=_declared(),
                                 secrets={"tok": "secret-value"})
        step = to_internal(_job())["steps"][0]
        if cap < 10_000:
            with pytest.raises(ValueError, match=r"step rows reached 151 records, more than "
                                                 r"runtime\.limits\.call_records \(150\)"):
                executor._step(None, step)
        else:
            assert len(executor._step(None, step).lines) == 10_000
    finally:
        components.unregister("fakesrc")
    # over the limit: the page that passed it, and at most the source runner's bounded read-ahead (64 records)
    assert session.pages in pages


def test_an_output_write_retries_a_transient_error(tmp_path, definition_adapter):
    store = open_sqlite(str(tmp_path / "spark.db"))
    store.initialize()

    class FlakyAdapter:
        def __init__(self):
            self.attempts = 0

        def write(self, context, frame, operation, arguments):
            self.attempts += 1
            if self.attempts == 1:
                raise TransientError("transient write")
            return WriteReceipt(context.name, "dest", 1)

    adapter = FlakyAdapter()
    defined = definition_adapter("sink", adapter,
                                 operations=(Operation(service="data", method="put", kind=OperationKind.WRITE),))
    job = {"name": "w", "steps": [],
           "outputs": [{"name": "o", "connection": "s", "service": "data", "method": "put", "sql": "SELECT 1",
                        "retry": {"max_attempts": 3, "backoff": "constant"}}]}
    executor = SparkExecutor(FakeSpark(), job, {"s": Connection("s", "sink", {})}, {"sink": defined},
                             config={}, limits={}, state=store)
    runner = Runner("w", store, executor, engine_metadata={"driver_host": "d"})
    assert runner.run() == 0
    assert adapter.attempts == 2
    assert store.get_batch("w", runner.processed[0])["status"] == "completed"



ECHO = dataclasses.replace(CONNECTOR, name="fakeecho", adapters={"source": "tests.test_call_steps:EchoSourceAdapter"})


@pytest.fixture
def fakeecho():
    components.register(ECHO)
    yield
    components.unregister("fakeecho")


class FakeRuntime:
    def values(self, step, column):
        assert (step, column) == ("campaigns", "id")
        return ["c10", None, "c20"]                         # distinct values, pinned by the runtime


def _partitioned_step(connection="src"):
    return {"name": "ad_groups", "connection": connection,
            "operation": {"service": "data", "method": "list", "arguments": {"query": "{{ partition.campaign_id }}"}},
            "partitions": [{"from": "campaigns", "field": "id", "name": "campaign_id"}]}


def test_a_call_step_runs_once_per_partition_value_of_an_earlier_step(fakeecho):
    declared = {"echo": {"provider": "fakeecho", "token": "{{ secrets.tok }}"}}
    executor = SparkExecutor(FakeSpark(), {"name": "ads", "steps": [], "outputs": []}, {}, {}, config={},
                             limits={}, declared_connections=declared, secrets={"tok": "secret-value"})
    frame = executor._step(FakeRuntime(), to_internal({"steps": [_partitioned_step("echo")]})["steps"][0])
    assert [json.loads(line) for line in frame.lines] == [{"campaign": "c10"}, {"campaign": "c20"}]


def test_partitions_validate_and_order_the_steps(fakesrc):
    job = _job()
    job["steps"] = [_partitioned_step(), {"name": "campaigns", "sql": "SELECT 1 AS id"}, *job["steps"]]
    report = validate(job)
    assert report.valid, [str(i) for i in report.issues if i.severity == "error"]
    order = build_plan(job, {}).order
    assert order.index("campaigns") < order.index("ad_groups")  # listed after, run before: partitions read it
    job["steps"][0]["partitions"][0]["column"] = "id"
    assert any("partitions[0]" in str(issue) for issue in validate(job).issues)


def test_a_spark_adapter_that_cannot_load_names_the_cause_and_the_extra():
    broken = dataclasses.replace(CONNECTOR, adapters={"source": CONNECTOR.adapters["source"],
                                                      "spark": "tests.no_such_module:Adapter"})
    with pytest.raises(registry.SparkAdapterLoadError) as caught:
        bind(broken, "fakesrc")
    assert "ModuleNotFoundError" in str(caught.value)
    assert "streamwright connectors install fakesrc" in str(caught.value)


def test_validation_reports_an_adapter_that_cannot_load_even_for_a_source_connector(fakesrc, monkeypatch):
    def load(self, name, kind="adapter", allow=None):
        raise registry.SparkAdapterLoadError("connector %r: its Spark adapter cannot be loaded" % name)
    monkeypatch.setattr(registry.Registry, "load", load)
    report = validate(_job())
    assert not report.valid and any("cannot be loaded" in str(issue) for issue in report.issues)


class LedgerSession:
    def apply(self, operation, arguments, rows, context):
        return [RowResult.applied({"id": "%s/e-%s/%d" % (arguments["account"], row["key"], len(rows))})
                for row in rows]


class LedgerAdapter:
    def open(self, connection, context):
        assert connection["token"] == "secret-value"      # rendered from the declared secret reference
        return LedgerSession()


LEDGER_API = ConnectorDefinition(
    name="ledger_api", title="Ledger API", family="test",
    connection=(Field("token", "string", required=True, secret=True),),
    operations=(Operation(service="entries", method="mutate", kind=OperationKind.APPLY, max_chunk=2,
                          actions=("create",), arguments=(Field("account", "string", required=True),)),),
    adapters={"source": "tests.test_call_steps:LedgerAdapter"})


class RowsFrame(FakeFrame):
    def __init__(self, rows):
        super().__init__()
        self.rows = rows

    def limit(self, count):
        return RowsFrame(self.rows[:count])

    def collect(self):
        return [dataclasses.make_dataclass("Row", ["values"])(row) for row in self.rows]


class RowsSpark(FakeSpark):
    def sql(self, text, args=None):
        return RowsFrame([{"key": key} for key in range(3)])


def _apply_job():
    return {"kind": "SparkJob", "name": "push",
            "connections": {"ledger": {"provider": "ledger_api", "token": "{{ secrets.tok }}"}},
            "spec": {"secrets": {"tok": {"type": "string"}}},
            "steps": [{"name": "rows", "sql": "SELECT 1 AS key"}],
            "outputs": [{"name": "push", "connection": "ledger",
                         "operation": {"service": "entries", "method": "mutate", "arguments": {"account": "acc-1"}},
                         "sql": "SELECT key FROM rows", "apply": {"chunk_size": 2}}],
            "runtime": {"state": {"sqlite": "state.db"}, "platform": {"type": "local"}}}


@pytest.fixture
def ledger_api(monkeypatch):
    monkeypatch.setattr(RowsFrame, "collect", lambda self: [
        type("Row", (), {"asDict": lambda _, recursive=False, row=row: row})() for row in self.rows])
    components.register(LEDGER_API)
    yield
    components.unregister("ledger_api")


def test_an_adapter_write_output_is_not_taken_for_an_apply_output(ledger_api):
    # a connector with a Spark adapter writes through it (`database.write`); only adapter-less connectors apply rows
    job = _apply_job()
    job["connections"]["pg"] = {"provider": "postgres", "host": "db", "dbname": "shop"}
    job["outputs"].append({"name": "audit", "connection": "pg", "sql": "SELECT key FROM rows",
                           "operation": {"service": "database", "method": "write",
                                         "arguments": {"mode": "append", "table": "audit"}}})
    job = to_internal(job)
    found = apply_calls(job, job["connections"], apply_connectors({"postgres": object()}))
    assert set(found) == {"push"}


def test_a_spark_apply_output_validates_and_its_rows_are_applied_through_the_shared_outbox(tmp_path, ledger_api):
    job = _apply_job()
    report = validate(job)
    assert report.valid, [str(i) for i in report.issues if i.severity == "error"]
    assert not validate(dict(job, outputs=[dict(job["outputs"][0], apply={"chunk_size": 5})])).valid
    job = to_internal(job)                                  # what `sw spark run` hands the engine

    store = open_sqlite(str(tmp_path / "spark.db"))
    store.initialize()
    applier = Applier(apply_calls(job, job["connections"], {"ledger_api"}), config={},
                      secrets={"tok": "secret-value"}, redact=Redactor(["secret-value"]))
    executor = SparkExecutor(RowsSpark(), job, {}, {}, config={}, limits={}, state=store,
                             apply_outputs=applier.calls)
    runner = Runner("push", store, executor, engine_metadata={"driver_host": "d"}, applier=applier)
    assert runner.run() == 0, runner.error
    outbox = store.list_events("push", runner.processed[0])
    assert [(row["status"], row["result"]["id"]) for row in outbox] == [
        ("applied", "acc-1/e-0/2"), ("applied", "acc-1/e-1/2"), ("applied", "acc-1/e-2/1")]     # chunks of 2
    assert applier.counts == {"push": {"applied": 3, "failed": 0}}


@pytest.fixture(scope="module")
def spark():
    sql = pytest.importorskip("pyspark.sql")
    session = (sql.SparkSession.builder.master("local[1]").appName("test_call_steps")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


@pytest.mark.parametrize("records", [RECORDS, ()], ids=["records", "none"])
def test_a_call_steps_sql_reads_records_as_json_text_like_duckdb(spark, monkeypatch, records):
    monkeypatch.setattr(calls, "call_records", lambda *args, **kwargs: (
        iter(records), SimpleNamespace(partial=False, state=None)))
    executor = SparkExecutor(spark, {"name": "ads", "steps": [], "outputs": []}, {}, {}, config={}, limits={},
                             declared_connections=_declared(), secrets={"tok": "secret-value"})
    executor.params = {}
    step = dict(to_internal(_job())["steps"][0],
                sql="SELECT get_json_object(record, '$.id') AS account_id FROM records")
    frame = executor._step(None, step)
    assert frame.columns == ["account_id"]          # also with no records: the step's columns do not depend on them
    assert sorted(row.account_id for row in frame.collect()) == [str(record["id"]) for record in records]


def test_persist_and_checkpoint_on_one_step_do_both(monkeypatch):
    calls = []

    class Tracked(FakeFrame):
        def persist(self, level):
            calls.append("persist")
            return self

        def checkpoint(self, eager):
            calls.append("checkpoint")
            return self

    monkeypatch.setattr(FakeSpark, "sql", lambda self, text, args=None: Tracked())
    job = {"name": "j", "steps": [{"name": "s", "sql": "SELECT 1", "persist": "memory", "checkpoint": True}],
           "outputs": []}
    executor = SparkExecutor(FakeSpark(), job, {}, {}, config={}, limits={})
    executor.params = {}
    executor._run_steps(None, [])
    assert calls == ["persist", "checkpoint"]


def test_event_outputs_are_capped_by_runtime_limits_events(ledger_api):
    output = {"name": "announce", "sql": "SELECT key FROM rows"}
    executor = SparkExecutor(RowsSpark(), {"name": "j", "steps": [], "outputs": []}, {}, {}, config={},
                             limits={"events": 2, "reference_values": 5})
    executor.params = {}
    with pytest.raises(ValueError, match=r"more than 2 rows \(runtime\.limits\.events\)"):
        executor._publish(None, [output])
    executor.limits = {"events": 3, "reference_values": 1}
    assert len(executor._publish(None, [output])["announce"]) == 3
