"""SparkJob source-connector call steps (contract v1.2 §6a engine reach) and execution policy, with a fake source."""

import dataclasses
import json

import pytest

from streamwright.core.jobs import Runner, WriteReceipt, calls
from streamwright.core.jobs.policy import TransientError
from streamwright.core.jobs.state import open_sqlite
from streamwright.core.runtime import components
from streamwright.core.sdk.contract import ConnectorDefinition, Field, Operation, OperationKind
from streamwright.spark.cli.common import connections_of
from streamwright.spark.contracts import Connection
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
    def parallelize(self, lines, slices):
        return list(lines)


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
        "steps": [{"name": "rows", "connection": "src", "service": "data", "method": "list",
                   "arguments": {"query": "SELECT 1"}, "retry": {"max_attempts": 3}, "rate_limit": {"requests": 2,
                   "per": "1s"}, "on_error": "skip", "timeout": "30s", "page_size": 1}],
        "outputs": [{"name": "out", "connection": "lake", "service": "object", "method": "write",
                     "arguments": {"path": "out/", "format": "parquet", "mode": "overwrite"},
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
    executor = SparkExecutor(FakeSpark(), {"name": "ads", "steps": [], "outputs": []}, connections, {},
                             config={}, limits={}, declared_connections=_declared(),
                             secrets={"tok": "secret-value"})
    frame = executor._step(None, _job()["steps"][0])
    assert [json.loads(line) for line in frame.lines] == list(RECORDS)


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
    assert Runner("w", store, executor, engine_metadata={"driver_host": "d"}).run() == 0
    assert adapter.attempts == 2
    assert store.get_batch("w", "w-00000001")["status"] == "completed"



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
    return {"name": "ad_groups", "connection": connection, "service": "data", "method": "list",
            "arguments": {"query": "{{ partition.campaign_id }}"},
            "partitions": [{"from": "campaigns", "field": "id", "name": "campaign_id"}]}


def test_a_call_step_runs_once_per_partition_value_of_an_earlier_step(fakeecho):
    declared = {"echo": {"provider": "fakeecho", "token": "{{ secrets.tok }}"}}
    executor = SparkExecutor(FakeSpark(), {"name": "ads", "steps": [], "outputs": []}, {}, {}, config={},
                             limits={}, declared_connections=declared, secrets={"tok": "secret-value"})
    frame = executor._step(FakeRuntime(), _partitioned_step("echo"))
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
    assert 'pip install "streamwright-fakesrc[spark]"' in str(caught.value)


def test_validation_reports_an_adapter_that_cannot_load_even_for_a_source_connector(fakesrc, monkeypatch):
    def load(self, name, kind="adapter", allow=None):
        raise registry.SparkAdapterLoadError("connector %r: its Spark adapter cannot be loaded" % name)
    monkeypatch.setattr(registry.Registry, "load", load)
    report = validate(_job())
    assert not report.valid and any("cannot be loaded" in str(issue) for issue in report.issues)
