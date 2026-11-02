"""Broker hook contract: destination and polling policy stay outside the engine."""

from types import SimpleNamespace

from streamwright.connectors.kafka import CONNECTOR
from streamwright.core.jobs import BrokerCall, ExecutionResult, Message, Runner
from streamwright.core.jobs.state import open_sqlite
from streamwright.core.jobs.types import Broker as BrokerClient
from streamwright.spark.cli.run import _broker_calls
from streamwright.spark.runtime.definition import DefinedAdapter


class Client(BrokerClient):
    def __init__(self):
        self.polled, self.published, self.acked = [], [], []

    def poll_limit(self, arguments, maximum):
        return min(maximum, arguments["page_size"])

    def poll_wait_seconds(self, arguments, maximum):
        return min(maximum, arguments["wait"])

    def poll(self, arguments, limit, max_wait_seconds, watermarks=None):
        self.polled.append((limit, max_wait_seconds))
        return [Message("message", None, "{}", {}, None)] if len(self.polled) == 1 else []

    def ack(self, messages, arguments):
        self.acked.extend(message.message_id for message in messages)

    def publish(self, arguments, events, dedupe_key):
        self.published.append((arguments, events, dedupe_key))

    def destination(self, arguments):
        return arguments.get("address") or arguments["destination"]


class EventExecutor:
    def execute(self, context, manifest, done, hooks):
        hooks.save_manifest(manifest)
        return ExecutionResult(events={"published": [{"id": context.batch_id}]})


def test_core_runner_uses_adapter_policy_and_destination(tmp_path):
    store = open_sqlite(str(tmp_path / "events.db"))
    store.initialize()
    client = Client()
    consumer = BrokerCall(client, {"page_size": 4, "wait": 2})
    publisher = BrokerCall(client, {"address": "destination"})
    assert client.destination(publisher.arguments) == "destination"
    runner = Runner("events", store, EventExecutor(), consumer=consumer, publishers={"published": publisher})
    assert runner.run(max_messages=50, max_wait=20) == 0
    assert client.polled == [(4, 2)]
    assert client.acked == ["message"]
    assert client.published[0][0]["address"] == "destination"


def test_spark_opens_both_calls_from_one_connector_definition():
    client = Client()

    class Adapter:
        def open_broker(self, connection):
            assert connection["bootstrap_servers"] == "broker:9092"
            return client

    job = {
        "connections": {"bus": {"provider": "kafka"}},
        "steps": [
            {
                "connection": "bus",
                "service": "message",
                "method": "consume",
                "arguments": {"destination": "arrivals", "group_id": "workers"},
            }
        ],
        "outputs": [
            {
                "name": "notice",
                "connection": "bus",
                "service": "message",
                "method": "publish",
                "arguments": {"destination": "commits"},
            }
        ],
    }
    consumer, publishers = _broker_calls(
        job,
        {"bus": SimpleNamespace(provider="kafka", auth={"bootstrap_servers": "broker:9092"})},
        {"kafka": DefinedAdapter(CONNECTOR, Adapter())},
    )
    assert consumer.broker is client
    assert publishers["notice"].broker is client
    assert publishers["notice"].broker.destination(publishers["notice"].arguments) == "commits"
