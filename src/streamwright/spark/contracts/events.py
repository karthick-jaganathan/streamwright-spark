"""Broker protocols: what the batch loop needs from Kafka, SQS or any other message source. PROPOSED."""

from typing import Any, Iterable, List, Mapping, Protocol, runtime_checkable

from streamwright.spark.contracts.types import Message


__all__ = ["Broker"]


@runtime_checkable
class Broker(Protocol):
    def poll(self, arguments: Mapping[str, Any], limit: int, max_wait_seconds: float) -> List[Message]:
        """Never commits on its own."""

    def ack(self, messages: Iterable[Message]) -> None:
        """Called only after the manifest and the outbox are durable."""

    def publish(self, arguments: Mapping[str, Any], events: Iterable[Mapping[str, Any]], dedupe_key: str) -> None:
        """At-least-once; `dedupe_key` is stable across replays so consumers can drop a replayed event."""
