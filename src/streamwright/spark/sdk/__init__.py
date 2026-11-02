"""The Spark engine contract for declarative connectors."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from streamwright.core.jobs import Message, WriteReceipt
from streamwright.core.jobs.types import Observation
from streamwright.core.sdk.contract import Operation, ResolvedConnection

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from streamwright.spark.engine.runtime import StepRuntime

__all__ = ["Observation", "SparkAdapter", "SparkContext", "SparkMessagingAdapter", "WriteReceipt"]


@dataclass(frozen=True)
class SparkContext:
    """The batch runtime and the name of the step or output being executed."""

    runtime: "StepRuntime"
    name: str
    connection: ResolvedConnection

    @property
    def spark(self) -> "SparkSession":
        return self.runtime.spark


class SparkAdapter(Protocol):
    def hadoop_settings(self, name: str, connection: ResolvedConnection) -> Mapping[str, str]: ...

    def read(self, context: SparkContext, operation: Operation, arguments: Mapping[str, Any]) -> "DataFrame": ...

    def write(
        self, context: SparkContext, frame: "DataFrame", operation: Operation, arguments: Mapping[str, Any]
    ) -> WriteReceipt: ...


class SparkMessagingAdapter(Protocol):
    def poll(
        self,
        arguments: Mapping[str, Any],
        limit: int,
        max_wait_seconds: float,
        watermarks: Mapping[str, Any] | None = None,
    ) -> list[Message]: ...

    def ack(self, messages: Sequence[Message], arguments: Mapping[str, Any]) -> None: ...

    def publish(self, arguments: Mapping[str, Any], events: Sequence[dict[str, Any]], dedupe_key: str) -> None: ...

    def destination(self, arguments: Mapping[str, Any]) -> str: ...
