"""Spark messaging capabilities and broker lifecycle, shared by every connector."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from streamwright.core.sdk.contract import EngineCapabilities, OperationKind
from streamwright.core.sdk.messaging import check_poll, poll_limit, poll_wait_seconds

__all__ = ["CAPABILITIES", "PollSettings", "check_poll", "open_broker"]


@dataclass(frozen=True)
class PollSettings:
    """The broker's declared poll policy; the runner never inspects vendor arguments."""

    max_messages: int
    max_wait_seconds: float

    @classmethod
    def from_arguments(cls, arguments, maximum, max_wait):
        return cls(poll_limit(arguments, maximum), poll_wait_seconds(arguments, max_wait))


def open_broker(adapter, connection):
    """Open a definition's broker without loading a legacy provider or storing credentials in Spark."""
    return adapter.adapter.open_broker(connection)


CAPABILITIES: Final = EngineCapabilities(
    engine="spark",
    operation_kinds=frozenset({OperationKind.CONSUME, OperationKind.PUBLISH}),
    arguments=MappingProxyType(
        {
            "message.consume": dict.fromkeys(("destination", "max_messages", "max_wait")),
            "message.publish": dict.fromkeys(("destination",)),
        }
    ),
    checks=MappingProxyType({"message.consume": check_poll}),
)
