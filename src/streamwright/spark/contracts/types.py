"""Spark-specific connection and lazily imported Spark types."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from streamwright.core.jobs.types import CONTRACT_VERSION, BatchContext, Lease, Message, Observation, WriteReceipt

__all__ = [
    "CONTRACT_VERSION",
    "BatchContext",
    "Connection",
    "DataFrame",
    "Lease",
    "Message",
    "Observation",
    "SparkSession",
    "WriteReceipt",
]

DataFrame = Any
SparkSession = Any


@dataclass(frozen=True)
class Connection:
    name: str
    provider: str
    auth: Mapping[str, Any]
