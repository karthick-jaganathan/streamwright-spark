"""Spark adapter errors and launcher contracts; job types come from core."""

from streamwright.spark.contracts.adapter import AdapterError
from streamwright.spark.contracts.events import Broker
from streamwright.spark.contracts.launcher import LAUNCHER_OWNED_CONF, Launcher, LaunchError, LaunchFailureError
from streamwright.spark.contracts.state import StateStore
from streamwright.spark.contracts.types import (
    CONTRACT_VERSION,
    BatchContext,
    Connection,
    DataFrame,
    Lease,
    Message,
    Observation,
    SparkSession,
    WriteReceipt,
)

__all__ = [
    "CONTRACT_VERSION",
    "LAUNCHER_OWNED_CONF",
    "AdapterError",
    "BatchContext",
    "Broker",
    "Connection",
    "DataFrame",
    "LaunchError",
    "LaunchFailureError",
    "Launcher",
    "Lease",
    "Message",
    "Observation",
    "SparkSession",
    "StateStore",
    "WriteReceipt",
]
