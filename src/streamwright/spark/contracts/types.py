"""
Plain data types shared by the core, the engine and the adapters. PROPOSED.

Spark is never imported here: a DataFrame is `Any`; adapters import pyspark lazily inside the methods that need it, so
`sw spark validate` works on a machine without Spark.
"""

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence


__all__ = ["CONTRACT_VERSION", "Kind", "Role", "Connection", "BatchContext", "Message", "Observation", "WriteReceipt",
           "Lease", "DataFrame", "SparkSession"]

CONTRACT_VERSION = "0.2"          # PROPOSED. 0.2: adapters over the existing connectors replace provider specs

DataFrame = Any                   # pyspark.sql.DataFrame, kept out of the import graph on purpose
SparkSession = Any


class Kind:
    READ = "read"                 # builds a DataFrame (a step)
    WRITE = "write"               # consumes a DataFrame, returns a WriteReceipt (an output)
    EVENT = "event"               # a broker call: consume (a step) or publish (an output)


class Role:
    """What the plan and the checks need to know about an event method, without matching provider or service names."""
    CONSUME = "consume"
    PUBLISH = "publish"


@dataclass(frozen=True)
class Connection:
    name: str
    provider: str
    auth: Mapping[str, Any]                          # resolved: no {{ }} left, secrets substituted


@dataclass(frozen=True)
class BatchContext:
    """Run constants, fixed in the manifest when the batch is first received; a replay reuses them."""
    batch_id: str
    batch_seq: int                                   # increases by one per batch of a job (Delta txnVersion)
    batch_ts: str                                    # ISO-8601 UTC, pinned in the manifest
    job: str
    spark: Optional[SparkSession]
    epoch: int                                       # lease epoch, for fencing


@dataclass(frozen=True)
class Message:
    message_id: str
    key: Optional[str]
    body: str
    headers: Mapping[str, str]
    timestamp: Optional[str]
    receipt: Any = None                              # opaque to the engine; the broker acks with it


@dataclass(frozen=True)
class Observation:
    """Metrics gathered inside the single write action (Dataset.observe, or the commit's own metrics)."""
    name: str
    values: Mapping[str, Any]


@dataclass(frozen=True)
class WriteReceipt:
    """What a write returns; stored in the manifest, and the only source for `_commits`."""
    output: str
    target: str                                      # table path, JDBC table or object prefix
    rows: Optional[int]
    version: Optional[int] = None                    # Delta commit version
    files: Sequence[str] = ()                        # final object names (split exports)
    predicate: Optional[str] = None                  # replace_where / merge target predicate that was applied
    observations: Sequence[Observation] = ()


@dataclass(frozen=True)
class Lease:
    job: str
    owner: str
    epoch: int
    expires_at: str
