"""
The provider KINDS: the generic, provider-agnostic Spark machinery a connector's adapter builds on. PROPOSED.

A connector that can run on Spark ships an adapter in its own package (e.g. `streamwright.connectors.s3.spark:S3Spark`)
that subclasses one of these and states only its facts and dialect. The kinds are the single home of the generic
behaviour; nothing here names a provider, and streamwright-spark holds no per-provider code or catalog.
"""

from streamwright.spark.contracts import Kind, Method, Observation, Role, WriteReceipt, arg
from streamwright.spark.kinds._sql import IDENT, OPERATORS, quote_identifier, where_clause
from streamwright.spark.kinds.base import Provider
from streamwright.spark.kinds.broker import Broker, BrokerError
from streamwright.spark.kinds.delta import Delta, merge_condition
from streamwright.spark.kinds.filesystem import FORMATS, Filesystem
from streamwright.spark.kinds.jdbc import APPEND, QUERY, TABLE, Jdbc, bind_params, identifier, table_sql

__all__ = ["Provider", "Filesystem", "Jdbc", "Delta", "Broker", "BrokerError", "FORMATS", "merge_condition",
           "QUERY", "TABLE", "APPEND", "identifier", "bind_params", "table_sql", "quote_identifier", "where_clause",
           "IDENT", "OPERATORS", "Kind", "Method", "Role", "arg", "WriteReceipt", "Observation"]
