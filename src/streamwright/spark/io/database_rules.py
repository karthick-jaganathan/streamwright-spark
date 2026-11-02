"""
What the `spark` engine supports of the database and table operations (`streamwright.core.sdk.operations.databases`
and `.tables`), declared once as its EngineCapabilities, with the semantic checks of their arguments (the core
engines' plus Spark's own). Importable without pyspark.
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from streamwright.core.io.database_rules import (
    QUERY_NAME,
    TABLE_NAME,
    TABLE_READ_NAME,
    TABLE_WRITE_NAME,
    WRITE_NAME,
    check_database_write,
    check_query_call,
    check_table_call,
    check_table_read,
    check_table_write,
)
from streamwright.core.jobs.refs import MOUSTACHE, references
from streamwright.core.sdk.contract import EngineCapabilities, OperationKind
from streamwright.spark.io.sql import IDENTIFIER

__all__ = ["CAPABILITIES", "STAGING_TOKENS", "WRITE_MODES", "check_jdbc_write"]

WRITE_MODES: Final = ("append", "merge", "delete")
# what a `staging_table` name may use: a fresh token, the output's name, the batch
STAGING_TOKENS: Final = ("uuid", "uuid8", "uuid16", "output.name", "batch.id_safe", "batch.seq")


def _name_problem(key: str, value: Any) -> str | None:
    """Why `table` or `staging_table` is no `table` or `schema.table` (its staging tokens stand for letters)."""
    for token in references(value):
        if token not in STAGING_TOKENS or key != "staging_table":
            return "`%s` %r: {{ %s }} is not one of %s" % (key, value, token, ", ".join(STAGING_TOKENS))
    parts = MOUSTACHE.sub("x", str(value)).split(".")
    if not 1 <= len(parts) <= 2 or not all(IDENTIFIER.match(part) for part in parts):
        return "`%s` %r: expected table or schema.table (letters, digits and _)" % (key, value)
    return None


def check_jdbc_write(arguments: Mapping[str, Any]) -> list[str]:
    """The core write checks, then the table and staging table names as JDBC writes take them."""
    problems = check_database_write(arguments)
    return problems + [
        problem
        for key in ("table", "staging_table")
        if arguments.get(key) is not None
        for problem in [_name_problem(key, arguments[key])]
        if problem
    ]


CAPABILITIES: Final = EngineCapabilities(
    engine="spark",
    operation_kinds=frozenset({OperationKind.READ, OperationKind.WRITE}),
    arguments=MappingProxyType(
        {
            QUERY_NAME: dict.fromkeys(("query", "params", "options")),
            TABLE_NAME: dict.fromkeys(("table", "schema", "columns", "where", "options")),
            WRITE_NAME: dict(
                dict.fromkeys(("table", "primary_key", "update", "staging_table", "options")), mode=WRITE_MODES
            ),
            TABLE_READ_NAME: dict.fromkeys(("path", "format", "columns", "where", "limit", "version", "options")),
        }
    ),  # table.write: every argument
    checks=MappingProxyType(
        {
            QUERY_NAME: check_query_call,
            TABLE_NAME: check_table_call,
            WRITE_NAME: check_jdbc_write,
            TABLE_READ_NAME: check_table_read,
            TABLE_WRITE_NAME: check_table_write,
        }
    ),
)
