"""
SQL text shared by the Spark io helpers (jdbc, delta) and the engine's predicates: Spark and JDBC string literals,
validated identifiers (anything unusual is rejected, not escaped) and `where` conditions rendered as literals.
"""

import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final

from streamwright.core.jobs.refs import sql_literal
from streamwright.core.sql.conditions import render_conditions

__all__ = ["IDENTIFIER", "quote_identifier", "quote_literal", "where_clause"]

IDENTIFIER: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUOTED_COLUMN: Final = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"')
_PARAMETER: Final = re.compile(r"\$(p_\d+(?:_\d+)?)\b")


def quote_literal(value: str) -> str:
    """A Spark SQL string literal: backslashes are escapes in Spark, so they are doubled as the quotes are."""
    if "\x00" in value:
        raise ValueError("an SQL literal cannot contain a NUL character")
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def quote_identifier(
    name: str,
    quote: str = '"',
    label: str = "identifier",
    *,
    schema_qualified: bool = True,
    max_length: int | None = None,
) -> str:
    """A plain or schema-qualified identifier, quoted; ValueError for anything but letters, digits and _."""
    parts = str(name).split(".")
    if not 1 <= len(parts) <= (2 if schema_qualified else 1) or not all(IDENTIFIER.match(part) for part in parts):
        raise ValueError("invalid %s %r" % (label, name))
    for part in parts:
        if max_length is not None and len(part) > max_length:
            raise ValueError(
                "%s identifier segment %r exceeds the maximum length of %d characters (%d chars)"
                % (label, part, max_length, len(part))
            )
    return ".".join("%s%s%s" % (quote, part, quote) for part in parts)


def where_clause(
    where: Sequence[Mapping[str, Any]] | None, quote: str = '"', literal: Callable[[str], str] = quote_literal
) -> str:
    """` WHERE a AND b` from a list of {column, op, value, type} conditions; values are escaped literals."""
    fragment, parameters = render_conditions(where, dialect="spark")
    fragment = _QUOTED_COLUMN.sub(
        lambda match: quote_identifier(match.group(1), quote, "column", schema_qualified=False), fragment
    )
    return _PARAMETER.sub(lambda match: sql_literal(parameters[match.group(1)], quote_text=literal), fragment)
