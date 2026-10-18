"""SQL text helpers shared by providers: quoted identifiers and `where` conditions (the connectors' {column, op, value})."""

import re

from streamwright.spark.runtime.refs import sql_literal


__all__ = ["OPERATORS", "IDENT", "quote_identifier", "where_clause"]

OPERATORS = ("=", "!=", "<", "<=", ">", ">=", "IN", "NOT IN", "IS NULL", "IS NOT NULL")   # as in the postgres connector
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_ ]*(\([0-9 ,]+\))?(\[\])?$")


def quote_identifier(name, quote='"', label="identifier", schema_qualified=True):
    """A plain or schema-qualified identifier, quoted; anything unusual is rejected, not escaped."""
    parts = str(name).split(".")
    limit = 2 if schema_qualified else 1
    if not 1 <= len(parts) <= limit or not all(IDENT.match(p) for p in parts):
        raise ValueError("invalid %s %r" % (label, name))
    return ".".join("%s%s%s" % (quote, p, quote) for p in parts)


def where_clause(where, quote='"'):
    """` WHERE a AND b` from a list of {column, op, value, type} conditions; values are escaped literals."""
    conditions = []
    for item in where or []:
        column, op = quote_identifier(item["column"], quote, "column", False), str(item.get("op", "=")).upper()
        if op not in OPERATORS:
            raise ValueError("unknown operator %r" % op)
        if op in ("IS NULL", "IS NOT NULL"):
            conditions.append("%s %s" % (column, op))
        elif op in ("IN", "NOT IN"):
            conditions.append("%s %s (%s)" % (column, op, ", ".join(sql_literal(v) for v in item["value"]) or "NULL"))
        else:
            literal = sql_literal(item["value"])
            if item.get("type"):
                if not _TYPE.match(str(item["type"])):
                    raise ValueError("invalid type %r" % item["type"])
                literal = "CAST(%s AS %s)" % (literal, item["type"])
            conditions.append("%s %s %s" % (column, op, literal))
    return " WHERE " + " AND ".join(conditions) if conditions else ""
