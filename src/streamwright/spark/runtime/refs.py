"""
`{{ step.column }}` references inside Spark SQL fragments (Delta predicates, replace_where). PROPOSED.

Values come from data, so they are rendered as escaped SQL *literals*, never spliced as text: strings are quoted with
quotes and backslashes escaped, numbers must be finite, dates and timestamps become typed literals. A reference
collects the distinct values of one column (one Spark action, listed by `--analyze`) and is capped by
runtime.limits.reference_values; an empty list renders as NULL so `x IN ({{ ... }})` matches nothing.
"""

import datetime
import math
import re

from streamwright.spark.job.resolve import references


__all__ = ["RefError", "sql_literal", "sql_list", "substitute", "step_columns_used"]

_REF = re.compile(r"^([a-z][a-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$")


class RefError(ValueError):
    pass


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RefError("a non-finite number cannot be used in a predicate")
        return repr(value)
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP '%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, datetime.date):
        return "DATE '%s'" % value.isoformat()
    if isinstance(value, str):
        if "\x00" in value:
            raise RefError("a value contains a NUL character")
        return "'%s'" % value.replace("\\", "\\\\").replace("'", "''")
    if hasattr(value, "__str__") and type(value).__name__ == "Decimal":
        return str(value)
    raise RefError("cannot use a %s value in a predicate" % type(value).__name__)


def sql_list(values, limit):
    values = list(values)
    if len(values) > limit:
        raise RefError("%d values exceed runtime.limits.reference_values (%d); use a coarser column" % (len(values), limit))
    return ", ".join(sql_literal(v) for v in values) if values else "NULL"


def encode_value(value):
    """JSON-safe, type-tagged form for the manifest (so a replay uses the exact same typed values)."""
    if isinstance(value, datetime.datetime):
        return {"$ts": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"$date": value.isoformat()}
    if type(value).__name__ == "Decimal":
        return {"$dec": str(value)}
    if isinstance(value, (bytes, bytearray)):
        raise RefError("binary values cannot be used in references")
    return value


def decode_value(value):
    if isinstance(value, dict) and len(value) == 1:
        (tag, text), = value.items()
        if tag == "$ts":
            return datetime.datetime.fromisoformat(text)
        if tag == "$date":
            return datetime.date.fromisoformat(text)
        if tag == "$dec":
            import decimal
            return decimal.Decimal(text)
    return value


def step_columns_used(text):
    """[(step, column)] for the {{ step.column }} references in a fragment."""
    return [m.groups() for m in (_REF.match(expr) for expr in references(text)) if m]


def substitute(text, values, batch_id, batch_ts, limit):
    """
    Replaces {{ step.column }} (from `values[(step, column)]`), {{ batch.id }} and {{ batch.ts }}. Anything else is an
    error: a predicate must not silently keep braces.
    """
    def one(match):
        expr = match.group(1).strip()
        if expr == "batch.id":
            return sql_literal(batch_id)
        if expr == "batch.ts":
            return sql_literal(batch_ts)
        hit = _REF.match(expr)
        if hit and (hit.group(1), hit.group(2)) in values:
            return sql_list(values[(hit.group(1), hit.group(2))], limit)
        raise RefError("cannot resolve {{ %s }} in %r" % (expr, text[:80]))
    return re.sub(r"\{\{\s*([^{}]*?)\s*\}\}", one, text)
