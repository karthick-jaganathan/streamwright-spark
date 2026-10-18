"""Opening the bookkeeping store: sqlite (tests, platform: local) or postgres (psycopg, imported lazily). PROPOSED."""

import sqlite3

from streamwright.spark.state.dialects import PostgresDialect, SqliteDialect
from streamwright.spark.state.store import SqlStateStore, StateError


__all__ = ["open_sqlite", "open_dialect"]


def open_sqlite(path):
    return SqlStateStore(lambda: sqlite3.connect(path, timeout=30), SqliteDialect())


def open_dialect(connect, dialect, schema=None):
    """The store over any DB-API connect callable; dialect: "sqlite" or "postgres" (what an adapter's
    state_connection returns)."""
    dialects = {"sqlite": SqliteDialect, "postgres": PostgresDialect}
    if dialect not in dialects:
        raise StateError("unknown state dialect %r" % dialect)
    return SqlStateStore(connect, dialects[dialect](), schema=schema)
