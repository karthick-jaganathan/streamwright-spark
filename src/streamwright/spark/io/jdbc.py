"""
Relational databases for the `spark` engine over JDBC, composed by a connector's Spark adapter (the counterpart of
`streamwright.core.io.databases`): functions over a frozen JdbcDialect (the database's facts, from the connector's
client) and a JdbcTarget (the URL and properties of one resolved connection).

- reads: `read_query` (`database.query`: `$name` parameters rendered as escaped literals; the database runs it) and
  `read_table` (`database.table`), both through `spark.read.jdbc` on the executors.
- writes: `write_table` (`database.write`): `append` straight from the executors; `merge` and `delete` in two phases -
  the executors bulk-load a staging table (a temporary one the run creates and drops, or the output's
  `staging_table`, truncated), then the driver runs the dialect's set-based statement in one transaction.

The host, database and port are checked after the connection is resolved (jdbc_target); credentials travel only in
the JDBC properties, never in the URL or Spark's configuration. What Spark supports of these operations:
CAPABILITIES (`database_rules`, importable without pyspark).
"""

import contextlib
import ipaddress
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from pyspark.sql import DataFrame, functions
from pyspark.sql import Observation as SparkObservation

from streamwright.core.jobs.refs import sql_literal
from streamwright.core.jobs.types import Observation
from streamwright.core.sdk.contract import Operation, vendor_arguments
from streamwright.core.sql.quote import quote_literal
from streamwright.spark.io.database_rules import CAPABILITIES, STAGING_TOKENS
from streamwright.spark.io.sql import quote_identifier, where_clause
from streamwright.spark.sdk import SparkContext, WriteReceipt

__all__ = [
    "CAPABILITIES",
    "STAGING_TOKENS",
    "JdbcDialect",
    "JdbcTarget",
    "bind_params",
    "driver_options",
    "jdbc_target",
    "read_query",
    "read_table",
    "table_sql",
    "write_table",
]

STAGING_PREFIX: Final = "_sw_stg_"
_PARAMETER: Final = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|--[^\n]*|\$([A-Za-z_][A-Za-z0-9_]*)")
_HOST: Final = re.compile(r"(?:[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?|\[[0-9A-Fa-f:.]+\])")
_DATABASE: Final = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")


@dataclass(frozen=True)
class JdbcDialect:
    """
    What differs between JDBC databases, stated once by a connector's client.
      url_template        e.g. "jdbc:postgresql://{host}:{port}/{database}"
      staging_statement   (target, staging) -> the CREATE of an empty staging table like the target
      merge_statement     (target, staging, columns, key, update) -> the INSERT ... ON CONFLICT (or the dialect's)
      delete_statement    (target, staging, key) -> the DELETE of the target rows matching the staging rows
      (statements receive quoted names)
      literal             a string literal in the database's SQL (`$name` parameters, `where` values)
      commit_before_cleanup  DROP and TRUNCATE commit on their own (MySQL): commit the statement before them
    """

    name: str
    url_template: str
    driver: str
    default_port: int
    staging_statement: Callable[[str, str], str]
    merge_statement: Callable[[str, str, Sequence[str], Sequence[str], Sequence[str]], str]
    delete_statement: Callable[[str, str, Sequence[str]], str]
    quote: str = '"'
    default_schema: str | None = "public"
    identifier_limit: int = 128
    literal: Callable[[str], str] = quote_literal
    commit_before_cleanup: bool = False

    def quote_name(self, name: str, label: str = "identifier", *, schema_qualified: bool = True) -> str:
        return quote_identifier(
            name, self.quote, label, schema_qualified=schema_qualified, max_length=self.identifier_limit
        )


@dataclass(frozen=True)
class JdbcTarget:
    """The JDBC URL (no credential) and connection properties (user, password, driver, options) of a connection."""

    url: str
    properties: Mapping[str, str] = field(repr=False)

    def with_options(self, options: Mapping[str, Any] | None) -> dict[str, str]:
        """The properties with an operation's driver `options` (fetchsize, batchsize, ...)."""
        return dict(self.properties, **{str(key): str(value) for key, value in (options or {}).items()})


def _host_problem(host: str) -> str | None:
    if not _HOST.fullmatch(host):
        return "invalid JDBC host"
    address = host[1:-1] if host.startswith("[") else host if all(c.isdigit() or c == "." for c in host) else None
    if address is not None:
        try:
            ipaddress.ip_address(address)
        except ValueError:
            return "invalid JDBC host"
    elif any(not label or len(label) > 63 or label.startswith("-") or label.endswith("-") for label in host.split(".")):
        return "invalid JDBC host"
    return None


def jdbc_target(
    dialect: JdbcDialect,
    *,
    host: str,
    database: str,
    user: str,
    password: str | None,
    port: Any = None,
    properties: Mapping[str, str] | None = None,
) -> JdbcTarget:
    """The JdbcTarget of resolved values; ValueError for a host, database or port a JDBC URL cannot take."""
    host, database, port = str(host), str(database), dialect.default_port if port in (None, "") else port
    problem = _host_problem(host)
    if problem:
        raise ValueError(problem)
    if not _DATABASE.fullmatch(database):
        raise ValueError("invalid JDBC database")
    if not str(port).isdigit() or not 1 <= int(port) <= 65535:
        raise ValueError("invalid JDBC port")
    values = {"user": str(user), "driver": dialect.driver, **(properties or {})}
    if password is not None:
        values["password"] = str(password)
    return JdbcTarget(dialect.url_template.format(host=host, port=int(port), database=database), values)


def bind_params(query: str, params: Mapping[str, Any] | None, literal: Callable[[str], str] = quote_literal) -> str:
    """`$name` placeholders (outside quotes and comments) replaced by escaped literals; unknown or unused names fail."""
    params, used = dict(params or {}), set()

    def bind(match: re.Match) -> str:
        name = match.group(1)
        if name is None:
            return match.group(0)
        if name not in params:
            raise ValueError("the query uses $%s but params has no %r" % (name, name))
        used.add(name)
        value = params[name]
        if isinstance(value, (list, tuple)):
            return "(%s)" % ", ".join(sql_literal(item, quote_text=literal) for item in value)
        return sql_literal(value, quote_text=literal)

    bound = _PARAMETER.sub(bind, query)
    unused = sorted(set(params) - used)
    if unused:
        raise ValueError("params not used by the query: %s" % ", ".join(unused))
    return bound


def table_sql(dialect: JdbcDialect, arguments: Mapping[str, Any]) -> str:
    """The SELECT of a `database.table` read: validated identifiers, the shared operators, literal values."""
    columns = arguments.get("columns")
    select = (
        ", ".join(dialect.quote_name(column, "column", schema_qualified=False) for column in columns)
        if columns
        else "*"
    )
    schema = arguments.get("schema") or dialect.default_schema
    table = dialect.quote_name("%s.%s" % (schema, arguments["table"]) if schema else arguments["table"], "table")
    return "SELECT %s FROM %s%s" % (
        select,
        table,
        where_clause(arguments.get("where"), dialect.quote, literal=dialect.literal),
    )


def driver_options(operation: Operation | None, arguments: Mapping[str, Any]) -> dict[str, Any]:
    """The JDBC driver options of a read or write: the operation's undeclared arguments (vendor options, e.g.
    fetchsize, queryTimeout, batchsize, sessionInitStatement), then the explicit `options` where they collide."""
    vendor = vendor_arguments(operation, arguments) if operation is not None else {}
    return {**vendor, **(arguments.get("options") or {})}


def read_query(
    context: SparkContext, arguments: Mapping[str, Any], *, dialect: JdbcDialect, target: JdbcTarget,
    operation: Operation | None = None
) -> DataFrame:
    query = bind_params(arguments["query"], arguments.get("params"), dialect.literal)
    return context.spark.read.jdbc(
        target.url, "(%s) AS sw_q" % query, properties=target.with_options(driver_options(operation, arguments))
    )


def read_table(
    context: SparkContext, arguments: Mapping[str, Any], *, dialect: JdbcDialect, target: JdbcTarget,
    operation: Operation | None = None
) -> DataFrame:
    return context.spark.read.jdbc(
        target.url,
        "(%s) AS sw_q" % table_sql(dialect, arguments),
        properties=target.with_options(driver_options(operation, arguments)),
    )


def _observed(frame: DataFrame, name: str) -> tuple[DataFrame, SparkObservation]:
    metrics = SparkObservation("jdbc_" + name)
    return frame.observe(metrics, functions.count(functions.lit(1)).alias("rows")), metrics


def _receipt(name: str, table: str, metrics: SparkObservation) -> WriteReceipt:
    rows = int(metrics.get.get("rows", 0))
    return WriteReceipt(name, table, rows, observations=[Observation(name, {"rows": rows})])


def _staging_name(dialect: JdbcDialect, context: SparkContext, table: str, given: str | None) -> str:
    """The output's staging table with its tokens rendered, or a fresh one in the target's schema."""
    token, output = uuid.uuid4().hex[:16], re.sub(r"[^A-Za-z0-9_]", "_", context.name)
    batch = context.runtime.ctx
    if given:
        return context.runtime.text(
            given, extra={"uuid": token, "uuid16": token, "uuid8": token[:8], "output.name": output}
        )
    schema = table.split(".", maxsplit=1)[0] + "." if "." in table else ""
    base = "%s%s_%s_%s" % (STAGING_PREFIX, output[:16].strip("_") or "out", batch.batch_sequence, token)
    return schema + base[: dialect.identifier_limit]


def _execute(connect: Callable[[], Any], statements: Sequence[str], *, commit_each: bool = False) -> None:
    """Runs the statements in one driver transaction (rolled back on failure)."""
    connection = connect()
    try:
        cursor = connection.cursor()
        for statement in statements:
            cursor.execute(statement)
            if commit_each:
                connection.commit()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _staged_write(
    context: SparkContext,
    frame: DataFrame,
    arguments: Mapping[str, Any],
    *,
    dialect: JdbcDialect,
    target: JdbcTarget,
    connect: Callable[[], Any],
    options: Mapping[str, Any],
) -> WriteReceipt:
    table, key = arguments["table"], list(arguments["primary_key"])
    ephemeral = not arguments.get("staging_table")
    staging = _staging_name(dialect, context, table, arguments.get("staging_table"))
    quoted_table, quoted_staging = dialect.quote_name(table, "table"), dialect.quote_name(staging, "staging table")
    if arguments.get("mode") == "merge":
        update = [column for column in (arguments.get("update") or frame.columns) if column not in key]
        columns = [dialect.quote_name(column, "column", schema_qualified=False) for column in key + update]
        statement = dialect.merge_statement(
            quoted_table, quoted_staging, columns, columns[: len(key)], columns[len(key) :]
        )
    else:
        statement = dialect.delete_statement(
            quoted_table,
            quoted_staging,
            [dialect.quote_name(column, "key column", schema_qualified=False) for column in key],
        )
    if ephemeral:
        _execute(connect, [dialect.staging_statement(quoted_table, quoted_staging)])
    try:
        observed, metrics = _observed(frame, context.name)
        writer = observed.write if ephemeral else observed.write.option("truncate", "true")
        writer.jdbc(
            target.url,
            staging,
            mode="append" if ephemeral else "overwrite",
            properties=target.with_options(options),
        )
        cleanup = ("DROP TABLE %s" if ephemeral else "TRUNCATE TABLE %s") % quoted_staging
        _execute(connect, [statement, cleanup], commit_each=dialect.commit_before_cleanup)
    except Exception:
        if ephemeral:
            _drop_quietly(connect, quoted_staging)
        raise
    return _receipt(context.name, table, metrics)


def _drop_quietly(connect: Callable[[], Any], staging: str) -> None:
    with contextlib.suppress(Exception):  # the failure being raised is the one that matters
        _execute(connect, ["DROP TABLE IF EXISTS %s" % staging])


def write_table(
    context: SparkContext,
    frame: DataFrame,
    arguments: Mapping[str, Any],
    *,
    dialect: JdbcDialect,
    target: JdbcTarget,
    connect: Callable[[], Any],
    operation: Operation | None = None,
) -> WriteReceipt:
    """
    `database.write`: append from the executors; merge and delete through a staging table and one driver
    transaction (`connect` returns a DB-API connection of the database, opened on the driver).
    """
    options = driver_options(operation, arguments)
    if arguments.get("mode", "append") != "append":
        return _staged_write(context, frame, arguments, dialect=dialect, target=target, connect=connect,
                             options=options)
    table = arguments["table"]
    dialect.quote_name(table, "table")
    observed, metrics = _observed(frame, context.name)
    observed.write.jdbc(target.url, table, mode="append", properties=target.with_options(options))
    return _receipt(context.name, table, metrics)
