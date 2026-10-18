"""
The Jdbc kind: the generic Spark side of JDBC connectors (postgres, mysql, ...). PROPOSED.

This base implements only what is identical across JDBC providers: `database.query` and `database.table` reads over
`spark.read.jdbc`, `database.append` over `df.write.jdbc`, and the SQL-building helpers (`$name` binding, the `table`
SELECT, identifier quoting). The JDBC url template, driver, and any dialect writes (`upsert`, `delete`) are declared by
the connector's adapter subclass (e.g. `streamwright.connectors.postgres.spark:PostgresSpark`), so nothing
provider-specific lives here. NOT RUN against a database yet.
"""

import re

from streamwright.spark.contracts import Kind, Method, Observation, WriteReceipt, arg
from streamwright.spark.kinds._sql import IDENT, OPERATORS, quote_identifier, where_clause
from streamwright.spark.kinds.base import Provider
from streamwright.spark.runtime.refs import sql_literal


__all__ = ["Jdbc", "QUERY", "TABLE", "APPEND", "identifier", "bind_params", "table_sql", "OPERATORS"]

_PARAM = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|--[^\n]*|\$([A-Za-z_][A-Za-z0-9_]*)")

# the generic method specs a JDBC connector reuses (it adds its own upsert/delete to these)
QUERY = Method("database", "query", Kind.READ,
               {"query": arg("string", True), "params": arg("object"),
                "options": arg("object", description="Vendor JDBC read options (fetchsize, queryTimeout, etc.)")},
               "read_query", passthrough=("query", "params"), idempotent=True)
TABLE = Method("database", "table", Kind.READ,
               {"schema": arg("string"), "table": arg("string", True), "columns": arg("list"), "where": arg("list"),
                "options": arg("object", description="Vendor JDBC read options (fetchsize, queryTimeout, etc.)")},
               "read_table", passthrough=("schema", "table", "columns", "where"), idempotent=True)
APPEND = Method("database", "append", Kind.WRITE,
                {"table": arg("string", True),
                 "options": arg("object", description="Vendor JDBC write options (batchsize, isolationLevel, etc.)")},
                "write_append", check="check_write")


def identifier(name, label="identifier"):
    return quote_identifier(name, '"', label)


def bind_params(query, params):
    """`$name` placeholders (outside quotes and comments) replaced by escaped literals; unknown or unused names fail."""
    params, used = dict(params or {}), set()

    def one(match):
        name = match.group(1)
        if name is None:
            return match.group(0)
        if name not in params:
            raise ValueError("the query uses $%s but params has no %r" % (name, name))
        used.add(name)
        value = params[name]
        if isinstance(value, (list, tuple)):
            return "(%s)" % ", ".join(sql_literal(v) for v in value)
        return sql_literal(value)
    bound = _PARAM.sub(one, query)
    unused = sorted(set(params) - used)
    if unused:
        raise ValueError("params not used by the query: %s" % ", ".join(unused))
    return bound


def table_sql(arguments):
    """The SELECT for `database.table`: validated identifiers, the connector's operators, literal values."""
    columns = arguments.get("columns")
    select = ", ".join(identifier(c, "column") for c in columns) if columns else "*"
    table = identifier("%s.%s" % (arguments.get("schema") or "public", arguments["table"]), "table")
    return "SELECT %s FROM %s%s" % (select, table, where_clause(arguments.get("where"), '"'))


class Jdbc(Provider):
    url = None               # a template filled from auth, e.g. "jdbc:postgresql://{host}:{port}/{database}"
    driver = None
    default_port = None
    jar_specs = ()
    methods = (QUERY, TABLE, APPEND)         # a connector adds its dialect writes (upsert, delete)

    def check_write(self, arguments):
        problems = []
        for key in ("table", "staging_table"):
            if key in arguments:
                try:
                    identifier(arguments[key], key)
                except ValueError as exc:
                    problems.append(str(exc))
        for name in list(arguments.get("key") or []) + list(arguments.get("update") or []):
            if not isinstance(name, str) or not IDENT.match(name):
                problems.append("invalid column name %r" % (name,))
        return problems

    def jars(self, spark_minor):
        return [tuple(spec) for spec in self.jar_specs]

    # --- generic JDBC read/write ------------------------------------------------------------------------------

    def jdbc(self, auth):
        values = dict(auth, port=auth.get("port", self.default_port))
        props = {"user": auth["user"], "password": auth["password"], "driver": self.driver,
                 "sslmode": str(auth.get("sslmode", "require"))}
        return self.url.format(**values), props

    def read_query(self, rt, conn, step, arguments):
        url, props = self.jdbc(conn.auth)
        for k, v in (arguments.get("options") or {}).items():
            props[str(k)] = str(v)
        return rt.spark.read.jdbc(url, "(%s) AS sw_q" % bind_params(arguments["query"], arguments.get("params")),
                                  properties=props)

    def read_table(self, rt, conn, step, arguments):
        url, props = self.jdbc(conn.auth)
        for k, v in (arguments.get("options") or {}).items():
            props[str(k)] = str(v)
        return rt.spark.read.jdbc(url, "(%s) AS sw_q" % table_sql(arguments), properties=props)

    def write_append(self, rt, conn, output, method, arguments, frame):
        from pyspark.sql import Observation as SparkObservation
        from pyspark.sql import functions as F
        url, props = self.jdbc(conn.auth)
        for k, v in (arguments.get("options") or {}).items():
            props[str(k)] = str(v)
        identifier(arguments["table"], "table")
        metrics = SparkObservation("jdbc_" + output)
        frame.observe(metrics, F.count(F.lit(1)).alias("rows")).write.jdbc(
            url, arguments["table"], mode="append", properties=props)
        rows = int(metrics.get.get("rows", 0))
        return WriteReceipt(output, arguments["table"], rows, observations=[Observation(output, {"rows": rows})])
