"""
The Delta kind: the Spark side of the `deltalake` connector. PROPOSED.

Service `tables`, as in the connector. `tables.scan` keeps the connector's arguments (`table_path`, `columns`, `where`,
`limit`) and adds `version` (time travel). `tables.query` is DuckDB SQL and is not offered: use a step's `sql`. The
writes are Spark-only: `tables.append`, `tables.overwrite` (always with `replace_where`, or an explicit `whole_table`)
and `tables.merge`.

Declarative Catalog Registration:
Writes can register tables into Hive Metastore or DeltaCatalog automatically via `table_name`, `schema`,
`schema_location`, `schema_comment`, `table_properties`, and `comment`. Connection `deltalake` can declare default
`schema`, `schema_location`, and `schema_comment`. Registration is cached in driver memory to avoid any overhead on
subsequent batches.

Credentials: Spark takes them from the object-store connection named by `storage` (s3, gcs, azure_blob, files), so a
job defines each credential once. The DuckDB-style `aws_*` keys of the connector are accepted but not used.

Predicates: `replace_where` and `target_predicate` may use `{{ step.column }}` references, rendered as escaped literals
from the pinned values. A merge predicate derived from the source rows lets Delta skip files AND keeps a source row
outside it from being inserted twice.
"""

from streamwright.spark.kinds._sql import quote_identifier, where_clause
from streamwright.spark.contracts import Kind, Method, Observation, WriteReceipt, arg
from streamwright.spark.kinds.base import Provider
from streamwright.spark.storage.paths import check_path, spark_text


__all__ = ["Delta", "merge_condition"]

_COMMON = {
    "table_path": arg("string", True),
    "table_name": arg("string", description="catalog table name, e.g. 'orders' or 'analytics.orders'"),
    "schema": arg("string", description="catalog schema / database name"),
    "schema_location": arg("string", description="storage location for schema / database"),
    "schema_comment": arg("string", description="schema-level comment / documentation"),
    "table_properties": arg("object", description="persistent Delta Lake TBLPROPERTIES"),
    "comment": arg("string", description="table-level comment / documentation"),
    "partition_by": arg("list"),
    "options": arg("object"),
}
_WHEN = ("update", "delete", "insert")


def merge_condition(condition, target_predicate):
    """The ON clause: the keys, AND the (source-derived) target predicate that lets Delta skip target files."""
    return "(%s) AND (%s)" % (target_predicate, condition) if target_predicate else condition


class Delta(Provider):
    extra_auth = {
        "storage": {"type": "string"},
        "schema": {"type": "string"},
        "schema_location": {"type": "string"},
        "schema_comment": {"type": "string"},
    }
    packages = ("delta-spark",)

    methods = (
        Method("tables", "scan", Kind.READ,
               {"table_path": arg("string", True), "columns": arg("list"), "where": arg("list"), "limit": arg("integer"),
                "version": arg("integer", description="time travel: read this table version"),
                "options": arg("object", description="Vendor Delta Lake read options (versionAsOf, timestampAsOf, readChangeFeed)")},
               "read_table", passthrough=("table_path", "columns", "where", "limit"), idempotent=True),
        Method("tables", "append", Kind.WRITE, dict(_COMMON), "write_append", idempotent=True),
        Method("tables", "overwrite", Kind.WRITE,
               dict(_COMMON, replaceWhere=arg("string", description="vendor Delta option: rewrite only matching rows"),
                    replace_where=arg("string", description="alias for replaceWhere"),
                    whole_table=arg("boolean", False, False, description="required when there is no replaceWhere")),
               "write_overwrite", check="check_overwrite", idempotent=True),
        Method("tables", "merge", Kind.WRITE,
               dict(_COMMON, condition=arg("string", True, description="uses target. and source."),
                    target_predicate=arg("string", description="prunes target files, e.g. target.d IN ({{ step.d }})"),
                    when_matched=arg("list"), when_not_matched=arg("list"), when_not_matched_by_source=arg("list")),
               "write_merge", check="check_merge", idempotent=True),
    )

    # --- checks ----------------------------------------------------------------------------------------------

    def check_links(self, auth, connections):
        target = connections.get(auth.get("storage"))
        if target is None:
            return ["storage must name the object-store connection that holds the tables (s3, gcs, azure_blob, files)"]
        if "roots" not in target:
            return ["storage %r has no roots: it must be an object-store connection" % auth["storage"]]
        return []

    def check_overwrite(self, arguments):
        pred = arguments.get("replaceWhere") or arguments.get("replace_where")
        if not pred and not arguments.get("whole_table"):
            return ["overwrite needs replaceWhere (or replace_where), or whole_table: true to rewrite the table"]
        if pred and arguments.get("whole_table"):
            return ["replaceWhere and whole_table are exclusive"]
        return []

    def check_merge(self, arguments):
        problems = []
        for clause in ("when_matched", "when_not_matched", "when_not_matched_by_source"):
            allowed = ("insert",) if clause == "when_not_matched" else ("update", "delete")
            for number, entry in enumerate(arguments.get(clause) or []):
                if not isinstance(entry, dict) or entry.get("action") not in allowed:
                    problems.append("%s[%d]: action must be one of %s" % (clause, number, "/".join(allowed)))
                elif "set" in entry and not isinstance(entry["set"], dict):
                    problems.append("%s[%d]: 'set' must be a mapping of target column to expression" % (clause, number))
        if not (arguments.get("when_matched") or arguments.get("when_not_matched")
                or arguments.get("when_not_matched_by_source")):
            problems.append("merge needs at least one when_* clause")
        return problems

    def warn_call(self, method, arguments):
        if method.name != "merge":
            return []
        predicate = arguments.get("target_predicate")
        if not predicate:
            return ["merge without target_predicate scans every target file; add a partition/cluster predicate"]
        if "{{" not in str(predicate):
            return ["target_predicate is not derived from the source rows; a source row outside it is inserted again "
                    "even if it exists. Use e.g. target.event_date IN ({{ step.event_date }})"]
        return []

    # --- reads -----------------------------------------------------------------------------------------------

    def read_table(self, rt, conn, step, arguments):
        from delta.tables import DeltaTable
        path = self._path(rt, conn, arguments["table_path"])
        pin = "%s.version" % step
        if pin not in rt.pinned:
            wanted = arguments.get("version")
            rt.pinned[pin] = int(wanted) if wanted is not None else int(
                DeltaTable.forPath(rt.spark, path).history(1).collect()[0]["version"])
        reader = rt.spark.read.format("delta").option("versionAsOf", rt.pinned[pin])
        for k, v in (arguments.get("options") or {}).items():
            reader = reader.option(str(k), str(v))
        frame = reader.load(path)
        if arguments.get("columns"):
            frame = frame.select(*[quote_identifier(c, "`", "column", False) for c in arguments["columns"]])
        clause = where_clause(arguments.get("where"), "`")
        if clause:
            frame = frame.where(clause[len(" WHERE "):])
        return frame.limit(int(arguments["limit"])) if arguments.get("limit") else frame

    def _path(self, rt, conn, raw):
        adapter, storage = rt.storage_of(conn)
        path = check_path(rt.text(raw), adapter.roots(storage.auth), "table_path")
        return adapter.to_hadoop(path, storage.auth)

    def _resolve_schema_location(self, rt, conn, raw):
        if not raw:
            return None
        adapter, storage = rt.storage_of(conn)
        path = check_path(rt.text(raw), adapter.roots(storage.auth), "schema_location")
        return adapter.to_hadoop(path, storage.auth)

    def _ensure_registered(self, rt, conn, arguments, frame, path):
        table_name = arguments.get("table_name")
        conn_auth = getattr(conn, "auth", {}) or {}
        schema = arguments.get("schema") or conn_auth.get("schema")
        schema_loc = arguments.get("schema_location") or conn_auth.get("schema_location")
        schema_comment = arguments.get("schema_comment") or conn_auth.get("schema_comment")
        table_props = arguments.get("table_properties") or {}
        comment = arguments.get("comment")

        if not table_name and not schema:
            return

        if not table_name and schema:
            table_name = path.rstrip("/").split("/")[-1]

        if "." in table_name:
            parts = table_name.split(".", 1)
            schema = parts[0]
            table_name = parts[1]

        cache = getattr(rt, "_registered_tables", None)
        if cache is None:
            cache = set()
            rt._registered_tables = cache
        cache_key = (schema or "default", table_name)
        if cache_key in cache:
            return

        if schema:
            loc = self._resolve_schema_location(rt, conn, schema_loc)
            loc_clause = f" LOCATION '{loc}'" if loc else ""
            comment_clause = f" COMMENT '{str(schema_comment).replace(chr(39), chr(39)+chr(39))}'" if schema_comment else ""
            quoted_schema = quote_identifier(schema, quote="`", label="schema", schema_qualified=False)
            rt.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}{comment_clause}{loc_clause}")

        if schema:
            full_name = f"{quote_identifier(schema, quote='`', label='schema', schema_qualified=False)}.{quote_identifier(table_name, quote='`', label='table_name', schema_qualified=False)}"
        else:
            full_name = quote_identifier(table_name, quote="`", label="table_name", schema_qualified=False)

        rt.spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} USING DELTA LOCATION '{path}'")

        props = dict(table_props)
        if comment and "comment" not in props:
            props["comment"] = str(comment)
        if props:
            escaped = [f"'{str(k).replace(chr(39), chr(39)+chr(39))}' = '{str(spark_text(v)).replace(chr(39), chr(39)+chr(39))}'" for k, v in props.items()]
            rt.spark.sql(f"ALTER TABLE {full_name} SET TBLPROPERTIES ({', '.join(escaped)})")

        cache.add(cache_key)

    # --- writes ----------------------------------------------------------------------------------------------

    def _observed(self, frame, output):
        from pyspark.sql import Observation as SparkObservation
        from pyspark.sql import functions as F
        metrics = SparkObservation("delta_" + output)
        return frame.observe(metrics, F.count(F.lit(1)).alias("rows")), metrics

    def _version(self, rt, path):
        from delta.tables import DeltaTable
        row = DeltaTable.forPath(rt.spark, path).history(1).collect()[0]
        return int(row["version"]), (row["operationMetrics"] or {})     # the transaction log, not the data

    def _writer(self, frame, arguments):
        writer = frame.write.format("delta")
        for key, value in (arguments.get("options") or {}).items():
            writer = writer.option(str(key), str(value))
        if arguments.get("partition_by"):
            writer = writer.partitionBy(*arguments["partition_by"])
        return writer

    def write_append(self, rt, conn, output, method, arguments, frame):
        path = self._path(rt, conn, arguments["table_path"])
        frame, metrics = self._observed(frame, output)
        # idempotent replay: Delta skips a write whose (txnAppId, txnVersion) it already committed
        self._writer(frame, arguments).mode("append").option("txnAppId", "%s:%s" % (rt.ctx.job, output)).option(
            "txnVersion", rt.ctx.batch_seq).save(path)                  # the single action
        self._ensure_registered(rt, conn, arguments, frame, path)
        return self._receipt(rt, output, path, metrics)

    def write_overwrite(self, rt, conn, output, method, arguments, frame):
        path = self._path(rt, conn, arguments["table_path"])
        frame, metrics = self._observed(frame, output)
        writer = self._writer(frame, arguments).mode("overwrite")
        pred = arguments.get("replaceWhere") or arguments.get("replace_where")
        predicate = rt.predicate(pred) if pred else None
        if predicate:
            writer = writer.option("replaceWhere", predicate)
        writer.save(path)                                               # the single action
        self._ensure_registered(rt, conn, arguments, frame, path)
        return self._receipt(rt, output, path, metrics, predicate)

    def _receipt(self, rt, output, path, metrics, predicate=None):
        version, _ = self._version(rt, path)
        rows = int(metrics.get.get("rows", 0))
        return WriteReceipt(output, path, rows, version=version, predicate=predicate,
                            observations=[Observation(output, {"rows": rows})])

    def write_merge(self, rt, conn, output, method, arguments, frame):
        from delta.tables import DeltaTable
        path = self._path(rt, conn, arguments["table_path"])
        if not DeltaTable.isDeltaTable(rt.spark, path):
            # first write: create the table from the source's schema, empty, so MERGE has a target
            self._writer(frame.limit(0), arguments).save(path)
        self._ensure_registered(rt, conn, arguments, frame, path)
        predicate = rt.predicate(arguments["target_predicate"]) if arguments.get("target_predicate") else None
        builder = DeltaTable.forPath(rt.spark, path).alias("target").merge(
            frame.alias("source"), merge_condition(arguments["condition"], predicate))
        for clause in arguments.get("when_matched") or []:
            builder = self._matched(builder, clause)
        for clause in arguments.get("when_not_matched") or []:
            builder = (builder.whenNotMatchedInsert(condition=clause.get("condition"), values=clause["set"])
                       if clause.get("set") else builder.whenNotMatchedInsertAll(condition=clause.get("condition")))
        for clause in arguments.get("when_not_matched_by_source") or []:
            builder = (builder.whenNotMatchedBySourceDelete(condition=clause.get("condition"))
                       if clause.get("action") == "delete" else builder.whenNotMatchedBySourceUpdate(
                            condition=clause.get("condition"), set=clause.get("set") or {}))
        builder.execute()                                               # the single action
        version, metrics = self._version(rt, path)
        rows = metrics.get("numSourceRows")                             # Delta's own commit metrics: no count() pass
        return WriteReceipt(output, path, int(rows) if rows is not None else None, version=version, predicate=predicate,
                            observations=[Observation(output, dict(metrics))])

    def _matched(self, builder, clause):
        if clause.get("action") == "delete":
            return builder.whenMatchedDelete(condition=clause.get("condition"))
        if clause.get("set"):
            return builder.whenMatchedUpdate(condition=clause.get("condition"), set=clause["set"])
        return builder.whenMatchedUpdateAll(condition=clause.get("condition"))

    # --- Spark facts -----------------------------------------------------------------------------------------

    def local_conf(self):
        return {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"}

    jar_specs = ()

    def jars(self, spark_minor):
        return [tuple(spec) for spec in self.jar_specs]
