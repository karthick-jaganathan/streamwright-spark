"""
Delta Lake tables for the `spark` engine, composed by a connector's Spark adapter (`table.read`, `table.write`): a
table path is resolved inside the connection's roots and then through the storage connection's FileLocation (its
roots and Hadoop URL), so Spark reads the table with the storage's credentials and settings.

- `read_table`: a version of the table (the latest one is pinned in the batch manifest on the first read, so a
  replay reads the same data), its columns, `where` conditions and `limit`.
- `write_table`: one Delta transaction per output. `append` carries an application transaction (txnAppId
  `<job>:<output>:<store identity>`, txnVersion = the batch sequence) and is skipped when the log already holds it, so
  replaying a batch never appends twice (the receipt is rebuilt from that commit) and a different state store never
  collides on the same table; `overwrite` replaces what `replace_where` selects or the `whole_table`; `merge` runs
  MERGE with `condition` (AND the source-derived `target_predicate`) and its when_*
  clauses. Predicates may use `{{ step.column }}`, rendered as escaped literals from the pinned values.
  `schema_evolution: add_columns` lets a write add columns. `table_name` (and `schema`) register the table in the
  catalog when it is not there yet.
"""

import json
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions
from pyspark.sql import Observation as SparkObservation

from streamwright.core.jobs.types import Observation
from streamwright.core.jobs.write_rules import merge_condition
from streamwright.core.sdk.contract import Operation, vendor_arguments
from streamwright.spark.io.database_rules import CAPABILITIES
from streamwright.spark.io.files import FileLocation
from streamwright.spark.io.sql import quote_identifier, quote_literal, where_clause
from streamwright.spark.sdk import SparkContext, WriteReceipt
from streamwright.spark.storage.paths import check_path, spark_text

__all__ = ["CAPABILITIES", "LOCAL_CONF", "PACKAGES", "read_table", "resolve_table_path", "write_table"]

# the session extension and catalog Delta needs (platform: local; elsewhere the launcher owns them)
LOCAL_CONF: Final = MappingProxyType(
    {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }
)
PACKAGES: Final = ("delta-spark",)
AUTO_MERGE: Final = "spark.databricks.delta.schema.autoMerge.enabled"
# Delta reader/writer options the adapter sets itself from declared arguments or the batch: a vendor argument may not
# override them (it would break replay-safety or time travel), so it is rejected with a clear error.
_READ_OWNED: Final = ("versionAsOf",)
_WRITE_OWNED: Final = ("mergeSchema", "txnAppId", "txnVersion", "replaceWhere")


def _vendor_options(
    operation: Operation | None, arguments: Mapping[str, Any], owned: tuple[str, ...]
) -> dict[str, Any]:
    """The operation's vendor arguments as Delta options, then the explicit `options`; `owned` keys are rejected."""
    vendor = vendor_arguments(operation, arguments) if operation is not None else {}
    clash = sorted(key for key in vendor if key in owned)
    if clash:
        raise ValueError("%s set by the adapter; use the operation's declared arguments, not a vendor "
                         "argument" % ", ".join(clash))
    return {**vendor, **(arguments.get("options") or {})}


def resolve_table_path(context: SparkContext, path: str, roots: tuple[str, ...], storage: FileLocation) -> str:
    """The Hadoop URL of a table path inside the connection's `roots` (relative: below the first) and the storage's."""
    text = context.runtime.text(path)
    if "://" not in text and not text.startswith("/"):
        text = roots[0].rstrip("/") + "/" + text
    return storage.to_hadoop(storage.resolve_path(check_path(text, roots)))


def read_table(context: SparkContext, arguments: Mapping[str, Any], *, path: str,
               operation: Operation | None = None) -> DataFrame:
    """The table at `path` (a Hadoop URL), at the batch's pinned version."""
    runtime, pin = context.runtime, "%s.version" % context.name
    if pin not in runtime.pinned:
        wanted = arguments.get("version")
        runtime.pinned[pin] = int(wanted) if wanted is not None else _latest_version(context, path)[0]
    reader = context.spark.read.format("delta").option("versionAsOf", runtime.pinned[pin])
    for key, value in _vendor_options(operation, arguments, _READ_OWNED).items():
        reader = reader.option(str(key), spark_text(value))
    frame = reader.load(path)
    if arguments.get("columns"):
        frame = frame.select(
            *[quote_identifier(column, "`", "column", schema_qualified=False) for column in arguments["columns"]]
        )
    clause = where_clause(arguments.get("where"), "`")
    if clause:
        frame = frame.where(clause[len(" WHERE ") :])
    return frame.limit(int(arguments["limit"])) if arguments.get("limit") is not None else frame


def _latest_version(context: SparkContext, path: str) -> tuple[int, Mapping[str, Any]]:
    """The table's latest version and its commit metrics (the transaction log, not the data)."""
    row = DeltaTable.forPath(context.spark, path).history(1).collect()[0]
    return int(row["version"]), (row["operationMetrics"] or {})


def _writer(frame: DataFrame, arguments: Mapping[str, Any], options: Mapping[str, Any]) -> Any:
    writer = frame.write.format("delta")
    if arguments.get("schema_evolution") == "add_columns":
        writer = writer.option("mergeSchema", "true")
    for key, value in options.items():
        writer = writer.option(str(key), spark_text(value))
    if arguments.get("partition_by"):
        writer = writer.partitionBy(*arguments["partition_by"])
    return writer


def _read_log(context: SparkContext, log: Any, filesystem: Any, app_id: str, transaction: int) -> Any:
    """(found, commit metrics) of one log file."""
    jvm = context.spark._jvm
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(filesystem.open(log), "UTF-8"))
    found, metrics = False, {}
    try:
        line = reader.readLine()
        while line is not None:
            action = json.loads(str(line))
            txn = action.get("txn") or {}
            found = found or (txn.get("appId") == app_id and txn.get("version") == transaction)
            metrics = (action.get("commitInfo") or {}).get("operationMetrics") or metrics
            line = reader.readLine()
    finally:
        reader.close()
    return found, metrics


def _find_commit(context: SparkContext, path: str, app_id: str, transaction: int) -> Any:
    """(version, metrics) of the commit holding the application transaction, or None (not history(1)'s latest)."""
    log_path, filesystem = context.runtime._fs(path.rstrip("/") + "/_delta_log")
    if not filesystem.exists(log_path):
        return None
    logs = sorted(
        (
            status.getPath()
            for status in filesystem.listStatus(log_path)
            if str(status.getPath().getName()).endswith(".json")
        ),
        key=lambda item: str(item.getName()),
        reverse=True,
    )
    for log in logs:
        found, metrics = _read_log(context, log, filesystem, app_id, transaction)
        if found:
            return int(str(log.getName()).split(".", 1)[0]), metrics
    return None


def _commit_receipt(name: str, path: str, committed: tuple[int, Mapping[str, Any]]) -> WriteReceipt:
    version, metrics = committed
    rows = int(metrics["numOutputRows"]) if metrics.get("numOutputRows") is not None else None
    return WriteReceipt(name, path, rows, version=version, observations=[Observation(name, {"rows": rows})])


def _append(context: SparkContext, frame: DataFrame, arguments: Mapping[str, Any], path: str,
            options: Mapping[str, Any]) -> WriteReceipt:
    batch = context.runtime.ctx
    app_id = "%s:%s:%s" % (batch.job_name, context.name, batch.store_identity)
    transaction = batch.batch_sequence
    committed = _find_commit(context, path, app_id, transaction)
    if committed is None:
        _writer(frame, arguments, options).mode("append").option("txnAppId", app_id).option(
            "txnVersion", transaction).save(path)
        committed = _find_commit(context, path, app_id, transaction)
        if committed is None:
            raise RuntimeError("the Delta append transaction was not found in the transaction log")
    return _commit_receipt(context.name, path, committed)


def _overwrite(context: SparkContext, frame: DataFrame, arguments: Mapping[str, Any], path: str,
               options: Mapping[str, Any]) -> WriteReceipt:
    metrics = SparkObservation("delta_" + context.name)
    observed = frame.observe(metrics, functions.count(functions.lit(1)).alias("rows"))
    writer = _writer(observed, arguments, options).mode("overwrite")
    predicate = context.runtime.predicate(arguments["replace_where"]) if arguments.get("replace_where") else None
    if predicate:
        writer = writer.option("replaceWhere", predicate)
    writer.save(path)
    rows = int(metrics.get.get("rows", 0))
    return WriteReceipt(
        context.name,
        path,
        rows,
        version=_latest_version(context, path)[0],
        predicate=predicate,
        observations=[Observation(context.name, {"rows": rows})],
    )


def _with_clauses(builder: Any, arguments: Mapping[str, Any]) -> Any:
    for clause in arguments.get("when_matched") or ():
        if clause["action"] == "delete":
            builder = builder.whenMatchedDelete(condition=clause.get("predicate"))
        elif clause.get("set"):
            builder = builder.whenMatchedUpdate(condition=clause.get("predicate"), set=dict(clause["set"]))
        else:
            builder = builder.whenMatchedUpdateAll(condition=clause.get("predicate"))
    for clause in arguments.get("when_not_matched") or ():
        builder = (
            builder.whenNotMatchedInsert(condition=clause.get("predicate"), values=dict(clause["set"]))
            if clause.get("set")
            else builder.whenNotMatchedInsertAll(condition=clause.get("predicate"))
        )
    for clause in arguments.get("when_not_matched_by_source") or ():
        builder = (
            builder.whenNotMatchedBySourceDelete(condition=clause.get("predicate"))
            if clause["action"] == "delete"
            else builder.whenNotMatchedBySourceUpdate(
                condition=clause.get("predicate"), set=dict(clause.get("set") or {})
            )
        )
    return builder


def _merge(context: SparkContext, frame: DataFrame, arguments: Mapping[str, Any], path: str,
           options: Mapping[str, Any]) -> WriteReceipt:
    spark = context.spark
    if not DeltaTable.isDeltaTable(spark, path):
        _writer(frame.limit(0), arguments, options).save(path)  # the first write: an empty table, so MERGE has a target
    target = arguments.get("target_predicate")
    predicate = context.runtime.predicate(target) if target else None
    builder = _with_clauses(
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(frame.alias("source"), merge_condition(arguments["condition"], predicate)),
        arguments,
    )
    previous = spark.conf.get(AUTO_MERGE, "false") if arguments.get("schema_evolution") == "add_columns" else None
    if previous is not None:
        spark.conf.set(AUTO_MERGE, "true")
    try:
        builder.execute()
    finally:
        if previous is not None:
            spark.conf.set(AUTO_MERGE, previous)
    version, metrics = _latest_version(context, path)
    rows = metrics.get("numSourceRows")  # Delta's own commit metrics: no count() pass
    return WriteReceipt(
        context.name,
        path,
        int(rows) if rows is not None else None,
        version=version,
        predicate=predicate,
        observations=[Observation(context.name, dict(metrics))],
    )


def _register(
    context: SparkContext,
    arguments: Mapping[str, Any],
    defaults: Mapping[str, Any],
    path: str,
    schema_location: str | None,
) -> None:
    """CREATE SCHEMA and TABLE ... USING DELTA LOCATION for `table_name` (or a `schema`) the catalog lacks."""
    table_name, schema = arguments.get("table_name"), arguments.get("schema") or defaults.get("schema")
    if not table_name and not schema:
        return
    table_name = table_name or path.rstrip("/").split("/")[-1]
    if "." in table_name:
        schema, table_name = table_name.split(".", 1)
    name = quote_identifier(table_name, "`", "table_name", schema_qualified=False)
    if schema:
        name = "%s.%s" % (quote_identifier(schema, "`", "schema", schema_qualified=False), name)
    if context.spark.catalog.tableExists(name):
        return
    if schema:
        comment = arguments.get("schema_comment") or defaults.get("schema_comment")
        context.spark.sql(
            "CREATE SCHEMA IF NOT EXISTS %s%s%s"
            % (
                name.split(".")[0],
                " COMMENT %s" % quote_literal(str(comment)) if comment else "",
                " LOCATION %s" % quote_literal(schema_location) if schema_location else "",
            )
        )
    context.spark.sql("CREATE TABLE IF NOT EXISTS %s USING DELTA LOCATION %s" % (name, quote_literal(path)))
    properties = dict(arguments.get("table_properties") or {})
    if arguments.get("comment"):
        properties.setdefault("comment", str(arguments["comment"]))
    if properties:
        context.spark.sql(
            "ALTER TABLE %s SET TBLPROPERTIES (%s)"
            % (
                name,
                ", ".join(
                    "%s = %s" % (quote_literal(str(key)), quote_literal(spark_text(value)))
                    for key, value in properties.items()
                ),
            )
        )


def write_table(
    context: SparkContext,
    frame: DataFrame,
    arguments: Mapping[str, Any],
    *,
    path: str,
    defaults: Mapping[str, Any],
    schema_location: str | None = None,
    operation: Operation | None = None,
) -> WriteReceipt:
    """
    `table.write` of `frame` to the table at `path` (a Hadoop URL). `defaults`: the connection's catalog `schema`
    and `schema_comment`; `schema_location`: the Hadoop URL of the schema's location, if any.
    """
    mode = arguments.get("mode", "append")
    options = _vendor_options(operation, arguments, _WRITE_OWNED)
    write = {"append": _append, "overwrite": _overwrite, "merge": _merge}[mode]
    receipt = write(context, frame, arguments, path, options)
    _register(context, arguments, defaults, path, schema_location)
    return receipt
