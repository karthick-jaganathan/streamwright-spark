"""Materialize engine-neutral ingest groups as Spark DataFrames."""

from pyspark.sql import Window, functions

from streamwright.core.jobs.ingest import (
    TYPE_PATTERN,
    FileReject,
    Group,
    IngestError,
    SchemaReport,
    check_vocabulary,
    compute_header_hash,
    deserialize_plan,
    is_true,
    match_column_groups,
    normalize_columns,
    normalize_row_index,
    parse_header,
    plan_ingest,
    plan_plain_groups,
    resolve_file_format,
    resolve_file_formats,
    serialize_plan,
)
from streamwright.spark.storage.paths import spark_text

__all__ = [
    "TYPE_PATTERN",
    "FileReject",
    "Group",
    "IngestError",
    "SchemaReport",
    "check_vocabulary",
    "compute_header_hash",
    "deserialize_plan",
    "is_true",
    "match_column_groups",
    "normalize_columns",
    "normalize_row_index",
    "pairwise_union_by_name",
    "parse_header",
    "plan_ingest",
    "plan_plain_groups",
    "read_groups",
    "resolve_file_format",
    "resolve_file_formats",
    "serialize_plan",
]


def pairwise_union_by_name(frames, allow_missing=True):
    """Balanced binary union avoids deeply nested Spark plans."""
    if not frames:
        return None
    current = list(frames)
    while len(current) > 1:
        next_level = []
        for index in range(0, len(current), 2):
            if index + 1 < len(current):
                next_level.append(current[index].unionByName(current[index + 1], allowMissingColumns=allow_missing))
            else:
                next_level.append(current[index])
        current = next_level
    return current[0]


def _column_groups(group):
    columns = []
    for name, details in (group.column_groups or {}).items():
        kind = details.get("type", "string")
        entries = details.get("entries", [])
        if entries:
            pairs = [
                functions.struct(
                    functions.lit(key), functions.expr("try_cast(`%s` AS %s)" % (source.replace("`", "``"), kind))
                )
                for source, key in entries
            ]
            mapped = functions.map_from_entries(functions.array(*pairs))
            if not details.get("keep_nulls", False):
                mapped = functions.map_filter(mapped, lambda key, value: value.isNotNull())
            columns.append(mapped.alias(name))
        else:
            columns.append(functions.expr("cast(map() as map<string, %s>)" % kind).alias(name))
    return columns


def _frame_from_plan(
    spark, group, fmt, options, to_hadoop, *, source_file, tracking_column, row_errors, required_set, row_index
):
    group_format = getattr(group, "format", None) or fmt
    spark_format = "csv" if group_format in ("csv", "tsv") else group_format
    group_options = dict(options or {})
    group_options.update(getattr(group, "options", None) or {})
    reader = spark.read.format(spark_format)
    if spark_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "false")
        if group_format == "tsv" and "sep" not in group_options and "delimiter" not in group_options:
            reader = reader.option("sep", "\t")
    for key, value in group_options.items():
        reader = reader.option(key, spark_text(value))
    paths = [to_hadoop(path) if to_hadoop else path for path in group.files]
    frame = reader.load(paths)
    columns, error_conditions = [], []
    for entry in group.columns:
        src, name, *kind_values = entry
        kind = kind_values[0] if kind_values else None
        quoted = "`%s`" % src.replace("`", "``")
        if kind is not None:
            columns.append(functions.expr("try_cast(%s AS %s)" % (quoted, kind)).alias(name))
            if row_errors:
                condition = (
                    "(%s IS NULL OR trim(cast(%s AS string)) = '' OR "
                    "try_cast(%s AS %s) IS NULL)" % (quoted, quoted, quoted, kind)
                    if name in required_set
                    else "(%s IS NOT NULL AND trim(cast(%s AS string)) != '' AND "
                    "try_cast(%s AS %s) IS NULL)" % (quoted, quoted, quoted, kind)
                )
                error_conditions.append(functions.when(functions.expr(condition), functions.lit(name)))
        else:
            columns.append(functions.col(src).alias(name))
    columns.extend(_column_groups(group))
    if row_errors:
        errors = (
            functions.array_compact(functions.array(*error_conditions))
            if error_conditions
            else functions.array().cast("array<string>")
        )
        columns.append(errors.alias("_row_errors"))
    origin = functions.input_file_name()
    if row_index is not None:
        name, start = row_index
        window = Window.partitionBy(origin).orderBy(functions.monotonically_increasing_id())
        columns.append((functions.row_number().over(window) - 1 + start).cast("bigint").alias(name))
    if group.path_vars_spec:
        pattern, indexes = group.path_vars_spec
        used = {entry[1] for entry in group.columns} | {tracking_column, "_row_errors"}
        for name, index in sorted(indexes.items()):
            if name not in used:
                columns.append(functions.regexp_extract(origin, pattern, int(index)).alias(name))
    columns.append((source_file(origin) if source_file else origin).alias(tracking_column))
    return frame.select(*columns)


def read_groups(
    spark,
    groups,
    *,
    fmt="csv",
    options=None,
    to_hadoop=None,
    source_file=None,
    tracking_column="_source_file",
    row_errors=False,
    required_columns=None,
    row_index=None,
):
    """One lazy DataFrame per core ingest Group, combined by name."""
    index = normalize_row_index(row_index)
    frames = [
        _frame_from_plan(
            spark,
            group,
            fmt,
            options,
            to_hadoop,
            source_file=source_file,
            tracking_column=tracking_column,
            row_errors=row_errors,
            required_set=set(required_columns or ()),
            row_index=index,
        )
        for group in groups
    ]
    return pairwise_union_by_name(frames)
