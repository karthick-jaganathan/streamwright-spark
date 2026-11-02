"""
Object-store files for the `spark` engine, composed by a connector's Spark adapter (the counterpart of
`streamwright.core.io.files`): a FileLocation (a connection's roots and URL schemes), reads with pinned discovery and
schema harmonization, writes and split exports. What Spark supports of the file operations: CAPABILITIES
(`file_rules`, importable without pyspark).
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType

from pyspark.sql import Observation, functions

from streamwright.core.jobs import naming
from streamwright.core.jobs.types import Observation as ReceiptObservation
from streamwright.spark.io.file_rules import CAPABILITIES, OPTION_NAMES, check_read, check_write
from streamwright.spark.sdk import SparkContext, WriteReceipt
from streamwright.spark.storage import exports, ingest
from streamwright.spark.storage.paths import StorageError, check_path, spark_text

__all__ = ["CAPABILITIES", "FileLocation", "check_read", "check_write", "read_files", "spark_options", "write_files"]

_FORMATS = MappingProxyType({"csv": "csv", "tsv": "csv", "json": "json", "jsonl": "json", "parquet": "parquet"})


@dataclass(frozen=True)
class FileLocation:
    """
    Authorized roots and the connector/Hadoop URIs of one connection. `hadoop_url` turns a connector URL into the
    Hadoop one (default: the scheme replaced); `source_pattern` and `source_replacement` (a Spark regexp_replace) turn
    a Hadoop URL back into the connector's (default: the scheme replaced).
    """

    roots: tuple[str, ...]
    source_scheme: str
    hadoop_scheme: str
    hadoop_url: Callable[[str], str] | None = None
    source_pattern: str | None = None
    source_replacement: str = ""

    def to_hadoop(self, path: str) -> str:
        if self.hadoop_url is not None:
            return self.hadoop_url(path)
        prefix = self.source_scheme + "://"
        return self.hadoop_scheme + "://" + path[len(prefix) :] if path.startswith(prefix) else path

    def resolve_path(self, path: str) -> str:
        if isinstance(path, str) and "://" not in path and ":" not in path and not path.startswith("/"):
            if not self.roots:
                raise StorageError("connection has no authorized roots")
            path = self.roots[0].rstrip("/") + "/" + path
        return check_path(path, self.roots)

    def source_file(self, column):
        if self.source_pattern is not None:
            return functions.regexp_replace(column, self.source_pattern, self.source_replacement)
        return functions.regexp_replace(column, "^" + self.hadoop_scheme + "://", self.source_scheme + "://")


def spark_options(format_name, options):
    """
    The Spark reader/writer options of a format: shared-vocabulary options translated to their Spark option names,
    and any other (vendor) option forwarded verbatim to DataFrameReader/Writer.option (contract v1.2 §6a).
    """
    result = {}
    for key, value in (options or {}).items():
        result[OPTION_NAMES.get(key, key)] = spark_text(value)
    if format_name == "tsv":
        result.setdefault("sep", "\t")
    return result


def _empty_frame(context, arguments):
    columns = ["_source_file STRING"]
    if arguments.get("path_pattern") and arguments.get("inject_path_vars", True):
        columns.extend(f"{name} STRING" for name in re.compile(arguments["path_pattern"]).groupindex)
    row_index = ingest.normalize_row_index(arguments.get("row_index"))
    if row_index:
        columns.append(f"{row_index[0]} BIGINT")
    if arguments.get("row_errors"):
        columns.append("_row_errors ARRAY<STRING>")
    return context.runtime.empty(", ".join(columns))


def _read_headers(runtime, paths, formats, options, location):
    headers = {}
    for path in paths:
        format_name = formats[path]
        if format_name in ("csv", "tsv", "json", "jsonl"):
            separator = "\t" if format_name == "tsv" else options.get("sep", ",")
            line = runtime.first_line(location.to_hadoop(path))
            headers[path] = ingest.parse_header(line, separator) if line else []
        else:
            headers[path] = []
    return headers


def _plan_read(context, paths, arguments, options, location):
    runtime = context.runtime
    formats, format_rejects = ingest.resolve_file_formats(paths, arguments.get("format", "auto"))
    if format_rejects and arguments.get("on_mismatch", "error") == "error":
        raise StorageError(f"{len(format_rejects)} file(s) had an unknown format")
    paths = [path for path in paths if path in formats]
    missing = [path for path in paths if not runtime.exists(location.to_hadoop(path))]
    if missing and arguments.get("on_missing", "skip") == "error":
        raise StorageError(f"{len(missing)} file(s) are missing")
    for path in missing:
        runtime.add_file_error(context.name, path, "file not found")
    paths = [path for path in paths if path not in missing]
    if len(paths) > runtime.max_files_per_batch:
        raise StorageError(f"{len(paths)} objects exceed runtime.limits.max_files_per_batch")
    if not paths:
        return [], format_rejects, []
    headers = _read_headers(runtime, paths, formats, options, location)
    settings = {
        "headers": headers,
        "formats": formats,
        "options": options,
        "max_groups": runtime.max_groups,
        "step_name": context.name,
        "column_groups": arguments.get("column_groups"),
    }
    if arguments.get("columns") is None:
        groups, rejects, reports = ingest.plan_ingest(
            paths, schema_mode=arguments.get("schema_mode", "by_name"), **settings
        )
    else:
        mappings = runtime.expand(arguments["columns"])
        vocabulary = runtime.expand(arguments["vocabulary"]) if arguments.get("vocabulary") is not None else None
        ingest.check_vocabulary(mappings, vocabulary)
        groups, rejects, reports = ingest.plan_ingest(
            paths,
            mappings=mappings,
            on_unmapped_column=arguments.get("on_unmapped_column", "warn"),
            previous_signatures=runtime.previous_signatures,
            path_pattern=arguments.get("path_pattern"),
            inject_path_vars=arguments.get("inject_path_vars", True),
            **settings,
        )
    rejects = format_rejects + rejects
    if rejects and arguments.get("on_mismatch", "error") == "error":
        raise StorageError(f"{len(rejects)} file(s) did not match schema or mapping")
    return groups, rejects, reports


def read_files(context: SparkContext, arguments, *, location: FileLocation):
    """Read authorized files with pinned discovery and the shared schema-harmonization planner."""
    runtime = context.runtime
    paths = [location.resolve_path(path) for path in runtime.path_values(arguments["path"])]
    options = spark_options("csv", arguments.get("options"))
    pinned_key = "ingest:" + context.name
    if pinned_key in runtime.pinned:
        groups, rejects, reports = ingest.deserialize_plan(runtime.pinned[pinned_key])
        for group in groups:
            for path in group.files:
                location.resolve_path(path)
                if not runtime.exists(location.to_hadoop(path)):
                    raise StorageError("batch is replaying a pinned plan, but a file has disappeared")
    else:
        groups, rejects, reports = _plan_read(context, paths, arguments, options, location)
        runtime.pinned[pinned_key] = ingest.serialize_plan(groups, rejects, reports)
    for reject in rejects:
        runtime.add_file_error(context.name, reject.path, reject.reason)
    for report in reports:
        runtime.add_schema_report(report)
    mappings = runtime.expand(arguments["columns"]) if arguments.get("columns") is not None else None
    normalized, _ = ingest.normalize_columns(mappings) if mappings else ([], None)
    required = [str(row["canonical"]) for row in normalized if ingest.is_true(row.get("required"))]
    frame = ingest.read_groups(
        runtime.spark,
        groups,
        options=options,
        to_hadoop=location.to_hadoop,
        source_file=location.source_file,
        row_errors=arguments.get("row_errors", False),
        required_columns=required,
        row_index=arguments.get("row_index"),
    )
    return frame if frame is not None else _empty_frame(context, arguments)


def write_files(context: SparkContext, frame, arguments, *, location: FileLocation, tokens=()):
    """Write a frame once, returning the core receipt used by replay and bookkeeping."""
    runtime = context.runtime
    root = location.resolve_path(runtime.text(arguments["path"]))
    uri = location.to_hadoop(root)
    format_name = arguments["format"]
    if arguments.get("split_by") or arguments.get("layout") or arguments.get("file_name"):
        columns = list(arguments.get("split_by") or [])
        constants = ("driver_host", *tuple(tokens))
        file_template = naming.parse_template(arguments["file_name"], columns, False, "file_name", constants=constants)
        layout_template = (
            naming.parse_template(arguments["layout"], columns, True, "layout", constants=constants)
            if arguments.get("layout")
            else None
        )
        options = dict(
            arguments,
            options=spark_options(format_name, arguments.get("options")),
            format=_FORMATS[format_name],
        )
        return exports.write_split_export(
            runtime.spark,
            frame,
            runtime.ctx,
            context.name,
            options,
            root_uri=uri,
            file_template=file_template,
            layout_template=layout_template,
        )
    metrics = Observation("object_" + context.name)
    frame = frame.observe(metrics, functions.count(functions.lit(1)).alias("rows"))
    if arguments.get("single_file"):
        frame = frame.coalesce(1)
    writer = frame.write.format(_FORMATS[format_name]).mode(arguments.get("mode", "error"))
    for key, value in spark_options(format_name, arguments.get("options")).items():
        writer = writer.option(key, value)
    writer.save(uri)
    rows = int(metrics.get.get("rows", 0))
    return WriteReceipt(context.name, root, rows, observations=[ReceiptObservation(context.name, {"rows": rows})])
