"""
What the `spark` engine supports of the file operations (`streamwright.core.sdk.operations.files`), declared once as
its EngineCapabilities, and the semantic checks of those operations' arguments - what the declarations cannot say.
Importable without pyspark (validation imports it), as `streamwright.core.io.file_rules`.
"""

import re
from types import MappingProxyType
from typing import Final

from streamwright.core.jobs import naming
from streamwright.core.jobs.ingest import (
    TYPE_PATTERN,
    IngestError,
    check_vocabulary,
    normalize_columns,
    normalize_row_index,
)
from streamwright.core.sdk.contract import EngineCapabilities, OperationKind

__all__ = ["CAPABILITIES", "OPTION_NAMES", "READ_OPTIONS", "WRITE_OPTIONS", "check_read", "check_write"]

READ_NAME: Final = "object.read"  # the catalog's names (streamwright.core.sdk.operations.files)
WRITE_NAME: Final = "object.write"
# The format options are the vocabulary of `streamwright.core.io.file_formats` (the other engines'); the Spark engine
# takes these of them, plus `encoding` and `multiLine` (its own), and says which Spark option each one is.
SHARED_OPTIONS: Final = (
    "header",
    "delimiter",
    "quote",
    "escape",
    "compression",
    "nullstr",
    "dateformat",
    "timestampformat",
)
READ_OPTIONS: Final = frozenset((*SHARED_OPTIONS, "encoding", "multiLine"))
WRITE_OPTIONS: Final = frozenset((*SHARED_OPTIONS, "encoding"))
OPTION_NAMES: Final = MappingProxyType(
    {"delimiter": "sep", "nullstr": "nullValue", "dateformat": "dateFormat", "timestampformat": "timestampFormat"}
)


def _check_options(options):
    """
    Translated options (`OPTION_NAMES` / `SHARED_OPTIONS`) are typed; any other option is a vendor argument,
    forwarded verbatim to DataFrameReader/Writer.option (contract v1.2 §6a).
    """
    if options is None:
        return []
    if not isinstance(options, dict):
        return ["options must be a mapping"]
    problems = []
    if "nullstr" in options and not isinstance(options["nullstr"], str):
        problems.append("option 'nullstr' is a single text on the spark engine")
    return problems


def _check_path(path):
    if path in ("", []):
        return ["path must name an object key, URL or glob"]
    return []


def _check_groups(groups):
    problems = []
    for group in groups or ():
        if not isinstance(group, dict) or bool(group.get("regex")) == bool(group.get("glob")):
            problems.append("column_groups must declare exactly one glob or regex")
            continue
        if not isinstance(group.get("name"), str) or not re.fullmatch(r"[A-Za-z_]\w*", group["name"]):
            problems.append("column_groups.name must be a plain identifier")
        if not TYPE_PATTERN.match(str(group.get("type", "string")).strip().lower()):
            problems.append("column_groups.type is not supported")
        if group.get("regex"):
            try:
                re.compile(group["regex"])
            except re.error:
                problems.append("invalid column_groups regex")
    return problems


def _check_row_index(value):
    if value is None:
        return []
    if isinstance(value, dict) and not isinstance(value.get("name"), str):
        return ["row_index.name must be a plain identifier"]
    try:
        normalized = normalize_row_index(value)
    except (ValueError, TypeError) as exc:
        return [str(exc)]
    if normalized and not re.fullmatch(r"[A-Za-z_]\w*", normalized[0]):
        return ["row_index name must be a plain identifier"]
    if normalized and normalized[1] < 0:
        return ["row_index.start must be a non-negative integer"]
    return []


def check_read(arguments):
    problems = _check_path(arguments.get("path"))
    problems.extend(_check_options(arguments.get("options")))
    columns = arguments.get("columns")
    vocabulary = arguments.get("vocabulary")
    if vocabulary is not None and columns is None:
        problems.append("vocabulary only applies together with columns")
    if isinstance(columns, (list, tuple, dict)):
        try:
            check_vocabulary(columns, vocabulary if isinstance(vocabulary, (list, tuple)) else None)
        except IngestError as exc:
            problems.append(str(exc))
    pattern = arguments.get("path_pattern")
    if pattern is not None and (not isinstance(pattern, str) or not pattern.strip()):
        problems.append("path_pattern must be a non-empty regex string")
    elif pattern is not None and "{{" not in pattern:
        try:
            re.compile(pattern)
        except re.error:
            problems.append("invalid path_pattern regex")
    problems.extend(_check_groups(arguments.get("column_groups")))
    problems.extend(_check_row_index(arguments.get("row_index")))
    if not problems and isinstance(columns, (list, tuple, dict)):
        index = normalize_row_index(arguments.get("row_index"))
        mappings, _ = normalize_columns(columns)
        if index and index[0] in {row.get("canonical") for row in mappings if isinstance(row, dict)}:
            problems.append(f"row_index column {index[0]!r} collides with mapped canonical column")
    return problems


def check_write(arguments, tokens=()):
    problems = _check_path(arguments.get("path")) + _check_options(arguments.get("options"))
    split_by = arguments.get("split_by") or []
    if not isinstance(split_by, list) or not all(
        isinstance(col, str) and re.fullmatch(r"[A-Za-z_]\w*", col) for col in split_by
    ):
        return ["split_by must be a list of column names"]
    parsed = {}
    for key, slash in (("file_name", False), ("layout", True)):
        if arguments.get(key) is not None:
            try:
                parsed[key] = naming.parse_template(
                    arguments[key], split_by, slash, key, constants=("driver_host", *tuple(tokens))
                )
            except naming.NamingError as exc:
                problems.append(str(exc))
    if (split_by or arguments.get("layout")) and not arguments.get("file_name"):
        problems.append("split_by and layout need a file_name")
    if not split_by and any(template.columns for template in parsed.values()):
        problems.append("names use columns but split_by is empty")
    unused = [col for col in split_by if not any(col in template.columns for template in parsed.values())]
    if split_by and parsed and unused:
        problems.append("split_by column(s) {} appear in neither file_name nor layout".format(", ".join(unused)))
    path = str(arguments.get("path", ""))
    uses_batch = bool(re.search(r"\{\{\s*batch\.id\s*\}\}", path)) or any(
        part.kind == "batch_id" for template in parsed.values() for part in template.parts
    )
    if arguments.get("mode") == "append" and not uses_batch:
        problems.append("mode append is not replay-safe; put {{ batch.id }} in path or {batch_id} in a name")
    return problems


CAPABILITIES: Final = EngineCapabilities(
    engine="spark",
    operation_kinds=frozenset({OperationKind.READ, OperationKind.WRITE}),
    arguments=MappingProxyType(
        {
            READ_NAME: dict.fromkeys(
                (
                    "path",
                    "format",
                    "options",
                    "on_missing",
                    "schema_mode",
                    "columns",
                    "column_groups",
                    "vocabulary",
                    "on_unmapped_column",
                    "path_pattern",
                    "row_index",
                    "on_mismatch",
                    "row_errors",
                    "inject_path_vars",
                )
            ),
            WRITE_NAME: dict.fromkeys(
                (
                    "path",
                    "format",
                    "mode",
                    "options",
                    "split_by",
                    "layout",
                    "file_name",
                    "single_file",
                    "max_splits",
                    "on_exists",
                )
            ),
        }
    ),
    checks=MappingProxyType({READ_NAME: check_read, WRITE_NAME: check_write}),
)
