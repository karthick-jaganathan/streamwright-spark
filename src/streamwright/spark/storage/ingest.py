"""
Per-client column mapping for `object.read` with `columns:`. PROPOSED.

Planning is pure Python (testable without Spark); only `read_groups()` touches Spark.

Inputs
  files      the file URIs the batch named
  mappings   rows of (prefix, header, canonical, type, required); one client's rows share a prefix
  headers    {file: [header, ...]}, read on the driver (first line only, no Spark job)
  vocabulary {canonical name: type}; every mapped name must be in it with the same type

Per file: the mapping rows are those with the *longest* prefix that the file URI starts with (startswith, so `_` and
`%` in a prefix are literal). Header cells are matched by exact text after stripping spaces. Unmapped columns are
dropped. A file is rejected (reason recorded) when a required canonical column is missing, when two columns map to the
same name, or when no mapping applies. Files are grouped by (prefix, header signature) so each group is one Spark
read; the groups are cast to the canonical types with try_cast (a bad cell becomes null, never fails the batch) and
combined with unionByName.
"""

import csv
import io
import re

from streamwright.spark.storage.paths import spark_text


__all__ = ["IngestError", "TYPE_PATTERN", "plan_groups", "parse_header", "check_vocabulary", "FileReject", "Group"]

TYPE_PATTERN = re.compile(r"^(string|int|bigint|long|double|float|boolean|date|timestamp|decimal\([0-9]{1,2},[0-9]{1,2}\))$")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_TRUE = ("true", "t", "yes", "y", "1")


class IngestError(Exception):
    """A problem with the job's mapping data itself (not one client's file): the batch must fail."""


class FileReject(object):
    def __init__(self, path, reason):
        self.path, self.reason = path, reason


class Group(object):
    def __init__(self, prefix, header, columns, files):
        self.prefix, self.header, self.columns, self.files = prefix, header, columns, files   # columns: [(src, name, type)]


def parse_header(line, sep=","):
    """The cells of a CSV header line, stripped."""
    rows = list(csv.reader(io.StringIO(line.lstrip("﻿")), delimiter=sep))
    return [cell.strip() for cell in rows[0]] if rows else []


def _is_true(value):
    return value is True or str(value).strip().lower() in _TRUE


def check_vocabulary(mappings, vocabulary):
    """Raises IngestError for mapping rows that are malformed or outside the controlled vocabulary."""
    problems = []
    vocab = dict((row["name"], str(row["type"]).strip().lower()) for row in vocabulary) if vocabulary else None
    for number, row in enumerate(mappings):
        label = "mapping row %d" % number
        for key in ("prefix", "header", "canonical", "type"):
            if row.get(key) in (None, ""):
                problems.append("%s: %s is empty" % (label, key))
        canonical, kind = str(row.get("canonical", "")), str(row.get("type", "")).strip().lower()
        if canonical and not _NAME.match(canonical):
            problems.append("%s: canonical name %r is not a plain identifier" % (label, canonical))
        if kind and not TYPE_PATTERN.match(kind):
            problems.append("%s: type %r is not supported" % (label, kind))
        if vocab is not None and canonical:
            if canonical not in vocab:
                problems.append("%s: %r (prefix %s) is not in the controlled vocabulary" % (
                    label, canonical, row.get("prefix")))
            elif kind and vocab[canonical] != kind:
                problems.append("%s: %r has type %s but the vocabulary says %s" % (label, canonical, kind, vocab[canonical]))
    if problems:
        raise IngestError("; ".join(problems[:20]) + (" (and %d more)" % (len(problems) - 20) if len(problems) > 20 else ""))


def plan_groups(files, mappings, headers, max_groups=200):
    """Returns (groups, rejects). Raises IngestError when the mapping data is wrong or there are too many groups."""
    by_prefix = {}
    for row in mappings:
        by_prefix.setdefault(str(row["prefix"]), []).append(row)
    prefixes = sorted(by_prefix, key=len, reverse=True)
    groups, rejects = {}, []
    for path in sorted(set(files)):
        prefix = next((p for p in prefixes if path.startswith(p)), None)
        if prefix is None:
            rejects.append(FileReject(path, "no column mapping for this path"))
            continue
        header = headers.get(path)
        if not header:
            rejects.append(FileReject(path, "empty or unreadable header"))
            continue
        by_header = {}
        for row in by_prefix[prefix]:
            by_header[str(row["header"]).strip()] = row
        chosen, seen, problem = [], {}, None
        for cell in header:
            row = by_header.get(cell)
            if row is None:
                continue
            name = str(row["canonical"])
            if name in seen:
                problem = "columns %r and %r both map to %s" % (seen[name], cell, name)
                break
            seen[name] = cell
            chosen.append((cell, name, str(row["type"]).strip().lower()))
        if problem is None:
            required = sorted(set(str(r["canonical"]) for r in by_prefix[prefix] if _is_true(r.get("required"))))
            missing = [name for name in required if name not in seen]
            if missing:
                problem = "missing required column(s): %s" % ", ".join(missing)
        if problem is None and not chosen:
            problem = "no header cell matches a mapped column"
        if problem is not None:
            rejects.append(FileReject(path, problem))
            continue
        key = (prefix, tuple(header))
        group = groups.get(key)
        if group is None:
            groups[key] = Group(prefix, list(header), chosen, [path])
        else:
            group.files.append(path)
    if len(groups) > max_groups:
        raise IngestError("%d read groups exceed runtime.limits.read_groups (%d): clients' headers vary more than "
                          "expected" % (len(groups), max_groups))
    return [groups[key] for key in sorted(groups)], rejects


def read_groups(spark, groups, fmt, options, to_hadoop=None, source_file=None, tracking_column="_source_file"):
    """
    One lazy DataFrame: each group is read once (all strings), renamed and try_cast to the canonical types, with the
    source file added; groups are combined with unionByName so a client's missing columns become null.

    `to_hadoop(path)` turns a connector-style path into the Hadoop form for the read; `source_file(column)` turns
    input_file_name() back into the connector-style path, so `_source_file` joins with the paths the messages carry.
    """
    from pyspark.sql import functions as F

    frames = []
    for group in groups:
        reader = spark.read.format(fmt).options(**dict((k, spark_text(v)) for k, v in (options or {}).items()))
        reader = reader.option("header", "true").option("inferSchema", "false")
        frame = reader.load([to_hadoop(p) if to_hadoop else p for p in group.files])
        columns = []
        for src, name, kind in group.columns:
            quoted = "`%s`" % src.replace("`", "``")
            columns.append(F.expr("try_cast(%s AS %s)" % (quoted, kind)).alias(name))
        origin = F.input_file_name()
        columns.append((source_file(origin) if source_file else origin).alias(tracking_column))
        frames.append(frame.select(*columns))
    if not frames:
        return None
    result = frames[0]
    for other in frames[1:]:
        result = result.unionByName(other, allowMissingColumns=True)
    return result
