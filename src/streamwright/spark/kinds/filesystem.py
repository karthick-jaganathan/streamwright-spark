"""
The Filesystem kind: the Spark side of the object-store connectors (s3, gcs, azure_blob) and the local-files connector. PROPOSED.

`object.read` / `object.write` (`file.read` / `file.write` for the local `files` connector, which names its service
`file`) are defined ONCE here. A provider's adapter only supplies what differs: its connector, its secret keys, the URL
translation to Hadoop, the Hadoop settings and the jars. There is no branching on the provider in this module.

Read: the connector's own arguments (`path`, `format`, `options`, `on_missing`) are validated by the connector; Spark adds
`columns` / `vocabulary` / `on_mismatch` (per-client column mapping) and rejects `match` / `recursive` (DuckDB listing
features). Write: Spark-only (the connectors are read-only): modes, `single_file`, and split exports.
"""

import re

from streamwright.spark.contracts import Kind, Method, WriteReceipt, arg
from streamwright.spark.kinds.base import Provider
from streamwright.spark.storage import exports, ingest, naming
from streamwright.spark.storage.paths import StorageError, check_path, spark_text


__all__ = ["Filesystem", "FORMATS"]

FORMATS = ("csv", "tsv", "json", "jsonl", "parquet")
_EXT = {".csv": "csv", ".tsv": "tsv", ".tab": "tsv", ".json": "json", ".jsonl": "jsonl", ".ndjson": "jsonl",
        ".parquet": "parquet", ".pq": "parquet"}
_COMPRESSED = (".gz", ".gzip", ".zst", ".zstd", ".bz2", ".snappy")
_SPARK_FORMAT = {"csv": "csv", "tsv": "csv", "json": "json", "jsonl": "json", "parquet": "parquet"}
_OPTION_NAMES = {"delimiter": "sep"}
_READ_OPTIONS = ("header", "delimiter", "sep", "quote", "escape", "encoding", "multiLine", "dateFormat",
                 "timestampFormat", "nullValue", "compression")
_WRITE_OPTIONS = ("header", "delimiter", "sep", "quote", "escape", "encoding", "dateFormat", "timestampFormat",
                  "nullValue", "compression")

READ_ARGS = {
    "path": arg("any", True, description="a path, or {{ step.column }}: the files an earlier step named"),
    "format": arg("string", False, "auto", enum=("auto",) + FORMATS),
    "options": arg("object"),
    "on_missing": arg("string", False, "skip", enum=("skip", "error")),
    "match": arg("string", description="not supported for Spark reads"),
    "recursive": arg("boolean", description="not supported for Spark reads"),
    "columns": arg("any", description="{{ step }}: per-client column mapping rows (csv, tsv)"),
    "vocabulary": arg("any", description="{{ step }}: the allowed canonical names and types"),
    "on_mismatch": arg("string", False, "error", enum=("skip", "error")),
}
WRITE_ARGS = {
    "path": arg("string", True),
    "format": arg("string", True, enum=FORMATS),
    "mode": arg("string", False, "error", enum=("error", "overwrite", "append")),
    "options": arg("object"),
    "single_file": arg("boolean", False, False),
    "split_by": arg("list", description="one file per distinct combination of these columns"),
    "layout": arg("string", description="folder template, e.g. {client_id}/dt={today:%Y-%m-%d}"),
    "file_name": arg("string", description="file name template, e.g. {client_id}_{today}.csv.gz"),
    "max_splits": arg("integer", False, 500),
    "on_exists": arg("string", False, "error", enum=("error", "skip", "overwrite")),
}


def _format_of(path, fmt):
    if fmt != "auto":
        return fmt
    name = path.lower().split("?")[0]
    for ending in _COMPRESSED:
        if name.endswith(ending):
            name = name[:-len(ending)]
    for ending, found in _EXT.items():
        if name.endswith(ending):
            return found
    raise StorageError("cannot tell the format of %r; set format" % path.rsplit("/", 1)[-1])


def _spark_options(fmt, options, allowed=None):
    out = {}
    for key, value in (options or {}).items():
        if allowed is not None and key not in allowed:
            raise StorageError("option %r is not supported for Spark (supported: %s)" % (key, ", ".join(allowed)))
        out[_OPTION_NAMES.get(key, key)] = spark_text(value)
    if fmt == "tsv":
        out.setdefault("sep", "\t")
    return out


class Filesystem(Provider):
    service = "object"            # the `files` connector overrides this to "file"
    scheme = None                 # the Hadoop scheme written to Spark, e.g. "s3a", "abfss", "file"
    source_scheme = None          # the connector scheme, e.g. "s3", "gs" (None for local paths)
    endpoint = None               # S3-compatible endpoint (gcs, minio)
    per_bucket = False            # gcs: fs.s3a.bucket.<name>.* instead of global keys
    path_style = False
    default_credentials = False   # s3: no keys -> the default AWS credential chain
    jar_specs = ()

    @property
    def methods(self):
        return (
            Method(self.service, "read", Kind.READ, READ_ARGS, "read_objects", idempotent=True, check="check_read",
                   passthrough=("path", "format", "options", "on_missing", "match", "recursive")),
            Method(self.service, "write", Kind.WRITE, WRITE_ARGS, "write_objects", check="check_write"))

    # --- checks ----------------------------------------------------------------------------------------------

    def check_read(self, arguments):
        problems = []
        for key in ("match", "recursive"):
            if key in arguments:
                problems.append("%s is not supported for Spark reads (use a glob in path)" % key)
        if arguments.get("columns") is not None and arguments.get("format") not in ("csv", "tsv"):
            problems.append("columns needs format: csv or tsv (a header row is mapped per client)")
        if arguments.get("vocabulary") is not None and arguments.get("columns") is None:
            problems.append("vocabulary only applies together with columns")
        return problems

    def check_write(self, arguments):
        problems = []
        split_by = arguments.get("split_by") or []
        if not all(isinstance(c, str) and re.match(r"^[A-Za-z_]\w*$", c) for c in split_by):
            return problems + ["split_by must be a list of column names"]
        parsed = {}
        for key, slash in (("file_name", False), ("layout", True)):
            if arguments.get(key) is None:
                continue
            try:
                parsed[key] = naming.parse_template(arguments[key], split_by, slash, key)
            except naming.NamingError as exc:
                problems.append(str(exc))
        if (split_by or arguments.get("layout")) and "file_name" not in arguments:
            problems.append("split_by and layout need a file_name")
        if not split_by and any(t.columns for t in parsed.values()):
            problems.append("names use columns but split_by is empty")
        unused = [c for c in split_by if not any(c in t.columns for t in parsed.values())]
        if split_by and parsed and unused:
            problems.append("split_by column(s) %s appear in neither file_name nor layout, so their splits would "
                            "collide" % ", ".join(unused))
        path = str(arguments.get("path", ""))
        uses_batch = bool(re.search(r"\{\{\s*batch\.id\s*\}\}", path)) or any(t.uses_batch_id for t in parsed.values())
        if arguments.get("mode") == "append" and not uses_batch:
            problems.append("mode append is not replay-safe; put {{ batch.id }} in path or {batch_id} in a name")
        return problems

    def warn_call(self, method, arguments):
        if method.name == "write" and arguments.get("mode", "error") == "error" and arguments.get("split_by") and not (
                re.search(r"\{\{\s*batch\.id\s*\}\}", str(arguments.get("path", ""))) or "{batch_id}" in str(
                    arguments.get("file_name", ""))):
            return ["a replay of this batch would fail on existing files; use mode overwrite or on_exists"]
        return []

    # --- reads -----------------------------------------------------------------------------------------------

    def read_objects(self, rt, conn, step_name, arguments):
        auth = conn.auth
        paths = [check_path(p, self.roots(auth)) for p in rt.path_values(arguments["path"])]
        fmt = _format_of(paths[0], arguments.get("format", "auto")) if paths else arguments.get("format", "csv")
        fmt = "csv" if fmt == "auto" else fmt
        options = _spark_options(fmt, arguments.get("options"), _READ_OPTIONS)
        missing = [p for p in paths if not rt.exists(self.to_hadoop(p, auth))]
        if missing and arguments.get("on_missing", "skip") == "error":
            raise StorageError("%d file(s) are missing, e.g. %s" % (len(missing), missing[0].rsplit("/", 1)[-1]))
        for path in missing:
            rt.add_file_error(step_name, path, "file not found")
        paths = [p for p in paths if p not in missing]
        if arguments.get("columns") is None:
            if not paths:
                return rt.empty("value STRING")
            return rt.spark.read.format(_SPARK_FORMAT[fmt]).options(**options).load([self.to_hadoop(p, auth) for p in paths])
        mappings = rt.expand(arguments["columns"])
        vocabulary = rt.expand(arguments["vocabulary"]) if arguments.get("vocabulary") is not None else None
        ingest.check_vocabulary(mappings, vocabulary)
        headers = {}
        for path in paths:
            line = rt.first_line(self.to_hadoop(path, auth))
            headers[path] = ingest.parse_header(line, options.get("sep", ",")) if line else []
        groups, rejects = ingest.plan_groups(paths, mappings, headers, rt.max_groups)
        for reject in rejects:
            rt.add_file_error(step_name, reject.path, reject.reason)
        if rejects and arguments.get("on_mismatch", "error") == "error":
            raise StorageError("%d file(s) did not match their column mapping, e.g. %s" % (len(rejects), rejects[0].reason))
        frame = ingest.read_groups(rt.spark, groups, "csv", options, to_hadoop=lambda p: self.to_hadoop(p, auth),
                                   source_file=self.source_file)
        return frame if frame is not None else rt.empty("_source_file STRING")

    # --- writes ----------------------------------------------------------------------------------------------

    def write_objects(self, rt, conn, output, method, arguments, frame):
        from pyspark.sql import Observation as SparkObservation
        from pyspark.sql import functions as F
        from streamwright.spark.contracts import Observation

        auth = conn.auth
        root = check_path(rt.text(arguments["path"]), self.roots(auth))
        uri = self.to_hadoop(root, auth)
        fmt = arguments["format"]
        if arguments.get("split_by") or arguments.get("layout") or arguments.get("file_name"):
            columns = list(arguments.get("split_by") or [])
            file_template = naming.parse_template(arguments["file_name"], columns, False, "file_name")
            layout_template = naming.parse_template(arguments["layout"], columns, True, "layout") \
                if arguments.get("layout") else None
            options = dict(arguments, options=_spark_options(fmt, arguments.get("options"), _WRITE_OPTIONS),
                           format=_SPARK_FORMAT[fmt])
            return exports.write_split_export(rt.spark, frame, rt.ctx, output, options, uri, file_template, layout_template)
        metrics = SparkObservation("object_" + output)
        frame = frame.observe(metrics, F.count(F.lit(1)).alias("rows"))
        if arguments.get("single_file"):
            frame = frame.coalesce(1)
        writer = frame.write.format(_SPARK_FORMAT[fmt]).mode(arguments.get("mode", "error"))
        for key, value in _spark_options(fmt, arguments.get("options"), _WRITE_OPTIONS).items():
            writer = writer.option(key, value)
        writer.save(uri)                                           # the single action
        rows = int(metrics.get.get("rows", 0))
        return WriteReceipt(output, root, rows, observations=[Observation(output, {"rows": rows})])

    # --- Spark facts (generic, from the leaf's attributes) ---------------------------------------------------

    def jars(self, spark_minor):
        return [tuple(spec) for spec in self.jar_specs]

    def prepare_auth(self, auth):
        auth = dict(auth)
        if self.default_credentials and "key_id" not in auth and "secret" not in auth:
            auth["key_id"] = auth["secret"] = "{{ secrets.instance_profile }}"
        return auth

    def to_hadoop(self, url, auth=None):
        if self.source_scheme and url.startswith(self.source_scheme + "://"):
            return self.scheme + "://" + url[len(self.source_scheme) + 3:]
        if self.scheme == "file" and "://" not in url and not url.startswith("file:"):
            return "file://" + url
        return url

    def source_file(self, column):
        from pyspark.sql import functions as F
        if self.scheme == "file":
            return F.regexp_replace(column, "^file:(//)?", "")
        if self.source_scheme:
            return F.regexp_replace(column, "^%s://" % self.scheme, self.source_scheme + "://")
        return column

    def hadoop_conf(self, auth):
        if self.scheme != "s3a":
            return {}
        return self._per_bucket_conf(auth) if self.per_bucket else self._global_conf(auth)

    def _global_conf(self, auth):
        conf = {}
        if auth.get("endpoint"):
            conf["fs.s3a.endpoint"] = str(auth["endpoint"])
            conf["fs.s3a.path.style.access"] = "true" if auth.get("url_style") == "path" else "false"
            conf["fs.s3a.connection.ssl.enabled"] = "false" if auth.get("use_ssl") is False else "true"
        if auth.get("region"):
            conf["fs.s3a.endpoint.region"] = str(auth["region"])
        if auth.get("key_id") and auth.get("secret"):
            conf["fs.s3a.access.key"] = str(auth["key_id"])
            conf["fs.s3a.secret.key"] = str(auth["secret"])
            if auth.get("session_token"):
                conf["fs.s3a.session.token"] = str(auth["session_token"])
                conf["fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            else:
                conf["fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        return conf

    def _per_bucket_conf(self, auth):
        conf = {}
        buckets = set(m.group(1) for m in (re.match(r"^%s://([^/]+)" % (self.source_scheme or "gs"), r)
                                           for r in auth.get("roots") or []) if m)
        for bucket in sorted(buckets):
            prefix = "fs.s3a.bucket.%s." % bucket
            if self.endpoint:
                conf[prefix + "endpoint"] = self.endpoint
            conf[prefix + "path.style.access"] = "true" if self.path_style else "false"
            conf[prefix + "access.key"] = str(auth.get("key_id"))
            conf[prefix + "secret.key"] = str(auth.get("secret"))
            conf[prefix + "aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        return conf
