"""
What an adapter's read/write handler may use while a batch runs. PROPOSED. Not run yet: needs Spark.

Handlers receive this object (`rt`) instead of reaching into the executor, so an adapter written by someone else needs
only this surface: the session and batch context, pinned reference values, the file-error list, small file-system
helpers (driver side, no Spark job), and SQL-literal rendering for predicates.
"""

from streamwright.spark.job.resolve import references
from streamwright.spark.runtime import refs


__all__ = ["StepRuntime"]


class StepRuntime(object):
    def __init__(self, spark, ctx, manifest, connections, adapters, limits):
        self.spark, self.ctx = spark, ctx
        self.pinned = manifest.setdefault("pinned", {})
        self.connections, self.adapters = connections, adapters
        self.cap = int((limits or {}).get("reference_values", 10000))
        self.max_groups = int((limits or {}).get("read_groups", 200))
        self.file_errors = []

    # --- pinned reference values (collected once per batch, then replayed from the manifest) ------------------

    def values(self, step, column):
        key = "%s.%s" % (step, column)
        if key not in self.pinned:
            rows = self.spark.table(step).select(column).distinct().limit(self.cap + 1).collect()
            if len(rows) > self.cap:
                raise ValueError("{{ %s }} has more than %d distinct values (runtime.limits.reference_values)" % (key, self.cap))
            self.pinned[key] = [refs.encode_value(r[0]) for r in rows]
        return [refs.decode_value(v) for v in self.pinned[key]]

    def rows(self, step):
        key = "%s.*" % step
        if key not in self.pinned:
            rows = self.spark.table(step).limit(self.cap + 1).collect()
            if len(rows) > self.cap:
                raise ValueError("{{ %s }} has more than %d rows (runtime.limits.reference_values)" % (step, self.cap))
            self.pinned[key] = [dict((k, refs.encode_value(v)) for k, v in r.asDict().items()) for r in rows]
        return [dict((k, refs.decode_value(v)) for k, v in row.items()) for row in self.pinned[key]]

    def path_values(self, value):
        """A path argument: text, a list, or {{ step.column }} (the distinct values of that column)."""
        found = refs.step_columns_used(value) if isinstance(value, str) else []
        if found:
            step, column = found[0]
            return [str(v) for v in self.values(step, column) if v is not None]
        return list(value) if isinstance(value, list) else [value]

    def expand(self, value):
        """A whole-step reference {{ step }}: that step's pinned rows; anything else is returned as it is."""
        found = references(value) if isinstance(value, str) else []
        return self.rows(found[0].strip()) if found else value

    # --- text and predicates ---------------------------------------------------------------------------------

    def text(self, value):
        """Plain-text substitution for paths and names ({{ batch.id }}). Never used for SQL."""
        if not isinstance(value, str):
            return value
        return value.replace("{{ batch.id }}", self.ctx.batch_id).replace("{{batch.id}}", self.ctx.batch_id)

    def predicate(self, text):
        """A SQL fragment with {{ step.column }} rendered as escaped literals from the pinned values."""
        values = dict(((s, c), self.values(s, c)) for s, c in refs.step_columns_used(text))
        return refs.substitute(text, values, self.ctx.batch_id, self.ctx.batch_ts, self.cap)

    # --- connections -----------------------------------------------------------------------------------------

    def storage_of(self, conn):
        """(adapter, Connection) of the object-store connection named by `conn.auth['storage']`."""
        target = self.connections[conn.auth["storage"]]
        return self.adapters[target.provider], target

    # --- file system (driver side) ---------------------------------------------------------------------------

    def _fs(self, uri):
        path = self.spark._jvm.org.apache.hadoop.fs.Path(uri)
        return path, path.getFileSystem(self.spark._jsc.hadoopConfiguration())

    def exists(self, uri):
        path, fs = self._fs(uri)
        if any(ch in uri for ch in "*?["):
            matches = fs.globStatus(path)
            return matches is not None and len(matches) > 0
        return fs.exists(path)

    def first_line(self, uri):
        jvm = self.spark._jvm
        conf = self.spark._jsc.hadoopConfiguration()
        path, fs = self._fs(uri)
        if any(ch in uri for ch in "*?["):
            matches = fs.globStatus(path)
            path = matches[0].getPath()
        codec = jvm.org.apache.hadoop.io.compress.CompressionCodecFactory(conf).getCodec(path)
        stream = fs.open(path)
        if codec is not None:
            stream = codec.createInputStream(stream)
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
        try:
            return reader.readLine()
        finally:
            reader.close()

    # --- results ---------------------------------------------------------------------------------------------

    def add_file_error(self, step, path, reason):
        self.file_errors.append((step, path, reason))

    def empty(self, ddl):
        return self.spark.createDataFrame([], ddl)
