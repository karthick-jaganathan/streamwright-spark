#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""
What an adapter's read/write handler may use while a batch runs.

Handlers receive this object (`rt`) instead of reaching into the executor, so an adapter written by someone else needs
only this surface: the session and batch context, pinned reference values, the file-error list, small file-system
helpers (driver side, no Spark job), and SQL-literal rendering for predicates.
"""

from streamwright.core.jobs import refs
from streamwright.core.jobs.refs import references
from streamwright.spark.io.sql import quote_literal
from streamwright.spark.job.vocabulary import limit

__all__ = ["StepRuntime"]


class StepRuntime:
    def __init__(self, spark, ctx, manifest, connections, adapters, *, limits, state=None):
        self.spark, self.ctx = spark, ctx
        self.pinned = manifest.setdefault("pinned", {})
        self.connections, self.adapters = connections, adapters
        self.cap = limit(limits, "reference_values")
        self.max_groups = limit(limits, "read_groups")
        self.max_files_per_batch = limit(limits, "max_files_per_batch")
        self.file_errors = []
        self.schema_reports = []
        self.state = state
        self.previous_signatures = {}
        if state is not None:
            self.previous_signatures = state.get_schema_signatures(ctx.job_name)

    # --- pinned reference values (collected once per batch, then replayed from the manifest) ------------------

    def values(self, step, column):
        key = "%s.%s" % (step, column)
        if key not in self.pinned:
            rows = self.spark.table(step).select(column).distinct().limit(self.cap + 1).collect()
            if len(rows) > self.cap:
                raise ValueError(
                    "{{ %s }} has more than %d distinct values (runtime.limits.reference_values)" % (key, self.cap)
                )
            self.pinned[key] = [refs.encode_value(r[0]) for r in rows]
        return [refs.decode_value(v) for v in self.pinned[key]]

    def rows(self, step):
        key = "%s.*" % step
        if key not in self.pinned:
            rows = self.spark.table(step).limit(self.cap + 1).collect()
            if len(rows) > self.cap:
                raise ValueError("{{ %s }} has more than %d rows (runtime.limits.reference_values)" % (step, self.cap))
            self.pinned[key] = [{k: refs.encode_value(v) for k, v in r.asDict().items()} for r in rows]
        return [{k: refs.decode_value(v) for k, v in row.items()} for row in self.pinned[key]]

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

    def builtins(self):
        """The `{{ }}` constants of the batch (job.vocabulary.BUILTIN_REFERENCES), by name."""
        host = self.ctx.engine_metadata.get("driver_host") or "unknown"
        return {"batch.id": self.ctx.batch_id, "batch.ts": self.ctx.batch_ts,
                "batch.id_safe": self.ctx.batch_id.replace("-", "_"), "batch.seq": self.ctx.batch_sequence,
                "driver.host": host, "driver_host": host}

    def text(self, value, extra=None):
        """Substitute the batch constants and `extra` tokens (paths, names: {{ batch.id }}, {{ uuid }}), never SQL."""
        if not isinstance(value, str):
            return value
        tokens = dict(self.builtins(), **(extra if isinstance(extra, dict) else {}))
        for name, token in tokens.items():
            value = value.replace("{{ %s }}" % name, str(token)).replace("{{%s}}" % name, str(token))
        return value

    def predicate(self, text):
        """A SQL fragment with {{ step.column }} rendered as escaped literals from the pinned values."""
        constants = self.builtins()
        values = {(step, column): self.values(step, column) for step, column in refs.step_columns_used(text)
                  if "%s.%s" % (step, column) not in constants}
        return refs.substitute(text, values, constants, self.cap, quote_text=quote_literal)

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

    def add_schema_report(self, report):
        self.schema_reports.append(report)

    def empty(self, ddl):
        return self.spark.createDataFrame([], ddl)
