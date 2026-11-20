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
A SparkJob's `source:` steps on the driver: where the source folder is (relative to a local manifest's folder, or
copied from a remote manifest's folder) and the typed empty frame of a stream that returned no rows.
"""

import os
import posixpath
import re
import tempfile
from types import MappingProxyType

from streamwright.core.jobs.manifest import JobError, manifest_directory
from streamwright.spark.launchers.common import FOLDER_READERS

__all__ = ["empty_frame", "source_folder"]

_MAX_FOLDER_BYTES = 5 * 1024 * 1024
_REMOTE = re.compile(r"^[a-z][a-z0-9+.-]*://")
# DuckDB column types of a source export, as Spark SQL types (anything else is read as text)
_SPARK_TYPES = MappingProxyType({"BIGINT": "BIGINT", "INTEGER": "INT", "SMALLINT": "SMALLINT", "TINYINT": "TINYINT",
                "DOUBLE": "DOUBLE", "FLOAT": "FLOAT", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
                "TIMESTAMP": "TIMESTAMP", "TIMESTAMP WITH TIME ZONE": "TIMESTAMP", "VARCHAR": "STRING"})


def source_folder(step, job_location):
    """
    The local folder of a `source:` step: a local path as it is (a relative one under a local manifest's folder),
    or a remote folder (an absolute URI, or a path relative to a remote manifest's folder) copied into a temporary
    folder. JobError when there is none.
    """
    path, manifest = step["source"], str(job_location or "")
    remote = path if _REMOTE.match(path) else None
    if remote is None and not os.path.isabs(path) and _REMOTE.match(manifest):
        remote = _remote_join(manifest, path, step)
    if remote is not None:
        reader = FOLDER_READERS.get(remote.split("://", 1)[0])
        if reader is None:
            raise JobError("step %s: source %r: remote source folders are read from %s" % (
                step["name"], path, ", ".join("%s://" % scheme for scheme in FOLDER_READERS)))
        local = tempfile.mkdtemp(prefix="streamwright_source_")
        reader(remote, local, _MAX_FOLDER_BYTES)
        return local
    if not os.path.isabs(path):
        base = manifest_directory(job_location) if job_location else None
        if base is None:
            raise JobError("step %s: a relative source needs the job's folder (run the job from a file)" % step["name"])
        path = os.path.join(base, path)
    if not os.path.isdir(path):
        raise JobError("step %s: source %r is not a folder (relative paths start at the job's folder)" % (
            step["name"], step["source"]))
    return path


def _remote_join(manifest, relative, step):
    """`relative` under the folder of the remote manifest `manifest` (a folder URI ends with /), as a URI."""
    scheme, _, rest = manifest.partition("://")
    bucket, _, key = rest.partition("/")
    folder = key.rstrip("/") if key.endswith("/") or not key.endswith((".yaml", ".yml")) else posixpath.dirname(key)
    joined = posixpath.normpath(posixpath.join(folder, relative))
    if joined.startswith(".."):
        raise JobError("step %s: source %r is outside the bucket of the job" % (step["name"], relative))
    return "%s://%s/%s" % (scheme, bucket, joined)


def empty_frame(spark, columns):
    """No rows, with the source export's columns (DuckDB types as Spark's), so later steps still resolve them."""
    fields = ", ".join("`%s` %s" % (name.replace("`", "``"), _spark_type(kind)) for name, kind in columns)
    return spark.createDataFrame([], fields)


def _spark_type(kind):
    text = str(kind).upper()
    if text.startswith("DECIMAL"):
        return text
    return _SPARK_TYPES.get(text, "STRING")
