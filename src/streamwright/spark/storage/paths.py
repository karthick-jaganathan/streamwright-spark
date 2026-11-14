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
Path safety for object-store and file adapters.

Jobs use the connectors' own URL forms (`s3://bucket/key`, `gs://bucket/key`, `az://container/key`, `/local/dir`);
the adapter translates to the Hadoop form (`s3a://`, `abfss://...`) only when it hands a path to Spark. A path must be
inside one of the connection's roots, with no `..` and no control characters.
"""

import os
import posixpath
import re
from urllib.parse import urlparse

__all__ = ["REDACTION_REGEX", "StorageError", "check_path", "inside", "normalize", "scheme_of", "spark_text"]

# Spark hides matching conf values in its UI and event logs
REDACTION_REGEX = "(?i)secret|password|token|access[.]key|private[.]key|account[.]key|credential|connection[.]string"
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


class StorageError(ValueError):
    pass


def scheme_of(path):
    return urlparse(path).scheme.lower() if "://" in path else ""


def normalize(path):
    """Slashes collapsed after the scheme; local paths normalized."""
    if _CONTROL.search(path):
        raise StorageError("a path may not contain control characters")
    scheme = scheme_of(path)
    if not scheme:
        return posixpath.normpath(path) + ("/" if path.endswith("/") and path != "/" else "")
    rest = path.split("://", 1)[1]
    rest = re.sub(r"/{2,}", "/", rest)
    return "%s://%s" % (scheme, rest)


def inside(root, path):
    root, path = normalize(root), normalize(path)
    if root.endswith("/"):
        return path.startswith(root) or path + "/" == root
    return path == root or path.startswith(root + "/")


def check_path(path, roots, label="path"):
    """Returns the path when it is inside one of `roots`; raises StorageError otherwise."""
    if not isinstance(path, str) or not path:
        raise StorageError("%s must be text" % label)
    if ".." in re.split(r"[/\\]", path):
        raise StorageError("%s may not contain '..'" % label)
    if not any(inside(root, path) for root in roots):
        raise StorageError("%s %r is outside this connection's roots" % (label, path))
    if not scheme_of(path):
        resolved = os.path.realpath(path)
        local_roots = [os.path.realpath(root) for root in roots if not scheme_of(root)]
        for root in local_roots:
            try:
                if os.path.commonpath((resolved, root)) == root:
                    return path
            except ValueError:  # paths on different volumes
                continue
        raise StorageError("%s %r resolves outside this connection's roots" % (label, path))
    return path


def spark_text(value):
    """An option value as Spark wants it: True -> "true" (str(True) would be "True")."""
    return str(value).lower() if isinstance(value, bool) else str(value)
