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

"""Where a SparkJob's bookkeeping lives (runtime.state): a sqlite file, or a connection's state database. Its batches
are looked after with `streamwright state ACTION --url URL`."""

import os

from streamwright.core.jobs.state import open_sqlite
from streamwright.core.jobs.state import open_store as open_dialect
from streamwright.core.jobs.state.target import describe_connection

__all__ = ["open_store"]


def open_store(resolved, connections, adapters, base_dir):
    """The bookkeeping store named by runtime.state: a sqlite file, or the connection's own adapter."""
    state = resolved["runtime"]["state"]
    if "sqlite" in state:
        path = state["sqlite"]
        if resolved["runtime"]["platform"]["type"] != "local" and not os.path.isabs(path):
            raise ValueError("remote jobs require an absolute runtime.state.sqlite path")
        if path != ":memory:" and not os.path.isabs(path):
            path = os.path.join(base_dir or os.getcwd(), path)
        return open_sqlite(path)
    conn = connections[state["connection"]]
    connect, dialect = adapters[conn.provider].state_connection(conn.auth)
    schema = state.get("schema", "streamwright")
    return open_dialect(connect, dialect, schema=schema,
                        target=describe_connection(state["connection"], conn.provider, conn.auth, schema))
