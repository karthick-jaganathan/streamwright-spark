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

"""sw spark state: inspect and repair bookkeeping."""

import contextlib
import logging
import os

from streamwright.core.cli.job_state import add_state_commands, run_state_command
from streamwright.core.cli.log_options import log_setup
from streamwright.core.jobs import inputs as job_inputs
from streamwright.core.jobs.locking import applies_migrations
from streamwright.core.jobs.lookups import lookup_optional, state_inputs
from streamwright.core.jobs.manifest import manifest_directory
from streamwright.core.jobs.state import StateError, open_sqlite
from streamwright.core.jobs.state import open_store as open_dialect
from streamwright.core.jobs.state.target import describe_connection
from streamwright.core.runtime.logs.lines import fields
from streamwright.spark.cli.common import connections_of, load_plugins
from streamwright.spark.cli.inputs import config_values, provided_config, provided_secrets
from streamwright.spark.job import check_job, load_job, resolve

__all__ = ["add_arguments", "open_store", "run"]

LOG = logging.getLogger("streamwright.spark")

DESCRIPTION = """\
A SparkJob's batches, leases and state tables, in its runtime.state (sqlite, or a connection), per
runtime.state.scope. Every action takes the job and its config values (--config/--set select the scope for status,
and render the state connection); only the secrets of the state connection are needed. A retried batch resumes at
the job's next run, with the config values and lock tokens it pinned; show, retry and abandon take its job id (as
`state list` shows it); retry and abandon fail while a run of its scope holds them."""


class _InvalidJobError(Exception):
    """The job cannot be opened: its problems."""

    def __init__(self, problems):
        super().__init__("; ".join(problems))
        self.problems = problems


def add_arguments(parser):
    add_state_commands(parser, description=DESCRIPTION)


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


@contextlib.contextmanager
def _open_state(args, initialize):
    adapters, launchers, _ = load_plugins(args.allow_connector)
    job = load_job(args.job, args.overlay)
    problems, _ = check_job(job, adapters, launchers)
    if problems:
        raise _InvalidJobError(problems)
    given, bound = state_inputs(job, provided_config(args), provided_secrets(job, args),
                                allow_exec=args.allow_lookup_exec, base_dir=manifest_directory(args.job))
    job = lookup_optional(job)
    config = config_values(job, args)
    secrets = job_inputs.state_secrets(job, given)
    resolved, missing = resolve(job, config, secrets)
    missing = [text for text in missing if text.split(" ", 1)[0] not in bound]  # e.g. a scope from a fan_out item
    if missing:
        raise _InvalidJobError(missing)
    connections = connections_of(resolved, adapters=adapters, declared=job, config=config, secrets=secrets)
    store = open_store(resolved, connections, adapters, manifest_directory(args.job))
    if initialize:
        store.prepare(migrate=applies_migrations(resolved))
    yield resolved["name"], store, resolved["runtime"]["state"].get("scope") or ""


def run(args):
    setup = log_setup(args)
    if setup is None:
        return 2
    try:
        return run_state_command(args, _open_state)
    except _InvalidJobError as exc:
        for text in exc.problems:
            LOG.error("%s", text, extra=fields(event="job_invalid", status="failed"))
        return 2
    except StateError as exc:                             # e.g. a state migration is pending
        LOG.error("%s", exc, extra=fields(event="state_error", status="failed"))
        return 1
    finally:
        setup.close()
