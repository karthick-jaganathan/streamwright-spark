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

"""Pieces shared by the sw spark commands."""

import sys

from streamwright.core.cli.jobs import JOB_HELP
from streamwright.core.jobs.inputs import InputError as InputsError
from streamwright.core.jobs.inputs import parse_set
from streamwright.core.sdk.contract import resolve_connection
from streamwright.spark.contracts import Connection
from streamwright.spark.runtime import registry

__all__ = ["InputsError", "add_job_arguments", "connections_of", "err", "load_plugins", "parse_set"]


def err(text):
    sys.stderr.write(text + "\n")


def add_job_arguments(parser, many=False, inputs=True):
    """The job path options (JOB or --job); `inputs`: also --config, --set."""
    if many:
        parser.add_argument("job", nargs="*", metavar="JOB", help="job manifests: YAML files, or their folders")
        parser.add_argument("--job", "-j", action="append", default=[], dest="flag_jobs", metavar="JOB",
                            help="a job manifest, as an option (repeatable)")
    else:
        parser.add_argument("job", nargs="?", metavar="JOB", help=JOB_HELP["job"])
        parser.add_argument("--job", "-j", dest="flag_job", metavar="JOB", help=JOB_HELP["job_option"])
    if inputs:
        parser.add_argument("--config", metavar="FILE", help=JOB_HELP["config"])
        parser.add_argument("--set", action="append", default=[], metavar="NAME=VALUE", help=JOB_HELP["set"])
    parser.add_argument("--overlay", action="append", default=[], metavar="FILE", help=JOB_HELP["overlay"])
    parser.add_argument("--allow-connector", action="append", default=None, metavar="NAME",
                        help=JOB_HELP["allow_connector"])


def load_plugins(allow=None):
    """({provider: adapter}, {platform: launcher}, problems)."""
    adapters, problems = registry.load_all("adapter", allow)
    launchers, more = registry.load_all("launcher")
    return adapters, launchers, problems + more


def connections_of(job, *, adapters=None, declared=None, config=None, secrets=None):
    """
    {name: Connection} of the connections the Spark engine runs natively (a Spark adapter): `provider` apart, the rest
    is the auth block. A provider without a Spark adapter is reached only through call steps, which resolve its
    connection in the source engine.
    """
    connections = {}
    for name, connection in (job.get("connections") or {}).items():
        provider = connection["provider"]
        adapter = (adapters or {}).get(provider)
        if adapter is None:
            continue
        original = (declared or job)["connections"][name]
        auth = {key: value for key, value in original.items() if key != "provider"}
        values = resolve_connection(adapter.definition, auth, config=config or {}, secrets=secrets or {})
        connections[name] = Connection(name, provider, values)
    return connections
