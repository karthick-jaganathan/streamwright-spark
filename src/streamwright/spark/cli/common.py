"""Pieces shared by the sw spark commands."""

import sys

from streamwright.core.jobs.inputs import InputError as InputsError
from streamwright.core.jobs.inputs import parse_set
from streamwright.core.sdk.contract import resolve_connection
from streamwright.spark.contracts import Connection
from streamwright.spark.runtime import registry

__all__ = ["InputsError", "add_job_arguments", "connections_of", "err", "load_plugins", "parse_set"]


def err(text):
    sys.stderr.write(text + "\n")


def add_job_arguments(parser, many=False):
    if many:
        parser.add_argument("job", nargs="*", metavar="PATH", help="path to the job definition YAML file(s)")
        parser.add_argument(
            "--job",
            "-j",
            action="append",
            default=[],
            dest="flag_jobs",
            help="path to the job definition YAML file (can be specified multiple times)",
        )
        parser.add_argument("--file", "-f", action="append", default=[], dest="flag_files", help="alias for --job")
        parser.add_argument("--pipeline", action="append", default=[], dest="flag_pipelines", help="alias for --job")
    else:
        parser.add_argument("job", nargs="?", metavar="JOB", help="path to the job definition YAML file or directory")
        parser.add_argument("--job", "-j", dest="flag_job", help="path to the job definition YAML file or directory")
        parser.add_argument("--file", "-f", dest="flag_file", help="alias for --job")
        parser.add_argument("--pipeline", dest="flag_pipeline", help="alias for --job")
    parser.add_argument("--config", help="a YAML file with a `config:` mapping (values for spec.config)")
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="override a spec.config value (e.g. --set key=value)",
    )
    parser.add_argument("--overlay", action="append", default=[], help="YAML merged over the job; a null deletes a key")
    parser.add_argument(
        "--allow-connector", action="append", default=None, metavar="PROVIDER", help="only load these providers"
    )


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
