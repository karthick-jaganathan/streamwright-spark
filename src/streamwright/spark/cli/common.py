"""Pieces shared by the sw spark commands. PROPOSED."""

import sys

from streamwright.spark.contracts import Connection
from streamwright.spark.runtime import registry


__all__ = ["err", "add_job_arguments", "load_plugins", "parse_set", "connections_of", "InputsError"]


class InputsError(Exception):
    pass


def err(text):
    sys.stderr.write(text + "\n")


def add_job_arguments(parser, many=False):
    if many:
        parser.add_argument("job", nargs="*", metavar="PATH",
                            help="path to the job definition YAML file(s)")
        parser.add_argument("--job", "-j", action="append", default=[], dest="flag_jobs",
                            help="path to the job definition YAML file (can be specified multiple times)")
        parser.add_argument("--file", "-f", action="append", default=[], dest="flag_files",
                            help="alias for --job")
        parser.add_argument("--pipeline", action="append", default=[], dest="flag_pipelines",
                            help="alias for --job")
    else:
        parser.add_argument("job", nargs="?", metavar="JOB",
                            help="path to the job definition YAML file or directory")
        parser.add_argument("--job", "-j", dest="flag_job",
                            help="path to the job definition YAML file or directory")
        parser.add_argument("--file", "-f", dest="flag_file",
                            help="alias for --job")
        parser.add_argument("--pipeline", dest="flag_pipeline",
                            help="alias for --job")
    parser.add_argument("--config", help="a YAML file with a `config:` mapping (values for spec.config)")
    parser.add_argument("--set", action="append", default=[], metavar="NAME=VALUE",
                        help="override a spec.config value (e.g. --set key=value)")
    parser.add_argument("--overlay", action="append", default=[], help="YAML merged over the job; a null deletes a key")
    parser.add_argument("--allow-connector", action="append", default=None, metavar="PROVIDER",
                        help="only load these providers")


def load_plugins(allow=None):
    """({provider: adapter}, {platform: launcher}, problems)."""
    adapters, problems = registry.load_all("adapter", allow)
    launchers, more = registry.load_all("launcher")
    return adapters, launchers, problems + more


def parse_set(items):
    values = {}
    for item in items or []:
        name, sep, value = item.partition("=")
        if not sep or not name:
            raise InputsError("--set expects NAME=VALUE, got %r" % name)
        values[name] = value
    return values


def connections_of(job):
    """{name: Connection} from a resolved job: `provider` apart, the rest is the auth block."""
    return dict((name, Connection(name, conn["provider"], dict((k, v) for k, v in conn.items() if k != "provider")))
                for name, conn in (job.get("connections") or {}).items())
