"""sw spark validate: check jobs without Spark, brokers or credentials."""

import contextlib

from streamwright.core.jobs.inputs import InputError, parse_set, read_config_values
from streamwright.spark.cli.common import err
from streamwright.spark.job import validate as validate_job
from streamwright.spark.plan import build_plan
from streamwright.spark.runtime.registry import Registry, RegistryError

__all__ = ["add_arguments", "run"]


def add_arguments(parser):
    parser.add_argument("--analyze", action="store_true", help="print the Spark actions and recomputation warnings")
    parser.add_argument("--graph", action="store_true", help="print step dependencies")
    parser.add_argument("--strict", action="store_true", help="warnings fail the check")
    parser.add_argument("--lock", help="path to a lock file to verify compatibility against")


def run(args):
    code = 0
    for path in args.job:
        try:
            config = read_config_values(args.config) if args.config else {}
            config.update(parse_set(args.set))
        except InputError as exc:
            err("%s: %s" % (path, exc))
            code = 2
            continue
        report = validate_job(
            path, overlays=args.overlay, allow=args.allow_connector, lock=args.lock, strict=args.strict, config=config
        )
        job = report.job or {}
        registry = Registry()
        adapters = {}
        for connection in (job.get("connections") or {}).values():
            if not isinstance(connection, dict):
                continue
            provider = connection.get("provider")
            if provider and provider not in adapters:
                with contextlib.suppress(RegistryError):
                    adapters[provider] = registry.load(provider, allow=args.allow_connector)
        if report.valid and (args.analyze or args.graph):
            plan = build_plan(job, adapters)
            if args.analyze:
                print(plan.describe())
            if args.graph:
                for node in plan.nodes:
                    print("%-8s %-22s <- %s" % (node.kind, node.name, ", ".join(node.uses) or "-"))
        for issue in report.issues:
            err("%s: %s: %s" % (path, issue.severity, issue.message))
        if report.valid and args.analyze:
            for warning in build_plan(job, adapters).warnings:
                err("%s: warning: %s" % (path, warning))
        if not report.valid:
            code = 2
        else:
            print("%s: ok" % path)
    return code
