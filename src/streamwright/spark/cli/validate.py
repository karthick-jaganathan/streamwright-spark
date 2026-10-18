"""sw spark validate: check job files without Spark, brokers or credentials. PROPOSED."""

from streamwright.spark.cli.common import err, load_plugins
from streamwright.spark.job import JobError, check_job, load_job
from streamwright.spark.plan import build_plan
from streamwright.spark.runtime import lock as lockmod


__all__ = ["add_arguments", "run"]


def add_arguments(parser):
    parser.add_argument("--analyze", action="store_true", help="print the Spark actions and recomputation warnings")
    parser.add_argument("--graph", action="store_true", help="print step dependencies")
    parser.add_argument("--strict", action="store_true", help="warnings fail the check")
    parser.add_argument("--lock", help="path to a lock file to verify compatibility against")


def run(args):
    code = 0
    adapters, launchers, plugin_problems = load_plugins(args.allow_connector)
    for path in args.job:
        try:
            job = load_job(path, args.overlay)
        except JobError as exc:
            err("%s: %s" % (path, exc))
            code = 2
            continue
        problems, warnings = check_job(job, adapters, launchers)
        problems = problems + plugin_problems
        if args.lock:
            try:
                lock = lockmod.load(args.lock)
                warnings += ["lock: " + p for p in lockmod.check_lock(lock, lock.get("spark", ""))]
            except lockmod.LockError as exc:
                problems.append(str(exc))
        if not problems and (args.analyze or args.graph):
            plan = build_plan(job, adapters)
            if args.analyze:
                warnings += plan.warnings
                print(plan.describe())
            if args.graph:
                for node in plan.nodes:
                    print("%-8s %-22s <- %s" % (node.kind, node.name, ", ".join(node.uses) or "-"))
        for text in problems:
            err("%s: error: %s" % (path, text))
        for text in warnings:
            err("%s: warning: %s" % (path, text))
        if problems or (args.strict and warnings):
            code = 2
        else:
            print("%s: ok" % path)
    return code
