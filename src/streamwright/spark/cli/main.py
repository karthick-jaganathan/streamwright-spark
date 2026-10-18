"""
`sw spark ...` (PROPOSED). Entry points `streamwright-spark` / `sw-spark`; attaching it to `streamwright` core is a lazy
dispatch like `streamwright mcp` (see README). Exit codes: 0 ok, 1 failed, 2 usage/validation, 130 interrupted.

    sw spark validate PATH... [--config F] [--set K=V] [--overlay F] [--analyze] [--graph] [--strict] [--lock F]
    sw spark run JOB [--config F] [--set K=V] [--secrets F] [--overlay F] [--once|--drain|--follow [--interval 10s]]
                     [--max-messages N] [--run-key K] [--conf K=V] [--lock F] [--lock-required] [--dry-run] [--detach]
    sw spark lock JOB --spark V [--out F] [--emr-release L] [--env-archive F] [--jar G:A=V] [--check]
    sw spark state init|status|batches|show|retry|abandon|purge|lease JOB [ARG]
"""

import argparse
import sys

from streamwright.spark.cli import lock, run, state, validate
from streamwright.spark.cli.common import InputsError, add_job_arguments, err
from streamwright.spark.cli.inputs import InputError
from streamwright.spark.job import JobError
from streamwright.spark.runtime.lock import LockError


__all__ = ["main"]

_COMMANDS = (("validate", validate, "check job files without Spark, brokers or credentials", True),
             ("run", run, "run a job", False),
             ("lock", lock, "write or check the version lock", False),
             ("state", state, "inspect and repair bookkeeping", None))


def _parser():
    parser = argparse.ArgumentParser(prog="sw spark", description="Spark jobs for StreamWright (proposed).")
    sub = parser.add_subparsers(dest="command")
    for name, module, help_text, many in _COMMANDS:
        command = sub.add_parser(name, help=help_text)
        if many is not None:
            add_job_arguments(command, many)
        module.add_arguments(command)
        command.set_defaults(handler=module.run)
    return parser


def _normalize_job_args(args):
    if hasattr(args, "flag_job"):
        flag_val = args.flag_job or getattr(args, "flag_file", None) or getattr(args, "flag_pipeline", None)
        if flag_val:
            if hasattr(args, "arg") and args.job and args.arg is None:
                args.arg = args.job
            args.job = flag_val
        elif not args.job:
            err("error: job is required (pass JOB positional argument or --job PATH)")
            return False

    if hasattr(args, "flag_jobs"):
        jobs = list(args.job or []) + list(args.flag_jobs or []) + list(getattr(args, "flag_files", None) or []) + list(getattr(args, "flag_pipelines", None) or [])
        if not jobs:
            err("error: at least one job path is required (pass PATH positional argument or --job PATH)")
            return False
        args.job = jobs

    return True


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(argv)
    if not getattr(args, "handler", None):
        parser.print_help()
        return 2
    if not _normalize_job_args(args):
        return 2
    try:
        return args.handler(args)
    except (JobError, InputError, InputsError, LockError) as exc:
        err("error: %s" % exc)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
