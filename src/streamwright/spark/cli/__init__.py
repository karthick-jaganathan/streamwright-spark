"""The `sw spark` command line."""

import argparse

from streamwright.spark.cli.main import main

__all__ = ["main", "register"]


def register(subparsers):
    parser = subparsers.add_parser("spark", help="validate, run and inspect SparkJob pipelines", add_help=False)
    parser.add_argument("-h", "--help", action="store_true")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    parser.set_defaults(command_handler=lambda args: main(["--help"] if args.help else args.arguments))
