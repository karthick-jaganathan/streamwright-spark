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

"""The `sw spark` command line."""

import argparse

from streamwright.spark.cli.main import build_parser, main

__all__ = ["build_parser", "main", "register"]


def register(subparsers):
    parser = subparsers.add_parser("spark", help="validate, run and inspect SparkJob pipelines", add_help=False)
    parser.add_argument("-h", "--help", action="store_true", help="show the sw spark help")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    parser.set_defaults(command_handler=lambda args: main(["--help"] if args.help else args.arguments),
                        full_parser=build_parser)
