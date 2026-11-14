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

"""Render a spark-submit argument list for dry-run previews."""

_BOOL_FLAGS = frozenset(
    {
        "--once",
        "--drain",
        "--available-now",
        "--follow",
        "--apply-state-migrations",
        "--allow-lookup-exec",
        "--dry-run",
        "--detach",
        "--summary",
        "--driver",
        "--supervise",
        "--verbose",
    }
)


def _quoted(text):
    value = str(text)
    return f'"{value}"' if " " in value and not (value.startswith('"') and value.endswith('"')) else value


def format_spark_submit(arguments, indent=4):
    """Group submit flags with their values in a human-readable multiline command."""
    lines = []
    index = 0
    if arguments and arguments[0] == "spark-submit":
        lines.append("spark-submit")
        index = 1
    prefix = " " * indent
    while index < len(arguments):
        argument = str(arguments[index])
        if argument.startswith("-") and argument not in _BOOL_FLAGS and index + 1 < len(arguments):
            value = str(arguments[index + 1])
            if not value.startswith("-"):
                lines.append(f"{prefix}{argument} {_quoted(value)}")
                index += 2
                continue
        lines.append(f"{prefix}{_quoted(argument)}")
        index += 1
    return " \\\n".join(lines)
