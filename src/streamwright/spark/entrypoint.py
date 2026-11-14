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

"""
Driver entrypoint executed on the Spark cluster driver node.

Invoked by spark-submit in cluster mode or standalone:
    spark-submit ... entrypoint.py <job-file> --lock <lock-file> [flags]

Delegates to `streamwright.spark.cli.main(['run', '--driver'] + sys.argv[1:])`.
"""

import os
import sys
import zipfile


def _bootstrap_wheels():
    """If wheels are passed via --py-files, unpack them into a local directory so
    PEP 420 namespace packages (streamwright.*) and entry points resolve cleanly."""
    unpacked = os.path.abspath("./.wheels")
    has_wheels = False
    for path in list(sys.path):
        if path.endswith(".whl") and os.path.isfile(path):
            has_wheels = True
            with zipfile.ZipFile(path) as archive:
                archive.extractall(unpacked)
    if has_wheels and unpacked not in sys.path:
        sys.path.insert(0, unpacked)


if __name__ == "__main__":
    _bootstrap_wheels()
    from streamwright.spark.cli import main

    raise SystemExit(main(["run", "--driver", *sys.argv[1:]]))
