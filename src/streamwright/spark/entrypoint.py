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
