"""
Test helpers. `install_entry_points()` writes a throw-away dist-info that declares the entry points of pyproject.toml,
so the real plugin loading (importlib.metadata) is what the tests exercise, without installing the package.
The real StreamWright connectors must be importable (PYTHONPATH, see Makefile `test`) and registered the same way.
"""

import os
import re
import sys
import tempfile

ROOT = os.path.join(os.path.dirname(__file__), "..")
_GROUP = re.compile(r'^\[project\.entry-points\."([^"]+)"\]\s*$')


def entry_points_text(pyproject=os.path.join(ROOT, "pyproject.toml")):  # launchers only, now
    out, active = [], False
    for line in open(pyproject, encoding="utf-8"):
        match = _GROUP.match(line.strip())
        if match:
            out.append("[%s]" % match.group(1))
            active = True
        elif line.startswith("["):
            active = False
        elif active and "=" in line:
            out.append(line.strip().replace('"', ""))
    return "\n".join(out) + "\n"


def connector_entry_points():
    """
    The connectors on PYTHONPATH (streamwright/connectors/<name>/connector.py) and, where present, their Spark adapters
    (spark.py). Emits both groups, exactly as the connector packages declare them in their own pyproject.toml:
    `streamwright.connectors` and `streamwright.spark.connectors`.
    """
    connectors, adapters, seen = ["[streamwright.connectors]"], ["[streamwright.spark.connectors]"], set()
    for base in sys.path:
        folder = os.path.join(base or ".", "streamwright", "connectors")
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            if name in seen:
                continue
            path = os.path.join(folder, name, "connector.py")
            if not os.path.isfile(path):
                continue
            cls = re.search(r"^class (\w+Connector)\(Connector\)", open(path, encoding="utf-8").read(), re.M)
            if not cls:
                continue
            seen.add(name)
            connectors.append("%s = streamwright.connectors.%s.connector:%s" % (name, name, cls.group(1)))
            spark = os.path.join(folder, name, "_spark.py")
            if os.path.isfile(spark):
                adapter = re.search(r"^class (\w+)\((?:Filesystem|Jdbc|Delta|Broker)\)", open(spark, encoding="utf-8").read(), re.M)
                if adapter:
                    adapters.append("%s = streamwright.connectors.%s.spark:load" % (name, name))
    return "\n".join(connectors) + "\n\n" + "\n".join(adapters) + "\n"


_INSTALLED = []


def install_entry_points():
    if _INSTALLED:
        return _INSTALLED[0]
    folder = tempfile.mkdtemp(prefix="sw-spark-dist-")
    for name, text in (("streamwright_spark_test-0.0.0", entry_points_text()),
                       ("streamwright_connectors_test-0.0.0", connector_entry_points())):
        dist = os.path.join(folder, name + ".dist-info")
        os.makedirs(dist)
        with open(os.path.join(dist, "METADATA"), "w") as stream:
            stream.write("Metadata-Version: 2.1\nName: %s\nVersion: 0.0.0\n" % name.split("-")[0])
        with open(os.path.join(dist, "entry_points.txt"), "w") as stream:
            stream.write(text)
    sys.path.insert(0, folder)
    _INSTALLED.append(folder)
    return folder


class _NoDuckdb(object):
    """A connector whose check_request needs DuckDB (postgres) when DuckDB is not installed: its request check is
    skipped (returns no problems). Only used in environments without DuckDB; a real install has it."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def check_request(self, request):
        try:
            return self._inner.check_request(request)
        except ModuleNotFoundError:
            return []


def tolerate_missing_duckdb():
    try:
        import duckdb  # noqa: F401
        return False
    except ImportError:
        pass
    from streamwright.core.runtime import components
    original = components.load
    if getattr(original, "_sw_tolerant", False):
        return True

    def load(name, *args, **kwargs):
        return _NoDuckdb(original(name, *args, **kwargs))
    load._sw_tolerant = True
    components.load = load
    return True
