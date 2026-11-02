"""
The version lock: one file that pins everything the job runs with.

    {"lock_version": 1, "job": ..., "spark": "3.5.3", "python": "3.10",
     "packages": {"delta-spark": "3.2.1", ...},
     "jars": [{"coordinates": "io.delta:delta-spark_2.12:3.2.1", "mode": "provided|ship|baked"}],
     "emr": {"release_label": "emr-7.x"}, "python_env": {"archive": "env.tar.gz", "sha256": ..., "platform": ...},
     "image": "repo@sha256:..."}

`sw spark lock` writes it, `--lock FILE` makes `run` verify it (driver startup: installed packages, Python minor
version, Spark version, and on EMR the cluster's release label) and export STREAMWRIGHT_LOCK_SHA so every executor
can be checked against the same fingerprint. A mismatch stops the job before any message is polled.
"""

import hashlib
import json
import sys
from importlib import metadata

__all__ = ["LockError", "build_lock", "check_lock", "dump", "fingerprint", "load"]


class LockError(Exception):
    pass


def _installed(name):
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def build_lock(
    job_name,
    adapters_used,
    spark,
    *,
    extra_jars=None,
    emr_release=None,
    python_env=None,
    image=None,
    installed=_installed,
):
    """Pins what the job runs with. Each adapter reports its own packages and jars: there is no table in core."""
    minor = ".".join(spark.split(".")[:2])
    packages, jars, missing = {"streamwright-spark": installed("streamwright-spark") or "0.0.0"}, [], []
    for adapter in adapters_used:
        for package in adapter.packages:
            version = installed(package)
            if version is None:
                missing.append("%s (%s)" % (package, adapter.install_hint or "install it"))
            else:
                packages[package] = version
        for coordinates, mode, default in adapter.jars(minor):
            version = (extra_jars or {}).get(coordinates) or default
            if version is None:
                missing.append("jar %s: give a version with --jar %s=VERSION" % (coordinates, coordinates))
            else:
                entry = {"coordinates": "%s:%s" % (coordinates, version), "mode": mode}
                if entry not in jars:
                    jars.append(entry)
    if missing:
        raise LockError("cannot lock: " + "; ".join(missing))
    lock = {
        "lock_version": 1,
        "job": job_name,
        "spark": spark,
        "python": "%d.%d" % sys.version_info[:2],
        "packages": dict(sorted(packages.items())),
        "jars": sorted(jars, key=lambda j: j["coordinates"]),
    }
    if emr_release:
        lock["emr"] = {"release_label": emr_release}
    if python_env:
        lock["python_env"] = python_env
    if image:
        lock["image"] = image
    return lock


def fingerprint(lock):
    return hashlib.sha256(json.dumps(lock, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def check_lock(lock, spark_version, python=None, installed=_installed, emr_release=None):
    """Problems (empty when the environment matches the lock)."""
    problems = []
    if lock.get("lock_version") != 1:
        problems.append("unsupported lock_version %r" % lock.get("lock_version"))
    if (
        ".".join(str(spark_version).split(".")[:3]) != lock.get("spark")
        and not str(lock.get("spark", "")).endswith("x")
        and str(spark_version) != lock.get("spark")
    ):
        problems.append("Spark is %s, the lock says %s" % (spark_version, lock.get("spark")))
    python = python or "%d.%d" % sys.version_info[:2]
    if python != lock.get("python"):
        problems.append("Python is %s, the lock says %s" % (python, lock.get("python")))
    for package, version in (lock.get("packages") or {}).items():
        have = installed(package)
        if have != version:
            problems.append("%s is %s, the lock says %s" % (package, have or "not installed", version))
    wanted = (lock.get("emr") or {}).get("release_label")
    if wanted and emr_release and emr_release != wanted:
        problems.append("cluster release is %s, the lock says %s" % (emr_release, wanted))
    return problems


def dump(lock, path):
    with open(path, "w", encoding="utf-8") as stream:
        json.dump(lock, stream, indent=2, sort_keys=True)
        stream.write("\n")


def load(path):
    try:
        with open(path, encoding="utf-8") as stream:
            data = json.load(stream)
    except OSError as exc:
        raise LockError("cannot read lock %r: %s" % (path, exc.strerror or "error")) from exc
    except ValueError:
        raise LockError("lock %r is not valid JSON" % path) from None
    if not isinstance(data, dict):
        raise LockError("lock %r must be an object" % path)
    return data
