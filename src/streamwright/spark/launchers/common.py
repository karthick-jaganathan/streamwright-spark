"""Pieces the launchers share: spark-submit arguments, content-addressed staging. PROPOSED."""

import hashlib
import os

from streamwright.spark.contracts import LAUNCHER_OWNED_CONF, LaunchError


__all__ = ["DRIVER_ENTRY", "ENTRYPOINT_PATH", "sha256_file", "conf_args", "jar_args", "stage"]

ENTRYPOINT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "entrypoint.py")
)

DRIVER_ENTRY = (
    "import sys\n"
    "from streamwright.spark.cli import main\n"
    "raise SystemExit(main(['run', '--driver'] + sys.argv[1:]))\n"
)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def conf_args(conf):
    args = []
    for key, value in sorted(conf.items()):
        if key in LAUNCHER_OWNED_CONF:
            raise LaunchError("%s is launcher-owned and cannot be set" % key)
        args += ["--conf", "%s=%s" % (key, value)]
    return args


def jar_args(lock):
    shipped = [j["coordinates"] for j in lock.get("jars", []) if j["mode"] == "ship"]
    return ["--jars", ",".join(shipped)] if shipped else []


def stage(s3, staging_uri, local_path):
    """Uploads `local_path` under the staging prefix as <prefix>/<sha256[:16]>/<name> (once); returns the s3 URI."""
    if not staging_uri.startswith("s3://"):
        raise LaunchError("staging must be an s3:// URI")
    if str(local_path).startswith("s3://"):
        return local_path
    bucket, _, prefix = staging_uri[5:].partition("/")
    digest = sha256_file(local_path)[:16]
    key = "%s%s/%s" % (prefix if prefix.endswith("/") or not prefix else prefix + "/", digest, os.path.basename(local_path))
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        s3.upload_file(local_path, bucket, key)
    return "s3://%s/%s" % (bucket, key)
