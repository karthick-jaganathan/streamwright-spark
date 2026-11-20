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

"""Pieces the launchers share: spark-submit arguments and content-addressed staging."""

import hashlib
import os
from importlib import import_module
from types import MappingProxyType

from streamwright.core.jobs.manifest import JobError
from streamwright.spark.contracts import LAUNCHER_OWNED_CONF, LaunchError

__all__ = [
    "ENTRYPOINT_PATH",
    "MANIFEST_READERS",
    "conf_args",
    "jar_args",
    "resolve_job_location",
    "sha256_file",
    "stage",
]

ENTRYPOINT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "entrypoint.py"))


def _read_s3(location, limit):
    try:
        reader = import_module("streamwright.spark.launchers.s3_jobs")
    except ImportError as exc:
        if exc.name not in ("boto3", "botocore"):
            raise
        raise JobError("reading remote jobs requires streamwright-spark[emr]") from None
    return reader.read_s3(location, limit)


def _download_s3_folder(location, target, limit):
    try:
        reader = import_module("streamwright.spark.launchers.s3_jobs")
    except ImportError as exc:
        if exc.name not in ("boto3", "botocore"):
            raise
        raise JobError("reading remote source folders requires streamwright-spark[emr]") from None
    reader.download_folder(location, target, limit)


MANIFEST_READERS = MappingProxyType({"s3": _read_s3})
# a remote `source:` folder's YAML files, copied into a local folder: (location, target, byte limit)
FOLDER_READERS = MappingProxyType({"s3": _download_s3_folder})


def resolve_job_location(path):
    location = str(path)
    if location.startswith("s3://") and location.endswith("/"):
        return location + "job.yaml"
    return location


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
    key = "%s%s/%s" % (
        prefix if prefix.endswith("/") or not prefix else prefix + "/",
        digest,
        os.path.basename(local_path),
    )
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        s3.upload_file(local_path, bucket, key)
    return "s3://%s/%s" % (bucket, key)
