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

"""Remote manifest loading, imported only for s3:// jobs."""

import os

import boto3
from botocore.exceptions import ClientError

from streamwright.core.jobs.manifest import JobError


def read_s3(location, limit):
    bucket, _, key = str(location)[5:].partition("/")
    if not bucket:
        raise JobError("an object-store job URI needs a bucket")
    key = (key.rstrip("/") + "/" if key.rstrip("/") else "") + "job.yaml" if not key or key.endswith("/") else key
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    try:
        client = boto3.client("s3", endpoint_url=endpoint) if endpoint else boto3.client("s3")
        return client.get_object(Bucket=bucket, Key=key)["Body"].read(limit + 1).decode("utf-8")
    except ClientError as exc:
        raise JobError(
            "cannot read remote job: %s" % (exc.response.get("Error", {}).get("Code", "ClientError"))
        ) from None
    except (OSError, UnicodeError) as exc:
        raise JobError("cannot read remote job: %s" % type(exc).__name__) from None


def download_folder(location, target, limit):
    """
    The YAML files under the s3:// folder `location` (a source folder: source.yaml and streams/*.yaml) copied into
    the local folder `target`, at most `limit` bytes in all. JobError when there are none or they are too large.
    """
    bucket, _, prefix = str(location)[5:].partition("/")
    prefix = prefix.rstrip("/") + "/"
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    client = boto3.client("s3", endpoint_url=endpoint) if endpoint else boto3.client("s3")
    total, copied = 0, 0
    try:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents") or ():
                relative = item["Key"][len(prefix):]
                parts = relative.split("/")
                if not relative.endswith((".yaml", ".yml")) or len(parts) > 2 or any(
                        part in ("", ".", "..") for part in parts):
                    continue                     # source.yaml and streams/<name>.yaml only, never outside `target`
                total += item.get("Size") or 0
                if total > limit:
                    raise JobError("source folder %s is larger than %d bytes" % (location, limit))
                path = os.path.join(target, *parts)
                if not os.path.realpath(path).startswith(os.path.realpath(target) + os.sep):
                    continue
                os.makedirs(os.path.dirname(path), exist_ok=True)
                client.download_file(bucket, item["Key"], path)
                copied += 1
    except ClientError as exc:
        raise JobError("cannot read source folder %s: %s" % (
            location, exc.response.get("Error", {}).get("Code", "ClientError"))) from None
    if not copied:
        raise JobError("source folder %s has no YAML files" % location)
