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
