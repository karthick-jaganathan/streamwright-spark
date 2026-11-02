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
