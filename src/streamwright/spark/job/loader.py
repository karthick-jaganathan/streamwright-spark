"""Reading a `kind: SparkJob` file and applying --overlay files. PROPOSED."""

import copy
import os

import yaml


__all__ = ["JobError", "load_job", "merge_overlay"]

_MAX_BYTES = 2 * 1024 * 1024


class JobError(Exception):
    pass


def load_job(path, overlays=()):
    job = _read(path)
    for overlay in overlays:
        job = merge_overlay(job, _read(overlay))
    return job


def _read(path):
    path_str = str(path)
    if path_str.startswith("s3://"):
        try:
            import boto3
        except ImportError:
            raise JobError("cannot read %r: boto3 is not installed" % path)
        bucket, _, key = path_str[5:].partition("/")
        if not key or key.endswith("/"):
            key = (key if key else "") + "job.yaml"
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        s3 = boto3.client("s3", endpoint_url=endpoint) if endpoint else boto3.client("s3")
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            text = resp["Body"].read(_MAX_BYTES + 1).decode("utf-8")
        except Exception as exc:
            raise JobError("cannot read S3 path %r: %s" % (path, exc))
    else:
        if os.path.isdir(path):
            candidate = os.path.join(path, "job.yaml")
            if os.path.isfile(candidate):
                path = candidate
            else:
                raise JobError("directory %r has no job.yaml" % path)
        try:
            with open(path, "r", encoding="utf-8") as stream:
                text = stream.read(_MAX_BYTES + 1)
        except (IOError, OSError) as exc:
            raise JobError("cannot read %r: %s" % (path, exc.strerror or type(exc).__name__))
    if len(text) > _MAX_BYTES:
        raise JobError("%r is larger than %d bytes" % (path, _MAX_BYTES))
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError:
        raise JobError("%r is not valid YAML" % path)
    if not isinstance(data, dict):
        raise JobError("%r must be a mapping" % path)
    return data


def merge_overlay(base, overlay):
    """Mappings merge by key, anything else replaces; a null in the overlay deletes the key."""
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_overlay(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result
