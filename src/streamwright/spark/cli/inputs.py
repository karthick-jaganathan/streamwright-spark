"""Config and secret values for a run, through streamwright core's own input handling. PROPOSED."""

import os

from streamwright.core.config import inputs as core_inputs

from streamwright.spark.cli.common import err, parse_set


__all__ = ["InputError", "config_values", "secret_values"]

InputError = core_inputs.InputError


def config_values(job, args):
    provided = {}
    if args.config:
        values, streams = core_inputs.read_config_file(args.config)
        if streams:
            raise InputError("%r: `streams` does not apply to a SparkJob" % args.config)
        provided.update(values)
    provided.update(parse_set(args.set))
    return core_inputs.resolve_inputs((job.get("spec") or {}).get("config"), provided, "config")


def secret_values(job, args):
    """STREAMWRIGHT_SECRET_<NAME> variables, --secrets FILE, or runtime.secrets (AWS Secrets Manager); never printed."""
    declared = ((job.get("spec") or {}).get("secrets")) or {}
    provided = {}
    if getattr(args, "secrets", None):
        provided.update(core_inputs.read_values_file(args.secrets, err))
    for name, value in core_inputs.secrets_from_env(list(declared)).items():
        provided.setdefault(name, value)
    source = (job.get("runtime") or {}).get("secrets") or {}
    if source.get("source") == "aws_secretsmanager" and len(set(provided) & set(declared)) < len(declared):
        for name, value in _aws_secrets(source, job).items():
            provided.setdefault(name, value)
    return core_inputs.resolve_inputs(declared, provided, "secrets", hide_values=True)


def _aws_secrets(source, job):
    import json
    try:
        import boto3
    except ImportError:
        raise InputError("boto3 is not installed; pip install \"streamwright-spark[emr]\"")
    region = ((job.get("runtime") or {}).get("platform") or {}).get("region")
    reply = boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId=source["id"])
    try:
        data = json.loads(reply["SecretString"])
    except ValueError:
        raise InputError("the secret %s is not a JSON object" % source["id"])
    return data if isinstance(data, dict) else {}
