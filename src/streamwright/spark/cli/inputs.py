"""Config and secret values for a run, through core input handling."""

from importlib import import_module

from streamwright.core.config import inputs as core_inputs
from streamwright.core.jobs import inputs as job_inputs
from streamwright.spark.cli.common import err, parse_set

__all__ = ["InputError", "config_values", "secret_values"]

InputError = core_inputs.InputError


def config_values(job, args):
    provided = {}
    if args.config:
        provided.update(job_inputs.read_config_values(args.config))
    provided.update(parse_set(args.set))
    return job_inputs.effective_config(job, provided)


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
        try:
            aws_secrets = import_module("streamwright.spark.cli.aws_secrets")
        except ImportError as exc:
            if exc.name != "boto3":
                raise
            raise InputError('boto3 is not installed; pip install "streamwright-spark[emr]"') from exc
        for name, value in aws_secrets.read_secrets(source, job).items():
            provided.setdefault(name, value)
    return job_inputs.secret_values(job, provided)
