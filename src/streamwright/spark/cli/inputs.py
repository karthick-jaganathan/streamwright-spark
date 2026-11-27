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

"""Config and secret values for a run, through core input handling."""

from streamwright.core.config import inputs as core_inputs
from streamwright.core.jobs import inputs as job_inputs
from streamwright.spark.cli.common import err, parse_set

__all__ = ["InputError", "config_values", "provided_config", "provided_secrets", "secret_values"]

InputError = core_inputs.InputError


def provided_config(args):
    """The config values given explicitly: --config FILE, then --set."""
    provided = {}
    if args.config:
        provided.update(job_inputs.read_config_values(args.config))
    provided.update(parse_set(args.set))
    return provided


def config_values(job, args):
    return job_inputs.effective_config(job, provided_config(args))


def provided_secrets(job, args):
    """The secret values given explicitly: --secrets FILE, then STREAMWRIGHT_SECRET_<NAME>."""
    return _provided_secrets(job, args)


def secret_values(job, args):
    """STREAMWRIGHT_SECRET_<NAME> variables or --secrets FILE (lookups fill the rest); never printed."""
    return job_inputs.secret_values(job, _provided_secrets(job, args))


def _provided_secrets(job, args):
    declared = ((job.get("spec") or {}).get("secrets")) or {}
    provided = {}
    if getattr(args, "secrets", None):
        provided.update(core_inputs.read_values_file(args.secrets, err))
    for name, value in core_inputs.secrets_from_env(list(declared)).items():
        provided.setdefault(name, value)
    return provided
