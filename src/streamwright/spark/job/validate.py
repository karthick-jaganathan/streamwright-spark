"""Credential-free SparkJob validation with core diagnostics."""

import copy
from collections.abc import Mapping

from streamwright.core.jobs import JobReport
from streamwright.core.jobs import validate_job as validate_shared_job
from streamwright.core.jobs.manifest import JobError, load_job_text, merge_overlay
from streamwright.core.runtime import components
from streamwright.core.validation import Issue
from streamwright.spark.job.reader import load_job
from streamwright.spark.job.rules import SparkRules
from streamwright.spark.plan import build_plan
from streamwright.spark.runtime import lock as lockmod
from streamwright.spark.runtime import registry

__all__ = ["validate", "validate_text"]


def _validate(raw, *, allow, lock, strict, config):
    discovery = registry.Registry()
    providers = {
        conn["provider"]
        for conn in (raw.get("connections") or {}).values()
        if isinstance(conn, dict) and isinstance(conn.get("provider"), str)
    }
    source_connectors = set(components.available())
    adapters, adapter_errors = {}, []
    for provider in sorted(providers):
        try:
            adapters[provider] = discovery.load(provider, allow=allow)
        except registry.SparkAdapterLoadError as exc:    # an adapter that exists but cannot load: always an error
            adapter_errors.append(str(exc))
        except registry.RegistryError as exc:
            if provider not in source_connectors:        # a source connector is reached through a call step (§6a)
                adapter_errors.append(str(exc))
    launchers, launcher_errors = registry.load_all("launcher")
    report = validate_shared_job(raw, lambda job: SparkRules(adapters, launchers, connectors=source_connectors),
                                 config=config, strict=strict)
    report.issues.extend(Issue("error", "plugin", text) for text in adapter_errors + launcher_errors)
    job = report.job or raw
    if lock is not None:
        try:
            data = lockmod.load(lock) if isinstance(lock, str) else lock
            report.issues.extend(
                Issue("warning", "lock", text) for text in lockmod.check_lock(data, data.get("spark", ""))
            )
        except lockmod.LockError as exc:
            report.issues.append(Issue("error", "lock", str(exc)))
    report.valid = not any(
        issue.severity == "error" or (strict and issue.severity == "warning") for issue in report.issues
    )
    if report.valid:
        report.plan = build_plan(job, adapters).describe()
    return report


def validate(path_or_mapping, *, overlays=(), allow=None, lock=None, strict=False, config=None) -> JobReport:
    """Validate a manifest path or mapping, never a YAML text string."""
    try:
        raw = (
            copy.deepcopy(dict(path_or_mapping))
            if isinstance(path_or_mapping, Mapping)
            else load_job(path_or_mapping, overlays=overlays)
        )
        if isinstance(path_or_mapping, Mapping):
            for overlay in overlays:
                raw = merge_overlay(raw, load_job(overlay))
    except JobError as exc:
        return JobReport(False, [Issue("error", "unreadable", str(exc))])
    return _validate(raw, allow=allow, lock=lock, strict=strict, config=config)


def validate_text(text, *, overlays=(), allow=None, lock=None, strict=False, config=None) -> JobReport:
    """Validate YAML text explicitly; never guess whether a string is a file path."""
    try:
        raw = load_job_text(text, overlays=overlays)
    except JobError as exc:
        return JobReport(False, [Issue("error", "unreadable", str(exc))])
    return _validate(raw, allow=allow, lock=lock, strict=strict, config=config)
