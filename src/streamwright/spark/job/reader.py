"""Use the shared manifest loader with platform-provided remote readers."""

from streamwright.core.jobs.manifest import load_job as load_manifest
from streamwright.spark.launchers.common import MANIFEST_READERS, resolve_job_location

__all__ = ["load_job", "resolve_job_location"]


def load_job(path, overlays=()):
    return load_manifest(path, overlays=overlays, readers=MANIFEST_READERS)
