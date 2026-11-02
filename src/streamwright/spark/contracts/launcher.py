"""Where the driver runs (entry-point group streamwright.spark.launchers)."""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

__all__ = ["LAUNCHER_OWNED_CONF", "LaunchError", "LaunchFailureError", "Launcher"]

# spark.* keys the launcher or the lock owns; a job may not set them (it would defeat the version lock)
LAUNCHER_OWNED_CONF = (
    "spark.master",
    "spark.submit.deployMode",
    "spark.jars",
    "spark.jars.packages",
    "spark.jars.repositories",
    "spark.archives",
    "spark.yarn.dist.archives",
    "spark.files",
    "spark.pyspark.python",
    "spark.pyspark.driver.python",
    "spark.yarn.appMasterEnv.PYSPARK_PYTHON",
    "spark.kubernetes.container.image",
    "spark.emr-serverless.driverEnv.PYSPARK_PYTHON",
    "spark.sql.extensions",
    "spark.sql.catalog.spark_catalog",
)


class LaunchError(Exception):
    pass


class LaunchFailureError(LaunchError):
    """A submitted job failed on the remote platform (exit 1, not a usage error)."""


@runtime_checkable
class Launcher(Protocol):
    platform: str  # the `runtime.platform.type` it serves

    def check_platform(self, platform: Mapping[str, Any]) -> list[str]:
        """Problems with the job's runtime.platform block (no network)."""

    def preview(
        self,
        job_path: str,
        argv: Sequence[str],
        conf: Mapping[str, str],
        lock: Mapping[str, Any],
        *,
        platform: Mapping[str, Any],
        lock_path: str | None = None,
        files: Sequence[str] = (),
    ) -> dict:
        """Pure launch description; no network and no files staged."""

    def check_lock(self, lock: Mapping[str, Any], platform: Mapping[str, Any] | None = None) -> list[str]:
        """Problems comparing the lock with the platform (EMR release label, cluster state); may call the platform."""

    def submit(
        self,
        job_path: str,
        argv: Sequence[str],
        conf: Mapping[str, str],
        lock: Mapping[str, Any],
        *,
        detach: bool,
        lock_path: str | None = None,
        env_archive: str | None = None,
        platform: Mapping[str, Any] | None = None,
    ) -> int:
        """Returns an exit code (0 ok, 1 failed, 2 usage). Rejects launcher-owned keys in `conf`."""
