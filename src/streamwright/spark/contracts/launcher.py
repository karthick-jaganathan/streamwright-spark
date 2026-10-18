"""Where the driver runs (entry-point group streamwright.spark.launchers). PROPOSED."""

from typing import Any, List, Mapping, Optional, Protocol, Sequence, runtime_checkable


__all__ = ["Launcher", "LaunchError", "LAUNCHER_OWNED_CONF"]

# spark.* keys the launcher or the lock owns; a job may not set them (it would defeat the version lock)
LAUNCHER_OWNED_CONF = ("spark.master", "spark.submit.deployMode", "spark.jars", "spark.jars.packages",
                       "spark.jars.repositories", "spark.archives", "spark.yarn.dist.archives", "spark.files",
                       "spark.pyspark.python", "spark.pyspark.driver.python", "spark.yarn.appMasterEnv.PYSPARK_PYTHON",
                       "spark.kubernetes.container.image", "spark.emr-serverless.driverEnv.PYSPARK_PYTHON",
                       "spark.sql.extensions", "spark.sql.catalog.spark_catalog")


class LaunchError(Exception):
    pass


@runtime_checkable
class Launcher(Protocol):
    platform: str                                    # the `runtime.platform.type` it serves

    def check_platform(self, platform: Mapping[str, Any]) -> List[str]:
        """Problems with the job's runtime.platform block (no network)."""

    def check_lock(self, lock: Mapping[str, Any], platform: Optional[Mapping[str, Any]] = None) -> List[str]:
        """Problems comparing the lock with the platform (EMR release label, cluster state); may call the platform."""

    def submit(self, job_path: str, argv: Sequence[str], conf: Mapping[str, str], lock: Mapping[str, Any],
               detach: bool, lock_path: Optional[str] = None, env_archive: Optional[str] = None,
               platform: Optional[Mapping[str, Any]] = None) -> int:
        """Returns an exit code (0 ok, 1 failed, 2 usage). Rejects launcher-owned keys in `conf`."""
