"""platform: standalone (a Spark standalone master, e.g. the compose `cluster` profile). PROPOSED."""

import subprocess

from streamwright.spark.launchers.common import conf_args, jar_args


__all__ = ["StandaloneLauncher", "standalone_args"]


def standalone_args(master, conf, lock, entry_path, driver_argv):
    return ["spark-submit", "--master", master] + jar_args(lock) + conf_args(conf) + [entry_path] + list(driver_argv)


class StandaloneLauncher(object):
    platform = "standalone"

    def check_platform(self, platform):
        if not str(platform.get("master", "")).startswith("spark://"):
            return ["master must be a spark:// URL"]
        return []

    def check_lock(self, lock, platform=None):
        return []

    def submit(self, job_path, argv, conf, lock, detach, lock_path=None, env_archive=None, platform=None):
        command = standalone_args((platform or {})["master"], conf, lock, job_path, argv)
        return subprocess.run(command, shell=False, check=False).returncode      # an argument list, never a shell string
