"""platform: standalone (a Spark standalone master, e.g. the compose `cluster` profile)."""

import os
import subprocess

from streamwright.spark.launchers.common import ENTRYPOINT_PATH, conf_args, jar_args

__all__ = ["StandaloneLauncher", "standalone_args"]


def standalone_args(master, conf, lock, entry_path, *, driver_argv, files=()):
    return (
        ["spark-submit", "--master", master]
        + jar_args(lock)
        + conf_args(conf)
        + (["--files", ",".join(files)] if files else [])
        + [entry_path]
        + list(driver_argv)
    )


class StandaloneLauncher:
    platform = "standalone"
    contract_version = "1"

    def check_platform(self, platform):
        if not str(platform.get("master", "")).startswith("spark://"):
            return ["master must be a spark:// URL"]
        return []

    def check_lock(self, lock, platform=None):
        return []

    def preview(self, job_path, argv, conf, lock, *, platform, lock_path=None, files=()):
        shipped = [job_path] + ([lock_path] if lock_path else []) + list(files)
        driver = (
            [os.path.basename(job_path)] + (["--lock", os.path.basename(lock_path)] if lock_path else []) + list(argv)
        )
        return {
            "command": standalone_args(
                platform["master"], conf, lock, ENTRYPOINT_PATH, driver_argv=driver, files=shipped
            )
        }

    def submit(self, job_path, argv, conf, lock, *, detach, lock_path=None, env_archive=None, platform=None, files=()):
        shipped = [job_path] + ([lock_path] if lock_path else []) + list(files)
        driver_argv = [job_path]
        if lock_path:
            driver_argv += ["--lock", lock_path]
        flags = list(argv)
        paths = {os.path.basename(path): path for path in files}
        for index in range(len(flags) - 1):
            if flags[index] in ("--overlay", "--config"):
                flags[index + 1] = paths.get(flags[index + 1], flags[index + 1])
        command = standalone_args(
            (platform or {})["master"], conf, lock, ENTRYPOINT_PATH, driver_argv=driver_argv + flags, files=shipped
        )
        return subprocess.run(command, shell=False, check=False).returncode  # an argument list, never a shell string
