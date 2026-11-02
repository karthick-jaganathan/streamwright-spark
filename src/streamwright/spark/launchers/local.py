"""platform: local. The driver runs in this process (Spark local[*]); nothing is submitted."""

import os

__all__ = ["LocalLauncher"]


class LocalLauncher:
    platform = "local"
    contract_version = "1"

    def check_platform(self, platform):
        return []

    def check_lock(self, lock, platform=None):
        return []

    def preview(self, job_path, argv, conf, lock, *, platform, lock_path=None, files=()):
        command = ["spark-submit", "--master", "local[*]"]
        for key, value in sorted(conf.items()):
            command += ["--conf", "%s=%s" % (key, value)]
        return {"command": [*command, os.path.basename(job_path), *list(argv)]}
