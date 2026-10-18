"""platform: local. The driver runs in this process (Spark local[*]); nothing is submitted. PROPOSED."""


__all__ = ["LocalLauncher"]


class LocalLauncher(object):
    platform = "local"

    def check_platform(self, platform):
        return []

    def check_lock(self, lock, platform=None):
        return []

    def submit(self, job_path, argv, conf, lock, detach, lock_path=None, env_archive=None, platform=None):
        raise NotImplementedError("the local platform runs in-process")
