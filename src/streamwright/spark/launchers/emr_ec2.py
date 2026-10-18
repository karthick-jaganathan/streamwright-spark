"""
platform: emr_ec2. Stages content-addressed artifacts to S3 and adds a spark-submit step to an existing cluster. PROPOSED.

Secrets are never put in step arguments, the step environment or the lock: the driver fetches them itself
(`runtime.secrets`). The Python environment archive named in the lock is shipped with --archives and selected with
PYSPARK_PYTHON, so every autoscaled node localizes the same bytes. Job code is never installed by a bootstrap action.
"""

import os

from streamwright.spark.contracts import LaunchError
from streamwright.spark.launchers.common import ENTRYPOINT_PATH, conf_args, jar_args, stage


__all__ = ["EmrEc2Launcher", "emr_step_args"]


def emr_step_args(conf, lock, staged, driver_argv, deploy_mode="cluster"):
    """The command-runner.jar arguments. `staged`: {'entry': uri, 'env': uri|None, 'job': uri, 'lock': uri}."""
    args = ["spark-submit", "--master", "yarn", "--deploy-mode", deploy_mode]
    if staged.get("env"):
        args += ["--archives", staged["env"] + "#env",
                 "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python",
                 "--conf", "spark.executorEnv.PYSPARK_PYTHON=./env/bin/python"]
    args += jar_args(lock) + conf_args(conf)
    args += ["--files", "%s,%s" % (staged["job"], staged["lock"]), staged["entry"]]
    return args + list(driver_argv)


class EmrEc2Launcher(object):
    platform = "emr_ec2"

    def __init__(self, boto3_module=None, platform=None):
        self._boto3, self._platform_config, self._clients = boto3_module, platform, None

    def check_platform(self, platform):
        problems = []
        if not platform.get("region"):
            problems.append("region is required for emr_ec2")
        if bool(platform.get("cluster")) == bool(platform.get("create")):
            problems.append("give exactly one of cluster and create")
        if platform.get("create"):
            problems.append("create (a transient cluster) is not supported by this launcher version; give cluster.id")
        if not str(platform.get("staging", "")).startswith("s3://"):
            problems.append("staging must be an s3:// location")
        return problems

    def _client(self, platform):
        if self._clients is None:
            module = self._boto3
            if module is None:
                try:
                    import boto3 as module
                except ImportError:
                    raise LaunchError("boto3 is not installed; pip install \"streamwright-spark[emr]\"")
            region = platform["region"]
            self._clients = (module.client("emr", region_name=region), module.client("s3", region_name=region), module)
        return self._clients

    def check_lock(self, lock, platform=None):
        emr, _, _ = self._client(platform or self._platform_config)
        info = emr.describe_cluster(ClusterId=(platform or self._platform_config)["cluster"]["id"])["Cluster"]
        problems = []
        wanted = (lock.get("emr") or {}).get("release_label")
        if wanted and info.get("ReleaseLabel") != wanted:
            problems.append("cluster %s is %s, the lock says %s" % (info["Id"], info.get("ReleaseLabel"), wanted))
        if info["Status"]["State"] not in ("RUNNING", "WAITING"):
            problems.append("cluster is %s" % info["Status"]["State"])
        return problems

    def submit(self, job_path, argv, conf, lock, detach, lock_path=None, env_archive=None, platform=None):
        platform = platform or self._platform_config
        emr, s3, _ = self._client(platform)
        if lock_path is None:
            raise LaunchError("a lock file is required on EMR")
        entry_path = platform.get("entrypoint") or ENTRYPOINT_PATH
        staged = {"entry": stage(s3, platform["staging"], entry_path), "job": stage(s3, platform["staging"], job_path),
                  "lock": stage(s3, platform["staging"], lock_path),
                  "env": stage(s3, platform["staging"], env_archive) if env_archive else None}
        driver_argv = [os.path.basename(job_path), "--lock", os.path.basename(lock_path)] + list(argv)
        step = {"Name": "streamwright-%s" % lock.get("job", "job"),
                "ActionOnFailure": (platform.get("step") or {}).get("action_on_failure", "CONTINUE"),
                "HadoopJarStep": {"Jar": "command-runner.jar",
                                  "Args": emr_step_args(conf, lock, staged, driver_argv, platform.get("deploy_mode", "cluster"))}}
        cluster_id = platform["cluster"]["id"]
        step_id = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])["StepIds"][0]
        if detach:
            return 0
        try:
            emr.get_waiter("step_complete").wait(ClusterId=cluster_id, StepId=step_id,
                                                 WaiterConfig={"Delay": 30, "MaxAttempts": 2880})
        except Exception:
            return 1
        return 0
