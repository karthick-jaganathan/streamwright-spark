"""
platform: emr_ec2. Stages content-addressed artifacts to S3 and adds a spark-submit step to an existing cluster.

Cluster targeting:
  * Static EMR Cluster ID: `cluster: {id: "j-XXXXXXXXXXXX"}`
  * Dynamic CloudFormation Stack: `cluster: {stack_name: "my-stack"}` or `cluster: {stack_arn: "arn:aws:..."}`
    (with optional `logical_resource_id: "MyEMRCluster"`). The launcher dynamically queries CloudFormation to resolve
    the active physical cluster ID, making job submissions resilient across cluster recreations and stack updates.

Secrets are never put in step arguments, the step environment or the lock: the driver fetches them itself
(`runtime.secrets`). The Python environment archive named in the lock is shipped with --archives and selected with
PYSPARK_PYTHON, so every autoscaled node localizes the same bytes. Job code is never installed by a bootstrap action.
"""

import os
from importlib import import_module

from streamwright.spark.contracts import LaunchError, LaunchFailureError
from streamwright.spark.launchers.common import ENTRYPOINT_PATH, conf_args, jar_args, stage

__all__ = ["EmrEc2Launcher", "emr_step_args"]


def emr_step_args(conf, lock, staged, driver_argv, deploy_mode="cluster"):
    """The command-runner.jar arguments. `staged`: {'entry': uri, 'env': uri|None, 'job': uri, 'lock': uri}."""
    args = ["spark-submit", "--master", "yarn", "--deploy-mode", deploy_mode]
    if staged.get("env"):
        args += [
            "--archives",
            staged["env"] + "#env",
            "--conf",
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python",
            "--conf",
            "spark.executorEnv.PYSPARK_PYTHON=./env/bin/python",
        ]
    args += jar_args(lock) + conf_args(conf)
    args += ["--files", ",".join([staged["job"], staged["lock"], *staged.get("files", [])]), staged["entry"]]
    return args + list(driver_argv)


class EmrEc2Launcher:
    platform = "emr_ec2"
    contract_version = "1"

    def __init__(self, boto3_module=None, platform=None):
        self._boto3, self._platform_config, self._clients = boto3_module, platform, None

    def check_platform(self, platform):
        problems = []
        if not platform.get("region"):
            problems.append("region is required for emr_ec2")
        has_cluster = platform.get("cluster") is not None
        has_create = platform.get("create") is not None
        if has_cluster == has_create:
            problems.append("give exactly one of cluster and create")
        if platform.get("create"):
            problems.append(
                "transient cluster creation is not supported; give cluster.id, cluster.stack_name or cluster.stack_arn"
            )
        if has_cluster:
            cluster = platform.get("cluster")
            if not isinstance(cluster, dict) or not any(
                cluster.get(k) for k in ("id", "stack_name", "stack_arn", "stack")
            ):
                problems.append("cluster requires id, stack_name, or stack_arn")
        if not str(platform.get("staging", "")).startswith("s3://"):
            problems.append("staging must be an s3:// location")
        return problems

    def _client(self, platform):
        if self._clients is None:
            module = self._boto3
            if module is None:
                try:
                    module = import_module("boto3")
                except ImportError as exc:
                    raise LaunchError('boto3 is not installed; pip install "streamwright-spark[emr]"') from exc
            region = platform["region"]
            self._clients = (module.client("emr", region_name=region), module.client("s3", region_name=region), module)
        return self._clients

    def resolve_cluster_id(self, platform=None):
        client_error = import_module("botocore.exceptions").ClientError
        platform = platform or self._platform_config or {}
        cluster = platform.get("cluster") or {}
        if cluster.get("id"):
            return cluster["id"]
        stack = cluster.get("stack_name") or cluster.get("stack_arn") or cluster.get("stack")
        if stack:
            _, _, module = self._client(platform)
            cfn = module.client("cloudformation", region_name=platform.get("region"))
            logical_id = cluster.get("logical_resource_id") or cluster.get("logical_id")
            try:
                res = cfn.describe_stack_resources(StackName=stack).get("StackResources", [])
                emr_clusters = [r for r in res if r.get("ResourceType") == "AWS::EMR::Cluster"]
                if logical_id:
                    emr_clusters = [r for r in emr_clusters if r.get("LogicalResourceId") == logical_id]
                if emr_clusters and emr_clusters[0].get("PhysicalResourceId"):
                    return emr_clusters[0]["PhysicalResourceId"]
            except client_error as exc:
                raise LaunchError(
                    "could not inspect CloudFormation stack %r: %s"
                    % (stack, exc.response.get("Error", {}).get("Code", "ClientError"))
                ) from None
            try:
                stacks = cfn.describe_stacks(StackName=stack).get("Stacks", [])
                if stacks:
                    for out in stacks[0].get("Outputs", []):
                        if out.get("OutputKey") in ("ClusterId", "EMRClusterId", "JobFlowId", "Cluster"):
                            return out["OutputValue"]
            except client_error as exc:
                raise LaunchError(
                    "cannot resolve EMR cluster from CloudFormation stack %r: %s"
                    % (stack, exc.response.get("Error", {}).get("Code", "ClientError"))
                ) from None
            raise LaunchError(
                "no AWS::EMR::Cluster resource or ClusterId output found in CloudFormation stack %r" % stack
            )
        raise LaunchError("cluster requires id, stack_name, or stack_arn")

    def check_lock(self, lock, platform=None):
        platform = platform or self._platform_config
        emr, _, _ = self._client(platform)
        cluster_id = self.resolve_cluster_id(platform)
        info = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
        problems = []
        wanted = (lock.get("emr") or {}).get("release_label")
        if wanted and info.get("ReleaseLabel") != wanted:
            problems.append("cluster %s is %s, the lock says %s" % (info["Id"], info.get("ReleaseLabel"), wanted))
        if info["Status"]["State"] not in ("RUNNING", "WAITING"):
            problems.append("cluster is %s" % info["Status"]["State"])
        return problems

    def preview(self, job_path, argv, conf, lock, *, platform, lock_path=None, files=()):
        staging = platform["staging"].rstrip("/") + "/"

        def artifact(path):
            return "%s<sha256>/%s" % (staging, os.path.basename(path))

        environment = (lock.get("python_env") or {}).get("archive")
        staged = {
            "entry": artifact(ENTRYPOINT_PATH),
            "job": artifact(job_path),
            "lock": artifact(lock_path or "job.lock.json"),
            "env": artifact(environment) if environment else None,
            "files": [artifact(path) for path in files],
        }
        driver = [os.path.basename(job_path), "--lock", os.path.basename(lock_path or "job.lock.json"), *list(argv)]
        command = emr_step_args(conf, lock, staged, driver, platform.get("deploy_mode", "cluster"))
        step = {
            "Name": "streamwright-%s" % lock.get("job", "job"),
            "ActionOnFailure": (platform.get("step") or {}).get("action_on_failure", "CONTINUE"),
            "HadoopJarStep": {"Jar": "command-runner.jar", "Args": command},
        }
        cluster = platform.get("cluster") or {}
        target = cluster.get("id") or cluster.get("stack_name") or cluster.get("stack_arn") or "<cluster-id>"
        return {"command": command, "step": step, "target": target}

    def submit(self, job_path, argv, conf, lock, *, detach, lock_path=None, env_archive=None, platform=None, files=()):
        exceptions = import_module("botocore.exceptions")
        platform = platform or self._platform_config
        emr, s3, _ = self._client(platform)
        if lock_path is None:
            raise LaunchError("a lock file is required on EMR")
        entry_path = platform.get("entrypoint") or ENTRYPOINT_PATH
        staged = {
            "entry": stage(s3, platform["staging"], entry_path),
            "job": stage(s3, platform["staging"], job_path),
            "lock": stage(s3, platform["staging"], lock_path),
            "env": stage(s3, platform["staging"], env_archive) if env_archive else None,
            "files": [stage(s3, platform["staging"], path) for path in files],
        }
        driver_argv = [os.path.basename(job_path), "--lock", os.path.basename(lock_path), *list(argv)]
        step = {
            "Name": "streamwright-%s" % lock.get("job", "job"),
            "ActionOnFailure": (platform.get("step") or {}).get("action_on_failure", "CONTINUE"),
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": emr_step_args(conf, lock, staged, driver_argv, platform.get("deploy_mode", "cluster")),
            },
        }
        cluster_id = self.resolve_cluster_id(platform)
        step_id = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])["StepIds"][0]
        if detach:
            return 0
        try:
            emr.get_waiter("step_complete").wait(
                ClusterId=cluster_id, StepId=step_id, WaiterConfig={"Delay": 30, "MaxAttempts": 2880}
            )
        except (exceptions.ClientError, exceptions.WaiterError) as exc:
            try:
                status = emr.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]["Status"]
            except exceptions.ClientError:
                raise LaunchFailureError("EMR step %s did not complete: %s" % (step_id, type(exc).__name__)) from None
            details = status.get("FailureDetails") or status.get("StateChangeReason") or {}
            raise LaunchFailureError(
                "EMR step %s %s: %s"
                % (step_id, status.get("State", "failed"), details.get("Message", "no failure details"))
            ) from None
        return 0
