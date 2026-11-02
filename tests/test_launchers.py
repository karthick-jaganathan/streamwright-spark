import os
import unittest
from types import MappingProxyType
from unittest.mock import MagicMock, patch

from botocore.exceptions import WaiterError

from streamwright.spark.cli.run import _spark_conf
from streamwright.spark.contracts import LaunchError
from streamwright.spark.launchers.common import ENTRYPOINT_PATH
from streamwright.spark.launchers.emr_ec2 import EmrEc2Launcher, emr_step_args
from streamwright.spark.launchers.local import LocalLauncher
from streamwright.spark.launchers.standalone import StandaloneLauncher


def lock():
    return {
        "jars": [
            {"coordinates": "org.postgresql:postgresql:42.7.4", "mode": "ship"},
            {"coordinates": "io.delta:delta-spark_2.12:3.2.1", "mode": "provided"},
        ]
    }


STAGED = MappingProxyType(
    {
        "entry": "s3://a/x/entry.py",
        "env": "s3://a/x/env.tar.gz",
        "job": "s3://a/x/job.yaml",
        "lock": "s3://a/x/l.json",
    }
)


class Args(unittest.TestCase):
    def test_cloudformation_ignores_unrelated_id_and_reports_waiter_failure(self):
        api = MagicMock()
        cloud = MagicMock()
        emr = MagicMock()
        s3 = MagicMock()
        api.client.side_effect = lambda name, region_name=None: {"emr": emr, "s3": s3, "cloudformation": cloud}[name]
        cloud.describe_stack_resources.return_value = {"StackResources": []}
        cloud.describe_stacks.return_value = {
            "Stacks": [{"Outputs": [{"OutputKey": "Unrelated", "OutputValue": "j-accidental"}]}]
        }
        launcher = EmrEc2Launcher(api)
        platform = {"region": "us-east-1", "cluster": {"stack_name": "stack"}, "staging": "s3://bucket/jobs/"}
        with self.assertRaisesRegex(LaunchError, "no AWS::EMR::Cluster"):
            launcher.resolve_cluster_id(platform)
        platform["cluster"] = {"id": "j-real"}
        emr.add_job_flow_steps.return_value = {"StepIds": ["s-step"]}
        emr.get_waiter.return_value.wait.side_effect = WaiterError(
            name="step_complete", reason="failed", last_response={}
        )
        emr.describe_step.return_value = {
            "Step": {"Status": {"State": "FAILED", "FailureDetails": {"Message": "driver stopped"}}}
        }
        with (
            patch("streamwright.spark.launchers.emr_ec2.stage", return_value="s3://bucket/staged"),
            self.assertRaisesRegex(LaunchError, "driver stopped"),
        ):
            launcher.submit("job.yaml", [], {}, lock(), detach=False, lock_path="job.lock", platform=platform)

    def test_launchers_preview_without_submitting_or_staging(self):
        flags = ["--overlay", "prod.yaml", "--once"]
        local = LocalLauncher().preview("job.yaml", flags, {}, lock(), platform={"type": "local"})
        standalone = StandaloneLauncher().preview(
            "job.yaml", flags, {}, lock(), platform={"master": "spark://master:7077"}
        )
        emr = EmrEc2Launcher().preview(
            "job.yaml", flags, {}, lock(), platform={"staging": "s3://bucket/jobs/", "cluster": {"id": "j-1"}}
        )
        self.assertIn("job.yaml", local["command"])
        self.assertIn("entrypoint.py", standalone["command"][standalone["command"].index("--files") + 2])
        self.assertEqual(emr["step"]["HadoopJarStep"]["Args"], emr["command"])
        self.assertEqual(emr["target"], "j-1")

    def test_standalone_submits_entrypoint_with_staged_job_and_lock(self):
        with patch("streamwright.spark.launchers.standalone.subprocess.run") as submitted:
            submitted.return_value.returncode = 0
            launcher = StandaloneLauncher()
            self.assertEqual(
                launcher.submit(
                    "jobs/job.yaml",
                    ["--overlay", "prod.yaml", "--config", "inputs.yaml"],
                    {},
                    lock(),
                    detach=False,
                    lock_path="locks/job.lock",
                    files=["overlays/prod.yaml", "inputs/inputs.yaml"],
                    platform={"master": "spark://master:7077"},
                ),
                0,
            )
        command = submitted.call_args.args[0]
        self.assertIn("entrypoint.py", command[command.index("--files") + 2])
        self.assertEqual(
            command[command.index("--files") + 1], "jobs/job.yaml,locks/job.lock,overlays/prod.yaml,inputs/inputs.yaml"
        )
        self.assertEqual(
            command[-7:],
            [
                "jobs/job.yaml",
                "--lock",
                "locks/job.lock",
                "--overlay",
                "overlays/prod.yaml",
                "--config",
                "inputs/inputs.yaml",
            ],
        )

    def test_emr_stages_overlay_and_config(self):
        module = MagicMock()
        emr, s3 = MagicMock(), MagicMock()
        module.client.side_effect = lambda name, region_name=None: emr if name == "emr" else s3
        emr.add_job_flow_steps.return_value = {"StepIds": ["step"]}
        launcher = EmrEc2Launcher(module)
        platform = {"region": "us-east-1", "cluster": {"id": "j-1"}, "staging": "s3://bucket/jobs/"}
        with unittest.mock.patch(
            "streamwright.spark.launchers.emr_ec2.stage", side_effect=lambda s3, root, path: "s3://bucket/" + path
        ):
            launcher.submit(
                "job.yaml",
                ["--overlay", "prod.yaml", "--config", "inputs.yaml"],
                {},
                lock(),
                detach=True,
                lock_path="job.lock",
                files=["prod.yaml", "inputs.yaml"],
                platform=platform,
            )
        args = emr.add_job_flow_steps.call_args.kwargs["Steps"][0]["HadoopJarStep"]["Args"]
        self.assertEqual(
            args[args.index("--files") + 1],
            "s3://bucket/job.yaml,s3://bucket/job.lock,s3://bucket/prod.yaml,s3://bucket/inputs.yaml",
        )
        self.assertEqual(args[-4:], ["--overlay", "prod.yaml", "--config", "inputs.yaml"])

    def test_secret_env_never_reaches_emr_step_arguments(self):
        job = {
            "runtime": {
                "env": {"STREAMWRIGHT_SECRET_TOKEN": "private-value", "MODE": "production"},
                "spark": {"conf": {}},
            }
        }
        conf = _spark_conf(job, [])
        args = emr_step_args(conf, lock(), STAGED, ["job.yaml"])
        self.assertNotIn("private-value", str(conf))
        self.assertNotIn("private-value", str(args))

    def test_emr_step(self):
        args = emr_step_args({"spark.executor.memory": "10g"}, lock(), STAGED, ["job.yaml", "--lock", "l.json"])
        self.assertEqual(args[:5], ["spark-submit", "--master", "yarn", "--deploy-mode", "cluster"])
        self.assertIn("s3://a/x/env.tar.gz#env", args)
        self.assertIn("spark.executorEnv.PYSPARK_PYTHON=./env/bin/python", args)
        self.assertEqual(args[args.index("--jars") + 1], "org.postgresql:postgresql:42.7.4")  # provided is not shipped
        self.assertIn("spark.executor.memory=10g", args)
        self.assertEqual(args[-3:], ["job.yaml", "--lock", "l.json"])

    def test_launcher_owned_keys_are_refused(self):
        with self.assertRaises(LaunchError):
            emr_step_args({"spark.jars.packages": "x"}, lock(), STAGED, [])

    def test_no_secret_can_ride_in_arguments(self):
        args = emr_step_args({}, lock(), STAGED, ["job.yaml"])
        self.assertFalse(any("password" in a.lower() for a in args))

    def test_entrypoint_path_exists(self):
        self.assertTrue(os.path.exists(ENTRYPOINT_PATH))
        self.assertTrue(ENTRYPOINT_PATH.endswith("entrypoint.py"))
        with open(ENTRYPOINT_PATH, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("streamwright.spark.cli", content)
        self.assertIn("--driver", content)

    def test_emr_submit_stages_entrypoint(self):
        mock_boto3 = MagicMock()
        mock_emr = MagicMock()
        mock_s3 = MagicMock()
        mock_boto3.client.side_effect = lambda service, region_name=None: mock_emr if service == "emr" else mock_s3
        mock_emr.add_job_flow_steps.return_value = {"StepIds": ["s-12345"]}

        platform = {
            "type": "emr_ec2",
            "region": "us-east-1",
            "staging": "s3://my-bucket/staging/",
            "cluster": {"id": "j-12345"},
        }
        launcher = EmrEc2Launcher(boto3_module=mock_boto3, platform=platform)
        mock_s3.head_object.return_value = {}

        ret = launcher.submit(
            job_path="s3://my-bucket/job.yaml",
            argv=["--once"],
            conf={},
            lock=lock(),
            detach=True,
            lock_path="s3://my-bucket/lock.json",
            platform=platform,
        )
        self.assertEqual(ret, 0)
        self.assertTrue(mock_emr.add_job_flow_steps.called)
        call_args = mock_emr.add_job_flow_steps.call_args[1]
        step = call_args["Steps"][0]
        step_args = step["HadoopJarStep"]["Args"]
        self.assertTrue(any("entrypoint.py" in a for a in step_args))
        self.assertEqual(step_args[-4:], ["job.yaml", "--lock", "lock.json", "--once"])

    def test_emr_resolve_cluster_from_cloudformation_resources(self):
        mock_boto3 = MagicMock()
        mock_cfn = MagicMock()
        mock_emr = MagicMock()
        mock_s3 = MagicMock()

        def client_factory(service, region_name=None):
            if service == "cloudformation":
                return mock_cfn
            if service == "emr":
                return mock_emr
            return mock_s3

        mock_boto3.client.side_effect = client_factory
        mock_cfn.describe_stack_resources.return_value = {
            "StackResources": [
                {"ResourceType": "AWS::S3::Bucket", "PhysicalResourceId": "my-bucket"},
                {
                    "ResourceType": "AWS::EMR::Cluster",
                    "PhysicalResourceId": "j-CFN123456",
                    "LogicalResourceId": "EMRCluster",
                },
            ]
        }
        platform = {
            "type": "emr_ec2",
            "region": "us-east-1",
            "staging": "s3://my-bucket/staging/",
            "cluster": {"stack_name": "prod-data-platform"},
        }
        launcher = EmrEc2Launcher(boto3_module=mock_boto3, platform=platform)
        self.assertEqual(launcher.resolve_cluster_id(platform), "j-CFN123456")
        mock_cfn.describe_stack_resources.assert_called_with(StackName="prod-data-platform")

    def test_emr_resolve_cluster_from_cloudformation_outputs(self):
        mock_boto3 = MagicMock()
        mock_cfn = MagicMock()
        mock_emr = MagicMock()
        mock_s3 = MagicMock()

        def client_factory(service, region_name=None):
            if service == "cloudformation":
                return mock_cfn
            if service == "emr":
                return mock_emr
            return mock_s3

        mock_boto3.client.side_effect = client_factory
        mock_cfn.describe_stack_resources.return_value = {"StackResources": []}
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"Outputs": [{"OutputKey": "EMRClusterId", "OutputValue": "j-OUTPUT789"}]}]
        }
        platform = {
            "type": "emr_ec2",
            "region": "us-east-1",
            "staging": "s3://my-bucket/staging/",
            "cluster": {"stack_arn": "arn:aws:cloudformation:us-east-1:123:stack/my-stack/abc"},
        }
        launcher = EmrEc2Launcher(boto3_module=mock_boto3, platform=platform)
        self.assertEqual(launcher.resolve_cluster_id(platform), "j-OUTPUT789")

    def test_emr_check_platform_validates_cluster(self):
        launcher = EmrEc2Launcher()
        self.assertEqual(
            launcher.check_platform(
                {"type": "emr_ec2", "region": "us-east-1", "staging": "s3://b/s/", "cluster": {"id": "j-1"}}
            ),
            [],
        )
        self.assertEqual(
            launcher.check_platform(
                {
                    "type": "emr_ec2",
                    "region": "us-east-1",
                    "staging": "s3://b/s/",
                    "cluster": {"stack_name": "my-stack"},
                }
            ),
            [],
        )
        self.assertTrue(
            any(
                "requires id, stack_name, or stack_arn" in p
                for p in launcher.check_platform(
                    {"type": "emr_ec2", "region": "us-east-1", "staging": "s3://b/s/", "cluster": {}}
                )
            )
        )


if __name__ == "__main__":
    unittest.main()
