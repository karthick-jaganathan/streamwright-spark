import unittest

from streamwright.spark.contracts import LaunchError
from streamwright.spark.launchers.emr_ec2 import emr_step_args
from streamwright.spark.launchers.standalone import standalone_args

LOCK = {"jars": [{"coordinates": "org.postgresql:postgresql:42.7.4", "mode": "ship"},
                 {"coordinates": "io.delta:delta-spark_2.12:3.2.1", "mode": "provided"}]}
STAGED = {"entry": "s3://a/x/entry.py", "env": "s3://a/x/env.tar.gz", "job": "s3://a/x/job.yaml", "lock": "s3://a/x/l.json"}


class Args(unittest.TestCase):
    def test_emr_step(self):
        args = emr_step_args({"spark.executor.memory": "10g"}, LOCK, STAGED, ["job.yaml", "--lock", "l.json"])
        self.assertEqual(args[:5], ["spark-submit", "--master", "yarn", "--deploy-mode", "cluster"])
        self.assertIn("s3://a/x/env.tar.gz#env", args)
        self.assertIn("spark.executorEnv.PYSPARK_PYTHON=./env/bin/python", args)
        self.assertEqual(args[args.index("--jars") + 1], "org.postgresql:postgresql:42.7.4")   # provided is not shipped
        self.assertIn("spark.executor.memory=10g", args)
        self.assertEqual(args[-3:], ["job.yaml", "--lock", "l.json"])

    def test_launcher_owned_keys_are_refused(self):
        with self.assertRaises(LaunchError):
            emr_step_args({"spark.jars.packages": "x"}, LOCK, STAGED, [])

    def test_no_secret_can_ride_in_arguments(self):
        args = emr_step_args({}, LOCK, STAGED, ["job.yaml"])
        self.assertFalse(any("password" in a.lower() for a in args))

    def test_entrypoint_path_exists(self):
        import os
        from streamwright.spark.launchers.common import ENTRYPOINT_PATH
        self.assertTrue(os.path.exists(ENTRYPOINT_PATH))
        self.assertTrue(ENTRYPOINT_PATH.endswith("entrypoint.py"))
        with open(ENTRYPOINT_PATH, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("streamwright.spark.cli", content)
        self.assertIn("--driver", content)

    def test_emr_submit_stages_entrypoint(self):
        from unittest.mock import MagicMock
        from streamwright.spark.launchers.emr_ec2 import EmrEc2Launcher

        mock_boto3 = MagicMock()
        mock_emr = MagicMock()
        mock_s3 = MagicMock()
        mock_boto3.client.side_effect = lambda service, region_name=None: (
            mock_emr if service == "emr" else mock_s3
        )
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
            lock=LOCK,
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


if __name__ == "__main__":
    unittest.main()
