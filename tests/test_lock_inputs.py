import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import support  # noqa: E402

support.install_entry_points()

from streamwright.spark.cli.common import InputsError, parse_set  # noqa: E402
from streamwright.spark.runtime import lock, registry  # noqa: E402

ADAPTERS = registry.load_all("adapter")[0]


def fake_installed(name):
    return {"delta-spark": "3.2.1", "confluent-kafka": "2.5.0", "streamwright-spark": "0.1.0"}.get(name)


class Lock(unittest.TestCase):
    def test_build_and_check(self):
        built = lock.build_lock("j", [ADAPTERS["deltalake"], ADAPTERS["kafka"]], "3.5.3", emr_release="emr-7.3.0",
                                installed=fake_installed)
        self.assertEqual(built["packages"]["delta-spark"], "3.2.1")
        self.assertIn({"coordinates": "io.delta:delta-spark_2.12:3.2.1", "mode": "provided"}, built["jars"])
        self.assertEqual(lock.check_lock(built, "3.5.3", installed=fake_installed, emr_release="emr-7.3.0"), [])
        problems = lock.check_lock(built, "3.5.3", installed=lambda n: "9.9", emr_release="emr-6.0")
        self.assertTrue(any("cluster release" in p for p in problems))
        self.assertTrue(any("delta-spark is 9.9" in p for p in problems))

    def test_missing_package_names_the_install_command(self):
        with self.assertRaises(lock.LockError) as caught:
            lock.build_lock("j", [ADAPTERS["sqs"]], "3.5.3", installed=lambda n: None)
        self.assertIn("boto3", str(caught.exception))
        self.assertIn("streamwright connectors install sqs", str(caught.exception))

    def test_fingerprint_is_stable(self):
        self.assertEqual(lock.fingerprint({"a": 1, "b": 2}), lock.fingerprint({"b": 2, "a": 1}))


class Set(unittest.TestCase):
    def test_set(self):
        self.assertEqual(parse_set(["a=1", "b=x=y"]), {"a": "1", "b": "x=y"})
        with self.assertRaises(InputsError):
            parse_set(["no-equals"])


if __name__ == "__main__":
    unittest.main()
