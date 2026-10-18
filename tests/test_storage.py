import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import support  # noqa: E402

support.install_entry_points()

from streamwright.connectors.azure_blob._spark import AzureBlobSpark
from streamwright.connectors.gcs._spark import GcsSpark
from streamwright.connectors.s3._spark import S3Spark
from streamwright.spark.engine.session import hadoop_settings  # noqa: E402
from streamwright.spark.contracts import Connection  # noqa: E402
from streamwright.spark.storage.paths import StorageError, check_path, spark_text  # noqa: E402

ROOTS = ["s3://lake/"]


class Paths(unittest.TestCase):
    def test_inside_roots(self):
        self.assertEqual(check_path("s3://lake/bronze/x", ROOTS), "s3://lake/bronze/x")
        self.assertEqual(check_path("s3a://lake/bronze", ROOTS), "s3a://lake/bronze")     # the Hadoop spelling

    def test_outside_or_traversal(self):
        for bad in ("s3://other/x", "s3://lake-evil/x", "s3://lake/../other/x", "", None):
            with self.assertRaises(StorageError, msg=bad):
                check_path(bad, ROOTS)

    def test_option_text(self):
        self.assertEqual((spark_text(True), spark_text(False), spark_text(5)), ("true", "false", "5"))


class Urls(unittest.TestCase):
    def test_each_adapter_translates_its_own_scheme(self):
        self.assertEqual(S3Spark().to_hadoop("s3://b/k"), "s3a://b/k")
        self.assertEqual(GcsSpark().to_hadoop("gs://b/k"), "s3a://b/k")
        self.assertTrue(AzureBlobSpark().to_hadoop("azure://c/k", {"account_name": "acct"}).startswith("abfss://c@acct"))


class Conf(unittest.TestCase):
    ADAPTERS = {"s3": S3Spark()}

    def test_localstack(self):
        conn = Connection("a", "s3", {"endpoint": "localstack:4566", "url_style": "path", "use_ssl": False,
                                      "key_id": "k", "secret": "s"})
        conf = hadoop_settings({"a": conn}, self.ADAPTERS)
        self.assertEqual(conf["fs.s3a.path.style.access"], "true")
        self.assertEqual(conf["fs.s3a.connection.ssl.enabled"], "false")
        self.assertEqual(conf["fs.s3a.secret.key"], "s")

    def test_emr_uses_default_chain(self):
        conf = hadoop_settings({"a": Connection("a", "s3", {"region": "us-east-1"})}, self.ADAPTERS)
        self.assertNotIn("fs.s3a.access.key", conf)


if __name__ == "__main__":
    unittest.main()
