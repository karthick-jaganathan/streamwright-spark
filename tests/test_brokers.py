import unittest

from streamwright.spark.kinds import BrokerError
from streamwright.connectors.kafka._spark import client_conf, parse_seconds


class Conf(unittest.TestCase):
    def test_msk_iam(self):
        conf = client_conf({"bootstrap_servers": "b:9098", "security_protocol": "SASL_SSL",
                            "sasl_mechanism": "AWS_MSK_IAM", "region": "us-east-1"})
        self.assertEqual(conf["sasl.mechanisms"], "OAUTHBEARER")
        self.assertEqual(conf["security.protocol"], "SASL_SSL")

    def test_plain_dev(self):
        self.assertEqual(client_conf({"bootstrap_servers": "kafka:9092"})["security.protocol"], "PLAINTEXT")

    def test_rejects_unknown_values(self):
        with self.assertRaises(BrokerError):
            client_conf({"bootstrap_servers": "b", "security_protocol": "HTTP"})
        with self.assertRaises(BrokerError):
            client_conf({"bootstrap_servers": "b", "sasl_mechanism": "MD5"})

    def test_seconds(self):
        self.assertEqual(parse_seconds("30s"), 30)
        self.assertEqual(parse_seconds("2m"), 120)
        with self.assertRaises(BrokerError):
            parse_seconds("soon")


if __name__ == "__main__":
    unittest.main()


class Errors(unittest.TestCase):
    def test_missing_topic_says_what_to_do(self):
        from streamwright.connectors.kafka._spark import describe_error

        class E(object):
            def __init__(self, name):
                self._name = name

            def name(self):
                return self._name
        text = describe_error(E("UNKNOWN_TOPIC_OR_PART"), "lake.file_arrived.v1")
        self.assertIn("does not exist", text)
        self.assertIn("lake.file_arrived.v1", text)
        self.assertEqual(describe_error(E("_TRANSPORT"), "t"), "kafka error: _TRANSPORT")

    def test_auto_offset_reset_validation(self):
        from streamwright.connectors.kafka._spark import KafkaClient
        client = KafkaClient({"bootstrap_servers": "localhost:9092"})
        with self.assertRaises(BrokerError) as ctx:
            client.poll({"group.id": "grp", "topic": "top", "auto.offset.reset": "invalid_val"}, 10, 1)
        self.assertIn("auto.offset.reset must be 'earliest' or 'latest'", str(ctx.exception))

    def test_kafka_vendor_keys_accepted(self):
        from streamwright.connectors.kafka._spark import KafkaSpark
        spark_kafka = KafkaSpark()
        method = spark_kafka.method("topic", "consume")
        self.assertIn("group.id", method.arguments)
        self.assertIn("auto.offset.reset", method.arguments)
        self.assertIn("max.poll.records", method.arguments)
        self.assertIn("fetch.max.wait.ms", method.arguments)
        self.assertIn("options", method.arguments)
        # Check validation accepts standard vendor keys
        problems = spark_kafka.check_call(method, {"topic": "my_top", "group.id": "my_grp", "auto.offset.reset": "latest",
                                                   "max.poll.records": 100, "options": {"session.timeout.ms": 30000}})
        self.assertEqual(problems, [])

    def test_sqs_vendor_keys_accepted(self):
        from streamwright.connectors.sqs._spark import SqsSpark
        spark_sqs = SqsSpark()
        method = spark_sqs.method("queue", "consume")
        self.assertIn("QueueUrl", method.arguments)
        self.assertIn("MaxNumberOfMessages", method.arguments)
        self.assertIn("WaitTimeSeconds", method.arguments)
        self.assertIn("VisibilityTimeout", method.arguments)
        self.assertIn("options", method.arguments)
        # Check validation accepts standard vendor keys
        problems = spark_sqs.check_call(method, {"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123/q",
                                                 "MaxNumberOfMessages": 10, "WaitTimeSeconds": 20,
                                                 "VisibilityTimeout": 60, "options": {"foo": "bar"}})
        self.assertEqual(problems, [])

