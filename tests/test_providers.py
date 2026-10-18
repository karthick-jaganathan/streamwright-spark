import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import support  # noqa: E402

support.install_entry_points()        # the connectors (streamwright.connectors); the catalog is static
support.tolerate_missing_duckdb()

from streamwright.spark.contracts import Role  # noqa: E402
from streamwright.spark.runtime import registry  # noqa: E402

CATALOG, _PROBLEMS = registry.load_all("adapter")


def check(name, service, method, arguments):
    p = CATALOG[name]
    return p.check_call(p.method(service, method), arguments, p.load_connector())


class Catalog(unittest.TestCase):
    def test_every_connector_has_an_adapter(self):
        self.assertEqual(_PROBLEMS, [])
        self.assertEqual(sorted(CATALOG), ["azure_blob", "deltalake", "files", "gcs", "kafka", "postgres", "s3", "sqs"])

    def test_provider_name_is_the_connector_name(self):
        for name, p in CATALOG.items():
            self.assertEqual((p.provider, p.connector), (name, name))

    def test_kinds(self):
        kind = lambda n: type(CATALOG[n]).__mro__[1].__name__
        self.assertEqual(kind("s3"), "Filesystem")
        self.assertEqual(kind("gcs"), "Filesystem")
        self.assertEqual(kind("postgres"), "Jdbc")
        self.assertEqual(kind("deltalake"), "Delta")
        self.assertEqual(kind("kafka"), "Broker")

    def test_every_method_names_a_handler_or_a_role(self):
        for name, p in CATALOG.items():
            for method in p.methods:
                if method.role is None:
                    self.assertTrue(callable(getattr(p, method.handler, None)), "%s.%s" % (name, method.name))
                if method.check:
                    self.assertTrue(callable(getattr(p, method.check)), "%s.%s check" % (name, method.name))
                self.assertIn(method.role, (None, Role.CONSUME, Role.PUBLISH))

    def test_missing_connector_names_the_install_command(self):
        p = CATALOG["s3"]

        def missing(_name):
            raise RuntimeError("not installed")
        with self.assertRaises(Exception) as caught:
            p.load_connector(missing)
        self.assertIn("streamwright connectors install s3", str(caught.exception))


class FactsDrivenBehaviour(unittest.TestCase):
    def test_scheme_translation_per_filesystem(self):
        self.assertEqual(CATALOG["s3"].to_hadoop("s3://b/k"), "s3a://b/k")
        self.assertEqual(CATALOG["gcs"].to_hadoop("gs://b/k"), "s3a://b/k")
        self.assertTrue(CATALOG["azure_blob"].to_hadoop("azure://c/k", {"account_name": "acct"}).startswith(
            "abfss://c@acct"))
        self.assertEqual(CATALOG["files"].to_hadoop("/tmp/x"), "file:///tmp/x")

    def test_jars_and_jdbc_url_from_facts(self):
        self.assertIn(("org.postgresql:postgresql", "ship", "42.7.4"), CATALOG["postgres"].jars("3.5"))
        url, props = CATALOG["postgres"].jdbc({"host": "h", "database": "d", "user": "u", "password": "p"})
        self.assertEqual(url, "jdbc:postgresql://h:5432/d")
        self.assertEqual(props["driver"], "org.postgresql.Driver")

    def test_default_credential_chain(self):
        self.assertEqual(CATALOG["s3"].check_connection({"roots": ["s3://b/"], "region": "us-east-1"},
                                                        CATALOG["s3"].load_connector()), [])


class ConnectorChecks(unittest.TestCase):
    def test_read_arguments_go_through_the_connector(self):
        self.assertEqual(check("s3", "object", "read",
                               {"path": "s3://b/x.csv", "format": "csv", "options": {"header": True}}), [])
        self.assertTrue(check("s3", "object", "read", {"path": "s3://b/x.csv", "format": "xml"}))

    def test_spark_only_write_arguments(self):
        self.assertTrue(any("must be one of" in p for p in check("s3", "object", "write",
                        {"path": "s3://b/x", "format": "csv", "mode": "explode"})))

    def test_delta_overwrite_and_merge(self):
        base = {"table_path": "s3://b/t"}
        self.assertTrue(any("replace_where" in p for p in check("deltalake", "tables", "overwrite", base)))
        self.assertEqual(check("deltalake", "tables", "overwrite", dict(base, whole_table=True)), [])
        p = CATALOG["deltalake"]
        merge = p.method("tables", "merge")
        m = {"table_path": "s3://b/t", "condition": "target.id = source.id", "when_matched": [{"action": "update"}]}
        self.assertEqual(len(p.warn_call(merge, m)), 1)                                   # no predicate
        self.assertEqual(p.warn_call(merge, dict(m, target_predicate="target.d IN ({{ s.d }})")), [])
        # when_matched with set mapping
        m_set = {"table_path": "s3://b/t", "condition": "target.id = source.id",
                 "when_matched": [{"action": "update", "set": {"amount": "source.amount"}}]}
        self.assertEqual(check("deltalake", "tables", "merge", m_set), [])
        # when_matched with invalid non-dict set
        m_bad_set = {"table_path": "s3://b/t", "condition": "target.id = source.id",
                     "when_matched": [{"action": "update", "set": "not_a_dict"}]}
        self.assertTrue(any("'set' must be a mapping" in err for err in check("deltalake", "tables", "merge", m_bad_set)))

    def test_broker_consume(self):
        self.assertEqual(check("kafka", "topic", "consume", {"topic": "t", "group_id": "g"}), [])
        self.assertTrue(check("kafka", "topic", "consume", {"topic": "bad topic", "group_id": "g"}))

    def test_postgres_state_and_azure_rule(self):
        self.assertTrue(CATALOG["postgres"].holds_state)
        self.assertFalse(CATALOG["s3"].holds_state)
        self.assertTrue(any("account_key or connection_string" in p
                            for p in CATALOG["azure_blob"].check_connection({"account_name": "a"},
                                                                            CATALOG["azure_blob"].load_connector())))


if __name__ == "__main__":
    unittest.main()


class LazyLoading(unittest.TestCase):
    NAMES = ("azure_blob", "deltalake", "files", "gcs", "kafka", "postgres", "s3", "sqs")

    def test_spark_entry_modules_import_without_streamwright_spark(self):
        import importlib
        import subprocess
        code = ("import sys, importlib\n"
                "class Block:\n"
                "    def find_spec(self, name, path=None, target=None):\n"
                "        if name == 'streamwright.spark' or name.startswith('streamwright.spark.'):\n"
                "            raise ModuleNotFoundError('blocked', name=name)\n"
                "sys.meta_path.insert(0, Block())\n"
                "for n in %r:\n"
                "    importlib.import_module('streamwright.connectors.%%s.spark' %% n)\n"
                "from streamwright.connectors.s3.spark import load\n"
                "try:\n    load()\nexcept ImportError as e:\n    print('MSG', e)\n" % (self.NAMES,))
        env = dict(os.environ)
        out = subprocess.run([sys.executable, "-B", "-c", code], env=env, capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn('pip install "streamwright-s3[spark]"', out.stdout)

    def test_factory_returns_the_adapter_class(self):
        from streamwright.connectors.postgres.spark import load
        self.assertEqual(load().__name__, "PostgresSpark")

    def test_registry_reports_the_fix_when_spark_is_missing(self):
        from streamwright.spark.runtime import registry
        entry = type("E", (), {"name": "s3", "load": staticmethod(lambda: (_ for _ in ()).throw(ImportError("needs X")))})
        original = registry._entry_points
        registry._entry_points = lambda group: [entry] if group.endswith("connectors") else []
        registry._LOADED["adapter"].pop("s3", None)
        try:
            with self.assertRaises(registry.RegistryError) as caught:
                registry.load("s3", "adapter")
            self.assertIn("needs X", str(caught.exception))
        finally:
            registry._entry_points = original
