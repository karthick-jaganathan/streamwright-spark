"""The SparkSession, configured from runtime.spark.conf and each adapter's hooks."""

import contextlib
import socket

from pyspark.sql import SparkSession

from streamwright.spark.storage.paths import REDACTION_REGEX

__all__ = ["build_session", "hadoop_settings", "resolve_driver_host", "resolve_spark_metadata"]


def hadoop_settings(connections, adapters):
    """{fs.* key: value} from every connection's own adapter, excluding inline secrets."""
    conf = {}
    for conn in connections.values():
        adapter = adapters[conn.provider]
        settings = adapter.hadoop_settings(conn.name, conn.auth)
        secrets = [str(conn.auth[key]) for key in adapter.secret_keys if conn.auth.get(key)]
        if any(secret in str(value) for secret in secrets for value in settings.values()):
            raise ValueError(
                "%s credentials cannot be placed in Spark conf; use the platform credential chain" % conn.provider
            )
        conf.update(settings)
    return conf


def build_session(job, connections, adapters, local=False, log_level=None):
    runtime = job["runtime"]["spark"]
    builder = SparkSession.builder.appName(runtime.get("app_name") or job["name"])
    for key, value in (runtime.get("conf") or {}).items():
        builder = builder.config(key, str(value))
    builder = builder.config("spark.redaction.regex", REDACTION_REGEX)
    for key, value in hadoop_settings(connections, adapters).items():
        builder = builder.config("spark.hadoop." + key, value)
    if local:  # elsewhere the launcher owns these keys (and the jars they need)
        needed_packages = set()
        for conn in connections.values():
            for pkg in adapters[conn.provider].local_packages():
                needed_packages.add(pkg)
            for key, value in adapters[conn.provider].local_conf().items():
                builder = builder.config(key, value)
        if needed_packages:
            existing = (runtime.get("conf") or {}).get("spark.jars.packages", "")
            all_pkgs = ",".join(sorted(needed_packages | (set(existing.split(",")) if existing else set())))
            builder = builder.config("spark.jars.packages", all_pkgs)
    session = builder.getOrCreate()
    chk = (runtime.get("conf") or {}).get("spark.checkpoint.dir")
    if chk:
        session.sparkContext.setCheckpointDir(chk)
    effective_log_level = log_level or (runtime.get("conf") or {}).get("spark.log.level")
    if effective_log_level:
        session.sparkContext.setLogLevel(str(effective_log_level).upper())
    return session


def resolve_driver_host(spark):
    """
    Resolve the EMR driver host IP or FQDN, or fall back to the Spark driver host / local hostname.
    YARN resource manager hostname on EMR is typically an internal IP (e.g. 10.0.0.1) which is
    reverse-resolved to its FQDN (e.g. ip-10-0-0-1.us-west-2.compute.internal).
    """
    host = None
    try:
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        host = hconf.get("yarn.resourcemanager.hostname")
        if not host:
            host = (hconf.get("yarn.resourcemanager.address") or "").split(":")[0] or None
    except Exception:
        host = None
    if not host:
        try:
            host = spark.conf.get("spark.driver.host", None)
        except Exception:
            host = None
    if not host:
        try:
            host = socket.gethostname()
        except Exception:
            host = None
    if not host:
        return "unknown"
    try:
        return socket.getfqdn(host)
    except Exception:
        return host


def resolve_spark_metadata(spark):
    """(app_id, driver_host): Returns Spark applicationId and resolved driver host FQDN/IP."""
    app_id = None
    with contextlib.suppress(Exception):
        app_id = spark.sparkContext.applicationId
    driver_host = resolve_driver_host(spark)
    return app_id, driver_host
