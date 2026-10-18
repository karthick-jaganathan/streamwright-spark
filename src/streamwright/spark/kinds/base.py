"""
The provider base class. PROPOSED.

`Provider` is the generic Spark side of a StreamWright connector: it validates a connection and a call by delegating to
the connector (the single source of truth for auth keys and known arguments), and exposes the Spark facts the engine
needs (jars, Hadoop conf, URL translation, broker, state). It names no provider.

Concrete providers are tiny subclasses of a KIND (Filesystem, Jdbc, Delta, Broker) registered in `catalog.py`; the kind
carries the behaviour, the subclass only states facts (name, scheme, jars, secret keys). Dispatch stays by table:
`Method.handler` names the method the engine calls, so nothing branches on a provider name.
"""

from streamwright.spark.contracts.adapter import AdapterError, _check_type


__all__ = ["Provider"]


class Provider(object):
    provider = None            # the `provider:` of a connection; equals the connector's name
    connector = None           # the existing connector (group streamwright.connectors), or None
    methods = ()               # Method objects: the dispatch table (often built by the kind from `service`)
    extra_auth = {}            # Spark-only connection keys {name: {"type": ...}}
    secret_keys = ()           # connection keys that must be {{ secrets.NAME }} references
    packages = ()              # Python distributions the run needs (checked against the lock)
    holds_state = False        # True when runtime.state can live in this provider (see state_connection)

    # --- table lookups ---------------------------------------------------------------------------------------

    def method(self, service, name):
        for candidate in self.methods:
            if candidate.service == service and candidate.name == name:
                return candidate
        return None

    def describe_methods(self):
        return ", ".join("%s.%s" % (m.service, m.name) for m in self.methods)

    @property
    def install_hint(self):
        return "streamwright connectors install %s" % self.connector if self.connector else ""

    # --- the existing connector ------------------------------------------------------------------------------

    def load_connector(self, loader=None):
        """The installed connector object; AdapterError with the install command when it is missing."""
        if self.connector is None:
            return None
        if loader is None:
            from streamwright.core.runtime import components
            loader = components.load
        try:
            return loader(self.connector)
        except Exception as exc:
            raise AdapterError("provider %r needs the %r connector: %s (streamwright connectors install %s)" % (
                self.provider, self.connector, str(exc).split(";")[0], self.connector))

    # --- validation (no Spark, no network) -------------------------------------------------------------------

    def prepare_auth(self, auth):
        """The auth keys as the connector should see them (override to add placeholders for optional credentials)."""
        return dict(auth)

    def check_connection(self, auth, connector=None):
        """Problems with a connection's keys. `auth` excludes `provider`."""
        problems = []
        known = set(self.extra_auth)
        rest = dict((k, v) for k, v in auth.items() if k not in known)
        if connector is not None:
            problems += list(connector.check_auth(self.prepare_auth(rest)))
        return problems

    def check_call(self, method, arguments, connector=None):
        """Problems with one call's arguments: table rules, then the connector for the pass-through ones."""
        problems = []
        for key in arguments:
            if key not in method.arguments:
                problems.append("unknown %r (accepts: %s)" % (key, ", ".join(sorted(method.arguments))))
        for key, rule in method.arguments.items():
            if rule.get("required") and key not in arguments:
                problems.append("missing required %r" % key)
            if key in arguments:
                problems += _check_type(key, rule, arguments[key])
        if connector is not None and method.passthrough and not problems:
            service, name = method.connector_method or method.key
            known = dict((k, arguments[k]) for k in method.passthrough if k in arguments)
            request = {"name": "spark", "sdk": self.connector, "service": service, "method": name, "arguments": known}
            problems += [str(p) for p in connector.check_request(request)]
        if method.check and not problems:
            problems += list(getattr(self, method.check)(arguments))
        return problems

    def check_links(self, auth, connections):
        """Problems with references to OTHER connections (e.g. deltalake `storage`). `connections` is {name: dict}."""
        return []

    def warn_call(self, method, arguments):
        """Warnings (not errors) about a call, e.g. a merge without a target predicate."""
        return []

    # --- Spark facts the engine asks for (override) ----------------------------------------------------------

    def hadoop_conf(self, auth):
        return {}

    def local_conf(self):
        """Extra Spark settings for platform: local only (elsewhere the launcher owns them), e.g. Delta's extension."""
        return {}

    def jars(self, spark_minor):
        """[(maven coordinates without version, mode ship|provided|baked, default version or None)]"""
        return []

    def to_hadoop(self, url, auth=None):
        """A connector-style URL (s3://...) in the form Spark's Hadoop layer needs (s3a://...)."""
        return url

    def source_file(self, column):
        """A Spark Column expression turning input_file_name() back into the connector-style path."""
        return column

    def roots(self, auth):
        return list(auth.get("roots") or [])

    def broker(self, auth):
        raise AdapterError("%s is not a message broker" % self.provider)

    def state_connection(self, auth):
        raise AdapterError("%s cannot hold bookkeeping state" % self.provider)
