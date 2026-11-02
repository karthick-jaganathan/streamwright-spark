"""Bind a declarative connector to its Spark adapter without dispatch tables."""

from dataclasses import dataclass
from typing import Any

from streamwright.core.sdk.contract import AdapterLoadError, ConnectorDefinition
from streamwright.spark.io.capabilities import CAPABILITIES as READ_WRITE_CAPABILITIES
from streamwright.spark.io.messaging import CAPABILITIES as MESSAGE_CAPABILITIES
from streamwright.spark.runtime.errors import RegistryError, SparkAdapterLoadError


@dataclass(frozen=True)
class DefinedAdapter:
    definition: ConnectorDefinition
    adapter: Any

    @property
    def capabilities(self):
        return MESSAGE_CAPABILITIES if self.definition.family == "messaging" else READ_WRITE_CAPABILITIES

    @property
    def provider(self):
        return self.definition.name

    @property
    def secret_keys(self):
        return tuple(field.name for field in self.definition.secret_fields)

    @property
    def packages(self):
        return tuple(getattr(self.adapter, "packages", ()))

    @property
    def install_hint(self):
        return "streamwright connectors install " + self.provider

    @property
    def holds_state(self):
        return callable(getattr(self.adapter, "state_connection", None))

    def state_connection(self, auth):
        """(connect, dialect) of the bookkeeping store on this connection (adapters that hold state)."""
        return self.adapter.state_connection(auth)

    def check_links(self, auth, connections):
        """Problems with references to other connections (e.g. a table format's `storage`)."""
        check = getattr(self.adapter, "check_links", None)
        return list(check(auth, connections)) if check else []

    def operation(self, service, method):
        try:
            operation = self.definition.operation(service, method)
        except (KeyError, ValueError):
            return None
        return operation if operation in self.definition.operations_for(self.capabilities) else None

    def local_packages(self):
        return tuple(f"{coordinates}:{version}" for coordinates, _, version in self.jars(None) if version)

    def local_conf(self):
        return dict(getattr(self.adapter, "local_conf", {}))

    def jars(self, spark_minor):
        return list(getattr(self.adapter, "jars", ()))

    def template_tokens(self):
        return tuple(getattr(self.adapter, "template_tokens", ()))

    def hadoop_settings(self, name, connection):
        return self.adapter.hadoop_settings(name, connection)

    def describe(self):
        return self.definition.describe(self.capabilities)


def bind(definition, name):
    if not isinstance(definition, ConnectorDefinition) or definition.name != name:
        raise RegistryError(f"connector {name!r} must expose a matching ConnectorDefinition")
    if READ_WRITE_CAPABILITIES.engine not in definition.engines:
        raise RegistryError(f"connector {name!r} does not support Spark")
    try:
        adapter = definition.load_adapter("spark")
    except (AdapterLoadError, ImportError, AttributeError, TypeError, ValueError) as exc:
        extra = 'pip install "streamwright-%s[spark]"' % name.replace("_", "-")
        raise SparkAdapterLoadError(f"connector {name!r}: its Spark adapter cannot be loaded "
                                    f"({type(exc).__name__}: {exc}); install the connector's Spark extra: {extra}"
                                    ) from None
    if isinstance(adapter, type):
        try:
            adapter = adapter()
        except (TypeError, ValueError) as exc:
            raise RegistryError(
                f"connector {name!r} Spark adapter could not be initialized ({type(exc).__name__})"
            ) from None
    for method in ("hadoop_settings",):
        if not callable(getattr(adapter, method, None)):
            raise RegistryError(f"connector {name!r} Spark adapter lacks {method}")
    capabilities = MESSAGE_CAPABILITIES if definition.family == "messaging" else READ_WRITE_CAPABILITIES
    kinds = {operation.kind for operation in definition.operations_for(capabilities)}
    required = ("open_broker",) if kinds & {"consume", "publish"} else ("read", "write")
    for method in required:
        if not callable(getattr(adapter, method, None)):
            raise RegistryError(f"connector {name!r} Spark adapter lacks {method}")
    return DefinedAdapter(definition, adapter)
