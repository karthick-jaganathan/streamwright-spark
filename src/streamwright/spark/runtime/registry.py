"""Discover Spark adapters through connector definitions."""

from types import MappingProxyType

from streamwright.core.jobs.types import CONTRACT_VERSION
from streamwright.core.runtime import plugins
from streamwright.core.runtime.definitions import load_definition
from streamwright.spark.runtime.definition import DefinedAdapter, bind
from streamwright.spark.runtime.errors import RegistryError, SparkAdapterLoadError

__all__ = ["GROUPS", "Registry", "RegistryError", "SparkAdapterLoadError", "available", "load", "load_all"]

GROUPS = MappingProxyType({"adapter": plugins.CONNECTORS, "launcher": plugins.SPARK_LAUNCHERS})


class UnsupportedSparkAdapterError(RegistryError):
    """A connector exists but does not declare a Spark adapter."""


class Registry:
    def __init__(self, *, entry_points=None, adapters=None, launchers=None):
        self.entry_points = entry_points
        self.registered = {"adapter": dict(adapters or {}), "launcher": dict(launchers or {})}

    def register(self, plugin, kind="adapter"):
        if kind == "adapter":
            if not isinstance(plugin, DefinedAdapter):
                raise RegistryError("Spark adapters must come from a ConnectorDefinition")
            name = plugin.provider
        else:
            name = self._validate_launcher(plugin, plugin.platform)
        self.registered[kind][name] = plugin

    def available(self, kind="adapter"):
        names = {entry["name"] for entry in plugins.discover(GROUPS[kind], entry_points=self.entry_points)}
        return sorted(names | set(self.registered[kind]))

    @staticmethod
    def _validate_launcher(plugin, name):
        required = ("check_platform", "check_lock", "preview")
        if name != "local":
            required += ("submit",)
        if any(not callable(getattr(plugin, method, None)) for method in required):
            raise RegistryError("Spark launcher does not implement its platform contract")
        if getattr(plugin, "contract_version", None) != CONTRACT_VERSION:
            raise RegistryError(f"Spark launcher {name!r} has incompatible contract version")
        if not isinstance(name, str) or not name or plugin.platform != name:
            raise RegistryError(f"Spark launcher {name!r} has an invalid platform")
        return name

    def load(self, name, kind="adapter", allow=None):
        if allow is not None and name not in allow:
            raise RegistryError(f"{kind} {name!r} is not in the allowed list")
        if name in self.registered[kind]:
            return self.registered[kind][name]
        try:
            if kind == "adapter":
                definition = load_definition(name, allowed=allow, entry_points=self.entry_points)
                if "spark" not in definition.engines:
                    raise UnsupportedSparkAdapterError(f"connector {name!r} does not support Spark")
                return bind(definition, name)
            component = plugins.load_plugin(
                plugins.SPARK_LAUNCHERS, name, allowed=allow, entry_points=self.entry_points
            )
        except RegistryError:
            raise
        except plugins.PluginError as exc:
            raise RegistryError(str(exc)) from None
        if not isinstance(component, type):
            raise RegistryError(f"{kind} {name!r} must expose a class")
        try:
            plugin = component()
        except Exception as exc:
            raise RegistryError(f"{kind} {name!r} could not be initialized ({type(exc).__name__})") from None
        self._validate_launcher(plugin, name)
        return plugin

    def load_all(self, kind="adapter", allow=None):
        found, problems = {}, []
        for name in self.available(kind):
            if allow is not None and name not in allow:
                continue
            try:
                found[name] = self.load(name, kind, allow)
            except UnsupportedSparkAdapterError:
                continue
            except RegistryError as exc:
                problems.append(str(exc))
        return found, problems


def available(kind="adapter"):
    return Registry().available(kind)


def load(name, kind="adapter", allow=None):
    return Registry().load(name, kind, allow)


def load_all(kind="adapter", allow=None):
    return Registry().load_all(kind, allow)
