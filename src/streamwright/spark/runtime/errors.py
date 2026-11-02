"""Spark plug-in validation errors."""

from streamwright.core.runtime.plugins import PluginError


class RegistryError(PluginError):
    """A Spark plug-in does not satisfy its declared contract."""


class SparkAdapterLoadError(RegistryError):
    """A connector declares a Spark adapter, but it cannot be imported (e.g. its Spark extra is not installed)."""
