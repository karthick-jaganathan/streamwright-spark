"""
Finding adapters and launchers. PROPOSED.

Both adapters and launchers are plugins found through entry points. An adapter is declared in the CONNECTOR's own
pyproject, so a provider is defined in exactly one place (its connector) and streamwright-spark holds no per-provider
code:

    # in connectors/readers/s3/pyproject.toml; `load` is a tiny lazy factory returning the adapter class
    [project.entry-points."streamwright.spark.connectors"]
    s3 = "streamwright.connectors.s3.spark:load"
    # in streamwright-spark
    [project.entry-points."streamwright.spark.launchers"]
    emr_ec2 = "streamwright.spark.launchers.emr_ec2:EmrEc2Launcher"

Nothing is imported until a plugin is asked for. A plugin that fails to load is reported, not fatal, so one broken
install cannot stop `sw spark validate` for jobs that do not use it. `register()` is for tests and embedding.
"""

from importlib import metadata


__all__ = ["GROUPS", "RegistryError", "register", "unregister", "available", "load", "load_all"]

GROUPS = {"adapter": "streamwright.spark.connectors", "launcher": "streamwright.spark.launchers"}
_REGISTERED = dict((kind, {}) for kind in GROUPS)
_LOADED = dict((kind, {}) for kind in GROUPS)


class RegistryError(Exception):
    pass


def _name_of(kind, plugin):
    return plugin.provider if kind == "adapter" else plugin.platform


def register(plugin, kind="adapter"):
    _REGISTERED[kind][_name_of(kind, plugin)] = plugin


def unregister(name, kind="adapter"):
    _REGISTERED[kind].pop(name, None)
    _LOADED[kind].pop(name, None)


def _entry_points(group):
    found = metadata.entry_points()
    return list(found.select(group=group)) if hasattr(found, "select") else list(found.get(group, []))


def available(kind="adapter"):
    """Names that can be loaded; nothing is imported."""
    return sorted(set(_REGISTERED[kind]) | set(e.name for e in _entry_points(GROUPS[kind])))


def load(name, kind="adapter"):
    if name in _REGISTERED[kind]:
        return _REGISTERED[kind][name]
    if name in _LOADED[kind]:
        return _LOADED[kind][name]
    entries = [e for e in _entry_points(GROUPS[kind]) if e.name == name]
    if not entries:
        raise RegistryError("%s %r is not installed (installed: %s)" % (kind, name, ", ".join(available(kind)) or "none"))
    try:
        loaded = entries[0].load()
        if callable(loaded) and not isinstance(loaded, type) and not hasattr(loaded, "provider") \
                and not hasattr(loaded, "platform"):
            loaded = loaded()                       # a lazy factory, e.g. streamwright.connectors.s3.spark:load
        plugin = loaded() if isinstance(loaded, type) else loaded
    except ImportError as exc:                      # the message names the fix (e.g. pip install "streamwright-s3[spark]")
        raise RegistryError("%s %r could not be loaded: %s" % (kind, name, exc))
    except Exception as exc:
        raise RegistryError("%s %r could not be loaded: %s" % (kind, name, type(exc).__name__))
    if _name_of(kind, plugin) != name:
        raise RegistryError("entry point %r provides %r" % (name, _name_of(kind, plugin)))
    _LOADED[kind][name] = plugin
    return plugin


def load_all(kind="adapter", allow=None):
    """({name: plugin}, problems) for everything available, optionally limited to `allow`."""
    found, problems = {}, []
    for name in available(kind):
        if allow is not None and name not in allow:
            continue
        try:
            found[name] = load(name, kind)
        except RegistryError as exc:
            problems.append(str(exc))
    return found, problems
