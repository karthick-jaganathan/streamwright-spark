"""
The adapter contract. PROPOSED.

The data holders shared by providers: `arg` (argument spec), `Method` (one service.method), `_check_type`. The provider
base class and the kinds live in `streamwright.spark.providers`. Historically a Spark adapter gave an EXISTING connector (s3, gcs, azure_blob, files, postgres, deltalake, kafka, sqs) a
Spark side. It does not redefine the connector:

  * `check_auth` of the connector validates the connection's keys (the same `auth:` block a source uses);
  * `check_request` of the connector validates the arguments the connector knows (`Method.passthrough`);
  * the adapter adds only what the connector cannot do: Spark-only arguments, writes, Hadoop settings and jars,
    URL translation (s3:// -> s3a://), and the distributed read/write itself.

The connector comes from the existing component loader (`streamwright.core.runtime.components.load`), so it is
installed the usual way (`streamwright connectors install s3`); a provider whose connector is missing is reported
with that command. Dispatch is by table: `Method.handler` names the adapter method that runs it, so nothing in the
core or the engine branches on a provider name.
"""

from streamwright.spark.contracts.types import Kind


__all__ = ["AdapterError", "Method", "arg", "_check_type"]


class AdapterError(Exception):
    pass


def arg(kind, required=False, default=None, enum=None, description=""):
    return {"type": kind, "required": required, "default": default, "enum": enum, "description": description}


_TYPES = {"string": str, "integer": int, "boolean": bool, "object": dict, "list": list}


class Method(object):
    """
    One `service.method`.
      arguments    {name: arg(...)} every argument the adapter accepts
      passthrough  argument names the connector itself understands; validated by connector.check_request
      connector_method  (service, method) to ask the connector about (default: the same names)
      handler      name of the adapter method that runs it (the dispatch table)
      role         Role.CONSUME / Role.PUBLISH for event methods
      check        optional adapter method name run with (arguments) for checks the table cannot express
    """

    def __init__(self, service, name, kind, arguments, handler, passthrough=(), connector_method=None, role=None,
                 idempotent=False, check=None):
        self.service, self.name, self.kind, self.arguments = service, name, kind, arguments
        self.handler, self.passthrough, self.role, self.idempotent, self.check = handler, tuple(passthrough), role, idempotent, check
        self.connector_method = connector_method

    @property
    def key(self):
        return (self.service, self.name)


def _check_type(key, rule, value):
    kind = rule.get("type")
    if isinstance(value, str) and "{{" in value:
        return []
    expected = _TYPES.get(kind)
    if expected and (not isinstance(value, expected) or (kind == "integer" and isinstance(value, bool))):
        return ["%s must be %s" % (key, kind)]
    if rule.get("enum") and isinstance(value, str) and value not in rule["enum"]:
        return ["%s must be one of %s" % (key, ", ".join(rule["enum"]))]
    return []
