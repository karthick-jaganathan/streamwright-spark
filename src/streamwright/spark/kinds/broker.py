"""
The Broker kind: the interface for message-broker connectors (kafka, sqs). PROPOSED.

Deliberately thin. The actual `poll` / `ack` / `publish` are provider-specific client code (confluent-kafka, boto3) and
live IN the connector, next to its peek-read client (e.g. `streamwright.connectors.kafka.spark`). A connector's broker
adapter subclasses this, declares its `topic`/`queue` consume/publish methods, and implements `broker(auth)` to return
its client. The one generic rule — acknowledge only after the batch's manifest and outbox are durable — is the runner's,
not the broker's.
"""

from streamwright.spark.kinds.base import Provider


__all__ = ["Broker", "BrokerError"]


class BrokerError(Exception):
    """Raised by a connector's broker client; recognised by the runner as a batch failure."""


class Broker(Provider):
    def broker(self, auth):
        """Return the connector's client, an object with poll(arguments, limit, max_wait_seconds), ack(messages) and
        publish(arguments, events, dedupe_key). Implemented by the connector's adapter, not here."""
        raise NotImplementedError("%s must implement broker() in its connector" % self.provider)
