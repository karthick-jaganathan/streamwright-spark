"""sw spark state: inspect and repair bookkeeping. PROPOSED."""

import datetime
import json
import os

from streamwright.spark.cli.common import add_job_arguments, connections_of, err, load_plugins
from streamwright.spark.cli.inputs import config_values, secret_values
from streamwright.spark.job import check_job, load_job, resolve
from streamwright.spark.state import open_dialect, open_sqlite


__all__ = ["add_arguments", "run", "open_store"]

ACTIONS = ["init", "status", "batches", "show", "retry", "abandon", "purge", "lease"]


def add_arguments(parser):
    parser.add_argument("action", choices=ACTIONS, help="state action to perform: " + ", ".join(ACTIONS))
    add_job_arguments(parser)                      # `state status JOB [ARG]`
    parser.add_argument("arg", nargs="?", help="optional argument for action (e.g. batch_id for show/retry/abandon)")
    parser.add_argument("--secrets", help="path to a secrets YAML file or JSON string of secret values")


def open_store(resolved, connections, adapters):
    """The bookkeeping store named by runtime.state: a sqlite file, or the connection's own adapter."""
    state = resolved["runtime"]["state"]
    if "sqlite" in state:
        return open_sqlite(state["sqlite"])
    conn = connections[state["connection"]]
    connect, dialect = adapters[conn.provider].state_connection(conn.auth)
    return open_dialect(connect, dialect, state["schema"])


def run(args):
    adapters, launchers, _ = load_plugins(args.allow_connector)
    job = load_job(args.job, args.overlay)
    problems, _ = check_job(job, adapters, launchers)
    if problems:
        for text in problems:
            err("error: " + text)
        return 2
    resolved, missing = resolve(job, config_values(job, args), secret_values(job, args))
    if missing:
        for text in missing:
            err("error: " + text)
        return 2
    store = open_store(resolved, connections_of(resolved), adapters)
    name = job["name"]
    if args.action == "init":
        store.init()
        print("initialized")
    elif args.action in ("status", "batches"):
        for batch in store.list_batches(name, 1 if args.action == "status" else 20):
            meta = []
            if batch.get("app_id"):
                meta.append("app=%s" % batch["app_id"])
            if batch.get("driver_host"):
                meta.append("host=%s" % batch["driver_host"])
            meta_str = (" " + " ".join(meta)) if meta else ""
            print("%-24s %-10s seq=%d updated=%s%s %s" % (batch["batch_id"], batch["status"], batch["seq"],
                                                           batch["updated_at"], meta_str, (batch["error"] or "")[:80]))
    elif args.action == "show":
        batch = store.batch(name, args.arg or "")
        if batch is None:
            err("no such batch")
            return 2
        manifest = dict(batch["manifest"], messages="%d message(s)" % len(batch["manifest"].get("messages", [])))
        print(json.dumps(dict(batch, manifest=manifest, receipts=store.receipts(name, batch["batch_id"])), indent=2, default=str))
    elif args.action == "lease":
        lease = store.lease(name)
        if lease is None:
            print("no lease recorded for %s" % name)
        else:
            print(json.dumps(lease, indent=2))
    elif args.action in ("retry", "abandon"):
        lease = store.acquire_lease(name, "cli:%d" % os.getpid(), 60)
        try:
            batch = store.batch(name, args.arg or "")
            if batch is None:
                err("no such batch")
                return 2
            store.record_batch(lease, batch["batch_id"], "processing" if args.action == "retry" else "abandoned",
                               batch["manifest"])
            print("%s: %s" % (batch["batch_id"], "queued for retry" if args.action == "retry" else "abandoned"))
        finally:
            store.release_lease(lease)
    elif args.action == "purge":
        cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=int(args.arg or 30))
                  ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        print("purged %d batch(es)" % store.purge(name, cutoff))
    return 0
