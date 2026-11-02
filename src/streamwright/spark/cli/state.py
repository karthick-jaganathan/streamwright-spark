"""sw spark state: inspect and repair bookkeeping."""

import datetime
import json
import os

from streamwright.core.jobs.manifest import manifest_directory
from streamwright.core.jobs.state import open_sqlite
from streamwright.core.jobs.state import open_store as open_dialect
from streamwright.spark.cli.common import add_job_arguments, connections_of, err, load_plugins
from streamwright.spark.cli.inputs import config_values, secret_values
from streamwright.spark.job import check_job, load_job, resolve

__all__ = ["add_arguments", "open_store", "run"]

ACTIONS = ("init", "status", "batches", "show", "retry", "abandon", "purge", "lease")


def add_arguments(parser):
    parser.add_argument("action", choices=ACTIONS, help="state action to perform: " + ", ".join(ACTIONS))
    add_job_arguments(parser)  # `state status JOB [ARG]`
    parser.add_argument("arg", nargs="?", help="optional argument for action (e.g. batch_id for show/retry/abandon)")
    parser.add_argument("--secrets", help="path to a secrets YAML file or JSON string of secret values")


def open_store(resolved, connections, adapters, base_dir):
    """The bookkeeping store named by runtime.state: a sqlite file, or the connection's own adapter."""
    state = resolved["runtime"]["state"]
    if "sqlite" in state:
        path = state["sqlite"]
        if resolved["runtime"]["platform"]["type"] != "local" and not os.path.isabs(path):
            raise ValueError("remote jobs require an absolute runtime.state.sqlite path")
        if path != ":memory:" and not os.path.isabs(path):
            path = os.path.join(base_dir or os.getcwd(), path)
        return open_sqlite(path)
    conn = connections[state["connection"]]
    connect, dialect = adapters[conn.provider].state_connection(conn.auth)
    return open_dialect(connect, dialect, schema=state.get("schema", "streamwright"))


def run(args):
    adapters, launchers, _ = load_plugins(args.allow_connector)
    job = load_job(args.job, args.overlay)
    problems, _ = check_job(job, adapters, launchers)
    if problems:
        for text in problems:
            err("error: " + text)
        return 2
    config = config_values(job, args)
    secrets = secret_values(job, args)
    resolved, missing = resolve(job, config, secrets)
    if missing:
        for text in missing:
            err("error: " + text)
        return 2
    connections = connections_of(resolved, adapters=adapters, declared=job, config=config, secrets=secrets)
    store = open_store(resolved, connections, adapters, manifest_directory(args.job))
    return _run_action(store, job["name"], args)


def _list_batches(store, name, limit):
    for batch in store.list_batches(name, limit):
        metadata = batch.get("engine_metadata") or {}
        meta = [
            f"{label}={metadata[key]}"
            for key, label in (("app_id", "app"), ("driver_host", "host"))
            if metadata.get(key)
        ]
        meta_str = (" " + " ".join(meta)) if meta else ""
        print(
            "%-24s %-10s seq=%d updated=%s%s %s"
            % (
                batch["batch_id"],
                batch["status"],
                batch["batch_sequence"],
                batch["updated_at"],
                meta_str,
                (batch["error"] or "")[:80],
            )
        )


def _show_batch(store, name, batch_id):
    batch = store.get_batch(name, batch_id)
    if batch is None:
        err("no such batch")
        return 2
    manifest = dict(batch["manifest"], messages="%d message(s)" % len(batch["manifest"].get("messages", [])))
    print(
        json.dumps(
            dict(batch, manifest=manifest, receipts=store.get_receipts(name, batch["batch_id"])), indent=2, default=str
        )
    )
    return 0


def _change_batch(store, name, args):
    lease = store.acquire_lease(name, "cli:%d" % os.getpid(), 60)
    try:
        batch = store.get_batch(name, args.arg or "")
        if batch is None:
            err("no such batch")
            return 2
        if args.action == "retry":
            store.retry_batch(lease, batch["batch_id"])
        else:
            store.record_batch(lease, batch["batch_id"], "abandoned", batch["manifest"])
        print("%s: %s" % (batch["batch_id"], "queued for retry" if args.action == "retry" else "abandoned"))
    finally:
        store.release_lease(lease)
    return 0


def _run_action(store, name, args):
    if args.action == "init":
        store.initialize()
        print("initialized")
    elif args.action in ("status", "batches"):
        _list_batches(store, name, 1 if args.action == "status" else 20)
    elif args.action == "show":
        return _show_batch(store, name, args.arg or "")
    elif args.action == "lease":
        lease = store.get_lease(name)
        if lease is None:
            print("no lease recorded for %s" % name)
        else:
            print(json.dumps(lease, indent=2))
    elif args.action in ("retry", "abandon"):
        return _change_batch(store, name, args)
    elif args.action == "purge":
        cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=int(args.arg or 30))
        print("purged %d batch(es)" % store.purge_batches(name, cutoff))
    return 0
