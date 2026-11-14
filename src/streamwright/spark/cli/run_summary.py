#!/usr/bin/env python
# /*************************************************************************
# * Copyright 2026 Karthick Jaganathan
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# * https://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# **************************************************************************/

"""The --summary of `sw spark run`, written on the driver."""

import logging

from streamwright.core.jobs.types import written_files
from streamwright.core.runtime import logs
from streamwright.core.runtime.logs.lines import iso_time, utc_now
from streamwright.spark.cli.common import err

__all__ = ["summary", "write_summary"]

LOG = logging.getLogger("streamwright.spark")


def _outputs(runner, store):
    outputs = []
    for batch_id in runner.processed if runner else ():
        for name, receipt in sorted(store.get_receipts(runner.job_name, batch_id).items()):
            outputs.append({"name": name, "job_id": batch_id, "rows": receipt.get("rows"),
                            "files": written_files(receipt) or [receipt.get("destination")],
                            "already_committed": bool(receipt.get("already_committed"))})
    return outputs


def _passes(outcome):
    """runtime.fan_out: one entry per item (its status, job ids, outputs and error)."""
    entries, redact = [], outcome.get("redact", str)
    for item in outcome["passes"]:
        part = item.result or {}
        runner = part.get("runner")
        entry = {"item": item.label, "status": item.status, "job_ids": list(runner.processed) if runner else [],
                 "outputs": _outputs(runner, part.get("store"))}
        error = item.error or (runner.error if runner and item.status != "ok" else None)
        if error:
            entry["error"] = redact("%s: %s" % (type(error).__name__, error) if item.error else str(error))
        entries.append(entry)
    return entries


def summary(args, outcome, started, code, failure):
    """The --summary of a run on the driver: status, times, job, batches, each output's rows and files, the error."""
    runner, store, finished = outcome.get("runner"), outcome.get("store"), utc_now()
    outputs = _outputs(runner, store)
    status = "failed" if failure is not None or code else "skipped" if runner and runner.skipped else "ok"
    summary = {"status": status, "started_at": iso_time(started), "finished_at": iso_time(finished),
               "duration_s": round((finished - started).total_seconds(), 3), "job": outcome.get("job"),
               "manifest": args.job, "scope": runner.scope if runner else None,
               "job_ids": list(runner.processed) if runner else [], "outputs": outputs}
    if outcome.get("passes") is not None:
        summary.update(fan_out={"over": outcome["fan_out"]["over"], "as": outcome["fan_out"]["as"]},
                       passes=_passes(outcome))
        summary["job_ids"] = [job_id for entry in summary["passes"] for job_id in entry["job_ids"]]
        summary["outputs"] = [output for entry in summary["passes"] for output in entry["outputs"]]
    if status == "failed":
        text = ("interrupted" if failure is not None and not isinstance(failure, Exception)
                else "%s: %s" % (type(failure).__name__, failure) if failure is not None
                else (runner.error if runner else None) or "the run failed (exit code %s)" % code)
        summary["error"] = outcome.get("redact", str)(text)
    return summary


def write_summary(args, summary):
    if args.summary == logs.LOG_SUMMARY:
        logs.log_summary(LOG, summary)
        return
    try:
        logs.write_summary(args.summary, summary)
    except (OSError, TypeError, ValueError) as exc:
        err("error: could not write the summary %s: %s" % (args.summary, exc))
