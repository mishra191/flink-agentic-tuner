"""The agent's tools: observe (read-only), act (guarded), meta (accountability)."""

import json
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from anthropic import beta_tool

import guardrails
from flink_client import FlinkClient, FlinkError


@dataclass
class Context:
    flink: FlinkClient
    state: guardrails.AgentState
    job_name_prefix: str
    jar_name: str
    entry_class: str
    savepoint_dir: str
    decision_log: Path
    dry_run: bool
    interactive: bool


CTX: Context | None = None  # set by agent.py before the first tick


def _json(obj) -> str:
    return json.dumps(obj, default=str)


def _job() -> dict:
    job = CTX.flink.find_job(CTX.job_name_prefix)
    if job is None:
        raise FlinkError(f"no job named '{CTX.job_name_prefix}*' on the cluster")
    return job


def _vertex(jid: str, name_part: str) -> dict | None:
    return next((v for v in CTX.flink.job(jid)["vertices"] if name_part in v["name"]), None)


def health_snapshot() -> dict:
    """Numbers the harness compares before and after an action. Also used by tools."""
    job = _job()
    jid = job["jid"]
    snap = {"job_id": jid, "state": job["state"], "restarts": None, "failed_checkpoints": None, "kafka_lag": None}
    try:
        snap["restarts"] = CTX.flink.job_metric(jid, "numRestarts")
        snap["failed_checkpoints"] = CTX.flink.checkpoints(jid)["counts"]["failed"]
        source = _vertex(jid, "kafka-source")
        if source:
            snap["kafka_lag"] = CTX.flink.vertex_metric_sum(jid, source["id"], "pendingRecords")
    except FlinkError:
        pass  # job may be restarting; partial snapshot is still useful
    return snap


def log_event(kind: str, **fields) -> None:
    entry = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "kind": kind, **fields}
    with CTX.decision_log.open("a") as f:
        f.write(_json(entry) + "\n")


# ---------------------------------------------------------------------------
# Observe
# ---------------------------------------------------------------------------

@beta_tool
def get_job_status() -> str:
    """Get the pipeline job's state, uptime, restart count, parallelism per operator, and the settings it was started with."""
    job = _job()
    jid = job["jid"]
    detail = CTX.flink.job(jid)
    return _json({
        "job_id": jid,
        "name": job["name"],
        "state": job["state"],
        "uptime_s": job["duration"] // 1000,
        "restarts": CTX.flink.job_metric(jid, "numRestarts"),
        "operators": [{"name": v["name"], "parallelism": v["parallelism"], "status": v["status"]}
                      for v in detail["vertices"]],
        "config": CTX.flink.job_user_config(jid),
    })


@beta_tool
def get_checkpoint_stats() -> str:
    """Get checkpoint counts (completed/failed/in progress), recent durations and state size. Parquet files are only committed when a checkpoint completes."""
    stats = CTX.flink.checkpoints(_job()["jid"])
    latest = stats.get("latest", {})
    completed = latest.get("completed") or {}
    failed = latest.get("failed") or {}
    return _json({
        "counts": stats.get("counts"),
        "end_to_end_duration_ms": (stats.get("summary") or {}).get("end_to_end_duration"),
        "latest_completed": {k: completed.get(k) for k in ("id", "latest_ack_timestamp", "end_to_end_duration", "state_size")} if completed else None,
        "latest_failed": {k: failed.get(k) for k in ("id", "failure_timestamp", "failure_message")} if failed else None,
    })


@beta_tool
def get_recent_exceptions() -> str:
    """Get the job's most recent exceptions (name, time, first lines of stack trace). Use this when restarts are increasing."""
    data = CTX.flink.exceptions(_job()["jid"])
    entries = (data.get("exceptionHistory") or {}).get("entries", [])
    return _json([{
        "exception": e.get("exceptionName"),
        "timestamp": e.get("timestamp"),
        "task": e.get("taskName"),
        "stacktrace_head": "\n".join((e.get("stacktrace") or "").splitlines()[:8]),
    } for e in entries[:5]] or "no exceptions recorded")


@beta_tool
def get_backpressure() -> str:
    """Get backpressure level (ok/low/high) for each operator, to find bottlenecks such as a slow sink."""
    jid = _job()["jid"]
    result = []
    for v in CTX.flink.job(jid)["vertices"]:
        bp = CTX.flink.backpressure(jid, v["id"])
        result.append({"operator": v["name"], "status": bp.get("status"), "level": bp.get("backpressureLevel")})
    return _json(result)


@beta_tool
def get_throughput_and_lag() -> str:
    """Get records/s read from Kafka, records/s written by the sink, and Kafka consumer lag (pending records)."""
    jid = _job()["jid"]
    source, sink = _vertex(jid, "kafka-source"), _vertex(jid, "parquet-sink")
    return _json({
        "source_records_out_per_s": CTX.flink.vertex_metric_sum(jid, source["id"], "numRecordsOutPerSecond") if source else None,
        "sink_records_in_per_s": CTX.flink.vertex_metric_sum(jid, sink["id"], "numRecordsInPerSecond") if sink else None,
        "kafka_lag_records": CTX.flink.vertex_metric_sum(jid, source["id"], "pendingRecords") if source else None,
    })


def _output_root() -> Path:
    path = CTX.flink.job_user_config(_job()["jid"]).get("output.path", "")
    parsed = urlparse(path)
    if parsed.scheme not in ("", "file"):
        raise FlinkError(f"only local output paths are supported for inspection, got {path}")
    return Path(parsed.path)


@beta_tool
def inspect_output_files() -> str:
    """Scan the Parquet output directory: per bucket, count finished vs in-progress files, file sizes, and age of the oldest uncommitted file. Detects the small-files problem and files that never get committed."""
    root = _output_root()
    if not root.exists():
        return _json({"output_path": str(root), "error": "output directory does not exist yet"})

    now = time.time()
    buckets = {}
    newest_finished = 0.0
    for f in root.rglob("*"):
        if not f.is_file():
            continue
        b = buckets.setdefault(str(f.parent.relative_to(root)), {"finished_sizes": [], "inprogress": 0, "oldest_inprogress_s": 0})
        if f.name.startswith(".") and ".inprogress" in f.name:
            b["inprogress"] += 1
            b["oldest_inprogress_s"] = max(b["oldest_inprogress_s"], int(now - f.stat().st_mtime))
        elif not f.name.startswith("."):
            b["finished_sizes"].append(f.stat().st_size)
            newest_finished = max(newest_finished, f.stat().st_mtime)

    rows = []
    for name, b in buckets.items():
        sizes = b["finished_sizes"]
        rows.append({
            "bucket": name,
            "finished_files": len(sizes),
            "inprogress_files": b["inprogress"],
            "oldest_inprogress_s": b["oldest_inprogress_s"],
            "median_file_kb": round(statistics.median(sizes) / 1024, 1) if sizes else None,
            "total_mb": round(sum(sizes) / 1024 / 1024, 2),
        })
    # Worst buckets first: most stuck files, then most small files
    rows.sort(key=lambda r: (r["inprogress_files"], r["finished_files"]), reverse=True)
    return _json({
        "output_path": str(root),
        "buckets": len(rows),
        "total_finished_files": sum(r["finished_files"] for r in rows),
        "total_inprogress_files": sum(r["inprogress_files"] for r in rows),
        # roughly how stale the newest readable data is
        "newest_finished_file_age_s": int(now - newest_finished) if newest_finished else None,
        "worst_buckets": rows[:20],
    })


# ---------------------------------------------------------------------------
# Act (every action goes through guardrails.check_action)
# ---------------------------------------------------------------------------

def _changes(checkpoint_interval_ms, rolling_max_part_mb, rolling_interval_min, bucket_strategy, parallelism) -> dict:
    proposed = {
        "flink.checkpoint.interval.ms": checkpoint_interval_ms,
        "sink.rolling.max.part.mb": rolling_max_part_mb,
        "sink.rolling.interval.min": rolling_interval_min,
        "sink.bucket.strategy": bucket_strategy,
        "flink.parallelism": parallelism,
    }
    return {k: v for k, v in proposed.items() if v is not None}


def _approved(verdict: str, reason: str, changes: dict) -> bool:
    if verdict == "allowed":
        return True
    if verdict == "needs_approval" and CTX.interactive:
        answer = input(f"\n[approval needed] {reason}. Apply {changes}? [y/N] ")
        return answer.strip().lower() == "y"
    return False


def _restart_with(jid: str, current: dict, changes: dict, reason: str) -> dict:
    """Stop with a savepoint, restart the jar with merged settings, and arm verification."""
    before = health_snapshot()
    savepoint = CTX.flink.stop_with_savepoint(jid, CTX.savepoint_dir)
    new_config = {**current, **{k: str(v) for k, v in changes.items()}}
    args = [x for k, v in new_config.items() for x in (f"--{k}", str(v))]
    jar_id = CTX.flink.find_jar(CTX.jar_name)
    try:
        new_jid = CTX.flink.run_jar(jar_id, CTX.entry_class, args, savepoint)
    except FlinkError as e:
        # New settings rejected at submission: bring the old job back from the same savepoint
        old_args = [x for k, v in current.items() for x in (f"--{k}", str(v))]
        CTX.flink.run_jar(jar_id, CTX.entry_class, old_args, savepoint)
        raise FlinkError(f"restart with new settings failed, restored previous settings: {e}")

    CTX.state.record_action()
    CTX.state.pending_verification = {
        "job_id": new_jid,
        "deadline": time.time() + guardrails.VERIFY_AFTER_S,
        "changes": changes,
        # counters reset with the new job id, so the baseline starts from zero
        "baseline": {"restarts": 0, "failed_checkpoints": 0, "kafka_lag": before.get("kafka_lag")},
        "rollback": {"savepoint": savepoint, "config": current},
    }
    CTX.state.save()
    log_event("action", action="reconfigure", changes=changes, reason=reason,
              old_job=jid, new_job=new_jid, savepoint=savepoint)
    return {"result": "restarted", "new_job_id": new_jid, "savepoint": savepoint, "changes": changes,
            "note": f"verification runs for {guardrails.VERIFY_AFTER_S // 60} min; automatic rollback if health regresses"}


@beta_tool
def propose_action(reason: str,
                   checkpoint_interval_ms: int | None = None,
                   rolling_max_part_mb: int | None = None,
                   rolling_interval_min: int | None = None,
                   bucket_strategy: Literal["symbol", "hour", "symbol_hour"] | None = None,
                   parallelism: int | None = None) -> str:
    """Dry-run a settings change against the guardrails without applying it. Returns allowed, denied or needs_approval with a reason. Call this before reconfigure_job.

    Args:
        reason: Why this change should help, citing the observed metrics.
        checkpoint_interval_ms: New checkpoint interval (5000-300000). Parquet files commit once per checkpoint.
        rolling_max_part_mb: Roll a part file once it reaches this size (64-512).
        rolling_interval_min: Roll a part file once it has been open this long (1-15).
        bucket_strategy: Output partitioning.
        parallelism: New job parallelism (1-8).
    """
    current = CTX.flink.job_user_config(_job()["jid"])
    changes = _changes(checkpoint_interval_ms, rolling_max_part_mb, rolling_interval_min, bucket_strategy, parallelism)
    verdict, why = guardrails.check_action(CTX.state, changes, current)
    log_event("proposal", changes=changes, reason=reason, verdict=verdict, why=why)
    return _json({"verdict": verdict, "why": why, "changes": changes,
                  "current": {k: current.get(k) for k in changes}})


@beta_tool
def reconfigure_job(reason: str,
                    checkpoint_interval_ms: int | None = None,
                    rolling_max_part_mb: int | None = None,
                    rolling_interval_min: int | None = None,
                    bucket_strategy: Literal["symbol", "hour", "symbol_hour"] | None = None,
                    parallelism: int | None = None) -> str:
    """Stop the job with a savepoint and restart it from that savepoint with new settings. No data is lost or duplicated. Only pass the settings you want to change.

    Args:
        reason: Why this change should help, citing the observed metrics.
        checkpoint_interval_ms: New checkpoint interval (5000-300000). Parquet files commit once per checkpoint.
        rolling_max_part_mb: Roll a part file once it reaches this size (64-512).
        rolling_interval_min: Roll a part file once it has been open this long (1-15).
        bucket_strategy: Output partitioning.
        parallelism: New job parallelism (1-8).
    """
    jid = _job()["jid"]
    current = CTX.flink.job_user_config(jid)
    changes = _changes(checkpoint_interval_ms, rolling_max_part_mb, rolling_interval_min, bucket_strategy, parallelism)
    verdict, why = guardrails.check_action(CTX.state, changes, current)
    if not _approved(verdict, why, changes):
        log_event("denied", action="reconfigure", changes=changes, reason=reason, verdict=verdict, why=why)
        return _json({"result": "not applied", "verdict": verdict, "why": why})
    if CTX.dry_run:
        log_event("dry_run", action="reconfigure", changes=changes, reason=reason)
        return _json({"result": "DRY RUN - not applied", "changes": changes})
    return _json(_restart_with(jid, current, changes, reason))


# Only merge files below this size; larger ones are already fine
COMPACT_MAX_FILE_BYTES = 32 * 1024 * 1024
# Leave recently committed files alone in case a reader is still listing the bucket
COMPACT_MIN_AGE_S = 120


@beta_tool
def compact_bucket(bucket: str, reason: str) -> str:
    """Merge the small finished Parquet files in one output bucket into a single file. Never touches in-progress files. Use for buckets with many small files.

    Args:
        bucket: Bucket path relative to the output root, exactly as reported by inspect_output_files.
        reason: Why this bucket needs compaction.
    """
    import pyarrow.parquet as pq
    import pyarrow as pa

    root = _output_root().resolve()
    target = (root / bucket).resolve()
    if root not in target.parents or not target.is_dir():
        return _json({"result": "not applied", "why": f"'{bucket}' is not a bucket under {root}"})

    now = time.time()
    files = sorted(f for f in target.iterdir()
                   if f.is_file() and not f.name.startswith(".")
                   and f.stat().st_size < COMPACT_MAX_FILE_BYTES
                   and now - f.stat().st_mtime > COMPACT_MIN_AGE_S)
    if len(files) < 2:
        return _json({"result": "nothing to do", "eligible_files": len(files)})

    if CTX.dry_run:
        log_event("dry_run", action="compact", bucket=bucket, files=len(files), reason=reason)
        return _json({"result": "DRY RUN - not applied", "would_merge": len(files)})

    # ParquetFile, not read_table: read_table would parse "symbol=x" dirs as a partition column
    table = pa.concat_tables([pq.ParquetFile(f).read() for f in files])
    # Write under a hidden name first so readers never see a partial file
    stamp = time.strftime("%Y%m%dT%H%M%S")
    tmp = target / f".compacted-{stamp}.parquet.tmp"
    final = target / f"compacted-{stamp}.parquet"
    pq.write_table(table, tmp)
    if pq.read_metadata(tmp).num_rows != table.num_rows:
        tmp.unlink()
        return _json({"result": "aborted", "why": "row count mismatch after write"})
    tmp.rename(final)
    for f in files:
        f.unlink()

    log_event("action", action="compact", bucket=bucket, merged_files=len(files), rows=table.num_rows, reason=reason)
    return _json({"result": "compacted", "merged_files": len(files), "rows": table.num_rows, "output": final.name})


def do_rollback(reason: str) -> dict:
    """Restore the settings from before the last action. Used by the tool and by the harness."""
    pending = CTX.state.pending_verification
    if not pending:
        return {"result": "nothing to roll back"}
    rollback = pending["rollback"]
    job = _job()
    if CTX.dry_run:
        return {"result": "DRY RUN - not applied", "would_restore": rollback["config"]}

    try:
        # Savepoint the current job so restored output continues exactly where it stopped
        savepoint = CTX.flink.stop_with_savepoint(job["jid"], CTX.savepoint_dir)
        note = "restored from a fresh savepoint, no duplicates"
    except FlinkError:
        # Job too unhealthy to savepoint: fall back to the pre-action savepoint
        savepoint = rollback["savepoint"]
        note = "restored from the pre-action savepoint; records written since then may be duplicated"

    args = [x for k, v in rollback["config"].items() for x in (f"--{k}", str(v))]
    new_jid = CTX.flink.run_jar(CTX.flink.find_jar(CTX.jar_name), CTX.entry_class, args, savepoint)
    CTX.state.pending_verification = None
    CTX.state.save()
    log_event("rollback", reason=reason, reverted=pending["changes"], new_job=new_jid, savepoint=savepoint, note=note)
    return {"result": "rolled back", "new_job_id": new_jid, "note": note}


@beta_tool
def rollback(reason: str) -> str:
    """Undo the most recent reconfiguration and restart with the previous settings.

    Args:
        reason: Why the last change should be undone.
    """
    return _json(do_rollback(reason))


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------

@beta_tool
def log_decision(observation: str, decision: Literal["no_action", "acted", "escalate"], reasoning: str) -> str:
    """Record what you observed and decided this tick. Call exactly once, as the last step of every tick.

    Args:
        observation: The key metrics you looked at, with numbers.
        decision: no_action if healthy, acted if you changed something, escalate if a human must look (e.g. a code bug).
        reasoning: Why, in two or three sentences.
    """
    log_event("decision", observation=observation, decision=decision, reasoning=reasoning)
    return "logged"


ALL_TOOLS = [
    get_job_status, get_checkpoint_stats, get_recent_exceptions, get_backpressure,
    get_throughput_and_lag, inspect_output_files,
    propose_action, reconfigure_job, compact_bucket, rollback,
    log_decision,
]
