"""Samples pipeline health and output files during a run.

samples.csv - one row per interval: job state, restarts, checkpoints, lag, file counts, staleness
files.csv   - one row per committed Parquet file: size, rows, event-time range, when it appeared

Staleness is measured from the data itself: now minus the newest event timestamp found in any
committed file. It is what a downstream query would actually see.
"""

import csv
import threading
import time
from pathlib import Path

import pyarrow.parquet as pq
import requests

SAMPLE_FIELDS = ["t", "elapsed_s", "job_id", "state", "restarts", "completed_checkpoints", "failed_checkpoints",
                 "kafka_lag", "checkpoint_interval_ms", "bucket_strategy", "parallelism",
                 "finished_files", "inprogress_files", "finished_bytes", "committed_rows", "staleness_s", "events_sent"]
FILE_FIELDS = ["path", "first_seen", "elapsed_s", "bytes", "rows", "min_ts", "max_ts", "compacted"]


def _event_time_range(path: Path) -> tuple[int, int, int]:
    meta = pq.ParquetFile(path).metadata
    col = meta.schema.names.index("timestamp")
    lo, hi = None, None
    for i in range(meta.num_row_groups):
        stats = meta.row_group(i).column(col).statistics
        if stats is not None and stats.has_min_max:
            lo = stats.min if lo is None else min(lo, stats.min)
            hi = stats.max if hi is None else max(hi, stats.max)
    return meta.num_rows, lo, hi


class Collector(threading.Thread):
    def __init__(self, flink_url: str, job_name_prefix: str, output_dir: Path, results_dir: Path,
                 loadgen=None, interval_s: float = 15):
        super().__init__(daemon=True)
        self.flink_url = flink_url.rstrip("/")
        self.job_name_prefix = job_name_prefix
        self.output_dir = output_dir
        self.loadgen = loadgen
        self.interval_s = interval_s
        self.samples_path = results_dir / "samples.csv"
        self.files_path = results_dir / "files.csv"
        self.seen = {}
        self.newest_event_ms = None
        self.t0 = time.time()
        self._halt = threading.Event()

    def stop(self):
        self._halt.set()

    def _get(self, path, **params):
        return requests.get(self.flink_url + path, params=params or None, timeout=5).json()

    def _job_metrics(self) -> dict:
        jobs = [j for j in self._get("/jobs/overview")["jobs"] if j["name"].startswith(self.job_name_prefix)]
        if not jobs:
            return {"state": "NONE"}
        jobs.sort(key=lambda j: (j["state"] == "RUNNING", j["start-time"]), reverse=True)
        jid = jobs[0]["jid"]
        out = {"job_id": jid, "state": jobs[0]["state"]}
        try:
            restarts = self._get(f"/jobs/{jid}/metrics", get="numRestarts")
            out["restarts"] = restarts[0]["value"] if restarts else None
            counts = self._get(f"/jobs/{jid}/checkpoints")["counts"]
            out["completed_checkpoints"], out["failed_checkpoints"] = counts["completed"], counts["failed"]
            cfg = self._get(f"/jobs/{jid}/config")["execution-config"]["user-config"]
            out["checkpoint_interval_ms"] = cfg.get("flink.checkpoint.interval.ms")
            out["bucket_strategy"] = cfg.get("sink.bucket.strategy")
            out["parallelism"] = cfg.get("flink.parallelism")
            source = next(v for v in self._get(f"/jobs/{jid}")["vertices"] if "kafka-source" in v["name"])
            ids = [m["id"] for m in self._get(f"/jobs/{jid}/vertices/{source['id']}/subtasks/metrics")
                   if m["id"].endswith("pendingRecords")]
            if ids:
                lag = self._get(f"/jobs/{jid}/vertices/{source['id']}/subtasks/metrics", get=ids[0], agg="sum")
                out["kafka_lag"] = lag[0]["sum"] if lag else None
        except (requests.RequestException, KeyError, StopIteration, ValueError):
            pass  # job is restarting; keep the partial sample
        return out

    def _scan_files(self, now: float, files_writer) -> dict:
        finished, inprogress, total_bytes, rows = 0, 0, 0, 0
        for f in self.output_dir.rglob("*"):
            if not f.is_file():
                continue
            if f.name.startswith("."):
                inprogress += ".inprogress" in f.name
                continue
            finished += 1
            size = f.stat().st_size
            total_bytes += size
            key = str(f.relative_to(self.output_dir))
            if key not in self.seen:
                try:
                    n, lo, hi = _event_time_range(f)
                except Exception:
                    continue  # file still being renamed or compacted; pick it up next round
                self.seen[key] = n
                files_writer.writerow({"path": key, "first_seen": round(now, 1), "elapsed_s": round(now - self.t0, 1),
                                       "bytes": size, "rows": n, "min_ts": lo, "max_ts": hi,
                                       "compacted": f.name.startswith("compacted-")})
                if hi is not None:
                    self.newest_event_ms = max(self.newest_event_ms or 0, hi)
            rows += self.seen.get(key, 0)
        return {"finished_files": finished, "inprogress_files": inprogress, "finished_bytes": total_bytes,
                "committed_rows": rows,
                "staleness_s": round(now - self.newest_event_ms / 1000, 1) if self.newest_event_ms else None}

    def run(self):
        with self.samples_path.open("w", newline="") as sf, self.files_path.open("w", newline="") as ff:
            samples = csv.DictWriter(sf, SAMPLE_FIELDS)
            files = csv.DictWriter(ff, FILE_FIELDS)
            samples.writeheader()
            files.writeheader()
            while not self._halt.is_set():
                now = time.time()
                row = {"t": round(now, 1), "elapsed_s": round(now - self.t0, 1),
                       "events_sent": self.loadgen.sent if self.loadgen else None}
                try:
                    row.update(self._job_metrics())
                except requests.RequestException as e:
                    row["state"] = f"UNREACHABLE: {type(e).__name__}"
                row.update(self._scan_files(now, files))
                samples.writerow(row)
                sf.flush()
                ff.flush()
                self._halt.wait(self.interval_s)
