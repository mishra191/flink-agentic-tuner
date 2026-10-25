"""Baseline controller: fixed threshold rules, same guardrails and restart mechanism as the agent.

  - committed data older than the freshness SLO   -> halve the checkpoint interval
  - files small and data well within the SLO      -> double the checkpoint interval (capped below the SLO)
"""

import json
import threading
import time

import agent
import guardrails
import tools

SMALL_FILE_BYTES = 1024 * 1024
MIN_INTERVAL_MS, MAX_INTERVAL_MS = guardrails.LIMITS["flink.checkpoint.interval.ms"]


class RuleController(threading.Thread):
    def __init__(self, freshness_slo_s: int, tick_s: int = 60):
        super().__init__(daemon=True)
        self.slo_s = freshness_slo_s
        self.tick_s = tick_s
        self._halt = threading.Event()

    def stop(self):
        self._halt.set()

    def decide(self, interval_ms: int, staleness_s: float | None, median_kb: float | None) -> int | None:
        if staleness_s is not None and staleness_s > self.slo_s:
            return max(MIN_INTERVAL_MS, interval_ms // 2)
        if median_kb is not None and median_kb * 1024 < SMALL_FILE_BYTES and (staleness_s or 0) < self.slo_s / 2:
            # keep a checkpoint interval comfortably inside the SLO
            return min(MAX_INTERVAL_MS, interval_ms * 2, int(self.slo_s * 1000 * 0.75))
        return None

    def tick(self):
        agent.verify_pending(tools.CTX.state)
        jid = tools._job()["jid"]
        current = tools.CTX.flink.job_user_config(jid)
        files = json.loads(tools.inspect_output_files.call({}))
        sizes = [b["median_file_kb"] for b in files.get("worst_buckets", []) if b["median_file_kb"] is not None]
        median_kb = sorted(sizes)[len(sizes) // 2] if sizes else None
        interval = int(current.get("flink.checkpoint.interval.ms", 10_000))

        new = self.decide(interval, files.get("newest_finished_file_age_s"), median_kb)
        observation = f"interval={interval}ms staleness={files.get('newest_finished_file_age_s')}s median_file_kb={median_kb}"
        if new is None or new == interval:
            tools.log_event("decision", observation=observation, decision="no_action", reasoning="rules not triggered")
            return
        changes = {"flink.checkpoint.interval.ms": new}
        verdict, why = guardrails.check_action(tools.CTX.state, changes, current)
        if verdict != "allowed":
            tools.log_event("denied", action="reconfigure", changes=changes, reason=observation, verdict=verdict, why=why)
            return
        tools._restart_with(jid, current, changes, f"rule: {observation}")

    def run(self):
        while not self._halt.is_set():
            try:
                self.tick()
            except Exception as e:  # keep the baseline running through restarts, like the agent harness
                tools.log_event("tick_error", error=f"{type(e).__name__}: {e}")
            self._halt.wait(self.tick_s)
