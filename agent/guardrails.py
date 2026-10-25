"""Hard limits on what the agent may change. Enforced in code, never trusted to the prompt."""

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

# Allowed ranges for each knob the agent can turn (JobConfig keys in application.properties)
LIMITS = {
    "flink.checkpoint.interval.ms": (5_000, 300_000),
    "flink.parallelism": (1, 8),
    "sink.rolling.max.part.mb": (64, 512),
    "sink.rolling.interval.min": (1, 15),
}
BUCKET_STRATEGIES = ("symbol", "hour", "symbol_hour")

COOLDOWN_S = 10 * 60          # minimum gap between two mutating actions
MAX_ACTIONS_PER_HOUR = 6
RESCALE_APPROVAL_FACTOR = 2   # parallelism changes larger than this need a human
VERIFY_AFTER_S = 5 * 60       # how long an action gets before it is judged


@dataclass
class AgentState:
    """Persisted between ticks so cooldowns and pending verifications survive restarts."""

    path: Path
    action_times: list[float] = field(default_factory=list)
    # Set after a mutating action; the harness checks it and rolls back if health got worse
    pending_verification: dict | None = None

    @classmethod
    def load(cls, path: Path) -> "AgentState":
        if path.exists():
            data = json.loads(path.read_text())
            return cls(path, data.get("action_times", []), data.get("pending_verification"))
        return cls(path)

    def save(self) -> None:
        self.path.write_text(json.dumps(
            {"action_times": self.action_times, "pending_verification": self.pending_verification}, indent=2))

    def record_action(self) -> None:
        self.action_times.append(time.time())
        self.action_times = [t for t in self.action_times if time.time() - t < 3600]
        self.save()


def check_action(state: AgentState, changes: dict, current: dict) -> tuple[str, str]:
    """Return (verdict, reason) where verdict is 'allowed', 'denied' or 'needs_approval'."""
    now = time.time()
    if state.pending_verification:
        return "denied", "previous action is still being verified; wait for it to settle"
    if state.action_times and now - state.action_times[-1] < COOLDOWN_S:
        wait = int(COOLDOWN_S - (now - state.action_times[-1]))
        return "denied", f"cooldown active, {wait}s remaining"
    if sum(1 for t in state.action_times if now - t < 3600) >= MAX_ACTIONS_PER_HOUR:
        return "denied", f"already took {MAX_ACTIONS_PER_HOUR} actions in the last hour"
    if not changes:
        return "denied", "no settings would change"

    for key, value in changes.items():
        if key == "sink.bucket.strategy":
            if value not in BUCKET_STRATEGIES:
                return "denied", f"bucket strategy must be one of {BUCKET_STRATEGIES}"
            continue
        if key not in LIMITS:
            return "denied", f"'{key}' is not a setting the agent may change"
        lo, hi = LIMITS[key]
        if not lo <= int(value) <= hi:
            return "denied", f"{key}={value} is outside the allowed range [{lo}, {hi}]"

    if "flink.parallelism" in changes:
        old = int(current.get("flink.parallelism", 1))
        new = int(changes["flink.parallelism"])
        if max(old, new) > RESCALE_APPROVAL_FACTOR * min(old, new):
            return "needs_approval", f"parallelism {old} -> {new} is more than a {RESCALE_APPROVAL_FACTOR}x change"

    return "allowed", "within limits"


def health_regressed(before: dict, after: dict) -> str | None:
    """Compare two health snapshots; return why the action made things worse, or None."""
    if after.get("state") != "RUNNING":
        return f"job is {after.get('state')} after the action"
    if (after.get("restarts") or 0) > (before.get("restarts") or 0):
        return "job restarted after the action"
    if (after.get("failed_checkpoints") or 0) > (before.get("failed_checkpoints") or 0):
        return "checkpoints started failing after the action"
    lag_before, lag_after = before.get("kafka_lag"), after.get("kafka_lag")
    if lag_before is not None and lag_after is not None and lag_after > max(1.5 * lag_before, 10_000):
        return f"Kafka lag grew from {lag_before:.0f} to {lag_after:.0f}"
    return None
