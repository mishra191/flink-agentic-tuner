"""Agent-in-the-loop tuner for the Kafka -> Parquet Flink job.

Every tick: verify the last action (in code), then let Claude observe the job and decide.

    python agent.py --dry-run --once          # one observation pass, change nothing
    python agent.py --interval 60             # run continuously
"""

import argparse
import json
import time
from pathlib import Path

import anthropic

import guardrails
import tools
from flink_client import FlinkClient, FlinkError

MODEL = "claude-opus-5-5"

SYSTEM_PROMPT = """You operate a production Apache Flink job that reads trade events from Kafka and writes them to Parquet files.

Goals, in priority order:
1. Correctness: the job is RUNNING, checkpoints complete, and Parquet files get committed (in-progress files do not pile up).
2. Freshness: Kafka lag stays low and committed data is never more than {freshness_slo_s} seconds behind real time.
3. Efficiency: within the freshness target, produce as few and as large files as possible, so downstream queries stay fast.

How to work each tick:
- Start with get_job_status, get_checkpoint_stats and inspect_output_files. Look further only when something looks off.
- Diagnose from the numbers. If restarts are rising, read get_recent_exceptions before anything else: a crash loop caused by a code or dependency bug cannot be fixed by tuning, so escalate it instead of changing settings.
- Change at most one thing per tick. Call propose_action first, and only call reconfigure_job if the verdict is allowed or needs_approval.
- Prefer doing nothing over a speculative change. Every restart costs a few seconds of latency.
- If the job is not running or the pipeline is in a state you do not understand, escalate.
- Finish every tick with exactly one log_decision call.

Useful facts about this job:
- Parquet part files only become visible when a checkpoint completes. Longer checkpoint intervals mean bigger files but staler data.
- Many tiny finished files per bucket usually means the checkpoint interval is too short or the bucketing is too fine for the data volume. compact_bucket can clean up existing small files; reconfiguring prevents new ones.
- The job's settings are visible in get_job_status under config; they are the knobs you can change."""


def build_tick_prompt(state: guardrails.AgentState, decision_log: Path) -> str:
    recent = decision_log.read_text().splitlines()[-5:] if decision_log.exists() else []
    lines = [f"Tick at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}."]
    if state.pending_verification:
        p = state.pending_verification
        lines.append(f"A previous change {p['changes']} is being verified for another "
                     f"{max(0, int(p['deadline'] - time.time()))}s; do not make another change.")
    if recent:
        lines.append("Recent events from the decision log:\n" + "\n".join(recent))
    lines.append("Check the pipeline and decide what, if anything, to do.")
    return "\n\n".join(lines)


def verify_pending(state: guardrails.AgentState) -> None:
    """Deterministic check of the last action; rolls back without asking the model."""
    pending = state.pending_verification
    if not pending:
        return
    try:
        after = tools.health_snapshot()
    except FlinkError as e:
        after = {"state": f"unreachable ({e})"}
    problem = guardrails.health_regressed(pending["baseline"], after)
    if problem:
        print(f"[verify] regression after {pending['changes']}: {problem} -> rolling back")
        print("[verify]", tools.do_rollback(f"automatic: {problem}"))
    elif time.time() >= pending["deadline"]:
        tools.log_event("verified", changes=pending["changes"], health=after)
        state.pending_verification = None
        state.save()
        print(f"[verify] change {pending['changes']} held up; verification complete")


def run_tick(client: anthropic.Anthropic, args) -> None:
    verify_pending(tools.CTX.state)

    runner = client.beta.messages.tool_runner(
        model=MODEL,
        max_tokens=16000,
        max_iterations=args.max_tool_rounds,
        system=SYSTEM_PROMPT.format(freshness_slo_s=args.freshness_slo_s),
        tools=tools.ALL_TOOLS,
        messages=[{"role": "user", "content": build_tick_prompt(tools.CTX.state, tools.CTX.decision_log)}],
        output_config={"effort": args.effort},
        # caches the stable tools + system prefix across ticks
        cache_control={"type": "ephemeral"},
        # on a safety decline, re-run on Anthropic's recommended fallback model
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
    )
    for message in runner:
        if message.stop_reason == "refusal":
            tools.log_event("refusal", details=str(message.stop_details))
            print("[agent] request declined; skipping this tick")
            return
        for block in message.content:
            if block.type == "text" and block.text.strip():
                print(f"[agent] {block.text.strip()}")
            elif block.type == "tool_use":
                print(f"[tool]  {block.name}({json.dumps(block.input)})")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--flink-url", default="http://localhost:8081")
    p.add_argument("--job-name-prefix", default="Kafka -> Parquet")
    p.add_argument("--jar-name", default="trade-parquet-pipeline")
    p.add_argument("--entry-class", default="io.github.mishra191.flinktuner.KafkaToParquetJob")
    p.add_argument("--savepoint-dir", default="file:///tmp/flink-savepoints")
    p.add_argument("--state-dir", default=".", help="where agent_state.json and decisions.jsonl live")
    p.add_argument("--interval", type=int, default=60, help="seconds between ticks")
    p.add_argument("--effort", default="medium", choices=["low", "medium", "high", "xhigh", "max"])
    p.add_argument("--max-tool-rounds", type=int, default=15)
    p.add_argument("--freshness-slo-s", type=int, default=300, help="max seconds committed data may lag real time")
    p.add_argument("--cooldown-s", type=int, default=guardrails.COOLDOWN_S, help="min seconds between changes")
    p.add_argument("--verify-s", type=int, default=guardrails.VERIFY_AFTER_S, help="seconds each change is verified")
    p.add_argument("--dry-run", action="store_true", help="observe and propose, never change anything")
    p.add_argument("--interactive", action="store_true", help="ask on the terminal for changes that need approval")
    p.add_argument("--once", action="store_true", help="run a single tick and exit")
    args = p.parse_args()
    guardrails.COOLDOWN_S = args.cooldown_s
    guardrails.VERIFY_AFTER_S = args.verify_s

    state_dir = Path(args.state_dir)
    tools.CTX = tools.Context(
        flink=FlinkClient(args.flink_url),
        state=guardrails.AgentState.load(state_dir / "agent_state.json"),
        job_name_prefix=args.job_name_prefix,
        jar_name=args.jar_name,
        entry_class=args.entry_class,
        savepoint_dir=args.savepoint_dir,
        decision_log=state_dir / "decisions.jsonl",
        dry_run=args.dry_run,
        interactive=args.interactive,
    )
    client = anthropic.Anthropic()

    while True:
        try:
            run_tick(client, args)
        except (FlinkError, anthropic.APIConnectionError, anthropic.RateLimitError, anthropic.InternalServerError) as e:
            # transient: the cluster or the API is briefly unavailable; try again next tick
            tools.log_event("tick_error", error=f"{type(e).__name__}: {e}")
            print(f"[error] {type(e).__name__}: {e}")
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
