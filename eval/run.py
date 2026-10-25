"""Run one experiment: start the job with a scenario's settings, drive load, and record everything.

    docker compose -f eval/docker-compose.yml up -d
    python eval/run.py --scenario small-files --controller static
    python eval/run.py --scenario small-files --controller agent      # needs ANTHROPIC_API_KEY
    python eval/run.py --all --controller rules

Each run writes eval/results/<scenario>__<controller>__<timestamp>/ with config.json,
samples.csv, files.csv and (for rules/agent) decisions.jsonl.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))
sys.path.insert(0, str(ROOT / "eval"))

import guardrails  # noqa: E402
import tools  # noqa: E402
from collector import Collector  # noqa: E402
from flink_client import FlinkClient  # noqa: E402
from loadgen import LoadGenerator, bursty, steady  # noqa: E402
from rule_controller import RuleController  # noqa: E402

FLINK_URL = "http://localhost:8081"
KAFKA_HOST = "localhost:9092"
KAFKA_INTERNAL = "kafka:19092"  # address the Flink containers use
EVAL_DIR = Path("/tmp/flink-tuner-eval")  # mounted at the same path in the Flink containers
JAR = ROOT / "pipeline" / "target" / "trade-parquet-pipeline-1.0.0.jar"
ENTRY_CLASS = "io.github.mishra191.flinktuner.KafkaToParquetJob"

FRESHNESS_SLO_S = 120
COOLDOWN_S = 4 * 60
VERIFY_S = 2 * 60

BASE_CONFIG = {
    "flink.parallelism": 2,
    "flink.checkpoint.interval.ms": 10_000,
    "sink.bucket.strategy": "symbol",
    "sink.rolling.max.part.mb": 128,
    "sink.rolling.interval.min": 15,
}

SCENARIOS = {
    # Low traffic, aggressive checkpoints, fine-grained buckets: thousands of tiny files
    "small-files": {"load": lambda: steady(20),
                    "config": {"flink.checkpoint.interval.ms": 5_000, "sink.bucket.strategy": "symbol_hour"}},
    # Healthy traffic but checkpoints far apart: data is readable only every 4 minutes
    "stale-data": {"load": lambda: steady(300),
                   "config": {"flink.checkpoint.interval.ms": 240_000}},
    # Quiet baseline with a 2-minute burst every 10 minutes
    "bursty": {"load": lambda: bursty(base=50, peak=2_000, period_s=600, burst_s=120),
               "config": {"flink.checkpoint.interval.ms": 10_000}},
    # Kafka becomes unreachable mid-run; staleness rises but no setting can fix it
    "broker-outage": {"load": lambda: steady(200),
                      "config": {"flink.checkpoint.interval.ms": 60_000},
                      "outage": {"start_s": 300, "duration_s": 240}},
}


def wait_for_cluster(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if requests.get(f"{FLINK_URL}/taskmanagers", timeout=3).json().get("taskmanagers"):
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise SystemExit("Flink cluster not reachable; run: docker compose -f eval/docker-compose.yml up -d")


def cancel_all_jobs() -> None:
    for job in requests.get(f"{FLINK_URL}/jobs/overview", timeout=5).json()["jobs"]:
        if job["state"] in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING"):
            requests.patch(f"{FLINK_URL}/jobs/{job['jid']}?mode=cancel", timeout=5)
    time.sleep(3)


def upload_jar() -> None:
    names = [f["name"] for f in requests.get(f"{FLINK_URL}/jars", timeout=5).json()["files"]]
    if JAR.name in names:
        return
    if not JAR.exists():
        raise SystemExit(f"{JAR} missing; run: (cd pipeline && mvn package)")
    with JAR.open("rb") as f:
        r = requests.post(f"{FLINK_URL}/jars/upload", files={"jarfile": (JAR.name, f, "application/x-java-archive")},
                          timeout=300)
    r.raise_for_status()


def create_topic(topic: str, timeout_s: int = 120) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_HOST})
    deadline = time.time() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            break
        except Exception:
            if time.time() > deadline:
                raise SystemExit("Kafka not reachable on localhost:9092")
            time.sleep(3)
    admin.create_topics([NewTopic(topic, 4, 1)])[topic].result(30)


def kafka_container() -> str:
    out = subprocess.run(["docker", "compose", "-f", str(ROOT / "eval" / "docker-compose.yml"), "ps", "-q", "kafka"],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def run(scenario: str, controller: str, minutes: float) -> Path:
    spec = SCENARIOS[scenario]
    run_id = f"{scenario}__{controller}__{time.strftime('%Y%m%dT%H%M%S')}"
    topic = f"trades-{run_id.replace('__', '-').lower()}"
    results = ROOT / "eval" / "results" / run_id
    output = EVAL_DIR / "output" / run_id
    results.mkdir(parents=True)
    output.mkdir(parents=True)
    (EVAL_DIR / "savepoints").mkdir(parents=True, exist_ok=True)

    config = {**BASE_CONFIG, **spec["config"],
              "kafka.bootstrap.servers": KAFKA_INTERNAL, "kafka.topic": topic, "kafka.group.id": run_id,
              "output.path": f"file://{output}"}
    (results / "config.json").write_text(json.dumps(
        {"scenario": scenario, "controller": controller, "minutes": minutes, "freshness_slo_s": FRESHNESS_SLO_S,
         "cooldown_s": COOLDOWN_S, "verify_s": VERIFY_S, "outage": spec.get("outage"), "job_config": config}, indent=2))

    wait_for_cluster()
    cancel_all_jobs()
    upload_jar()
    create_topic(topic)

    flink = FlinkClient(FLINK_URL)
    jar_id = flink.find_jar("trade-parquet-pipeline")
    flink.run_jar(jar_id, ENTRY_CLASS, [x for k, v in config.items() for x in (f"--{k}", str(v))], None)
    job_prefix = f"Kafka -> Parquet ({topic})"
    print(f"[run] {run_id}: job submitted, running {minutes} min")

    load = LoadGenerator(KAFKA_HOST, topic, spec["load"]())
    collector = Collector(FLINK_URL, job_prefix, output, results, loadgen=load)
    load.start()
    collector.start()

    ctrl, agent_proc = None, None
    if controller == "rules":
        guardrails.COOLDOWN_S, guardrails.VERIFY_AFTER_S = COOLDOWN_S, VERIFY_S
        tools.CTX = tools.Context(flink, guardrails.AgentState.load(results / "agent_state.json"), job_prefix,
                                  "trade-parquet-pipeline", ENTRY_CLASS, f"file://{EVAL_DIR}/savepoints",
                                  results / "decisions.jsonl", dry_run=False, interactive=False)
        ctrl = RuleController(FRESHNESS_SLO_S)
        ctrl.start()
    elif controller == "agent":
        if not os.environ.get("ANTHROPIC_API_KEY") and not shutil.which("ant"):
            raise SystemExit("agent runs need Claude API credentials (ANTHROPIC_API_KEY or `ant auth login`)")
        agent_proc = subprocess.Popen(
            [sys.executable, str(ROOT / "agent" / "agent.py"), "--flink-url", FLINK_URL, "--job-name-prefix", job_prefix,
             "--savepoint-dir", f"file://{EVAL_DIR}/savepoints", "--state-dir", str(results),
             "--freshness-slo-s", str(FRESHNESS_SLO_S), "--cooldown-s", str(COOLDOWN_S), "--verify-s", str(VERIFY_S)],
            stdout=(results / "agent.log").open("w"), stderr=subprocess.STDOUT)

    outage = spec.get("outage")
    paused = False
    start = time.time()
    try:
        while time.time() - start < minutes * 60:
            elapsed = time.time() - start
            if outage and not paused and outage["start_s"] <= elapsed < outage["start_s"] + outage["duration_s"]:
                subprocess.run(["docker", "pause", kafka_container()], check=True)
                paused = True
                print(f"[run] broker paused at {elapsed:.0f}s")
            if paused and elapsed >= outage["start_s"] + outage["duration_s"]:
                subprocess.run(["docker", "unpause", kafka_container()], check=True)
                paused = False
                print(f"[run] broker resumed at {elapsed:.0f}s")
            time.sleep(5)
    finally:
        if paused:
            subprocess.run(["docker", "unpause", kafka_container()])
        if ctrl:
            ctrl.stop()
        if agent_proc:
            agent_proc.terminate()
            agent_proc.wait(30)
        load.stop()
        collector.stop()
        collector.join(30)
        cancel_all_jobs()
    print(f"[run] done: {results} ({load.sent} events sent)")
    return results


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenario", choices=list(SCENARIOS))
    p.add_argument("--all", action="store_true", help="run every scenario in turn")
    p.add_argument("--controller", choices=["static", "rules", "agent"], required=True)
    p.add_argument("--minutes", type=float, default=15)
    args = p.parse_args()
    if not args.all and not args.scenario:
        p.error("pass --scenario or --all")
    for scenario in (SCENARIOS if args.all else [args.scenario]):
        run(scenario, args.controller, args.minutes)


if __name__ == "__main__":
    main()
