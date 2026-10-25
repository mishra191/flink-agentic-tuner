# Flink Agentic Tuner

An LLM agent that monitors and tunes a streaming Apache Flink pipeline at runtime.

The pipeline reads trade events from Kafka and writes them to Parquet with exactly-once guarantees. The agent watches the job through the Flink REST API, diagnoses problems such as stuck uncommitted files, small files, slow checkpoints and Kafka lag, and changes the job's settings through a savepoint-safe restart. Every change is checked against hard guardrails, verified afterwards, and rolled back automatically if the pipeline gets worse.

```
            ┌──────────────────────────── Flink cluster ───────────────────────────┐
 Kafka ───► │ kafka-source ─► clean-filter ─► keyBy(symbol) ─► parquet-sink (FileSink) │ ───► Parquet files
            └───────────────────────────────▲──────────────────────────────────────┘
                                            │ REST API: metrics, checkpoints, exceptions,
                                            │ stop-with-savepoint, run jar
                              ┌─────────────┴─────────────┐
                              │  Agent (Claude + tools)   │──► decisions.jsonl (audit log)
                              │  guardrails · verifier    │
                              └───────────────────────────┘
```

## Repository layout

| Path | Contents |
|---|---|
| `pipeline/` | Flink job (Java 17, Flink 1.19): Kafka source, Parquet sink with configurable bucketing and rolling |
| `agent/` | Python agent: Flink REST client, tools, guardrails, control loop, offline tests |

## Pipeline

All tuning settings are read from `pipeline/src/main/resources/application.properties` and can be overridden with `--key value` arguments. These are the settings the agent is allowed to change.

| Setting | Default | Effect |
|---|---|---|
| `flink.checkpoint.interval.ms` | `10000` | Parquet files are committed once per checkpoint: longer means larger files but staler data |
| `flink.parallelism` | `4` | Number of parallel subtasks |
| `sink.bucket.strategy` | `symbol` | Output partitioning: `symbol`, `hour` or `symbol_hour` (e.g. `symbol=btc_usd/hour=2026-10-25--10`) |
| `sink.rolling.max.part.mb` | `128` | Start a new part file once the current one reaches this size |
| `sink.rolling.interval.min` | `5` | Start a new part file once the current one has been open this long |

Design points that make runtime tuning safe:

- **Stable operator UIDs** (`kafka-source`, `clean-filter`, `parquet-sink`) and a fixed max parallelism, so the job can be restored from a savepoint after its settings change.
- **Effective configuration published as global job parameters**, so the agent reads the current settings from `GET /jobs/{id}/config`.
- **Malformed records are skipped, not fatal**, so one bad message cannot put the job into a restart loop.

### Build and run

```bash
cd pipeline
mvn package                                   # builds target/trade-parquet-pipeline-1.0.0.jar

# Upload to a running Flink 1.19 cluster and start it
flink run target/trade-parquet-pipeline-1.0.0.jar \
  --kafka.bootstrap.servers localhost:9092 --kafka.topic trades \
  --output.path file:///tmp/trade-parquet/output
```

`LocalSampleJob` writes three sample trades through the same sink without Kafka, as a quick local check that the sink commits files.

## Agent

Each tick (default: every 60 s), the harness first verifies the previous action in code, then starts a fresh Claude conversation with the recent decision log as context. The model observes the job, decides, and records its decision.

### Tools

| Kind | Tool | Purpose |
|---|---|---|
| Observe | `get_job_status` | State, uptime, restart count, operator parallelism, current settings |
| | `get_checkpoint_stats` | Completed/failed checkpoints, durations, state size |
| | `get_recent_exceptions` | Latest exceptions and stack trace heads |
| | `get_backpressure` | Backpressure level per operator |
| | `get_throughput_and_lag` | Records/s in and out, Kafka consumer lag |
| | `inspect_output_files` | Finished vs. in-progress files and file sizes per bucket |
| Act | `propose_action` | Dry-run a settings change against the guardrails |
| | `reconfigure_job` | Stop with savepoint, restart with new settings from that savepoint |
| | `compact_bucket` | Merge small finished Parquet files in one bucket |
| | `rollback` | Restore the settings from before the last change |
| Meta | `log_decision` | Record the observation, decision and reasoning for the tick |

### Guardrails

Enforced in code (`agent/guardrails.py`), independent of the model:

- Every setting has an allowed range; anything outside it is denied.
- At most one change per 10 minutes and 6 per hour.
- Parallelism changes larger than 2× require human approval (`--interactive`), and are denied otherwise.
- After each change, the harness compares job health (state, restarts, failed checkpoints, Kafka lag) for 5 minutes and **rolls back automatically** on a regression, without asking the model.
- Rollback restores from a fresh savepoint, so output continues exactly where it stopped.
- Every observation, proposal, action and rollback is appended to `decisions.jsonl`.

### Run

```bash
cd agent
pip install -r requirements.txt
export ANTHROPIC_API_KEY=...

python agent.py --dry-run --once      # one observation pass; proposes but changes nothing
python agent.py --interval 60         # continuous operation
```

Main options: `--flink-url` (default `http://localhost:8081`), `--savepoint-dir`, `--effort` (`low`…`max`, default `medium`), `--dry-run`, `--interactive`. Run `python agent.py --help` for all of them.

The job jar must be uploaded to the cluster (`POST /jars/upload` or the web UI) so the agent can restart it with new settings.

### Tests

```bash
cd agent
pip install -r requirements-dev.txt
python -m pytest tests
```

The tests run the real tools and guardrails against a simulated Flink REST API, including a full change → regression → automatic rollback cycle. They make no Claude API calls.

## Evaluation

`eval/` reproduces the experiments: single-node Kafka and Flink in Docker, a load generator, a metrics collector, a threshold-rule baseline controller and an analysis script.

| Scenario | Load | Starting configuration |
|---|---|---|
| `small-files` | 20 events/s | 5 s checkpoints, `symbol_hour` buckets |
| `stale-data` | 300 events/s | 240 s checkpoints |
| `bursty` | 50 events/s, 2,000/s for 2 min every 10 min | 10 s checkpoints |
| `broker-outage` | 200 events/s, Kafka paused from minute 5 to 9 | 60 s checkpoints |

Controllers: `static` (no changes), `rules` (threshold rules, same guardrails as the agent) and `agent`.

```bash
(cd pipeline && mvn package)
docker compose -f eval/docker-compose.yml up -d
pip install -r eval/requirements.txt

caffeinate -i python eval/run.py --all --controller static    # caffeinate: keep macOS awake
python eval/run.py --all --controller rules
python eval/run.py --all --controller agent                   # needs ANTHROPIC_API_KEY

python eval/analyze.py                                        # eval/results/summary.md + figures/
```

Staleness is measured from the data: the current time minus the newest event timestamp in any committed Parquet file. Runs in which the host was suspended are flagged `valid=False` in the summary.

## Limitations

- Output inspection and compaction support local (`file://`) output paths only, not object stores.
- Rescaling restarts the job from a savepoint rather than using Flink's adaptive scheduler.

## License

Apache License 2.0. See [LICENSE](LICENSE).
