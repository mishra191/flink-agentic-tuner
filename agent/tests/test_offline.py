"""Offline tests: the real tools and guardrails against a fake Flink REST API. No Claude calls.

    cd agent && python -m pytest tests
"""

import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import agent  # noqa: E402
import guardrails  # noqa: E402
import tools  # noqa: E402
from flink_client import FlinkClient  # noqa: E402


class FakeFlink:
    """Simulates a job stuck in the crash loop described in the paper (missing parquet-avro)."""

    def __init__(self, output_dir: Path):
        self.jid = "job1"
        self.restarts = 7
        self.runs = []
        self.stops = 0
        self.config = {
            "output.path": f"file://{output_dir}",
            "flink.checkpoint.interval.ms": "10000",
            "flink.parallelism": "4",
            "sink.bucket.strategy": "symbol",
            "kafka.topic": "trades",
        }
        self.vertices = [
            {"id": "v1", "name": "Source: kafka-source -> clean-filter", "parallelism": 4, "status": "RUNNING"},
            {"id": "v2", "name": "parquet-sink: Writer -> parquet-sink: Committer", "parallelism": 4, "status": "RUNNING"},
        ]

    def get(self, path: str, query: dict):
        j = self.jid
        routes = {
            "/jobs/overview": lambda: {"jobs": [{"jid": j, "name": "Kafka -> Parquet (trades)", "state": "RUNNING",
                                                 "start-time": 1, "duration": 120_000}]},
            f"/jobs/{j}": lambda: {"vertices": self.vertices},
            f"/jobs/{j}/config": lambda: {"execution-config": {"user-config": self.config}},
            f"/jobs/{j}/metrics": lambda: [{"id": "numRestarts", "value": str(self.restarts)}],
            f"/jobs/{j}/checkpoints": lambda: {"counts": {"completed": 0, "failed": 0, "in_progress": 0},
                                              "summary": {}, "latest": {}},
            f"/jobs/{j}/exceptions": lambda: {"exceptionHistory": {"entries": [{
                "exceptionName": "java.lang.NoClassDefFoundError: org/apache/parquet/avro/AvroParquetWriter",
                "timestamp": 1, "taskName": "parquet-sink", "stacktrace": "java.lang.NoClassDefFoundError\n\tat x"}]}},
            "/jars": lambda: {"files": [{"id": "jar1", "name": "trade-parquet-pipeline-1.0.0.jar", "uploaded": 1}]},
        }
        if path in routes:
            return routes[path]()
        if path.endswith("/backpressure"):
            return {"status": "ok", "backpressureLevel": "ok"}
        if path.endswith("/subtasks/metrics"):
            if "get" in query:
                return [{"id": query["get"][0], "sum": 1234.0}]
            return [{"id": "Source__kafka-source.pendingRecords"}, {"id": "numRecordsInPerSecond"},
                    {"id": "numRecordsOutPerSecond"}]
        if "/savepoints/" in path:
            return {"status": {"id": "COMPLETED"}, "operation": {"location": f"file:///sp/{self.stops}"}}
        return None

    def post(self, path: str, body: dict):
        if path.endswith("/stop"):
            self.stops += 1
            return {"request-id": "t1"}
        if path.endswith("/run"):
            self.runs.append(body)
            self.jid = f"job{len(self.runs) + 1}"
            self.restarts = 0
            return {"jobid": self.jid}
        return None


@pytest.fixture
def fake(tmp_path):
    out = tmp_path / "out"
    for bucket in ("symbol=btc_usd", "symbol=eth_usd"):
        (out / bucket).mkdir(parents=True)
        for i in range(3):
            (out / bucket / f".part-{i}.parquet.inprogress.x").write_bytes(b"x")
    old = time.time() - 600
    for i in range(4):
        f = out / "symbol=btc_usd" / f"part-{i}.parquet"
        pq.write_table(pa.table({"symbol": ["BTC/USD"] * 5, "price": [float(i)] * 5}), f)
        os.utime(f, (old, old))

    flink = FakeFlink(out)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send(self, obj):
            body = json.dumps(obj if obj is not None else {"errors": ["not found"]}).encode()
            self.send_response(200 if obj is not None else 404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urlparse(self.path)
            self._send(flink.get(u.path, parse_qs(u.query)))

        def do_POST(self):
            self._send(flink.post(self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    tools.CTX = tools.Context(
        flink=FlinkClient(f"http://127.0.0.1:{server.server_port}"),
        state=guardrails.AgentState.load(tmp_path / "state.json"),
        job_name_prefix="Kafka -> Parquet",
        jar_name="trade-parquet-pipeline",
        entry_class="io.github.mishra191.flinktuner.KafkaToParquetJob",
        savepoint_dir="file:///sp",
        decision_log=tmp_path / "decisions.jsonl",
        dry_run=False,
        interactive=False,
    )
    yield flink, out
    server.shutdown()


def call(tool, **kwargs):
    result = tool.call(kwargs)
    return json.loads(result) if result.startswith(("{", "[", '"')) else result


def arg(run: dict, key: str) -> str:
    args = run["programArgsList"]
    return args[args.index(f"--{key}") + 1]


def test_observe_tools(fake):
    status = call(tools.get_job_status)
    assert status["restarts"] == 7 and status["config"]["flink.parallelism"] == "4"
    assert "AvroParquetWriter" in call(tools.get_recent_exceptions)[0]["exception"]
    assert call(tools.get_throughput_and_lag)["kafka_lag_records"] == 1234.0
    assert len(call(tools.get_backpressure)) == 2
    assert call(tools.get_checkpoint_stats)["counts"]["completed"] == 0

    files = call(tools.inspect_output_files)
    assert files["total_inprogress_files"] == 6 and files["total_finished_files"] == 4


def test_guardrails(fake):
    assert call(tools.propose_action, reason="r", checkpoint_interval_ms=1000)["verdict"] == "denied"
    assert call(tools.propose_action, reason="r", parallelism=1)["verdict"] == "needs_approval"
    assert call(tools.reconfigure_job, reason="r", parallelism=1)["result"] == "not applied"
    assert call(tools.propose_action, reason="r", checkpoint_interval_ms=60000)["verdict"] == "allowed"


def test_reconfigure_then_automatic_rollback(fake):
    flink, _ = fake
    result = call(tools.reconfigure_job, reason="bigger files", checkpoint_interval_ms=60000)
    assert result["result"] == "restarted"
    assert arg(flink.runs[-1], "flink.checkpoint.interval.ms") == "60000"
    assert flink.runs[-1]["savepointPath"] == "file:///sp/1"

    # A second change is blocked while the first is being verified
    assert call(tools.propose_action, reason="r", rolling_interval_min=2)["verdict"] == "denied"

    # The new job starts restarting: the harness rolls back without asking the model
    flink.restarts = 2
    agent.verify_pending(tools.CTX.state)
    assert arg(flink.runs[-1], "flink.checkpoint.interval.ms") == "10000"
    assert flink.runs[-1]["savepointPath"] == "file:///sp/2"
    assert tools.CTX.state.pending_verification is None


def test_compaction(fake):
    _, out = fake
    result = call(tools.compact_bucket, bucket="symbol=btc_usd", reason="small files")
    assert result["result"] == "compacted" and result["rows"] == 20

    names = [f.name for f in (out / "symbol=btc_usd").iterdir()]
    assert sum(not n.startswith(".") for n in names) == 1
    assert sum(".inprogress" in n for n in names) == 3

    assert call(tools.compact_bucket, bucket="../../etc", reason="x")["result"] == "not applied"


def test_decision_log(fake):
    assert call(tools.log_decision, observation="o", decision="no_action", reasoning="r") == "logged"
    entry = json.loads(tools.CTX.decision_log.read_text().splitlines()[-1])
    assert entry["kind"] == "decision" and entry["decision"] == "no_action"
