"""Thin wrapper over the Flink REST API (https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/rest_api/)."""

import time

import requests


class FlinkError(RuntimeError):
    pass


class FlinkClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, **params):
        r = requests.get(self.base_url + path, params=params or None, timeout=self.timeout)
        if r.status_code >= 400:
            raise FlinkError(f"GET {path} -> {r.status_code}: {r.text[:300]}")
        return r.json()

    def _post(self, path: str, body: dict):
        r = requests.post(self.base_url + path, json=body, timeout=self.timeout)
        if r.status_code >= 400:
            raise FlinkError(f"POST {path} -> {r.status_code}: {r.text[:300]}")
        return r.json()

    # ---- discovery -------------------------------------------------------

    def find_job(self, name_prefix: str) -> dict | None:
        """Most recently started job whose name starts with name_prefix, preferring RUNNING ones."""
        jobs = [j for j in self._get("/jobs/overview")["jobs"] if j["name"].startswith(name_prefix)]
        if not jobs:
            return None
        jobs.sort(key=lambda j: (j["state"] == "RUNNING", j["start-time"]), reverse=True)
        return jobs[0]

    def job(self, jid: str) -> dict:
        return self._get(f"/jobs/{jid}")

    def job_user_config(self, jid: str) -> dict:
        """The global job parameters, i.e. the merged JobConfig the job was started with."""
        return self._get(f"/jobs/{jid}/config").get("execution-config", {}).get("user-config", {})

    # ---- metrics ---------------------------------------------------------

    def job_metric(self, jid: str, name: str) -> float | None:
        values = self._get(f"/jobs/{jid}/metrics", get=name)
        return float(values[0]["value"]) if values else None

    def checkpoints(self, jid: str) -> dict:
        return self._get(f"/jobs/{jid}/checkpoints")

    def exceptions(self, jid: str, max_entries: int = 5) -> dict:
        return self._get(f"/jobs/{jid}/exceptions", maxExceptions=max_entries)

    def backpressure(self, jid: str, vid: str) -> dict:
        return self._get(f"/jobs/{jid}/vertices/{vid}/backpressure")

    def vertex_metric_sum(self, jid: str, vid: str, suffix: str) -> float | None:
        """Sum across subtasks of the first vertex metric whose id ends with suffix."""
        available = self._get(f"/jobs/{jid}/vertices/{vid}/subtasks/metrics")
        ids = [m["id"] for m in available if m["id"] == suffix or m["id"].endswith("." + suffix)]
        if not ids:
            return None
        values = self._get(f"/jobs/{jid}/vertices/{vid}/subtasks/metrics", get=ids[0], agg="sum")
        return float(values[0]["sum"]) if values else None

    # ---- lifecycle -------------------------------------------------------

    def stop_with_savepoint(self, jid: str, target_dir: str, poll_timeout_s: float = 300) -> str:
        """Stop the job gracefully and return the savepoint location."""
        trigger = self._post(f"/jobs/{jid}/stop", {"targetDirectory": target_dir, "drain": False})
        return self._await_savepoint(jid, trigger["request-id"], poll_timeout_s)

    def trigger_savepoint(self, jid: str, target_dir: str, poll_timeout_s: float = 300) -> str:
        trigger = self._post(f"/jobs/{jid}/savepoints", {"target-directory": target_dir, "cancel-job": False})
        return self._await_savepoint(jid, trigger["request-id"], poll_timeout_s)

    def _await_savepoint(self, jid: str, trigger_id: str, poll_timeout_s: float) -> str:
        deadline = time.time() + poll_timeout_s
        while time.time() < deadline:
            status = self._get(f"/jobs/{jid}/savepoints/{trigger_id}")
            if status["status"]["id"] == "COMPLETED":
                op = status.get("operation", {})
                if "location" in op:
                    return op["location"]
                raise FlinkError(f"savepoint failed: {str(op.get('failure-cause', op))[:500]}")
            time.sleep(2)
        raise FlinkError(f"savepoint {trigger_id} did not complete within {poll_timeout_s}s")

    def find_jar(self, name_contains: str) -> str:
        jars = [f for f in self._get("/jars")["files"] if name_contains in f["name"]]
        if not jars:
            raise FlinkError(f"no uploaded jar matching '{name_contains}' (upload it via POST /jars/upload)")
        jars.sort(key=lambda f: f["uploaded"], reverse=True)
        return jars[0]["id"]

    def run_jar(self, jar_id: str, entry_class: str, program_args: list[str], savepoint_path: str | None) -> str:
        body = {"entryClass": entry_class, "programArgsList": program_args}
        if savepoint_path:
            body["savepointPath"] = savepoint_path
            body["allowNonRestoredState"] = False
        return self._post(f"/jars/{jar_id}/run", body)["jobid"]
