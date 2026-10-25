"""Summarize experiment runs into a table and figures.

    python eval/analyze.py                    # uses the latest run per (scenario, controller)

Writes eval/results/summary.csv, summary.md and figures/*.png.
"""

import csv
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"
SCENARIOS = ["small-files", "stale-data", "bursty", "broker-outage"]
CONTROLLERS = ["static", "rules", "agent"]
# Fixed categorical order: color follows the controller, never its rank
COLORS = {"static": "#2a78d6", "rules": "#eb6834", "agent": "#1baf7a"}
INK, INK_MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
WARMUP_S = 60  # ignore job startup when computing staleness statistics


def latest_runs() -> dict:
    runs = {}
    for d in sorted(RESULTS.glob("*__*__*")):
        if (d / "samples.csv").exists():
            scenario, controller, _ = d.name.split("__")
            runs[(scenario, controller)] = d  # sorted by timestamp, so the last one wins
    return runs


def read_csv(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def percentile(values: list[float], p: float):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def summarize(run_dir: Path) -> dict:
    cfg = json.loads((run_dir / "config.json").read_text())
    samples = read_csv(run_dir / "samples.csv")
    files = [f for f in read_csv(run_dir / "files.csv") if f["compacted"] != "True"]
    events = []
    if (run_dir / "decisions.jsonl").exists():
        events = [json.loads(line) for line in (run_dir / "decisions.jsonl").read_text().splitlines() if line.strip()]

    slo = cfg["freshness_slo_s"]
    staleness = [num(s["staleness_s"]) for s in samples
                 if num(s["elapsed_s"]) >= WARMUP_S and num(s["staleness_s"]) is not None]
    sizes_kb = [int(f["bytes"]) / 1024 for f in files]
    rows = [int(f["rows"]) for f in files]
    last = samples[-1]
    minutes = num(last["elapsed_s"]) / 60
    # a gap far above the sampling interval means the host was suspended; such runs are not valid
    times = [num(s["t"]) for s in samples]
    max_gap = max((b - a for a, b in zip(times, times[1:])), default=0)
    sent, committed = num(last["events_sent"]), num(last["committed_rows"])

    return {
        "scenario": cfg["scenario"],
        "controller": cfg["controller"],
        "minutes": round(minutes, 1),
        "max_sample_gap_s": round(max_gap, 1),
        "valid": max_gap < 60,
        "files_committed": len(files),
        "files_per_min": round(len(files) / minutes, 1),
        "median_file_kb": round(statistics.median(sizes_kb), 1) if sizes_kb else None,
        "median_rows_per_file": statistics.median(rows) if rows else None,
        "staleness_p50_s": round(percentile(staleness, 50), 1) if staleness else None,
        "staleness_p95_s": round(percentile(staleness, 95), 1) if staleness else None,
        "slo_violation_pct": round(100 * sum(s > slo for s in staleness) / len(staleness), 1) if staleness else None,
        "max_kafka_lag": max((num(s["kafka_lag"]) or 0) for s in samples),
        # files actually left on disk, after any compaction, vs. files the job committed
        "files_on_disk_end": int(num(last["finished_files"]) or 0),
        "restarts_by_controller": sum(e["kind"] == "action" and e.get("action") == "reconfigure" for e in events),
        "compactions": sum(e["kind"] == "action" and e.get("action") == "compact" for e in events),
        "rollbacks": sum(e["kind"] == "rollback" for e in events),
        "escalations": sum(e["kind"] == "decision" and e.get("decision") == "escalate" for e in events),
        "final_checkpoint_interval_ms": last["checkpoint_interval_ms"],
        "events_sent": int(sent) if sent else None,
        "rows_committed": int(committed) if committed else None,
    }


def style(ax, title: str):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=10, color=INK)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK_MUTED, labelsize=8)


def series(run_dir: Path, field: str):
    samples = read_csv(run_dir / "samples.csv")
    xs, ys = [], []
    for s in samples:
        y = num(s[field])
        if y is not None:
            xs.append(num(s["elapsed_s"]) / 60)
            ys.append(y)
    return xs, ys


def action_times(run_dir: Path) -> list[float]:
    """Minutes into the run at which the controller restarted the job."""
    path = run_dir / "decisions.jsonl"
    if not path.exists():
        return []
    t0 = num(read_csv(run_dir / "samples.csv")[0]["t"])
    from datetime import datetime, timezone
    out = []
    for line in path.read_text().splitlines():
        e = json.loads(line)
        if e["kind"] in ("action", "rollback") and e.get("action", "reconfigure") == "reconfigure":
            ts = datetime.strptime(e["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
            out.append((ts - t0) / 60)
    return out


def figure(runs: dict, field: str, ylabel: str, filename: str, slo_line: bool = False, log: bool = False):
    fig, axes = plt.subplots(1, len(SCENARIOS), figsize=(13, 3.2), sharex=False)
    fig.patch.set_facecolor(SURFACE)
    for ax, scenario in zip(axes, SCENARIOS):
        style(ax, scenario)
        present = [c for c in CONTROLLERS if (scenario, c) in runs]
        for c in present:
            run_dir = runs[(scenario, c)]
            xs, ys = series(run_dir, field)
            ax.plot(xs, ys, color=COLORS[c], linewidth=2, label=c)
            if xs:
                # direct label at the line end; text stays in ink, the line carries the color
                ax.annotate(c, (xs[-1], ys[-1]), xytext=(4, 0), textcoords="offset points",
                            fontsize=8, color=INK_MUTED, va="center")
            for t in action_times(run_dir):
                ax.axvline(t, color=COLORS[c], linewidth=1, linestyle=":", alpha=0.8)
        cfg = json.loads((runs[(scenario, present[0])] / "config.json").read_text()) if present else {}
        outage = cfg.get("outage")
        if outage:
            ax.axvspan(outage["start_s"] / 60, (outage["start_s"] + outage["duration_s"]) / 60,
                       color=GRID, alpha=0.6, linewidth=0)
            ax.text(outage["start_s"] / 60, ax.get_ylim()[1], " broker down", fontsize=7, color=INK_MUTED, va="top")
        if slo_line and cfg:
            ax.axhline(cfg["freshness_slo_s"], color=INK_MUTED, linewidth=1, linestyle="--")
            ax.text(0, cfg["freshness_slo_s"], " SLO", fontsize=7, color=INK_MUTED, va="bottom")
        if log:
            ax.set_yscale("log")
        ax.set_xlabel("minutes", fontsize=8, color=INK_MUTED)
    axes[0].set_ylabel(ylabel, fontsize=9, color=INK)
    handles = [plt.Line2D([], [], color=COLORS[c], linewidth=2) for c in CONTROLLERS]
    fig.legend(handles, CONTROLLERS, loc="upper right", ncol=3, frameon=False, fontsize=9)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    (RESULTS / "figures").mkdir(exist_ok=True)
    fig.savefig(RESULTS / "figures" / filename, dpi=200, facecolor=SURFACE)
    plt.close(fig)


def main():
    runs = latest_runs()
    if not runs:
        raise SystemExit(f"no runs in {RESULTS}")
    rows = [summarize(runs[k]) for k in sorted(runs, key=lambda k: (SCENARIOS.index(k[0]), CONTROLLERS.index(k[1])))]

    with (RESULTS / "summary.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    cols = ["scenario", "controller", "files_committed", "files_on_disk_end", "median_file_kb", "median_rows_per_file",
            "staleness_p50_s", "staleness_p95_s", "slo_violation_pct", "restarts_by_controller", "compactions", "rollbacks",
            "escalations", "final_checkpoint_interval_ms"]
    md = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    md += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    (RESULTS / "summary.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))

    figure(runs, "finished_files", "Parquet files on disk", "files_over_time.png")
    figure(runs, "staleness_s", "staleness of committed data (s)", "staleness_over_time.png", slo_line=True)
    print(f"figures written to {RESULTS / 'figures'}")


if __name__ == "__main__":
    main()
