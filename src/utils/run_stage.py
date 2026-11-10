"""Run one pipeline stage, record the run and push its metrics to Prometheus.

DVC calls every stage through this wrapper (see dvc.yaml):

    python -m src.utils.run_stage <stage> --report <report.json> -- <command ...>

It runs the command, then:
  * appends a run record to logs/pipeline_runs.jsonl (read by the backend's
    pipeline console: run history, durations, failures);
  * pushes stage metrics to the Prometheus Pushgateway (if it is reachable):
      logsentinel_pipeline_stage_duration_seconds{stage}
      logsentinel_pipeline_stage_success{stage}            1 = ok, 0 = failed
      logsentinel_pipeline_stage_last_run_timestamp_seconds{stage}
      logsentinel_pipeline_stage_lines_per_second{stage}   (if the report has it)
The stage's own exit code is returned, so DVC still stops on failure.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from src.utils.logger import get_logger

logger = get_logger("run_stage")
RUNS_FILE = Path(os.getenv("PIPELINE_RUNS_FILE", "logs/pipeline_runs.jsonl"))
THROUGHPUT_KEYS = ("lines_per_second", "rows_per_second")


def read_throughput(report: Path | None) -> float | None:
    """Return lines/rows per second from a stage report, if present."""
    if report is None or not report.exists():
        return None
    try:
        data = json.loads(report.read_text())
    except json.JSONDecodeError:
        return None
    # Top level first, then one level down (e.g. validation.json -> log.lines_per_second).
    sections = [data] + [v for v in data.values() if isinstance(v, dict)]
    for section in sections:
        for key in THROUGHPUT_KEYS:
            if isinstance(section.get(key), (int, float)):
                return float(section[key])
    return None


def push_metrics(stage: str, seconds: float, success: bool, throughput: float | None) -> None:
    """Push stage metrics to the Pushgateway; never fail the stage if it is down."""
    registry = CollectorRegistry()
    labels = ["stage"]
    Gauge(
        "logsentinel_pipeline_stage_duration_seconds", "Stage wall time", labels, registry=registry
    ).labels(stage).set(seconds)
    Gauge(
        "logsentinel_pipeline_stage_success", "1 if the stage succeeded", labels, registry=registry
    ).labels(stage).set(1 if success else 0)
    Gauge(
        "logsentinel_pipeline_stage_last_run_timestamp_seconds",
        "Unix time of the last run",
        labels,
        registry=registry,
    ).labels(stage).set(time.time())
    if throughput is not None:
        Gauge(
            "logsentinel_pipeline_stage_lines_per_second",
            "Stage throughput",
            labels,
            registry=registry,
        ).labels(stage).set(throughput)

    gateway = os.getenv("PUSHGATEWAY_URL", "localhost:9091")
    try:
        push_to_gateway(
            gateway,
            job="logsentinel_pipeline",
            grouping_key={"stage": stage},
            registry=registry,
            timeout=3,
        )
    except OSError as exc:
        logger.warning("Pushgateway %s not reachable, metrics not pushed (%s)", gateway, exc)


def main() -> int:
    """Run the wrapped command and return its exit code."""
    parser = argparse.ArgumentParser(
        description="Run a pipeline stage with monitoring.",
        usage="run_stage <stage> [--report REPORT] -- <command ...>",
    )
    parser.add_argument("stage", help="stage name, e.g. parse")
    parser.add_argument("--report", type=Path, default=None, help="stage report JSON")
    argv = sys.argv[1:]
    if "--" not in argv:
        parser.error("missing '--' before the stage command")
    split_at = argv.index("--")
    args = parser.parse_args(argv[:split_at])  # wrapper options only
    command = argv[split_at + 1 :]  # everything after "--" is the stage command
    if not command:
        parser.error("missing command after --")

    started = datetime.now(timezone.utc)
    start = time.perf_counter()
    returncode = subprocess.call(command)
    seconds = time.perf_counter() - start
    success = returncode == 0
    throughput = read_throughput(args.report) if success else None

    record = {
        "stage": args.stage,
        "success": success,
        "returncode": returncode,
        "duration_seconds": round(seconds, 2),
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "lines_per_second": throughput,
        "command": " ".join(command),
    }
    RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RUNS_FILE.open("a") as f:
        f.write(json.dumps(record) + "\n")

    push_metrics(args.stage, seconds, success, throughput)
    level = logger.info if success else logger.error
    level("Stage %s finished: success=%s in %.1fs", args.stage, success, seconds)
    return returncode


if __name__ == "__main__":
    sys.exit(main())
