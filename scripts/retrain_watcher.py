"""Retraining worker: turns retraining requests into pipeline runs.

Requests are JSON files in retrain_requests/, written by the backend when
  * Alertmanager fires the UnseenTemplateDrift alert (automatic), or
  * a user clicks "Request retraining" in the pipeline console (manual).

For each request this script runs, on the host (where DVC, Spark and the data live):
    dvc repro            - brings every stage up to date (new data/params/code)
    dvc repro -f train   - forces retraining (fresh MLflow run)
    dvc repro -f package - registers a new model version; it becomes "champion"
                           only if its test F1 is at least the current champion's
then (optionally) restarts the model container so it serves the new champion,
and moves the request to retrain_requests/processed/ with the result.

Usage:
    python scripts/retrain_watcher.py --once            # process pending requests and exit
    python scripts/retrain_watcher.py                   # keep watching (Ctrl+C to stop)
    python scripts/retrain_watcher.py --restart-model   # also restart the model container
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.utils.logger import get_logger  # noqa: E402

logger = get_logger("retrain_watcher")
QUEUE = Path("retrain_requests")
DONE = QUEUE / "processed"
STEPS = [["dvc", "repro"], ["dvc", "repro", "-f", "train"], ["dvc", "repro", "-f", "package"]]


def run(cmd: list[str]) -> bool:
    """Run a command, streaming its output; return True on success."""
    logger.info("Running: %s", " ".join(cmd))
    return subprocess.call(cmd) == 0


def process(request: Path, restart_model: bool) -> None:
    """Retrain for one request and archive it with the outcome."""
    data = json.loads(request.read_text())
    logger.info("Processing %s (source=%s)", request.name, data.get("source"))
    start = time.perf_counter()
    ok = all(run(step) for step in STEPS)
    if ok and restart_model:
        ok = run(["docker", "compose", "restart", "model"])
    package_report = Path("reports/package.json")
    data["result"] = {
        "success": ok,
        "seconds": round(time.perf_counter() - start, 1),
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "package": json.loads(package_report.read_text()) if package_report.exists() else None,
    }
    DONE.mkdir(parents=True, exist_ok=True)
    (DONE / request.name).write_text(json.dumps(data, indent=2) + "\n")
    request.unlink()
    logger.info("Request %s done: success=%s", request.name, ok)


def main() -> int:
    """Process pending requests once, or keep polling the queue."""
    parser = argparse.ArgumentParser(description="Process LogSentinel retraining requests.")
    parser.add_argument("--once", action="store_true", help="process pending requests and exit")
    parser.add_argument("--interval", type=int, default=30, help="poll interval in seconds")
    parser.add_argument("--restart-model", action="store_true", help="restart model container")
    args = parser.parse_args()

    QUEUE.mkdir(parents=True, exist_ok=True)
    while True:
        pending = sorted(QUEUE.glob("retrain_*.json"))
        if pending:
            # Several alerts in a row need only one retraining run: process the
            # newest request and archive the older ones as merged.
            for older in pending[:-1]:
                DONE.mkdir(parents=True, exist_ok=True)
                older.rename(DONE / older.name)
                logger.info("Merged duplicate request %s", older.name)
            process(pending[-1], args.restart_model)
        elif args.once:
            logger.info("No pending retraining requests")
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
