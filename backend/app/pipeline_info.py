"""Read pipeline and experiment state for the pipeline console.

Sources (the repo is mounted read-only into the backend container at REPO_DIR):
  * dvc.yaml                  - stage order and each stage's metrics file
  * reports/*.json            - stage metrics written by the pipeline
  * logs/pipeline_runs.jsonl  - one record per stage run (from src.utils.run_stage)
  * logs/logsentinel.log      - recent ERROR lines
  * MLflow tracking server    - training runs and the registered model
"""

import json
from collections import deque
from pathlib import Path

import yaml
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from .config import settings


def _repo(path: str) -> Path:
    return settings.repo_dir / path


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError, json.JSONDecodeError:
        return None


def run_records(limit: int | None = None) -> list[dict]:
    """Return pipeline run records, newest first."""
    path = _repo("logs/pipeline_runs.jsonl")
    if not path.exists():
        return []
    records = []
    for line in path.read_text().splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    records.reverse()
    return records[:limit] if limit else records


def stage_status() -> list[dict]:
    """Return every DVC stage in pipeline order with its latest run and metrics."""
    dvc_file = _repo("dvc.yaml")
    if not dvc_file.exists():
        return []
    stages = yaml.safe_load(dvc_file.read_text()).get("stages", {})
    latest: dict[str, dict] = {}
    for record in run_records():
        latest.setdefault(record["stage"], record)

    result = []
    for name, spec in stages.items():
        metrics_paths = []
        for entry in spec.get("metrics", []):
            metrics_paths.append(entry if isinstance(entry, str) else next(iter(entry)))
        metrics = {p: _read_json(_repo(p)) for p in metrics_paths}
        last = latest.get(name)
        if last is None:
            status = "never_run" if not any(metrics.values()) else "ok"
        else:
            status = "ok" if last["success"] else "failed"
        result.append({"stage": name, "status": status, "last_run": last, "metrics": metrics})
    return result


def recent_errors(max_lines: int = 50) -> dict:
    """Return failed stage runs, failed MLflow runs and recent ERROR log lines."""
    failed_stages = [r for r in run_records() if not r["success"]][:20]
    log_errors: deque = deque(maxlen=max_lines)
    log_file = _repo("logs/logsentinel.log")
    if log_file.exists():
        with log_file.open(errors="replace") as f:
            for line in f:
                if "| ERROR" in line:
                    log_errors.append(line.rstrip())
    failed_runs = [r for r in mlflow_runs(limit=50) if r["status"] == "FAILED"]
    return {
        "failed_stages": failed_stages,
        "failed_mlflow_runs": failed_runs,
        "log_errors": list(log_errors)[::-1],
    }


def benchmark() -> dict | None:
    """Return the Spark throughput benchmark report."""
    return _read_json(_repo("reports/benchmark.json"))


def _client() -> MlflowClient:
    return MlflowClient(tracking_uri=settings.mlflow_tracking_uri)


def mlflow_runs(limit: int = 20) -> list[dict]:
    """Return the latest parent training runs (nested tuning runs excluded)."""
    try:
        client = _client()
        experiment = client.get_experiment_by_name(settings.experiment)
        if experiment is None:
            return []
        runs = client.search_runs(
            [experiment.experiment_id],
            filter_string="tags.stage = 'train'",
            order_by=["attributes.start_time DESC"],
            max_results=limit,
        )
    except MlflowException, OSError:
        return []
    keys = ("lr_test_f1", "lr_test_pr_auc", "iforest_test_f1", "iforest_test_pr_auc")
    return [
        {
            "run_id": r.info.run_id,
            "run_name": r.info.run_name,
            "status": r.info.status,
            "start_time": r.info.start_time,
            "end_time": r.info.end_time,
            "git_commit": r.data.tags.get("git_commit", "")[:8],
            "metrics": {k: r.data.metrics[k] for k in keys if k in r.data.metrics},
        }
        for r in runs
    ]


def model_info() -> dict:
    """Return the registered model version currently served (alias 'champion')."""
    try:
        client = _client()
        version = client.get_model_version_by_alias(settings.registered_model, settings.model_alias)
        run = client.get_run(version.run_id)
    except (MlflowException, OSError) as exc:
        return {"available": False, "error": str(exc)[:300]}
    return {
        "available": True,
        "name": settings.registered_model,
        "alias": settings.model_alias,
        "version": version.version,
        "run_id": version.run_id,
        "tags": dict(version.tags),
        "metrics": {k: v for k, v in run.data.metrics.items() if "_test_" in k},
        "git_commit": run.data.tags.get("git_commit", ""),
    }
