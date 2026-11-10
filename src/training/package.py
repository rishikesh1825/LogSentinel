"""Stage 9 - Package the model as ONE MLflow pyfunc model, register and promote it.

The drain3 state, template list, vectorizer, classifier and metadata are bundled
with the LogSentinelModel wrapper (plus the ``src`` code it needs), so parsing
logic and model are always deployed together.

Steps:
  1. Save a local copy to models/pyfunc (used by tests; tracked by DVC).
  2. Log the same model to the training MLflow run and register it as a new
     version of the registered model (default name "LogSentinel").
  3. Promotion: if no "champion" exists, or the new version's test F1 is at
     least the champion's (+ min_improvement), the alias "champion" moves to the
     new version. Otherwise the new version gets the alias "challenger".
     Rolling back = pointing "champion" at an older version.

Usage:
    python -m src.training.package
"""

import json
import os
import shutil
import sys
from pathlib import Path

import mlflow
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from src.model.logsentinel_model import LogSentinelModel
from src.utils.config import load_params
from src.utils.logger import get_logger

logger = get_logger("stage.package")

RUNTIME_PACKAGES = ("mlflow", "drain3", "scikit-learn", "pandas", "numpy", "pyarrow")


def pinned_requirements(path: Path = Path("requirements.txt")) -> list[str]:
    """Return pinned runtime requirements the model needs (from requirements.txt)."""
    wanted = {p.lower() for p in RUNTIME_PACKAGES}
    pins = []
    for line in path.read_text().splitlines():
        name = line.split("==")[0].strip().lower().replace("_", "-")
        if name in wanted:
            pins.append(line.strip())
    return pins


def model_artifacts(params: dict) -> dict[str, str]:
    """Paths of every file the pyfunc model loads in ``load_context``."""
    model_dir = Path(params["train"]["model_dir"])
    return {
        "drain3_state": params["templates"]["state_path"],
        "templates": params["templates"]["templates_csv"],
        "vectorizer": params["featurize"]["vectorizer"],
        "classifier": str(model_dir / "classifier.joblib"),
        "meta": str(model_dir / "meta.json"),
    }


def input_example(sample_log: Path, n: int = 20) -> pd.DataFrame:
    """A few raw log lines used as the model's input example / signature."""
    lines = sample_log.read_text().splitlines()[:n]
    return pd.DataFrame({"log": lines})


def champion_f1(client: MlflowClient, name: str, metric: str) -> tuple[str | None, float | None]:
    """Return (version, test F1) of the current champion, or (None, None)."""
    try:
        version = client.get_model_version_by_alias(name, "champion")
    except MlflowException:
        return None, None
    value = client.get_run(version.run_id).data.metrics.get(metric)
    return version.version, value


def main() -> int:
    """Run the package stage and return a process exit code."""
    params = load_params()
    cfg = params["package"]
    meta = json.loads((Path(params["train"]["model_dir"]) / "meta.json").read_text())
    artifacts = model_artifacts(params)
    example = input_example(Path(params["data"]["sample_log"]))
    requirements = pinned_requirements()
    out_dir, report_path = Path(cfg["output"]), Path(cfg["report"])

    # 1. Local copy (overwrite).
    if out_dir.exists():
        shutil.rmtree(out_dir)
    mlflow.pyfunc.save_model(
        path=str(out_dir),
        python_model=LogSentinelModel(),
        artifacts=artifacts,
        code_paths=["src"],
        pip_requirements=requirements,
        input_example=example,
    )
    logger.info("Saved local pyfunc model to %s", out_dir)

    # 2. Log + register.
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    client = MlflowClient()
    metric = f"{meta['model_type']}_test_f1"
    try:
        prev_version, prev_f1 = champion_f1(client, cfg["registered_name"], metric)
        with mlflow.start_run(run_id=meta["mlflow_run_id"]):
            info = mlflow.pyfunc.log_model(
                name="model",
                python_model=LogSentinelModel(),
                artifacts=artifacts,
                code_paths=["src"],
                pip_requirements=requirements,
                input_example=example,
                registered_model_name=cfg["registered_name"],
            )
        new_version = str(info.registered_model_version)
        client.set_model_version_tag(
            cfg["registered_name"], new_version, "git_commit", meta["git_commit"]
        )
        client.set_model_version_tag(
            cfg["registered_name"], new_version, "model_type", meta["model_type"]
        )

        # 3. Promotion decision.
        new_f1 = float(meta["metrics"]["test"]["f1"])
        promote = prev_f1 is None or new_f1 >= prev_f1 + cfg["min_improvement"]
        alias = "champion" if promote else "challenger"
        client.set_registered_model_alias(cfg["registered_name"], alias, new_version)
    except MlflowException as exc:
        logger.error("MLflow registration failed - is the server running? (%s)", exc)
        return 1

    report = {
        "registered_name": cfg["registered_name"],
        "new_version": new_version,
        "new_test_f1": new_f1,
        "previous_champion_version": prev_version,
        "previous_champion_test_f1": prev_f1,
        "promoted_to_champion": promote,
        "alias": alias,
        "mlflow_run_id": meta["mlflow_run_id"],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    logger.info("Package report: %s", report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
