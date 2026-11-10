"""Stage 8 - Train, tune and evaluate the anomaly detectors with MLflow tracking.

Two models are compared on the same features:
  * Logistic Regression (supervised)   - log1p -> StandardScaler -> LogisticRegression
  * Isolation Forest   (unsupervised)  - trained on normal training blocks only

For each model a small hyper-parameter grid is searched; every configuration is a
nested MLflow run scored on the VALIDATION set (PR-AUC). The decision threshold is
chosen on validation (max F1). The TEST set is used exactly once, for the final
numbers. Because only ~3% of blocks are anomalous, we report precision, recall,
F1 and PR-AUC - never accuracy.

Besides sklearn autolog, the parent run records: git commit + dirty flag, DVC
data hash, full params, per-split dataset sizes, training times, threshold,
confusion matrix, PR curves, top coefficients with template text, params.yaml
and dvc.lock.

Usage:
    python -m src.training.train
Requires an MLflow tracking server (MLFLOW_TRACKING_URI, default http://localhost:5000).
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib
import numpy as np
import pandas as pd
import yaml

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import mlflow  # noqa: E402
from mlflow.exceptions import MlflowException  # noqa: E402
from sklearn.ensemble import IsolationForest  # noqa: E402
from sklearn.linear_model import LogisticRegression  # noqa: E402
from sklearn.metrics import (  # noqa: E402
    ConfusionMatrixDisplay,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline  # noqa: E402
from sklearn.preprocessing import FunctionTransformer, StandardScaler  # noqa: E402

from src.features.vectorizer import EventCountVectorizer  # noqa: E402
from src.utils.config import load_params  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402

logger = get_logger("stage.train")


# ---------------------------------------------------------------- helpers
def load_split(features_dir: Path, split: str) -> tuple[np.ndarray, np.ndarray]:
    """Load X and y for one split from ``<split>.npz``."""
    data = np.load(features_dir / f"{split}.npz", allow_pickle=False)
    return data["X"], data["y"]


def git_info() -> dict:
    """Return the current git commit and whether the working tree is dirty."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
            ).stdout.strip()
        )
    except subprocess.CalledProcessError, FileNotFoundError:
        return {"git_commit": "unknown", "git_dirty": "unknown"}
    return {"git_commit": commit, "git_dirty": str(dirty)}


def dvc_data_hash(stage: str = "sessionize") -> str:
    """Return the DVC md5 of the given stage's first output (from dvc.lock)."""
    lock = Path("dvc.lock")
    if not lock.exists():
        return "unknown"
    stages = yaml.safe_load(lock.read_text()).get("stages", {})
    outs = stages.get(stage, {}).get("outs", [])
    return outs[0].get("md5", "unknown") if outs else "unknown"


def best_f1_threshold(y_true: np.ndarray, scores: np.ndarray) -> float:
    """Return the score threshold that maximises F1 on the given data."""
    precision, recall, thresholds = precision_recall_curve(y_true, scores)
    f1 = 2 * precision[:-1] * recall[:-1] / np.clip(precision[:-1] + recall[:-1], 1e-12, None)
    return float(thresholds[int(np.argmax(f1))])


def evaluate(y_true: np.ndarray, scores: np.ndarray, threshold: float) -> dict:
    """Return classification metrics for scores at a threshold."""
    pred = (scores >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    return {
        "precision": round(float(precision_score(y_true, pred, zero_division=0)), 5),
        "recall": round(float(recall_score(y_true, pred, zero_division=0)), 5),
        "f1": round(float(f1_score(y_true, pred, zero_division=0)), 5),
        "pr_auc": round(float(average_precision_score(y_true, scores)), 5),
        "roc_auc": round(float(roc_auc_score(y_true, scores)), 5),
        "tp": int(tp),
        "fp": int(fp),
        "fn": int(fn),
        "tn": int(tn),
    }


def build_lr(C: float, class_weight: str | None, max_iter: int, seed: int) -> Pipeline:
    """log1p -> standardise -> logistic regression."""
    return Pipeline(
        [
            ("log1p", FunctionTransformer(np.log1p, feature_names_out="one-to-one")),
            ("scale", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    C=C, class_weight=class_weight, max_iter=max_iter, random_state=seed
                ),
            ),
        ]
    )


def lr_scores(model: Pipeline, X: np.ndarray) -> np.ndarray:
    """Anomaly probability from the logistic regression pipeline."""
    return model.predict_proba(X)[:, 1]


def if_scores(model: IsolationForest, X: np.ndarray) -> np.ndarray:
    """Anomaly score from Isolation Forest (higher = more anomalous)."""
    return -model.score_samples(X)


def log_metrics(prefix: str, metrics: dict) -> None:
    """Log every numeric metric under ``<prefix>_<name>``."""
    mlflow.log_metrics({f"{prefix}_{k}": float(v) for k, v in metrics.items()})


# ------------------------------------------------------------- tuning
def tune_lr(cfg: dict, seed: int, data: dict) -> tuple[Pipeline, dict]:
    """Grid-search C on validation PR-AUC. Each config is a nested MLflow run."""
    best = (None, {"pr_auc": -1.0}, None)
    for C in cfg["C"]:
        with mlflow.start_run(run_name=f"lr_C={C}", nested=True):
            start = time.perf_counter()
            model = build_lr(C, cfg["class_weight"], cfg["max_iter"], seed)
            model.fit(data["X_train"], data["y_train"])
            fit_seconds = time.perf_counter() - start
            val_scores = lr_scores(model, data["X_val"])
            threshold = best_f1_threshold(data["y_val"], val_scores)
            val = evaluate(data["y_val"], val_scores, threshold)
            mlflow.log_params({"model": "lr", "C": C, "class_weight": cfg["class_weight"]})
            log_metrics("val", val)
            mlflow.log_metrics({"fit_seconds": fit_seconds, "threshold": threshold})
            logger.info("LR C=%s -> val PR-AUC=%.4f F1=%.4f", C, val["pr_auc"], val["f1"])
            if val["pr_auc"] > best[1]["pr_auc"]:
                best = (model, val, {"C": C, "threshold": threshold, "fit_seconds": fit_seconds})
    return best[0], {"val": best[1], "best_params": best[2]}


def tune_iforest(cfg: dict, seed: int, data: dict) -> tuple[IsolationForest, dict]:
    """Grid-search n_estimators x max_samples on validation PR-AUC."""
    X_fit = data["X_train"]
    if cfg["train_on_normal_only"]:
        X_fit = X_fit[data["y_train"] == 0]
    best = (None, {"pr_auc": -1.0}, None)
    for n_estimators in cfg["n_estimators"]:
        for max_samples in cfg["max_samples"]:
            name = f"iforest_n={n_estimators}_s={max_samples}"
            with mlflow.start_run(run_name=name, nested=True):
                start = time.perf_counter()
                model = IsolationForest(
                    n_estimators=n_estimators,
                    max_samples=max_samples,
                    contamination="auto",
                    random_state=seed,
                    n_jobs=-1,
                ).fit(X_fit)
                fit_seconds = time.perf_counter() - start
                val_scores = if_scores(model, data["X_val"])
                threshold = best_f1_threshold(data["y_val"], val_scores)
                val = evaluate(data["y_val"], val_scores, threshold)
                mlflow.log_params(
                    {"model": "iforest", "n_estimators": n_estimators, "max_samples": max_samples}
                )
                log_metrics("val", val)
                mlflow.log_metrics({"fit_seconds": fit_seconds, "threshold": threshold})
                logger.info("%s -> val PR-AUC=%.4f F1=%.4f", name, val["pr_auc"], val["f1"])
                if val["pr_auc"] > best[1]["pr_auc"]:
                    best = (
                        model,
                        val,
                        {
                            "n_estimators": n_estimators,
                            "max_samples": max_samples,
                            "threshold": threshold,
                            "fit_seconds": fit_seconds,
                        },
                    )
    return best[0], {"val": best[1], "best_params": best[2]}


# ------------------------------------------------------------- plots
def plot_pr_curves(y_test: np.ndarray, curves: dict, path: Path) -> None:
    """Precision-recall curves of both models on the test set."""
    fig, ax = plt.subplots(figsize=(6.5, 5))
    for name, scores in curves.items():
        precision, recall, _ = precision_recall_curve(y_test, scores)
        ap = average_precision_score(y_test, scores)
        ax.plot(recall, precision, label=f"{name} (PR-AUC={ap:.3f})")
    ax.axhline(y_test.mean(), color="grey", linestyle="--", label="random baseline")
    ax.set(xlabel="Recall", ylabel="Precision", title="Precision-Recall on test set")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_confusion(y_test: np.ndarray, pred: np.ndarray, title: str, path: Path) -> None:
    """Confusion matrix of the served model on the test set."""
    fig, ax = plt.subplots(figsize=(5, 4.5))
    ConfusionMatrixDisplay.from_predictions(
        y_test, pred, display_labels=["Normal", "Anomaly"], ax=ax, colorbar=False
    )
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def coefficient_table(
    model: Pipeline, vectorizer: EventCountVectorizer, templates: dict
) -> pd.DataFrame:
    """Logistic regression coefficients with the human-readable template text."""
    coefs = model.named_steps["clf"].coef_[0]
    rows = []
    for name, coef in zip(vectorizer.feature_names, coefs, strict=True):
        template_id = int(name[1:]) if name.startswith("T") else -1
        text = templates.get(template_id, "Unseen / unknown log event")
        rows.append({"feature": name, "template_id": template_id, "template": text, "coef": coef})
    return pd.DataFrame(rows).sort_values("coef", key=abs, ascending=False)


def plot_coefficients(table: pd.DataFrame, path: Path, top: int = 15) -> None:
    """Horizontal bar chart of the largest |coefficients|."""
    data = table.head(top).iloc[::-1]
    labels = [
        f"T{t}: {s[:55]}" if t >= 0 else s
        for t, s in zip(data.template_id, data.template, strict=True)
    ]
    colors = ["#DD5555" if c > 0 else "#4C72B0" for c in data.coef]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(labels, data.coef, color=colors)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set(xlabel="Coefficient (red = pushes towards anomaly)", title="Top LR coefficients")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------- main
def main() -> int:
    """Run training and return a process exit code."""
    params = load_params()
    cfg = params["train"]
    seed = cfg["seed"]
    features_dir = Path(params["featurize"]["output_dir"])
    model_dir, report_path = Path(cfg["model_dir"]), Path(cfg["report"])
    fig_dir = Path(cfg["figures_dir"])
    model_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)

    data = {}
    for split in ("train", "val", "test"):
        data[f"X_{split}"], data[f"y_{split}"] = load_split(features_dir, split)
    vectorizer = EventCountVectorizer.load(Path(params["featurize"]["vectorizer"]))
    templates_df = pd.read_csv(params["templates"]["templates_csv"])
    templates = dict(zip(templates_df["cluster_id"], templates_df["template"], strict=True))

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    try:
        mlflow.set_experiment(cfg["experiment"])
    except MlflowException as exc:
        logger.error("Cannot reach MLflow at %s - is the server running? (%s)", tracking_uri, exc)
        return 1
    mlflow.sklearn.autolog(log_models=False, log_datasets=False, silent=True)

    info = git_info()
    with mlflow.start_run(run_name="train") as run:
        mlflow.set_tags(
            {
                **info,
                "dvc_sessions_md5": dvc_data_hash(),
                "stage": "train",
                "project": "LogSentinel",
            }
        )
        mlflow.log_params(
            {
                "seed": seed,
                "serve_model": cfg["serve_model"],
                "n_features": len(vectorizer.feature_names),
                "train_rows": len(data["y_train"]),
                "val_rows": len(data["y_val"]),
                "test_rows": len(data["y_test"]),
                "train_anomaly_ratio": round(float(data["y_train"].mean()), 5),
                "drain3_sim_th": params["templates"]["sim_th"],
                "featurize_use_tfidf": params["featurize"]["use_tfidf"],
            }
        )

        lr_model, lr_result = tune_lr(cfg["lr"], seed, data)
        if_model, if_result = tune_iforest(cfg["iforest"], seed, data)

        # Final evaluation on the untouched test set (threshold chosen on validation).
        lr_test_scores = lr_scores(lr_model, data["X_test"])
        if_test_scores = if_scores(if_model, data["X_test"])
        lr_result["test"] = evaluate(
            data["y_test"], lr_test_scores, lr_result["best_params"]["threshold"]
        )
        if_result["test"] = evaluate(
            data["y_test"], if_test_scores, if_result["best_params"]["threshold"]
        )
        for name, result in (("lr", lr_result), ("iforest", if_result)):
            log_metrics(f"{name}_val", result["val"])
            log_metrics(f"{name}_test", result["test"])
            mlflow.log_params({f"{name}_{k}": v for k, v in result["best_params"].items()})
            logger.info("%s TEST: %s", name, result["test"])

        served = cfg["serve_model"]
        served_model = lr_model if served == "lr" else if_model
        served_result = lr_result if served == "lr" else if_result
        threshold = served_result["best_params"]["threshold"]
        served_scores = lr_test_scores if served == "lr" else if_test_scores

        # Artifacts: plots, coefficient table, config snapshots.
        plot_pr_curves(
            data["y_test"],
            {"Logistic Regression": lr_test_scores, "Isolation Forest": if_test_scores},
            fig_dir / "pr_curves_test.png",
        )
        plot_confusion(
            data["y_test"],
            (served_scores >= threshold).astype(int),
            f"{served} on test (threshold={threshold:.3f})",
            fig_dir / "confusion_matrix_test.png",
        )
        coef_table = coefficient_table(lr_model, vectorizer, templates)
        coef_table.to_csv(fig_dir / "lr_coefficients.csv", index=False)
        plot_coefficients(coef_table, fig_dir / "lr_top_coefficients.png")
        for artifact in fig_dir.iterdir():
            mlflow.log_artifact(str(artifact), artifact_path="evaluation")
        for snapshot in ("params.yaml", "dvc.lock"):
            if Path(snapshot).exists():
                mlflow.log_artifact(snapshot, artifact_path="config")

        # Local model artifacts used by the package stage.
        joblib.dump(served_model, model_dir / "classifier.joblib")
        meta = {
            "model_type": served,
            "threshold": threshold,
            "feature_names": vectorizer.feature_names,
            "drain3": {
                "sim_th": params["templates"]["sim_th"],
                "depth": params["templates"]["depth"],
                "max_children": params["templates"]["max_children"],
            },
            "metrics": {"val": served_result["val"], "test": served_result["test"]},
            "mlflow_run_id": run.info.run_id,
            "git_commit": info["git_commit"],
            "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "top_k": cfg["top_k_events"],
        }
        (model_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
        mlflow.log_artifact(str(model_dir / "meta.json"), artifact_path="model_meta")

    report = {
        "served_model": served,
        "threshold": threshold,
        "mlflow_run_id": run.info.run_id,
        "lr": lr_result,
        "iforest": if_result,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    logger.info("Training done. MLflow run %s; served=%s", run.info.run_id, served)
    return 0


if __name__ == "__main__":
    sys.exit(main())
