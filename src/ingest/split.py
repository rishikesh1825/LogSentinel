"""Stage 3 - Stratified train/validation/test split of block IDs.

Splitting happens at block (session) level and before template mining, so no
information from validation/test blocks leaks into drain3 templates, the
vectorizer or the models.

Usage:
    python -m src.ingest.split
"""

import json
import sys
from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split

from src.utils.config import load_params
from src.utils.logger import get_logger

logger = get_logger("stage.split")


def split_blocks(
    labels: pd.DataFrame, train: float, val: float, test: float, seed: int
) -> pd.DataFrame:
    """Assign every block to train/val/test, stratified by its label.

    Args:
        labels: DataFrame with columns BlockId and Label.
        train, val, test: Fractions that must sum to 1.
        seed: Random seed for reproducibility.

    Returns:
        The input rows with an added ``split`` column.

    Raises:
        ValueError: If the fractions do not sum to 1.
    """
    if abs(train + val + test - 1.0) > 1e-9:
        raise ValueError(f"Split fractions must sum to 1, got {train + val + test}")

    train_df, rest = train_test_split(
        labels, train_size=train, stratify=labels["Label"], random_state=seed
    )
    val_df, test_df = train_test_split(
        rest, train_size=val / (val + test), stratify=rest["Label"], random_state=seed
    )
    return pd.concat(
        [train_df.assign(split="train"), val_df.assign(split="val"), test_df.assign(split="test")]
    ).sort_index()


def main() -> int:
    """Run the split stage and return a process exit code."""
    params = load_params()
    cfg = params["split"]
    out_path, report_path = Path(cfg["output"]), Path(cfg["report"])

    try:
        labels = pd.read_csv(params["data"]["labels"])
        splits = split_blocks(labels, cfg["train"], cfg["val"], cfg["test"], cfg["seed"])
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Split stage failed: %s", exc)
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    splits.to_csv(out_path, index=False)

    stats = {}
    for name, group in splits.groupby("split"):
        n_anomaly = int((group["Label"] == "Anomaly").sum())
        stats[name] = {
            "blocks": len(group),
            "anomalies": n_anomaly,
            "anomaly_ratio": round(n_anomaly / len(group), 5),
        }
    stats["seed"] = cfg["seed"]

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stats, indent=2) + "\n")
    logger.info("Split stats: %s", stats)
    logger.info("Wrote %s and %s", out_path, report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
