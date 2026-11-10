"""Stage 7 - Feature engineering: per-block template counts -> fixed-length vectors.

The vectorizer vocabulary is fitted on TRAINING sessions only and then applied
to train, validation and test, so no information leaks from val/test.

Outputs:
    data/processed/features/{train,val,test}.npz   (X, y, block_ids)
    models/features/vectorizer.json                (vocabulary, block frequencies)
    reports/featurize_stats.json                   (DVC metric)

Usage:
    python -m src.features.featurize
"""

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.features.vectorizer import EventCountVectorizer
from src.utils.config import load_params
from src.utils.logger import get_logger

logger = get_logger("stage.featurize")
SPLITS = ("train", "val", "test")


def to_count_map(value) -> dict[int, int]:
    """Convert a Parquet map cell (dict or list of (key, value) pairs) to a dict."""
    if isinstance(value, dict):
        return {int(k): int(v) for k, v in value.items()}
    return {int(k): int(v) for k, v in value}


def load_sessions(path: Path) -> pd.DataFrame:
    """Read the sessions Parquet with the columns needed for features."""
    sessions = pd.read_parquet(path, columns=["block_id", "label", "split", "counts"])
    sessions["counts"] = sessions["counts"].map(to_count_map)
    return sessions


def main() -> int:
    """Run the featurize stage and return a process exit code."""
    params = load_params()
    cfg = params["featurize"]
    out_dir = Path(cfg["output_dir"])
    vec_path, report_path = Path(cfg["vectorizer"]), Path(cfg["report"])

    try:
        start = time.perf_counter()
        sessions = load_sessions(Path(params["sessionize"]["output"]))
        logger.info("Loaded %d sessions", len(sessions))

        train = sessions[sessions["split"] == "train"]
        vectorizer = EventCountVectorizer(cfg["min_block_freq"], cfg["use_tfidf"])
        vectorizer.fit(train["counts"])
        vectorizer.save(vec_path)
        logger.info("Vocabulary: %d templates + __unseen__", len(vectorizer.template_ids))

        out_dir.mkdir(parents=True, exist_ok=True)
        stats = {"n_features": len(vectorizer.feature_names), "splits": {}}
        for split in SPLITS:
            part = sessions[sessions["split"] == split]
            features = vectorizer.transform(part["counts"])
            labels = part["label"].to_numpy(dtype=np.int8)
            np.savez_compressed(
                out_dir / f"{split}.npz",
                X=features,
                y=labels,
                block_ids=part["block_id"].to_numpy(dtype=str),
            )
            stats["splits"][split] = {
                "rows": int(features.shape[0]),
                "anomalies": int(labels.sum()),
                "avg_nonzero_features": round(float((features > 0).sum(axis=1).mean()), 2),
                "rows_with_unseen": int((features[:, vectorizer.unseen_index] > 0).sum()),
            }
        stats["featurize_seconds"] = round(time.perf_counter() - start, 2)
    except FileNotFoundError, KeyError, ValueError:
        logger.exception("Featurize stage failed")
        return 1

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stats, indent=2) + "\n")
    logger.info("Featurize stats: %s", stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
