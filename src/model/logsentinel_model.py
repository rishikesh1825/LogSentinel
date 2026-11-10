"""LogSentinel MLflow pyfunc model: raw HDFS log lines in, per-session verdicts out.

Everything needed to go from raw text to a prediction is packaged together, so
parsing logic and model can never drift apart:

    drain3 state (templates) -> template matching -> per-block counts
    -> EventCountVectorizer -> classifier -> score, verdict, explanation

Input : DataFrame with one column ``log`` (one raw log line per row).
Output: DataFrame with one row per block (session) found in the input:
    block_id, n_lines, n_unseen, score, risk, threshold, is_anomaly, top_events
    (``top_events`` is a JSON string: [{template_id, template, count, contribution}])
"""

import json
import math
import re
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path

import joblib
import mlflow.pyfunc
import numpy as np
import pandas as pd
from drain3 import TemplateMiner
from drain3.memory_buffer_persistence import MemoryBufferPersistence

from src.features.vectorizer import UNSEEN_FEATURE, EventCountVectorizer
from src.parsing.drain_config import build_config

# Same formats as the Spark parse stage.
LINE_RE = re.compile(r"^(\d{6}) (\d{6}) (\d+) (\w+) ([^:]+): (.*)$")
BLOCK_RE = re.compile(r"blk_-?\d+")
UNSEEN = -1
UNSEEN_TEXT = "Unseen / unknown log event (not in the training templates)"
OUTPUT_COLUMNS = [
    "block_id",
    "n_lines",
    "n_unseen",
    "score",
    "risk",
    "threshold",
    "is_anomaly",
    "top_events",
]


def parse_content(line: str) -> str | None:
    """Return the message part of an HDFS log line, or None if malformed."""
    match = LINE_RE.match(line.strip())
    return match.group(6) if match else None


def extract_blocks(content: str) -> list[str]:
    """Return the distinct block IDs mentioned in a message, in order of appearance."""
    return list(dict.fromkeys(BLOCK_RE.findall(content)))


def sessionize_lines(
    lines: Iterable[str], match_fn: Callable[[str], int]
) -> tuple[dict[str, list[int]], int]:
    """Group lines into per-block template sequences.

    Args:
        lines: Raw log lines.
        match_fn: Maps a message to a template ID (UNSEEN when nothing matches).

    Returns:
        (sessions, skipped) - ``{block_id: [template_id, ...]}`` in input order,
        and the number of lines skipped (malformed or without a block ID).
    """
    sessions: dict[str, list[int]] = {}
    skipped = 0
    for line in lines:
        content = parse_content(line)
        blocks = extract_blocks(content) if content else []
        if not blocks:
            skipped += 1
            continue
        template_id = match_fn(content)
        for block in blocks:
            sessions.setdefault(block, []).append(template_id)
    return sessions, skipped


class LogSentinelModel(mlflow.pyfunc.PythonModel):
    """pyfunc wrapper bundling drain3 templates, vectorizer and classifier."""

    def load_context(self, context):
        """Load all artifacts once when the model server starts."""
        art = context.artifacts
        self.meta = json.loads(Path(art["meta"]).read_text())
        self.vectorizer = EventCountVectorizer.load(Path(art["vectorizer"]))
        self.classifier = joblib.load(art["classifier"])
        templates = pd.read_csv(art["templates"])
        self.templates = dict(zip(templates["cluster_id"], templates["template"], strict=True))

        persistence = MemoryBufferPersistence()
        persistence.state = Path(art["drain3_state"]).read_bytes()
        self.miner = TemplateMiner(persistence, config=build_config(**self.meta["drain3"]))

        n = max(self.vectorizer.n_train_blocks, 1)
        # Rarity of each feature in training blocks (used to explain Isolation Forest).
        self.rarity = np.array(
            [-math.log((f + 1) / (n + 1)) for f in self.vectorizer.block_freq]
            + [-math.log(1 / (n + 1))],
            dtype=np.float32,
        )

    def _match(self, content: str) -> int:
        cluster = self.miner.match(content, full_search_strategy="fallback")
        return cluster.cluster_id if cluster else UNSEEN

    def _scores(self, features: np.ndarray) -> np.ndarray:
        if self.meta["model_type"] == "lr":
            return self.classifier.predict_proba(features)[:, 1]
        return -self.classifier.score_samples(features)

    def _risk(self, score: float) -> float:
        """Score mapped to 0..1 for display (LR scores already are probabilities)."""
        if self.meta["model_type"] == "lr":
            return float(score)
        return float(min(max(score / (2 * self.meta["threshold"]), 0.0), 1.0))

    def _contributions(self, features: np.ndarray) -> np.ndarray:
        """Per-feature contribution towards 'anomaly' for each row."""
        if self.meta["model_type"] == "lr":
            scaled = self.classifier[:-1].transform(features)  # log1p + standardise
            return scaled * self.classifier[-1].coef_[0]
        return (features > 0) * self.rarity  # IF: rare events explain the verdict

    def _explain(self, row: np.ndarray, contrib: np.ndarray) -> list[dict]:
        present = np.flatnonzero(row > 0)
        order = present[np.argsort(-contrib[present])][: self.meta.get("top_k", 5)]
        events = []
        for idx in order:
            name = self.vectorizer.feature_names[idx]
            template_id = UNSEEN if name == UNSEEN_FEATURE else int(name[1:])
            events.append(
                {
                    "template_id": template_id,
                    "template": self.templates.get(template_id, UNSEEN_TEXT),
                    "count": int(row[idx]),
                    "contribution": round(float(contrib[idx]), 4),
                }
            )
        return events

    def predict(self, context, model_input, params=None):
        """Score every block found in the input lines."""
        if isinstance(model_input, pd.DataFrame):
            lines = model_input["log"].astype(str).tolist()
        else:
            lines = [str(x) for x in model_input]

        sessions, _skipped = sessionize_lines(lines, self._match)
        if not sessions:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        block_ids = list(sessions)
        count_maps = [Counter(sessions[b]) for b in block_ids]
        features = self.vectorizer.transform(count_maps)
        scores = self._scores(features)
        contrib = self._contributions(features)
        threshold = float(self.meta["threshold"])

        rows = []
        for i, block in enumerate(block_ids):
            rows.append(
                {
                    "block_id": block,
                    "n_lines": len(sessions[block]),
                    "n_unseen": int(sum(1 for t in sessions[block] if t == UNSEEN)),
                    "score": round(float(scores[i]), 6),
                    "risk": round(self._risk(scores[i]), 4),
                    "threshold": threshold,
                    "is_anomaly": bool(scores[i] >= threshold),
                    "top_events": json.dumps(self._explain(features[i], contrib[i])),
                }
            )
        return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
