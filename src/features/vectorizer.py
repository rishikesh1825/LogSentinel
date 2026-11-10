"""Event-count vectorizer: per-block template-count maps -> fixed-length vectors.

The vocabulary (one column per template) is learned from TRAINING blocks only.
Any template that is not in the vocabulary, including UNSEEN (-1), is counted
in a final ``__unseen__`` column, so new log events at inference time are not
silently dropped - they become a feature and a drift signal.

The same class is used by the featurize stage and inside the served MLflow
model, so training-time and serving-time features are always identical.
"""

import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path

import numpy as np

UNSEEN_FEATURE = "__unseen__"


class EventCountVectorizer:
    """Turn ``{template_id: count}`` maps into dense count (or TF-IDF) vectors."""

    def __init__(self, min_block_freq: int = 1, use_tfidf: bool = False) -> None:
        """Create an unfitted vectorizer.

        Args:
            min_block_freq: A template must appear in at least this many training
                blocks to get its own column (rarer ones go to ``__unseen__``).
            use_tfidf: If True, weight counts by inverse block frequency.
        """
        self.min_block_freq = min_block_freq
        self.use_tfidf = use_tfidf
        self.template_ids: list[int] = []
        self.block_freq: list[int] = []
        self.n_train_blocks: int = 0
        self.idf: list[float] | None = None

    def fit(self, count_maps: Iterable[Mapping[int, int]]) -> "EventCountVectorizer":
        """Learn the template vocabulary (and IDF weights) from training blocks."""
        doc_freq: Counter = Counter()
        n_blocks = 0
        for counts in count_maps:
            n_blocks += 1
            for template_id in counts:
                if int(template_id) >= 0:  # never give UNSEEN (-1) its own column
                    doc_freq[int(template_id)] += 1

        self.template_ids = sorted(t for t, c in doc_freq.items() if c >= self.min_block_freq)
        self.block_freq = [doc_freq[t] for t in self.template_ids]
        self.n_train_blocks = n_blocks
        if self.use_tfidf:
            self.idf = [math.log((1 + n_blocks) / (1 + df)) + 1.0 for df in self.block_freq]
            self.idf.append(1.0)  # weight for the __unseen__ column
        return self

    @property
    def feature_names(self) -> list[str]:
        """Column names: ``T<id>`` per template, then ``__unseen__``."""
        return [f"T{t}" for t in self.template_ids] + [UNSEEN_FEATURE]

    @property
    def unseen_index(self) -> int:
        """Column index of the ``__unseen__`` feature."""
        return len(self.template_ids)

    def transform(self, count_maps: Iterable[Mapping[int, int]]) -> np.ndarray:
        """Return a float32 matrix with one row per block.

        Raises:
            RuntimeError: If the vectorizer has not been fitted.
        """
        if not self.template_ids:
            raise RuntimeError("Vectorizer is not fitted - call fit() first")
        index = {t: i for i, t in enumerate(self.template_ids)}
        rows = list(count_maps)
        matrix = np.zeros((len(rows), len(self.template_ids) + 1), dtype=np.float32)
        for r, counts in enumerate(rows):
            for template_id, count in counts.items():
                matrix[r, index.get(int(template_id), self.unseen_index)] += count
        if self.idf is not None:
            matrix *= np.asarray(self.idf, dtype=np.float32)
        return matrix

    def fit_transform(self, count_maps: Iterable[Mapping[int, int]]) -> np.ndarray:
        """Fit on ``count_maps`` and return their matrix."""
        rows = list(count_maps)
        return self.fit(rows).transform(rows)

    def to_dict(self) -> dict:
        """Return a JSON-serialisable representation."""
        return {
            "min_block_freq": self.min_block_freq,
            "use_tfidf": self.use_tfidf,
            "template_ids": self.template_ids,
            "block_freq": self.block_freq,
            "n_train_blocks": self.n_train_blocks,
            "idf": self.idf,
            "feature_names": self.feature_names,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "EventCountVectorizer":
        """Rebuild a fitted vectorizer from ``to_dict`` output."""
        vec = cls(data["min_block_freq"], data["use_tfidf"])
        vec.template_ids = [int(t) for t in data["template_ids"]]
        vec.block_freq = [int(f) for f in data["block_freq"]]
        vec.n_train_blocks = int(data["n_train_blocks"])
        vec.idf = data["idf"]
        return vec

    def save(self, path: Path) -> None:
        """Write the vectorizer to a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2) + "\n")

    @classmethod
    def load(cls, path: Path) -> "EventCountVectorizer":
        """Load a vectorizer saved with ``save``."""
        return cls.from_dict(json.loads(Path(path).read_text()))
