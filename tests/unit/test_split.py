"""Unit tests for stage 3 - stratified block split (src/ingest/split.py)."""

import pandas as pd
import pytest

from src.ingest.split import split_blocks


@pytest.fixture
def labels() -> pd.DataFrame:
    """1,000 blocks, 3% anomalous - like the real dataset."""
    rows = [(f"blk_{i}", "Anomaly" if i % 100 < 3 else "Normal") for i in range(1000)]
    return pd.DataFrame(rows, columns=["BlockId", "Label"])


def test_split_sizes_and_no_overlap(labels):
    """TC-07: 70/10/20 sizes, every block in exactly one split."""
    out = split_blocks(labels, 0.7, 0.1, 0.2, seed=42)
    counts = out["split"].value_counts()
    assert counts["train"] == 700 and counts["val"] == 100 and counts["test"] == 200
    assert out["BlockId"].is_unique and len(out) == len(labels)


def test_split_is_stratified(labels):
    """TC-08: each split keeps the 3% anomaly ratio."""
    out = split_blocks(labels, 0.7, 0.1, 0.2, seed=42)
    for _, group in out.groupby("split"):
        assert abs((group["Label"] == "Anomaly").mean() - 0.03) < 0.01


def test_split_is_reproducible(labels):
    """TC-09: same seed -> identical split."""
    a = split_blocks(labels, 0.7, 0.1, 0.2, seed=42)
    b = split_blocks(labels, 0.7, 0.1, 0.2, seed=42)
    pd.testing.assert_frame_equal(a, b)


def test_split_rejects_bad_fractions(labels):
    """TC-10: fractions that do not sum to 1 raise ValueError."""
    with pytest.raises(ValueError):
        split_blocks(labels, 0.7, 0.2, 0.2, seed=42)
