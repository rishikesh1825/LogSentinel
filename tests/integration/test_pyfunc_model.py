"""Integration test: the packaged MLflow pyfunc model scores raw log lines end to end."""

import json
from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.model
MODEL_DIR = Path("models/pyfunc")


@pytest.fixture(scope="module")
def model():
    """Load the locally saved pyfunc model (created by the package stage)."""
    if not MODEL_DIR.exists():
        pytest.skip("models/pyfunc missing - run `dvc repro package` first")
    mlflow_pyfunc = pytest.importorskip("mlflow.pyfunc")
    return mlflow_pyfunc.load_model(str(MODEL_DIR))


def test_model_scores_sample_lines(model, sample_log):
    """TC-37: raw lines in -> one row per block with score, verdict and explanation."""
    lines = sample_log.read_text().splitlines()[:300]
    out = model.predict(pd.DataFrame({"log": lines}))
    assert len(out) > 0
    assert {"block_id", "score", "is_anomaly", "top_events"} <= set(out.columns)
    assert out["block_id"].is_unique
    assert all(isinstance(json.loads(e), list) for e in out["top_events"])


def test_model_handles_garbage_input(model):
    """TC-38: input with no HDFS lines returns an empty result instead of crashing."""
    out = model.predict(pd.DataFrame({"log": ["hello", "world"]}))
    assert len(out) == 0


def test_model_flags_unseen_events(model):
    """TC-39: a message type never seen in training is counted as unseen (drift signal)."""
    line = "081109 203518 1 INFO dfs.DataNode: Totally new event about blk_123 happened"
    out = model.predict(pd.DataFrame({"log": [line]}))
    assert int(out.iloc[0]["n_unseen"]) == 1
