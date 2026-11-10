"""Thin REST client for the MLflow model server (``mlflow models serve``)."""

import json
import time

import requests

from .config import settings
from .metrics import MODEL_LATENCY


class ModelServiceError(RuntimeError):
    """The model server is unreachable or returned an error."""


def predict(lines: list[str]) -> list[dict]:
    """Send raw log lines to the model server and return one dict per session.

    Raises:
        ModelServiceError: On connection errors, timeouts or non-200 responses.
    """
    payload = {"dataframe_split": {"columns": ["log"], "data": [[line] for line in lines]}}
    start = time.perf_counter()
    try:
        response = requests.post(settings.model_url, json=payload, timeout=settings.model_timeout_s)
    except requests.RequestException as exc:
        raise ModelServiceError(f"Model server not reachable: {exc}") from exc
    finally:
        MODEL_LATENCY.observe(time.perf_counter() - start)

    if response.status_code != 200:
        detail = response.text[:300]
        raise ModelServiceError(f"Model server returned {response.status_code}: {detail}")
    predictions = response.json().get("predictions", [])
    for row in predictions:
        if isinstance(row.get("top_events"), str):
            row["top_events"] = json.loads(row["top_events"])
    return predictions


def ping() -> bool:
    """Return True if the model server answers its /ping health endpoint."""
    try:
        return requests.get(settings.model_ping_url, timeout=3).status_code == 200
    except requests.RequestException:
        return False
