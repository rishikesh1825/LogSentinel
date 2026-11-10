"""Unit tests for the FastAPI backend (model server mocked)."""

import pytest
from fastapi.testclient import TestClient

from backend.app import main
from backend.app.config import settings
from backend.app.model_client import ModelServiceError

HEADERS = {"X-API-Key": settings.api_key}

FAKE_PREDICTIONS = [
    {
        "block_id": "blk_1",
        "n_lines": 3,
        "n_unseen": 0,
        "score": 0.02,
        "risk": 0.02,
        "threshold": 0.5,
        "is_anomaly": False,
        "top_events": [],
    },
    {
        "block_id": "blk_2",
        "n_lines": 2,
        "n_unseen": 1,
        "score": 0.97,
        "risk": 0.97,
        "threshold": 0.5,
        "is_anomaly": True,
        "top_events": [
            {"template_id": 33, "template": "Got exception", "count": 1, "contribution": 2.1}
        ],
    },
]


def fake_predict(lines):
    """Stand-in for the model server."""
    return [dict(p) for p in FAKE_PREDICTIONS]


@pytest.fixture
def client(monkeypatch, tmp_path):
    """Test client with a fake model server and temp folders."""
    monkeypatch.setattr(main.model_client, "predict", fake_predict)
    monkeypatch.setattr(main.model_client, "ping", lambda: True)
    monkeypatch.setattr(settings, "feedback_file", tmp_path / "feedback.jsonl")
    monkeypatch.setattr(settings, "retrain_dir", tmp_path / "retrain")
    return TestClient(main.app)


def post_logs(client, lines, headers=HEADERS):
    """POST the given lines to /api/v1/predict."""
    return client.post("/api/v1/predict", json={"logs": "\n".join(lines)}, headers=headers)


def test_health(client):
    """TC-24: liveness endpoint answers 200."""
    assert client.get("/health").json() == {"status": "ok"}


def test_predict_requires_api_key(client, normal_lines):
    """TC-25: requests without the API key are rejected with 401."""
    assert post_logs(client, normal_lines, headers={}).status_code == 401


def test_predict_rejects_empty_input(client):
    """TC-26: empty text is rejected with 422 and a readable message."""
    response = client.post("/api/v1/predict", json={"logs": "   \n  "}, headers=HEADERS)
    assert response.status_code == 422
    assert "error" in response.json()


def test_predict_rejects_non_hdfs_text(client):
    """TC-27: text that is not HDFS logs gets 422 with an example of the expected format."""
    response = post_logs(client, ["hello", "world"])
    assert response.status_code == 422
    assert "Expected a line like" in response.json()["error"]


def test_predict_success_sorted_by_score(client, normal_lines):
    """TC-28: valid logs -> sessions sorted by score with a correct summary."""
    response = post_logs(client, normal_lines + ["not a log line"])
    assert response.status_code == 200
    body = response.json()
    assert [s["block_id"] for s in body["sessions"]] == ["blk_2", "blk_1"]
    assert body["summary"]["anomalies"] == 1
    assert body["summary"]["skipped_lines"] == 1
    assert body["summary"]["unseen_lines"] == 1


def test_predict_file_upload(client, normal_lines):
    """TC-29: uploading a log file works like pasting."""
    files = {"file": ("x.log", "\n".join(normal_lines).encode(), "text/plain")}
    response = client.post("/api/v1/predict/file", files=files, headers=HEADERS)
    assert response.status_code == 200


def test_model_down_returns_503(client, monkeypatch, normal_lines):
    """TC-30: if the model server fails, the API answers 503 with a friendly message."""

    def boom(lines):
        raise ModelServiceError("connection refused")

    monkeypatch.setattr(main.model_client, "predict", boom)
    response = post_logs(client, normal_lines)
    assert response.status_code == 503
    assert "not available" in response.json()["error"]


def test_too_many_lines_returns_413(client, monkeypatch, normal_lines):
    """TC-31: inputs above the line limit are rejected with 413."""
    monkeypatch.setattr(settings, "max_lines", 2)
    assert post_logs(client, normal_lines).status_code == 413


def test_metrics_endpoint_exposes_counters(client, normal_lines):
    """TC-32: /metrics exposes LogSentinel's Prometheus metrics."""
    post_logs(client, normal_lines)
    text = client.get("/metrics").text
    assert "logsentinel_http_requests_total" in text
    assert "logsentinel_lines_processed_total" in text


def test_feedback_is_saved(client):
    """TC-33: feedback is appended to the feedback file."""
    body = {"block_id": "blk_-5", "correct_label": "Anomaly", "comment": "missed"}
    assert client.post("/api/v1/feedback", json=body, headers=HEADERS).status_code == 200
    assert "blk_-5" in settings.feedback_file.read_text()


def test_alert_webhook_queues_retraining(client):
    """TC-34: a firing drift alert (with the right token) queues a retraining request."""
    alert = {"alertname": "UnseenTemplateDrift", "action": "retrain"}
    payload = {"alerts": [{"status": "firing", "labels": alert}]}
    assert client.post("/api/v1/webhooks/alertmanager", json=payload).status_code == 401
    token = {"Authorization": f"Bearer {settings.alert_webhook_token}"}
    ok = client.post("/api/v1/webhooks/alertmanager", json=payload, headers=token)
    assert ok.json()["status"] == "queued"
    assert len(list(settings.retrain_dir.glob("retrain_*.json"))) == 1
