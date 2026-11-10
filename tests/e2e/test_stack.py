"""End-to-end tests against the running Docker Compose stack.

Run only when the stack is up:   E2E=1 pytest tests/e2e -v
"""

import os
from pathlib import Path

import pytest
import requests
import urllib3

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.getenv("E2E") != "1", reason="set E2E=1 with the stack running"),
]
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # self-signed cert
UI = "https://localhost:8443"
API = "http://localhost:8000"


def api_key() -> str:
    """Read the API key from .env."""
    for line in Path(".env").read_text().splitlines():
        if line.startswith("API_KEY="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("API_KEY not found in .env")


def test_ui_served_over_https():
    """TC-40: the web UI loads over HTTPS and has its runtime config."""
    assert requests.get(UI, verify=False, timeout=10).status_code == 200
    config = requests.get(f"{UI}/config.js", verify=False, timeout=10).text
    assert "API_BASE_URL" in config


def test_backend_ready():
    """TC-41: the backend can reach the model server."""
    assert requests.get(f"{API}/ready", timeout=10).status_code == 200


def test_predict_through_ui_proxy(normal_lines):
    """TC-42: UI origin -> nginx -> backend -> model server returns sessions."""
    response = requests.post(
        f"{UI}/api/v1/predict",
        json={"logs": "\n".join(normal_lines)},
        headers={"X-API-Key": api_key()},
        verify=False,
        timeout=60,
    )
    assert response.status_code == 200
    assert response.json()["summary"]["sessions"] == 1


def test_prometheus_targets_up():
    """TC-43: Prometheus scrapes backend, pushgateway, cadvisor and node exporter."""
    targets = requests.get("http://localhost:9090/api/v1/targets", timeout=10).json()
    health = {t["labels"]["job"]: t["health"] for t in targets["data"]["activeTargets"]}
    for job in ("backend", "pushgateway", "cadvisor", "node"):
        assert health.get(job) == "up", f"{job} is {health.get(job)}"


def test_grafana_dashboard_provisioned():
    """TC-44: the LogSentinel dashboard is provisioned in Grafana."""
    url = "http://localhost:3000/grafana/api/dashboards/uid/logsentinel"
    assert requests.get(url, timeout=10).status_code == 200
