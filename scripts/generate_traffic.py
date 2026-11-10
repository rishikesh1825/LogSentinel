"""Send realistic traffic to the running stack (for Grafana screenshots and the drift demo).

Usage:
    python scripts/generate_traffic.py                    # 3 minutes of normal traffic
    python scripts/generate_traffic.py --drift            # add never-seen log events (drift)
    python scripts/generate_traffic.py --minutes 5 --url https://localhost:8443
"""

import argparse
import random
import time
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # self-signed cert

# Message types that never occur in HDFS_v1 -> they match no template (unseen).
DRIFT_TEMPLATES = [
    "081110 101010 {pid} WARN dfs.DataNode: Disk quota exceeded while writing blk_{blk} "
    "on volume {n}",
    "081110 101011 {pid} INFO dfs.DataNode$DataXceiver: Checksum mismatch detected for blk_{blk} "
    "replica {n}",
    "081110 101012 {pid} WARN dfs.FSNamesystem: Erasure coding fallback triggered for blk_{blk}",
]


def api_key() -> str:
    """Read API_KEY from .env."""
    for line in Path(".env").read_text().splitlines():
        if line.startswith("API_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("API_KEY not found in .env - run scripts/setup_secrets.sh")


def drift_lines(n: int) -> list[str]:
    """Generate n never-seen log lines."""
    return [
        random.choice(DRIFT_TEMPLATES).format(
            pid=random.randint(1, 999), blk=random.randint(10**17, 10**18), n=random.randint(1, 9)
        )
        for _ in range(n)
    ]


def main() -> None:
    """Post batches of demo log lines every few seconds."""
    parser = argparse.ArgumentParser(description="Generate traffic for LogSentinel.")
    parser.add_argument("--url", default="https://localhost:8443")
    parser.add_argument("--minutes", type=float, default=3)
    parser.add_argument("--drift", action="store_true", help="mix in never-seen log events")
    parser.add_argument("--batch", type=int, default=200, help="lines per request")
    args = parser.parse_args()

    demo = Path("demo/demo_logs.log")
    source = demo if demo.exists() else Path("data/sample/HDFS_2k.log")
    lines = source.read_text().splitlines()
    headers = {"X-API-Key": api_key()}
    args.batch = min(args.batch, len(lines))
    end = time.time() + 60 * args.minutes
    sent = 0
    while time.time() < end:
        start = random.randint(0, len(lines) - args.batch)
        batch = lines[start : start + args.batch]
        if args.drift:
            batch += drift_lines(args.batch // 4)  # ~20% unseen lines
        response = requests.post(
            f"{args.url}/api/v1/predict",
            json={"logs": "\n".join(batch)},
            headers=headers,
            verify=False,
            timeout=60,
        )
        sent += 1
        summary = response.json().get("summary", {}) if response.ok else response.text[:120]
        print(f"[{sent}] {response.status_code} {summary}")
        time.sleep(2)


if __name__ == "__main__":
    main()
