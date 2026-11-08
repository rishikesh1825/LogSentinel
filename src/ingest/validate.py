"""Stage 1 - Data validation for raw HDFS logs and anomaly labels.

Checks the raw inputs before they enter the pipeline and writes a JSON report
that DVC tracks as a metric. Exits with a non-zero code if any check fails,
which stops ``dvc repro`` so bad data never reaches training.

Usage:
    python -m src.ingest.validate            # full dataset
    python -m src.ingest.validate --sample   # 2k-line sample (fast, for dev/tests)
"""

import argparse
import csv
import hashlib
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path

from src.utils.config import load_params
from src.utils.logger import get_logger

logger = get_logger("stage.validate")

# date(yyMMdd) time(HHmmss) pid level component: content
LINE_RE = re.compile(r"^(\d{6}) (\d{6}) (\d+) (\w+) ([^:]+): (.*)$")
BLOCK_RE = re.compile(r"blk_-?\d+")
VALID_LABELS = {"Normal", "Anomaly"}
EXPECTED_HEADER = ["BlockId", "Label"]


def md5sum(path: Path, chunk_size: int = 1 << 20) -> str:
    """Return the MD5 checksum of a file, read in 1 MB chunks."""
    digest = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_logs(path: Path) -> tuple[dict, set[str]]:
    """Stream the log file once and collect format statistics.

    Returns:
        (stats, block_ids) - counts of total/malformed/no-block lines, log-level
        distribution and throughput, plus the set of block IDs seen.
    """
    total = malformed = no_block = 0
    levels: Counter = Counter()
    blocks: set[str] = set()
    start = time.perf_counter()

    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            total += 1
            match = LINE_RE.match(line.rstrip("\n"))
            if match is None:
                malformed += 1
                continue
            levels[match.group(4)] += 1
            ids = BLOCK_RE.findall(match.group(6))
            if ids:
                blocks.update(ids)
            else:
                no_block += 1
            if total % 1_000_000 == 0:
                logger.info("Scanned %d lines...", total)

    elapsed = time.perf_counter() - start
    stats = {
        "total_lines": total,
        "malformed_lines": malformed,
        "malformed_ratio": malformed / total if total else 1.0,
        "no_block_lines": no_block,
        "no_block_ratio": no_block / total if total else 1.0,
        "levels": dict(levels),
        "unique_blocks_in_log": len(blocks),
        "scan_seconds": round(elapsed, 2),
        "lines_per_second": round(total / elapsed) if elapsed else 0,
    }
    return stats, blocks


def scan_labels(path: Path) -> tuple[dict, dict[str, str]]:
    """Read the label CSV and check header, values and duplicates.

    Returns:
        (stats, labels) - label statistics and a BlockId -> Label mapping.
    """
    labels: dict[str, str] = {}
    invalid = duplicates = 0

    with path.open(newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if len(row) != 2 or row[1] not in VALID_LABELS:
                invalid += 1
                continue
            block_id, label = row
            if block_id in labels:
                duplicates += 1
            labels[block_id] = label

    n_anomaly = sum(1 for v in labels.values() if v == "Anomaly")
    stats = {
        "header_ok": header == EXPECTED_HEADER,
        "total_blocks": len(labels),
        "invalid_rows": invalid,
        "duplicate_blocks": duplicates,
        "anomaly_blocks": n_anomaly,
        "anomaly_ratio": n_anomaly / len(labels) if labels else 0.0,
    }
    return stats, labels


def build_checks(log_stats, label_stats, log_blocks, labels, cfg, sample) -> list[dict]:
    """Compare the collected statistics against thresholds from params.yaml."""
    label_blocks = set(labels)
    log_blocks_labelled = len(log_blocks & label_blocks) / len(log_blocks) if log_blocks else 0.0
    label_blocks_in_log = len(label_blocks & log_blocks) / len(label_blocks) if labels else 0.0
    low, high = cfg["anomaly_ratio_range"]

    checks = [
        ("log_not_empty", log_stats["total_lines"], "> 0", log_stats["total_lines"] > 0),
        (
            "malformed_ratio",
            log_stats["malformed_ratio"],
            f"<= {cfg['max_malformed_ratio']}",
            log_stats["malformed_ratio"] <= cfg["max_malformed_ratio"],
        ),
        (
            "no_block_ratio",
            log_stats["no_block_ratio"],
            f"<= {cfg['max_no_block_ratio']}",
            log_stats["no_block_ratio"] <= cfg["max_no_block_ratio"],
        ),
        ("label_header", label_stats["header_ok"], "== True", label_stats["header_ok"]),
        (
            "label_invalid_rows",
            label_stats["invalid_rows"],
            "== 0",
            label_stats["invalid_rows"] == 0,
        ),
        (
            "label_duplicates",
            label_stats["duplicate_blocks"],
            "== 0",
            label_stats["duplicate_blocks"] == 0,
        ),
        (
            "anomaly_ratio",
            label_stats["anomaly_ratio"],
            f"in [{low}, {high}]",
            low <= label_stats["anomaly_ratio"] <= high,
        ),
        # Every block seen in the logs must have a label (true for sample and full data).
        (
            "log_blocks_labelled",
            log_blocks_labelled,
            f">= {cfg['min_label_coverage']}",
            log_blocks_labelled >= cfg["min_label_coverage"],
        ),
    ]
    # Only the full log is expected to contain (almost) every labelled block.
    if not sample:
        checks.append(
            (
                "label_blocks_in_log",
                label_blocks_in_log,
                f">= {cfg['min_label_coverage']}",
                label_blocks_in_log >= cfg["min_label_coverage"],
            )
        )
    return [{"name": n, "value": v, "threshold": t, "passed": bool(p)} for n, v, t, p in checks]


def main() -> int:
    """Run validation and return a process exit code (0 = all checks passed)."""
    parser = argparse.ArgumentParser(description="Validate raw HDFS logs and labels.")
    parser.add_argument("--sample", action="store_true", help="validate the 2k-line sample")
    args = parser.parse_args()

    params = load_params()
    cfg = params["validation"]
    log_path = Path(params["data"]["sample_log"] if args.sample else params["data"]["raw_log"])
    label_path = Path(params["data"]["labels"])
    report_path = Path(cfg["report"])
    if args.sample:
        report_path = report_path.with_name(report_path.stem + "_sample.json")

    try:
        for p in (log_path, label_path):
            if not p.exists() or p.stat().st_size == 0:
                raise FileNotFoundError(f"Missing or empty input: {p}")

        logger.info("Validating log file %s", log_path)
        log_stats, log_blocks = scan_logs(log_path)
        logger.info("Validating label file %s", label_path)
        label_stats, labels = scan_labels(label_path)
        checks = build_checks(log_stats, label_stats, log_blocks, labels, cfg, args.sample)
    except (FileNotFoundError, OSError) as exc:
        logger.error("Validation could not run: %s", exc)
        return 2

    passed = all(c["passed"] for c in checks)
    report = {
        "mode": "sample" if args.sample else "full",
        "log_file": str(log_path),
        "log_md5": md5sum(log_path),
        "log": log_stats,
        "labels": label_stats,
        "checks": checks,
        "all_passed": passed,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + "\n")

    for c in checks:
        level = logger.info if c["passed"] else logger.error
        level(
            "[%s] %s = %s (expected %s)",
            "PASS" if c["passed"] else "FAIL",
            c["name"],
            c["value"],
            c["threshold"],
        )
    logger.info("Report written to %s - all_passed=%s", report_path, passed)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
