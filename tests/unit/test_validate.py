"""Unit tests for stage 1 - data validation (src/ingest/validate.py)."""

from src.ingest.validate import BLOCK_RE, LINE_RE, build_checks, scan_labels, scan_logs

CFG = {
    "max_malformed_ratio": 0.001,
    "max_no_block_ratio": 0.001,
    "min_label_coverage": 0.99,
    "anomaly_ratio_range": [0.01, 0.10],
}


def test_line_regex_accepts_hdfs_line(normal_lines):
    """TC-01: a real HDFS line is split into its 6 fields."""
    match = LINE_RE.match(normal_lines[1])
    assert match is not None
    assert match.group(4) == "INFO"
    assert match.group(5) == "dfs.DataNode$DataXceiver"


def test_line_regex_rejects_garbage():
    """TC-02: non-HDFS text is rejected."""
    assert LINE_RE.match("hello world, this is not a log line") is None
    assert LINE_RE.match("2024-01-01 10:00:00 INFO something") is None


def test_block_regex_handles_negative_ids():
    """TC-03: block IDs may be negative."""
    assert BLOCK_RE.findall("blk_-123 and blk_456") == ["blk_-123", "blk_456"]


def test_scan_logs_counts_malformed_and_blockless(tmp_path, normal_lines):
    """TC-04: malformed lines and lines without a block are counted."""
    log = tmp_path / "x.log"
    extra = ["garbage", "081109 203518 1 INFO comp: no block here"]
    log.write_text("\n".join(normal_lines + extra) + "\n")
    stats, blocks = scan_logs(log)
    assert stats["total_lines"] == 5
    assert stats["malformed_lines"] == 1
    assert stats["no_block_lines"] == 1
    assert blocks == {"blk_-1608999687919862906"}


def test_scan_labels_detects_invalid_and_duplicates(tmp_path):
    """TC-05: invalid labels and duplicate blocks are detected."""
    labels = tmp_path / "labels.csv"
    labels.write_text("BlockId,Label\nblk_1,Normal\nblk_2,Anomaly\nblk_1,Normal\nblk_3,Maybe\n")
    stats, mapping = scan_labels(labels)
    assert stats["header_ok"] is True
    assert stats["invalid_rows"] == 1
    assert stats["duplicate_blocks"] == 1
    assert mapping == {"blk_1": "Normal", "blk_2": "Anomaly"}


def test_build_checks_fails_on_bad_data():
    """TC-06: validation fails when too many lines are malformed."""
    log_stats = {"total_lines": 100, "malformed_ratio": 0.5, "no_block_ratio": 0.0}
    label_stats = {
        "header_ok": True,
        "invalid_rows": 0,
        "duplicate_blocks": 0,
        "anomaly_ratio": 0.03,
    }
    checks = build_checks(log_stats, label_stats, {"blk_1"}, {"blk_1": "Normal"}, CFG, True)
    by_name = {c["name"]: c["passed"] for c in checks}
    assert by_name["malformed_ratio"] is False
    assert by_name["log_blocks_labelled"] is True
    assert "label_blocks_in_log" not in by_name  # skipped in sample mode
