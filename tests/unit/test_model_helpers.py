"""Unit tests for the parsing helpers inside the served model (src/model/logsentinel_model.py)."""

from src.model.logsentinel_model import UNSEEN, extract_blocks, parse_content, sessionize_lines


def test_parse_content_returns_message(normal_lines):
    """TC-20: the message part after 'component:' is extracted."""
    assert parse_content(normal_lines[2]).startswith("PacketResponder 1 for block")
    assert parse_content("not a log line") is None


def test_extract_blocks_deduplicates():
    """TC-21: a block mentioned twice in one line counts once."""
    content = "Deleting block blk_7 file /mnt/x/subdir1/blk_7 and blk_-8"
    assert extract_blocks(content) == ["blk_7", "blk_-8"]


def test_sessionize_groups_by_block_and_skips_bad_lines(normal_lines):
    """TC-22: lines are grouped per block; malformed / block-less lines are skipped."""
    lines = normal_lines + ["garbage", "081109 203518 1 INFO comp: no block"]
    sessions, skipped = sessionize_lines(lines, lambda content: 7)
    assert skipped == 2
    assert sessions == {"blk_-1608999687919862906": [7, 7, 7]}


def test_sessionize_keeps_unseen_marker(normal_lines):
    """TC-23: lines that match no template keep the UNSEEN id (drift signal)."""
    sessions, _ = sessionize_lines(normal_lines[:1], lambda content: UNSEEN)
    assert sessions["blk_-1608999687919862906"] == [UNSEEN]
