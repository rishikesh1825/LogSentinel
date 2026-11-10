"""Unit tests for the shared drain3 configuration (src/parsing/drain_config.py)."""

from drain3 import TemplateMiner

from src.parsing.drain_config import build_config


def make_miner() -> TemplateMiner:
    """Miner with the production settings (sim_th 0.85)."""
    return TemplateMiner(config=build_config(0.85, 4, 100))


def test_block_and_ip_are_masked():
    """TC-11: block IDs and IP:port are replaced by placeholders."""
    result = make_miner().add_log_message(
        "Receiving block blk_-1608999687919862906 "
        "src: /10.250.19.102:54106 dest: /10.250.19.102:50010"
    )
    assert result["template_mined"] == "Receiving block <BLK> src: /<IP> dest: /<IP>"


def test_block_list_becomes_one_token():
    """TC-12: a list of blocks is one <BLKS> token, whatever its length."""
    miner = make_miner()
    a = miner.add_log_message("BLOCK* ask 10.250.1.1:50010 to delete blk_1 blk_2 blk_3")
    b = miner.add_log_message("BLOCK* ask 10.250.1.2:50010 to delete blk_4 blk_5 blk_6 blk_7 blk_8")
    assert a["cluster_id"] == b["cluster_id"]
    assert "<BLKS>" in a["template_mined"]


def test_paths_are_masked():
    """TC-13: file paths become <PATH>, so path lines do not fragment templates."""
    miner = make_miner()
    a = miner.add_log_message(
        "Deleting block blk_1 file /mnt/hadoop/dfs/data/current/subdir5/blk_1"
    )
    b = miner.add_log_message(
        "Deleting block blk_2 file /mnt/hadoop/dfs/data/current/subdir51/blk_2"
    )
    assert a["cluster_id"] == b["cluster_id"]
    assert b["template_mined"] == "Deleting block <BLK> file <PATH>/<BLK>"


def test_terminating_and_interrupted_stay_separate():
    """TC-14: sim_th 0.85 keeps 'terminating' (normal) apart from 'Interrupted.' (anomaly)."""
    miner = make_miner()
    a = miner.add_log_message("PacketResponder 1 for block blk_1 terminating")
    b = miner.add_log_message("PacketResponder 2 for block blk_2 Interrupted.")
    assert a["cluster_id"] != b["cluster_id"]
