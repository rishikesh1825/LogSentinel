"""Integration tests: Spark parse + broadcast template matching on the 2k sample."""

import pytest
from drain3 import TemplateMiner
from drain3.memory_buffer_persistence import MemoryBufferPersistence

from src.parsing.drain_config import build_config
from src.parsing.match import UNSEEN, match_lines
from src.parsing.parse import parse_logs

pytestmark = pytest.mark.spark
TEMPLATE_CFG = {"sim_th": 0.85, "depth": 4, "max_children": 100}
PARSED_COLUMNS = {"line_id", "ts", "pid", "level", "component", "content", "block_id"}


def test_parse_explodes_multi_block_lines(spark, sample_log):
    """TC-35: 2,000 sample lines -> 2,206 rows (multi-block lines exploded), no line lost."""
    parsed = parse_logs(spark, sample_log)
    assert parsed.count() == 2206
    assert parsed.select("line_id").distinct().count() == 2000
    assert set(parsed.columns) == PARSED_COLUMNS


def test_broadcast_matching_reproduces_mined_templates(spark, sample_log, tmp_path):
    """TC-36: drain3 state rebuilt from broadcast bytes matches every mined line (0 unseen)."""
    parsed = parse_logs(spark, sample_log)
    parsed_path = tmp_path / "parsed"
    parsed.write.parquet(str(parsed_path))

    persistence = MemoryBufferPersistence()
    miner = TemplateMiner(persistence, config=build_config(**TEMPLATE_CFG))
    for row in parsed.select("content").distinct().collect():
        miner.add_log_message(row.content)
    miner.save_state("test")

    params = {"templates": TEMPLATE_CFG, "match": {"search_strategy": "fallback"}}
    matched = match_lines(spark, parsed_path, persistence.state, params)
    assert matched.count() == 2206
    assert matched.filter(matched.template_id == UNSEEN).count() == 0
