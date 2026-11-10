"""Shared pytest fixtures.

Test IDs (TC-xx) in docstrings match the test-case list in docs/test_plan.md.
"""

import os
import sys
from pathlib import Path

import pytest

SAMPLE_LOG = Path("data/sample/HDFS_2k.log")

NORMAL_LINES = [
    "081109 203518 35 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: "
    "/mnt/hadoop/mapred/system/job_200811092030_0001/job.jar. blk_-1608999687919862906",
    "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 "
    "src: /10.250.19.102:54106 dest: /10.250.19.102:50010",
    "081109 203519 145 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block "
    "blk_-1608999687919862906 terminating",
]


@pytest.fixture
def normal_lines() -> list[str]:
    """Three real HDFS lines of one healthy block."""
    return list(NORMAL_LINES)


@pytest.fixture(scope="session")
def sample_log() -> Path:
    """The Loghub 2k-line sample (DVC-tracked). Skips if it has not been pulled."""
    if not SAMPLE_LOG.exists():
        pytest.skip("data/sample/HDFS_2k.log missing - run `dvc pull`")
    return SAMPLE_LOG


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session shared by all Spark tests."""
    pyspark_sql = pytest.importorskip("pyspark.sql")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        pyspark_sql.SparkSession.builder.master("local[2]")
        .appName("logsentinel-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
