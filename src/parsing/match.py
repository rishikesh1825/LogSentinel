"""Stage 5 - Match every parsed line to a frozen drain3 template, in parallel on Spark.

The drain3 state learned from training blocks is broadcast to all workers. Each
partition rebuilds a TemplateMiner from the broadcast bytes once and matches its
lines with ``mapInPandas`` (Arrow-backed batches). Lines that fit no template get
``template_id = -1`` (unseen), the data-drift signal used later in monitoring.

Usage:
    python -m src.parsing.match                     # full dataset
    python -m src.parsing.match --sample            # 2k-line sample
    python -m src.parsing.match --master local[2]   # override cores (benchmark)
"""

import argparse
import json
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pandas as pd
from drain3 import TemplateMiner
from drain3.memory_buffer_persistence import MemoryBufferPersistence
from pyspark import Broadcast
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.parsing.drain_config import build_config
from src.utils.config import load_params
from src.utils.logger import get_logger
from src.utils.spark import get_spark

logger = get_logger("stage.match")

UNSEEN = -1
OUTPUT_SCHEMA = "line_id long, ts timestamp, block_id string, template_id int"


def load_miner(state: bytes, cfg: dict) -> TemplateMiner:
    """Rebuild a read-only TemplateMiner from serialized drain3 state."""
    persistence = MemoryBufferPersistence()
    persistence.state = state
    return TemplateMiner(
        persistence, config=build_config(cfg["sim_th"], cfg["depth"], cfg["max_children"])
    )


def make_matcher(
    state_bc: Broadcast, cfg: dict, strategy: str
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """Return a mapInPandas function that tags each line with its template ID."""

    def match_partition(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        miner = load_miner(state_bc.value, cfg)  # built once per partition
        for batch in batches:
            ids = []
            for content in batch["content"]:
                cluster = miner.match(content, full_search_strategy=strategy)
                ids.append(cluster.cluster_id if cluster else UNSEEN)
            yield batch[["line_id", "ts", "block_id"]].assign(
                template_id=pd.Series(ids, index=batch.index, dtype="int32")
            )

    return match_partition


def match_lines(spark: SparkSession, parsed_path: Path, state: bytes, params: dict) -> DataFrame:
    """Return parsed lines with a template_id column (UNSEEN for unmatched lines)."""
    state_bc = spark.sparkContext.broadcast(state)
    matcher = make_matcher(state_bc, params["templates"], params["match"]["search_strategy"])
    parsed = spark.read.parquet(str(parsed_path)).select("line_id", "ts", "block_id", "content")
    return parsed.mapInPandas(matcher, schema=OUTPUT_SCHEMA)


def unseen_by_split(spark: SparkSession, matched: DataFrame, splits_path: Path) -> dict:
    """Return per-split line counts and unseen-template ratios."""
    splits = spark.read.csv(str(splits_path), header=True).select(
        F.col("BlockId").alias("block_id"), "split"
    )
    rows = (
        matched.join(splits, "block_id")
        .groupBy("split")
        .agg(
            F.count("*").alias("lines"),
            F.sum((F.col("template_id") == UNSEEN).cast("int")).alias("unseen"),
        )
        .collect()
    )
    return {
        r["split"]: {
            "lines": r["lines"],
            "unseen": r["unseen"],
            "unseen_ratio": round(r["unseen"] / r["lines"], 6),
        }
        for r in rows
    }


def main() -> int:
    """Run the match stage and return a process exit code."""
    parser = argparse.ArgumentParser(description="Match parsed lines to drain3 templates.")
    parser.add_argument("--sample", action="store_true", help="use the 2k-line sample")
    parser.add_argument("--master", default=None, help="Spark master override, e.g. local[2]")
    args = parser.parse_args()

    params = load_params()
    cfg = params["match"]
    parsed_path = Path(params["parse"]["output"])
    state_path = Path(params["templates"]["state_path"])
    out_path, report_path = Path(cfg["output"]), Path(cfg["report"])
    if args.sample:
        parsed_path = Path("data/processed/sample") / parsed_path.name
        state_path = state_path.parent / "sample" / state_path.name
        out_path = Path("data/processed/sample") / out_path.name
        report_path = report_path.with_name(report_path.stem + "_sample.json")

    spark = get_spark("logsentinel-match", args.master)
    try:
        state = state_path.read_bytes()
        logger.info(
            "Matching with master=%s, state=%s (%d bytes)",
            spark.sparkContext.master,
            state_path,
            len(state),
        )
        start = time.perf_counter()
        match_lines(spark, parsed_path, state, params).write.mode("overwrite").parquet(
            str(out_path)
        )
        elapsed = time.perf_counter() - start

        matched = spark.read.parquet(str(out_path))
        total = matched.count()
        stats = {
            "master": spark.sparkContext.master,
            "cores": spark.sparkContext.defaultParallelism,
            "rows": total,
            "match_seconds": round(elapsed, 2),
            "rows_per_second": round(total / elapsed) if elapsed else 0,
            "by_split": unseen_by_split(spark, matched, Path(params["split"]["output"])),
        }
    except Exception:  # noqa: BLE001 - log any Spark/drain3 failure with its traceback
        logger.exception("Match stage failed")
        return 1
    finally:
        spark.stop()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stats, indent=2) + "\n")
    logger.info("Match stats: %s", stats)
    logger.info("Wrote %s and %s", out_path, report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
