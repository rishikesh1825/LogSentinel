"""Stage 4 - Mine log templates with drain3 from TRAINING blocks only.

Spark selects the training lines and sorts them into original log order; they
are then streamed to drain3, which learns templates sequentially (Drain is an
online, order-dependent algorithm, so this step runs on a single core). The
frozen state is later broadcast to Spark workers for parallel matching.

Usage:
    python -m src.parsing.mine_templates            # full dataset
    python -m src.parsing.mine_templates --sample   # 2k-line sample
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.parsing.drain_config import build_config
from src.utils.config import load_params
from src.utils.logger import get_logger
from src.utils.spark import get_spark

logger = get_logger("stage.mine_templates")


def training_lines(spark: SparkSession, parsed_path: Path, splits_path: Path) -> DataFrame:
    """Return (line_id, content) for lines of training blocks, in original log order."""
    parsed = spark.read.parquet(str(parsed_path))
    train_ids = (
        spark.read.csv(str(splits_path), header=True)
        .filter(F.col("split") == "train")
        .select(F.col("BlockId").alias("block_id"))
    )
    return (
        parsed.join(F.broadcast(train_ids), "block_id")
        .select("line_id", "content")
        .dropDuplicates(["line_id"])  # a multi-block line is mined once
        .orderBy("line_id")
    )


def mine(lines: DataFrame, miner: TemplateMiner) -> int:
    """Feed every line to drain3 and return the number of lines processed."""
    count = 0
    for row in lines.toLocalIterator():
        miner.add_log_message(row.content)
        count += 1
        if count % 500_000 == 0:
            logger.info("Mined %d lines, %d templates so far", count, len(miner.drain.clusters))
    return count


def write_templates(miner: TemplateMiner, path: Path) -> list[dict]:
    """Write cluster_id, size and template text to CSV, largest first."""
    rows = sorted(
        (
            {"cluster_id": c.cluster_id, "size": c.size, "template": c.get_template()}
            for c in miner.drain.clusters
        ),
        key=lambda r: r["size"],
        reverse=True,
    )
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cluster_id", "size", "template"])
        writer.writeheader()
        writer.writerows(rows)
    return rows


def main() -> int:
    """Run template mining and return a process exit code."""
    parser = argparse.ArgumentParser(description="Mine drain3 templates from training lines.")
    parser.add_argument("--sample", action="store_true", help="use the 2k-line sample")
    args = parser.parse_args()

    params = load_params()
    cfg = params["templates"]
    parsed_path = Path(params["parse"]["output"])
    state_path, csv_path = Path(cfg["state_path"]), Path(cfg["templates_csv"])
    report_path = Path(cfg["report"])
    if args.sample:
        parsed_path = Path("data/processed/sample") / parsed_path.name
        state_path = state_path.parent / "sample" / state_path.name
        csv_path = csv_path.parent / "sample" / csv_path.name
        report_path = report_path.with_name(report_path.stem + "_sample.json")

    # Start from an empty state: FilePersistence would otherwise resume an old run.
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.unlink(missing_ok=True)
    miner = TemplateMiner(
        FilePersistence(str(state_path)),
        config=build_config(cfg["sim_th"], cfg["depth"], cfg["max_children"]),
    )

    spark = get_spark("logsentinel-mine-templates")
    try:
        start = time.perf_counter()
        n_lines = mine(training_lines(spark, parsed_path, Path(params["split"]["output"])), miner)
        elapsed = time.perf_counter() - start
    except Exception:  # noqa: BLE001 - log any Spark/drain3 failure with its traceback
        logger.exception("Template mining failed")
        return 1
    finally:
        spark.stop()

    miner.save_state("final")
    templates = write_templates(miner, csv_path)
    stats = {
        "training_lines": n_lines,
        "n_templates": len(templates),
        "templates_with_<10_lines": sum(1 for t in templates if t["size"] < 10),
        "mine_seconds": round(elapsed, 2),
        "lines_per_second": round(n_lines / elapsed) if elapsed else 0,
        "sim_th": cfg["sim_th"],
        "depth": cfg["depth"],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stats, indent=2) + "\n")
    logger.info("Template stats: %s", stats)
    logger.info("Wrote %s, %s and %s", state_path, csv_path, report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
