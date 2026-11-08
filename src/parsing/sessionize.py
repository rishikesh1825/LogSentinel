"""Stage 6 - Build one session per block from matched log lines, with Spark.

For every block this produces the template-count map (model features), the
ordered template sequence (for explanations), session length, unseen-template
count, the label and the train/val/test split.

Usage:
    python -m src.parsing.sessionize            # full dataset
    python -m src.parsing.sessionize --sample   # 2k-line sample
"""

import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.parsing.match import UNSEEN
from src.utils.config import load_params
from src.utils.logger import get_logger
from src.utils.spark import get_spark

logger = get_logger("stage.sessionize")


def build_sessions(spark: SparkSession, matched_path: Path, splits_path: Path) -> DataFrame:
    """Return one row per block with counts, sequence, length, label and split."""
    matched = spark.read.parquet(str(matched_path))

    # Ordered sequence: sort (line_id, template_id) pairs, then keep the template IDs.
    sequences = (
        matched.groupBy("block_id")
        .agg(
            F.sort_array(F.collect_list(F.struct("line_id", "template_id"))).alias("events"),
            F.count("*").alias("n_lines"),
            F.sum((F.col("template_id") == UNSEEN).cast("int")).alias("n_unseen"),
            F.min("ts").alias("start_ts"),
            F.max("ts").alias("end_ts"),
        )
        .withColumn("sequence", F.col("events.template_id"))
        .drop("events")
    )

    # Template-count map: {template_id: count}.
    counts = (
        matched.groupBy("block_id", "template_id")
        .count()
        .groupBy("block_id")
        .agg(F.map_from_entries(F.collect_list(F.struct("template_id", "count"))).alias("counts"))
    )

    splits = spark.read.csv(str(splits_path), header=True).select(
        F.col("BlockId").alias("block_id"),
        (F.col("Label") == "Anomaly").cast("int").alias("label"),
        "split",
    )
    return sequences.join(counts, "block_id").join(splits, "block_id")


def main() -> int:
    """Run the sessionize stage and return a process exit code."""
    parser = argparse.ArgumentParser(description="Build per-block sessions from matched lines.")
    parser.add_argument("--sample", action="store_true", help="use the 2k-line sample")
    args = parser.parse_args()

    params = load_params()
    cfg = params["sessionize"]
    matched_path = Path(params["match"]["output"])
    out_path, report_path = Path(cfg["output"]), Path(cfg["report"])
    if args.sample:
        matched_path = Path("data/processed/sample") / matched_path.name
        out_path = Path("data/processed/sample") / out_path.name
        report_path = report_path.with_name(report_path.stem + "_sample.json")

    spark = get_spark("logsentinel-sessionize")
    try:
        start = time.perf_counter()
        build_sessions(spark, matched_path, Path(params["split"]["output"])).write.mode(
            "overwrite"
        ).parquet(str(out_path))
        elapsed = time.perf_counter() - start

        sessions = spark.read.parquet(str(out_path))
        rows = (
            sessions.groupBy("split")
            .agg(
                F.count("*").alias("blocks"),
                F.sum("label").alias("anomalies"),
                F.round(F.avg("n_lines"), 2).alias("avg_lines"),
                F.round(F.avg(F.size(F.map_keys("counts"))), 2).alias("avg_distinct_templates"),
                F.sum("n_unseen").alias("unseen_lines"),
            )
            .collect()
        )
        stats = {
            "total_blocks": sessions.count(),
            "sessionize_seconds": round(elapsed, 2),
            "by_split": {r["split"]: r.asDict() for r in rows},
        }
    except Exception:  # noqa: BLE001 - log any Spark failure with its traceback
        logger.exception("Sessionize stage failed")
        return 1
    finally:
        spark.stop()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stats, indent=2) + "\n")
    logger.info("Session stats: %s", stats)
    logger.info("Wrote %s and %s", out_path, report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
