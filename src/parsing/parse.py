"""Stage 2 - Parse raw HDFS logs into a structured Parquet table with Spark.

Each raw line is split into its fields, timestamped, and exploded so that every
output row carries exactly one block ID (the session key). ``line_id`` keeps the
original file order, which template mining and event sequences rely on.

Usage:
    python -m src.parsing.parse                     # full dataset, master from params.yaml
    python -m src.parsing.parse --sample            # 2k-line sample
    python -m src.parsing.parse --master local[2]   # override cores (benchmark)
"""

import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config import load_params
from src.utils.logger import get_logger
from src.utils.spark import get_spark

logger = get_logger("stage.parse")

# date(yyMMdd) time(HHmmss) pid level component: content
LINE_PATTERN = r"^(\d{6}) (\d{6}) (\d+) (\w+) ([^:]+): (.*)$"
BLOCK_PATTERN = r"(blk_-?\d+)"
FIELDS = {"date": 1, "time": 2, "pid": 3, "level": 4, "component": 5, "content": 6}


def parse_logs(spark: SparkSession, log_path: Path) -> DataFrame:
    """Return a DataFrame with one row per (log line, block ID) pair.

    Columns: line_id, ts, pid, level, component, content, block_id.
    Lines that do not match the HDFS format are dropped.
    """
    # monotonically_increasing_id increases with file order across partitions.
    df = spark.read.text(str(log_path)).withColumn("line_id", F.monotonically_increasing_id())
    for name, group in FIELDS.items():
        df = df.withColumn(name, F.regexp_extract("value", LINE_PATTERN, group))

    block_ids = F.array_distinct(F.regexp_extract_all("content", F.lit(BLOCK_PATTERN), 1))
    return (
        df.filter(F.col("date") != "")  # regexp_extract returns "" when the line is malformed
        .withColumn("ts", F.to_timestamp(F.concat("date", "time"), "yyMMddHHmmss"))
        .withColumn("pid", F.col("pid").cast("int"))
        .withColumn("block_id", F.explode(block_ids))
        .select("line_id", "ts", "pid", "level", "component", "content", "block_id")
    )


def main() -> int:
    """Run the parse stage and return a process exit code."""
    parser = argparse.ArgumentParser(description="Parse raw HDFS logs with Spark.")
    parser.add_argument("--sample", action="store_true", help="parse the 2k-line sample")
    parser.add_argument("--master", default=None, help="Spark master override, e.g. local[2]")
    args = parser.parse_args()

    params = load_params()
    cfg = params["parse"]
    log_path = Path(params["data"]["sample_log"] if args.sample else params["data"]["raw_log"])
    out_path = Path(cfg["output"])
    report_path = Path(cfg["report"])
    if args.sample:
        out_path = Path("data/processed/sample") / out_path.name
        report_path = report_path.with_name(report_path.stem + "_sample.json")

    spark = get_spark("logsentinel-parse", args.master)
    try:
        logger.info("Parsing %s with master=%s", log_path, spark.sparkContext.master)
        start = time.perf_counter()
        parse_logs(spark, log_path).write.mode("overwrite").parquet(str(out_path))
        elapsed = time.perf_counter() - start

        parsed = spark.read.parquet(str(out_path))
        total_lines = spark.read.text(str(log_path)).count()
        parsed_lines = parsed.select("line_id").distinct().count()
        stats = {
            "master": spark.sparkContext.master,
            "cores": spark.sparkContext.defaultParallelism,
            "input_lines": total_lines,
            "parsed_lines": parsed_lines,
            "dropped_lines": total_lines - parsed_lines,
            "output_rows": parsed.count(),
            "unique_blocks": parsed.select("block_id").distinct().count(),
            "parse_seconds": round(elapsed, 2),
            "lines_per_second": round(total_lines / elapsed) if elapsed else 0,
        }
    except Exception:  # noqa: BLE001 - log any Spark failure with its traceback
        logger.exception("Parse stage failed")
        return 1
    finally:
        spark.stop()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(stats, indent=2) + "\n")
    logger.info("Parse stats: %s", stats)
    logger.info("Wrote %s and %s", out_path, report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
