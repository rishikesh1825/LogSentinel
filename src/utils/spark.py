"""Shared SparkSession factory so every Spark stage uses identical settings."""

import os
import sys

from pyspark.sql import SparkSession

from src.utils.config import load_params


def get_spark(app_name: str, master: str | None = None) -> SparkSession:
    """Create (or reuse) a SparkSession configured from params.yaml.

    Args:
        app_name: Name shown in the Spark UI and logs.
        master: Optional override, e.g. ``local[2]`` for the throughput benchmark.

    Returns:
        A configured SparkSession.
    """
    cfg = load_params()["spark"]
    # Make Spark's Python workers use the same interpreter (our venv) as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # Bind to localhost so a Wi-Fi IP change cannot break a running job.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master or cfg["master"])
        .config("spark.driver.memory", cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
