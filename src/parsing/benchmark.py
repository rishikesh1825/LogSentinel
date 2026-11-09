"""Throughput benchmark: Spark parse and match stages against the number of cores.

Each (stage, cores) run happens in a fresh subprocess so every measurement starts
with a clean Spark JVM. The timed job uses Spark's ``noop`` writer: the full
computation runs but nothing is written, so the result reflects compute scaling
rather than disk speed, and real pipeline outputs are left untouched.

Usage:
    python -m src.parsing.benchmark
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # render to file, no display needed
import matplotlib.pyplot as plt  # noqa: E402

from src.parsing.match import match_lines  # noqa: E402
from src.parsing.parse import parse_logs  # noqa: E402
from src.utils.config import load_params  # noqa: E402
from src.utils.logger import get_logger  # noqa: E402
from src.utils.spark import get_spark  # noqa: E402

logger = get_logger("benchmark")


def run_worker(stage: str, cores: int) -> None:
    """Time one stage on ``cores`` cores and print the result as JSON on stdout."""
    params = load_params()
    spark = get_spark(f"bench-{stage}-{cores}", f"local[{cores}]")
    try:
        if stage == "parse":
            df = parse_logs(spark, Path(params["data"]["raw_log"]))
        else:
            state = Path(params["templates"]["state_path"]).read_bytes()
            df = match_lines(spark, Path(params["parse"]["output"]), state, params)
        start = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()
        elapsed = time.perf_counter() - start
    finally:
        spark.stop()
    print(json.dumps({"stage": stage, "cores": cores, "seconds": round(elapsed, 2)}))


def run_all(cfg: dict, n_lines: int) -> list[dict]:
    """Run every (stage, cores) combination in its own subprocess."""
    results = []
    for stage in cfg["stages"]:
        for cores in cfg["cores"]:
            logger.info("Running %s on %d core(s)...", stage, cores)
            proc = subprocess.run(
                [sys.executable, "-m", "src.parsing.benchmark", "--worker", stage, str(cores)],
                capture_output=True,
                text=True,
                check=True,
            )
            result = json.loads(proc.stdout.strip().splitlines()[-1])
            result["lines_per_second"] = round(n_lines / result["seconds"])
            logger.info("  %s", result)
            results.append(result)

    for stage in cfg["stages"]:
        rows = [r for r in results if r["stage"] == stage]
        base = next(r["seconds"] for r in rows if r["cores"] == min(cfg["cores"]))
        for r in rows:
            r["speedup"] = round(base / r["seconds"], 2)
            r["efficiency"] = round(r["speedup"] / r["cores"], 2)
    return results


def plot(results: list[dict], cfg: dict, path: Path) -> None:
    """Plot throughput and speedup against cores for each stage."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.5))
    for stage in cfg["stages"]:
        rows = [r for r in results if r["stage"] == stage]
        cores = [r["cores"] for r in rows]
        ax1.plot(cores, [r["lines_per_second"] for r in rows], marker="o", label=stage)
        ax2.plot(cores, [r["speedup"] for r in rows], marker="o", label=stage)
    ax2.plot(cfg["cores"], cfg["cores"], linestyle="--", color="grey", label="ideal (linear)")

    ax1.set(xlabel="Cores", ylabel="Lines per second", title="Throughput vs cores")
    ax2.set(xlabel="Cores", ylabel="Speedup vs 1 core", title="Speedup vs cores")
    for ax in (ax1, ax2):
        ax.set_xticks(cfg["cores"])
        ax.grid(alpha=0.3)
        ax.legend()
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)


def main() -> int:
    """Run the benchmark (or a single worker run) and return an exit code."""
    parser = argparse.ArgumentParser(description="Spark throughput benchmark.")
    parser.add_argument("--worker", nargs=2, metavar=("STAGE", "CORES"), help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        run_worker(args.worker[0], int(args.worker[1]))
        return 0

    params = load_params()
    cfg = params["benchmark"]
    n_lines = json.loads(Path(params["validation"]["report"]).read_text())["log"]["total_lines"]
    try:
        results = run_all(cfg, n_lines)
    except subprocess.CalledProcessError as exc:
        logger.error("Benchmark run failed:\n%s", exc.stderr[-2000:])
        return 1

    report = {"lines": n_lines, "results": results}
    Path(cfg["report"]).write_text(json.dumps(report, indent=2) + "\n")
    plot(results, cfg, Path(cfg["figure"]))
    logger.info("Wrote %s and %s", cfg["report"], cfg["figure"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
