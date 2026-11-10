"""Build realistic demo log files from TEST blocks (never seen in training).

Raw log snippets (e.g. the first N lines of HDFS.log or the 2k sample) contain
incomplete sessions, which look abnormal to a model trained on complete ones.
This script collects ALL lines of a few test blocks from the parsed data and
rebuilds the raw lines, so every block in the demo file is a complete session.

Outputs (git-ignored):
    demo/demo_logs.log     normal + anomalous test blocks, in log order
    demo/demo_labels.csv   the true label of each block (to compare with the UI)

Usage:
    python scripts/make_demo_logs.py [--normal 30] [--anomalies 10] [--seed 7]
"""

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def main() -> None:
    """Write demo/demo_logs.log and demo/demo_labels.csv."""
    parser = argparse.ArgumentParser(description="Create demo logs from test blocks.")
    parser.add_argument("--normal", type=int, default=30)
    parser.add_argument("--anomalies", type=int, default=10)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    splits = pd.read_csv("data/processed/splits.csv")
    test = splits[splits["split"] == "test"]
    chosen = pd.concat(
        [
            test[test["Label"] == "Normal"].sample(args.normal, random_state=args.seed),
            test[test["Label"] == "Anomaly"].sample(args.anomalies, random_state=args.seed),
        ]
    )
    ids = chosen["BlockId"].tolist()

    table = pq.read_table(
        "data/processed/parsed",
        columns=["line_id", "ts", "pid", "level", "component", "content", "block_id"],
        filters=[("block_id", "in", ids)],
    )
    rows = table.to_pandas().drop_duplicates("line_id").sort_values("line_id")
    raw = (
        rows["ts"].dt.strftime("%y%m%d %H%M%S")
        + " "
        + rows["pid"].astype(str)
        + " "
        + rows["level"]
        + " "
        + rows["component"]
        + ": "
        + rows["content"]
    )

    out = Path("demo")
    out.mkdir(exist_ok=True)
    (out / "demo_logs.log").write_text("\n".join(raw) + "\n")
    chosen.drop(columns="split").to_csv(out / "demo_labels.csv", index=False)
    print(f"Wrote {len(raw)} lines for {len(ids)} blocks to demo/demo_logs.log")
    print("True labels: demo/demo_labels.csv")


if __name__ == "__main__":
    main()
