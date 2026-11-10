"""Turn pytest's JUnit XML into the test report (Markdown) required by the rubric.

Usage:
    pytest --junitxml=reports/tests/junit.xml
    python scripts/test_report.py            # writes reports/tests/test_report.md
"""

import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

JUNIT = Path("reports/tests/junit.xml")
REPORT = Path("reports/tests/test_report.md")


def outcome(case: ET.Element) -> str:
    """Return passed / failed / error / skipped for one <testcase>."""
    if case.find("failure") is not None:
        return "failed"
    if case.find("error") is not None:
        return "error"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def main() -> int:
    """Write the Markdown report and return 1 if any test failed."""
    if not JUNIT.exists():
        print(f"{JUNIT} not found - run: pytest --junitxml={JUNIT}")
        return 1
    cases = list(ET.parse(JUNIT).getroot().iter("testcase"))
    rows = []
    counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0}
    total_time = 0.0
    for case in cases:
        result = outcome(case)
        counts[result] += 1
        seconds = float(case.get("time", 0))
        total_time += seconds
        rows.append((case.get("classname", ""), case.get("name", ""), result, seconds))

    executed = counts["passed"] + counts["failed"] + counts["error"]
    pass_rate = counts["passed"] / executed if executed else 0.0
    icon = {"passed": "PASS", "failed": "FAIL", "error": "ERROR", "skipped": "SKIP"}
    lines = [
        "# LogSentinel - Test Report",
        "",
        f"Generated: {datetime.now():%Y-%m-%d %H:%M}",
        "",
        "| Total | Passed | Failed | Errors | Skipped | Pass rate (executed) | Duration |",
        "|---|---|---|---|---|---|---|",
        f"| {len(cases)} | {counts['passed']} | {counts['failed']} | {counts['error']} "
        f"| {counts['skipped']} | {pass_rate:.1%} | {total_time:.1f} s |",
        "",
        "## Test cases",
        "",
        "| # | Module | Test | Result | Time (s) |",
        "|---|---|---|---|---|",
    ]
    for i, (module, name, result, seconds) in enumerate(rows, start=1):
        lines.append(f"| {i} | `{module}` | `{name}` | {icon[result]} | {seconds:.2f} |")
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(lines) + "\n")
    print(f"Wrote {REPORT}: {counts}")
    return 1 if counts["failed"] or counts["error"] else 0


if __name__ == "__main__":
    sys.exit(main())
