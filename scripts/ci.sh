#!/usr/bin/env bash
# Local CI gate - runs automatically before every `git push` (pre-commit pre-push hook)
# and can be run by hand:  bash scripts/ci.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

echo "== [1/5] Lint: ruff, black, isort"
ruff check .
black --check --quiet .
isort --check-only --quiet .

echo "== [2/5] Unit tests"
pytest tests/unit -q

echo "== [3/5] DVC pipeline definition is valid"
dvc dag > /dev/null

echo "== [4/5] DVC pipeline is up to date (code/data/params match dvc.lock)"
STATUS="$(dvc status 2>&1)"
echo "$STATUS"
if ! echo "$STATUS" | grep -q "up to date"; then
  echo "CI FAILED: pipeline is not up to date - run 'dvc repro' and commit dvc.lock"
  exit 1
fi

echo "== [5/5] Validation metrics passed"
python -c "import json,sys; sys.exit(0 if json.load(open('reports/validation.json'))['all_passed'] else 1)"

echo "CI PASSED"
