# LogSentinel – Anomaly detection in distributed system logs

End-to-end AIOps application: raw HDFS logs → Spark + drain3 parsing → per-block sessions →
Logistic Regression / Isolation Forest → explained verdicts in a web UI, with DVC, MLflow,
Prometheus/Grafana and Docker Compose. Everything runs locally (no cloud).

* Architecture: `docs/architecture.md` · HLD: `docs/HLD.md` · LLD (API): `docs/LLD.md`
* Test plan: `docs/test_plan.md` · Test report: `reports/tests/test_report.md`
* User manual: `docs/user_manual.md` · Design decisions: `docs/design_notes.md`

## Requirements

Ubuntu (or similar Linux), Python 3.14, Java 17 (for Spark), Docker + Docker Compose, ~30 GB free disk,
≥ 12 GB RAM.

## Setup

```bash
git clone <repo-url> LogSentinel && cd LogSentinel
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
# if pip refuses the drain3/cachetools pins: pip install drain3==0.9.11 --no-deps, then rerun with --no-deps
pre-commit install && pre-commit install --hook-type pre-push
dvc pull            # data + pipeline outputs from the DVC remote
```
The DVC remote is a local folder (`dvc remote list`). On another machine either point it at a shared
location (`dvc remote modify localremote url ssh://…`) or download HDFS_v1 from Loghub/Zenodo – the
`.dvc` hash verifies it is the same file.

## Run the ML pipeline

```bash
# MLflow tracking server (needed by train/package). Either the Docker service:
docker compose up -d mlflow
# ...or on the host: mlflow server --backend-store-uri sqlite:///mlflow_data/mlflow.db \
#                      --artifacts-destination ./mlflow_data/artifacts --port 5000
dvc repro           # runs only the stages whose code/data/params changed
dvc dag             # pipeline graph
dvc metrics show    # all stage metrics
```

## Run the application

```bash
bash scripts/setup_secrets.sh      # once: .env, TLS certificate, folders
# once: wheels for the MLflow image (installed offline - downloads inside Docker can be slow)
mkdir -p docker/wheels
pip download -r docker/requirements-mlflow.txt -d docker/wheels --only-binary=:all:
pip wheel drain3==0.9.11 --no-deps -w docker/wheels
docker compose up -d --build
docker compose ps                  # wait until services are healthy
python scripts/make_demo_logs.py   # demo/demo_logs.log: 40 complete, unseen test blocks
```

| URL | What |
|---|---|
| https://localhost:8443 | Web UI |
| http://localhost:8000/docs | REST API (Swagger) |
| http://localhost:5000 | MLflow |
| http://localhost:9090 | Prometheus |
| http://localhost:9093 | Alertmanager |
| https://localhost:8443/grafana/ | Grafana (also http://localhost:3000/grafana/) |

Retraining worker (host): `python scripts/retrain_watcher.py --restart-model`

## Tests

```bash
pytest tests/unit -v                     # unit
pytest -m "spark or model" -v            # integration
E2E=1 pytest tests/e2e -v                # end-to-end (stack running)
pytest --junitxml=reports/tests/junit.xml && python scripts/test_report.py
bash scripts/ci.sh                       # same checks as the pre-push hook
```

## Reproduce an experiment

Every MLflow run is tagged with `git_commit` and `dvc_sessions_md5`:
```bash
git checkout <git_commit> && dvc checkout && dvc repro
```
`mlflow run . -e train --env-manager local` runs a single entry point of the MLproject.

## Roll back the served model

```bash
python -c "from mlflow import MlflowClient as C; C('http://localhost:5000').set_registered_model_alias('LogSentinel','champion','<old-version>')"
docker compose restart model
```
