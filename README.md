# LogSentinel – Anomaly detection in distributed system logs

End-to-end AIOps application: raw HDFS logs → Spark + drain3 parsing → per-block sessions →
Logistic Regression / Isolation Forest → explained verdicts in a web UI, with DVC, MLflow,
Prometheus/Grafana and Docker Compose. Everything runs locally (no cloud).

* Architecture: `docs/architecture.md` · HLD: `docs/HLD.md` · LLD (API): `docs/LLD.md`
* Test plan: `docs/test_plan.md` · Test report: `reports/tests/test_report.md`
* User manual: `docs/user_manual.md` · Design decisions: `docs/design_notes.md`

## Project demonstration video

[Watch the LogSentinel project demonstration](https://drive.google.com/file/d/1ZScpYXvXHtyEHFTrMUZ_85VbHonAn_j4/view?usp=sharing)

## Repository structure

The repository is organised by responsibility. The main application code is under `src/`, `backend/` and `frontend/`; pipeline, deployment and monitoring configuration are kept in their own directories; and `docs/`, `reports/` and `tests/` contain the supporting documentation and verification material.

```text
LogSentinel/
├── .dvc/                        # DVC local metadata and cache
├── backend/                     # FastAPI application
│   ├── app/
│   │   ├── config.py            # Application configuration
│   │   ├── logging_setup.py     # Backend logging configuration
│   │   ├── main.py              # API routes and application entry point
│   │   ├── metrics.py           # Prometheus metrics exposed by the backend
│   │   ├── model_client.py      # Client used to call the MLflow model server
│   │   ├── pipeline_info.py     # Pipeline status and run information
│   │   └── schemas.py           # Request and response schemas
│   └── requirements.txt         # Backend-specific dependencies
│
├── certs/                       # Local TLS certificate files
├── data/
│   ├── raw/                     # Raw HDFS data and DVC-tracked data files
│   │   ├── HDFS_v1/             # Full HDFS_v1 dataset, including HDFS.log
│   │   └── HDFS_v1.dvc          # DVC pointer for the full dataset
│   ├── sample/                  # HDFS_2k.log and its DVC pointer
│   └── processed/               # Outputs produced by the data pipeline
│       ├── parsed/              # Parsed log records
│       ├── matched/             # Log records matched to Drain3 templates
│       ├── sessions/            # Per-block session data
│       ├── features/            # Train, validation and test feature matrices
│       └── splits.csv           # Dataset split information
│
├── demo/                        # demo_logs.log and demo_labels.csv for the UI demonstration
├── docker/                      # Dockerfiles and MLflow image dependencies
│   ├── Dockerfile.backend
│   ├── Dockerfile.frontend
│   ├── Dockerfile.mlflow
│   ├── requirements-mlflow.txt
│   └── wheels/                  # Locally prepared Python wheels for the MLflow image
│
├── docs/                        # Project and usage documentation
│   ├── architecture.md          # System architecture
│   ├── HLD.md                   # High-level design
│   ├── LLD.md                   # Low-level design and API details
│   ├── design_notes.md          # Design decisions and rationale
│   ├── test_plan.md             # Test plan and test cases
│   ├── user_manual.md           # Instructions for application users
│   ├── screenshots/             # Implementation and verification screenshots
│   ├── screenshots.zip          # Archive of the screenshots
│
├── frontend/                    # Browser-based application and Nginx configuration
│   ├── 40-config-js.sh          # Frontend runtime configuration helper
│   ├── nginx.conf.template      # Nginx configuration template
│   └── site/
│       ├── index.html           # Main UI page
│       ├── app.js               # Frontend application logic
│       ├── config.js            # Frontend configuration
│       ├── config.template.js   # Configuration template
│       └── styles.css           # UI styles
│
├── models/                      # Model and preprocessing artifacts
│   ├── drain3/                  # drain3_state.bin and mined templates.csv
│   ├── features/                # vectorizer.json feature vocabulary/configuration
│   ├── model/                   # classifier.joblib and meta.json
│   └── pyfunc/                  # Packaged MLflow PyFunc model, environment and artifacts
│
├── monitoring/                  # Monitoring and alerting configuration
│   ├── prometheus/              # Prometheus scrape and alert rules
│   ├── grafana/                 # Grafana dashboards and provisioning
│   └── alertmanager/            # Alertmanager configuration
│
├── notebooks/                   # Exploratory data analysis notebook
│   └── 01_eda.ipynb
│
├── reports/                     # Generated pipeline and model reports
│   ├── figures/                 # EDA, throughput and model evaluation plots
│   │   └── training/            # Confusion matrix, PR curve and coefficient plots
│   ├── tests/                   # test_report.md and junit.xml
│   ├── benchmark.json           # Throughput benchmark results
│   ├── train_metrics.json       # Model training and evaluation metrics
│   ├── validation.json          # Data validation results
│   └── *_stats*.json            # Stage-level pipeline statistics
│
├── scripts/                     # Utility, setup, CI and retraining scripts
│   ├── ci.sh                    # Local CI checks
│   ├── generate_traffic.py       # Traffic generation for monitoring tests
│   ├── make_demo_logs.py        # Creates the demo log file
│   ├── retrain_watcher.py       # Processes queued retraining requests
│   ├── setup_secrets.sh         # Creates local environment and secret files
│   └── test_report.py           # Generates the test summary report
│
├── src/                         # Core data and machine-learning pipeline code
│   ├── ingest/                  # validate.py, split.py: data checks and splitting
│   ├── parsing/                 # parse.py, mine_templates.py, match.py,
│   │                            # sessionize.py, drain_config.py, benchmark.py
│   ├── features/                # featurize.py and vectorizer.py
│   ├── training/                # train.py and package.py
│   ├── model/                   # logsentinel_model.py: model inference logic
│   └── utils/                   # config.py, logger.py, run_stage.py, spark.py
│
├── tests/                       # Automated test suite
│   ├── unit/                    # test_backend.py, test_drain_config.py,
│   │                            # test_model_helpers.py, test_split.py,
│   │                            # test_validate.py, test_vectorizer.py
│   ├── integration/              # test_spark_stages.py and test_pyfunc_model.py
│   ├── e2e/                     # test_stack.py
│   └── conftest.py              # Shared pytest fixtures and configuration
│
├── feedback/                    # Stored user feedback data
├── logs/                        # logsentinel.log and pipeline_runs.jsonl
├── mlflow_data/                 # Local MLflow database (mlflow.db) and artifacts
├── retrain_requests/             # Queued and processed retraining request records
│
├── README.md                     # Project overview, setup and usage instructions
├── docker-compose.yml            # Multi-service application definition
├── dvc.yaml                      # DVC pipeline stages and dependencies
├── dvc.lock                      # Locked pipeline dependency and output versions
├── params.yaml                   # Pipeline and model parameters
├── MLproject                     # MLflow project entry points
├── requirements.txt              # Main Python dependencies
├── requirements-dev.txt          # Development and testing dependencies
├── python_env.yaml               # Python environment definition
├── pyproject.toml                # Python project and tooling configuration
├── .dockerignore                 # Files excluded from Docker build contexts
├── .pre-commit-config.yaml       # Pre-commit and pre-push hooks
├── .dvcignore                    # Files excluded from DVC tracking
├── .gitignore                    # Files excluded from Git
└── .env.example                  # Example environment-variable configuration
```

### Where to look for common tasks

- **Change the data pipeline:** start with `dvc.yaml`, `params.yaml` and the relevant module under `src/`.
- **Change an API endpoint:** edit `backend/app/main.py`; request and response models are in `backend/app/schemas.py`.
- **Change the web interface:** edit files under `frontend/site/`.
- **Change model training or packaging:** use `src/training/` and `src/model/`; model artifacts are stored under `models/`.
- **Change monitoring or alert rules:** see `monitoring/prometheus/`, `monitoring/grafana/` and `monitoring/alertmanager/`.
- **Run or extend tests:** use `tests/unit/`, `tests/integration/` and `tests/e2e/`.
- **Read project documentation:** start with `docs/architecture.md`, `docs/HLD.md`, `docs/LLD.md` and `docs/user_manual.md`.

> **Note:** `data/processed/`, `models/`, `reports/`, `mlflow_data/`, `logs/` and `retrain_requests/` contain generated outputs or local runtime state. The `.dvc/` directory is DVC's local metadata/cache. Some local/runtime directories may not appear in the GitHub file listing. The actual `.env` file, TLS private keys and webhook tokens are local secrets; use `.env.example` as the configuration template and do not commit secret values.

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
