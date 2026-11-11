# LogSentinel – Low-Level Design

Interactive API docs (generated from the code): **http://localhost:8000/docs**.
All `/api/v1/*` endpoints require the header `X-API-Key: <API_KEY from .env>`.
Every error has the shape `{"error": "<message>", "detail": "<optional>"}`.

## 1. Backend REST API (FastAPI, port 8000; also via nginx at https://localhost:8443)

### POST /api/v1/predict – analyse pasted logs
Request (`application/json`):
```json
{ "logs": "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-160... src: ...\n..." }
```
| Field | Type | Rules |
|---|---|---|
| `logs` | string | required, ≥ 1 char; ≤ 50,000 non-empty lines |

Response 200 (`PredictResponse`):
```json
{
  "summary": { "total_lines": 20, "parsed_lines": 20, "skipped_lines": 0, "sessions": 2,
               "anomalies": 1, "anomaly_rate": 0.5, "unseen_lines": 0, "unseen_ratio": 0.0,
               "latency_ms": 41.3 },
  "sessions": [
    { "block_id": "blk_-2918118818249673980", "n_lines": 7, "n_unseen": 0,
      "score": 0.9981, "risk": 0.9981, "threshold": 0.62, "is_anomaly": true,
      "top_events": [ { "template_id": 33, "template": "<IP>:Got exception while serving <BLK> to /<IP>:",
                        "count": 1, "contribution": 3.41 } ] }
  ],
  "model": { "name": "LogSentinel", "alias": "champion" }
}
```
Sessions are sorted by `score`, highest first. `risk` is the score mapped to 0–1 for display.

Errors: `401` missing/invalid API key · `413` more than 50,000 lines · `422` empty input, invalid
JSON, or no HDFS lines with a block ID (message includes an example line) · `503` model server
unavailable · `500` unexpected error.

### POST /api/v1/predict/file – analyse an uploaded file
`multipart/form-data` with field `file` (.log/.txt, ≤ 5 MB). Response and errors as above
(`413` if the file is larger than 5 MB, `422` if empty).

### GET /api/v1/model/info
200: `{ "available": true, "name": "LogSentinel", "alias": "champion", "version": "3", "run_id": "…",
"tags": {"git_commit": "…", "model_type": "lr"}, "metrics": {"lr_test_f1": 0.97, …}, "git_commit": "…" }`
or `{ "available": false, "error": "…" }`.

### GET /api/v1/pipeline/status
200: `{ "stages": [ { "stage": "parse", "status": "ok|failed|never_run",
"last_run": {stage, success, returncode, duration_seconds, started_at, finished_at, lines_per_second, command},
"metrics": { "reports/parse_stats.json": { … } } }, … ] }` – stages in `dvc.yaml` order.

### GET /api/v1/pipeline/runs?limit=30
200: `{ "stage_runs": [run records, newest first], "mlflow_runs": [ { "run_id", "run_name", "status",
"start_time", "end_time", "git_commit", "metrics": {"lr_test_f1", "lr_test_pr_auc", "iforest_test_f1", "iforest_test_pr_auc"} } ] }`

### GET /api/v1/pipeline/errors
200: `{ "failed_stages": […], "failed_mlflow_runs": […], "log_errors": ["<log line>", …] }`

### GET /api/v1/pipeline/benchmark
200: contents of `reports/benchmark.json`: `{ "lines": 11175629, "results": [ { "stage": "parse",
"cores": 8, "seconds": 19.7, "lines_per_second": 567291, "speedup": 3.76, "efficiency": 0.47 }, … ] }`

### POST /api/v1/feedback
Request: `{ "block_id": "blk_-123", "correct_label": "Normal" | "Anomaly", "comment": "≤ 500 chars" }`
→ 200 `{ "status": "saved" }` (appended to `feedback/feedback.jsonl`); `422` on invalid block ID/label.

### POST /api/v1/retrain
Request: `{ "reason": "≤ 300 chars" }` → 200 `{ "status": "queued", "request": "retrain_<ts>_manual.json" }`

### GET /api/v1/retrain/requests
200: `{ "pending": ["retrain_….json"], "processed": ["retrain_….json"] }`

### POST /api/v1/webhooks/alertmanager
Header `Authorization: Bearer <ALERT_WEBHOOK_TOKEN>`. Body: standard Alertmanager webhook payload.
Firing alerts with label `action="retrain"` → 200 `{ "status": "queued", "request": "…" }`;
otherwise `{ "status": "ignored" }`; `401` on wrong token.

### Operational endpoints (no API key)
| Endpoint | Response |
|---|---|
| `GET /health` | 200 `{"status": "ok"}` – process alive |
| `GET /ready` | 200 `{"status": "ready", "model_server": true}` or 503 `model_unavailable` |
| `GET /metrics` | Prometheus text format |

## 2. Model server API (MLflow scoring server, port 5001)

`POST /invocations`, `Content-Type: application/json`:
```json
{ "dataframe_split": { "columns": ["log"], "data": [["<raw line 1>"], ["<raw line 2>"]] } }
```
Response: `{ "predictions": [ { "block_id", "n_lines", "n_unseen", "score", "risk", "threshold",
"is_anomaly", "top_events" (JSON string) }, … ] }` – one element per block found.
`GET /ping` → 200 when the model is loaded.

## 3. pyfunc model internals (`src/model/logsentinel_model.py`)

| Step | Implementation |
|---|---|
| Load | `load_context`: drain3 state bytes → `MemoryBufferPersistence` → `TemplateMiner(build_config(**meta.drain3))`; `EventCountVectorizer.load`; `joblib.load(classifier)`; templates CSV |
| Parse | regex `^(\d{6}) (\d{6}) (\d+) (\w+) ([^:]+): (.*)$`; block IDs `blk_-?\d+` (deduplicated per line) |
| Match | `miner.match(content, "fallback")` → cluster ID or `-1` (unseen) |
| Features | per-block `Counter` of template IDs → vectorizer (unknown IDs → `__unseen__`) |
| Score | LR: `predict_proba[:,1]`; IF: `-score_samples` |
| Verdict | `score >= threshold` (threshold chosen on validation, max F1) |
| Explain | LR: standardised value × coefficient, top 5 positive among present events; IF: rarity `-log((df+1)/(n+1))` |

Artifacts: `drain3_state.bin`, `templates.csv`, `vectorizer.json`, `classifier.joblib`, `meta.json`;
code: `src/`; pinned pip requirements.

## 4. Pipeline stage I/O (`dvc.yaml`)

| Stage | Command | Inputs | Outputs |
|---|---|---|---|
| validate | `src.ingest.validate` | raw log, labels | `reports/validation.json` |
| parse | `src.parsing.parse` (Spark) | raw log | `data/processed/parsed/` (line_id, ts, pid, level, component, content, block_id) |
| split | `src.ingest.split` | labels | `data/processed/splits.csv` (BlockId, Label, split) |
| mine_templates | `src.parsing.mine_templates` | parsed, splits | `models/drain3/drain3_state.bin`, `templates.csv` |
| match | `src.parsing.match` (Spark) | parsed, drain3 state | `data/processed/matched/` (line_id, ts, block_id, template_id) |
| sessionize | `src.parsing.sessionize` (Spark) | matched, splits | `data/processed/sessions/` (block_id, counts, sequence, n_lines, n_unseen, label, split, …) |
| featurize | `src.features.featurize` | sessions | `data/processed/features/{train,val,test}.npz`, `models/features/vectorizer.json` |
| train | `src.training.train` | features, vectorizer | `models/model/{classifier.joblib, meta.json}`, `reports/train_metrics.json`, `reports/figures/training/` |
| package | `src.training.package` | all model artifacts | `models/pyfunc/`, registered model version, `reports/package.json` |

Every stage runs through `src.utils.run_stage`, which appends to `logs/pipeline_runs.jsonl` and pushes
`logsentinel_pipeline_stage_*` metrics to the Pushgateway.

## 5. Prometheus metrics

| Metric | Type | Labels |
|---|---|---|
| `logsentinel_http_requests_total` | counter | endpoint, method, status |
| `logsentinel_request_latency_seconds` | histogram | endpoint |
| `logsentinel_model_latency_seconds` | histogram | – |
| `logsentinel_lines_processed_total`, `_lines_skipped_total`, `_unseen_lines_total` | counter | – |
| `logsentinel_sessions_scored_total`, `_anomalies_total` | counter | – |
| `logsentinel_last_anomaly_rate`, `_last_unseen_ratio`, `_model_up` | gauge | – |
| `logsentinel_errors_total` | counter | error_type |
| `logsentinel_retrain_requests_total` | counter | source |
| `logsentinel_feedback_total` | counter | correct_label |
| `logsentinel_pipeline_stage_duration_seconds`, `_success`, `_last_run_timestamp_seconds`, `_lines_per_second` | gauge (pushed) | stage |

Alert rules: `monitoring/prometheus/alerts.yml` (UnseenTemplateDrift, HighErrorRate, ModelServerDown,
BackendDown, HighPredictLatency, PipelineStageFailed).

## 6. Code organisation (OO + functional)

* Functional, stateless stage scripts (`src/ingest`, `src/parsing`, `src/features/featurize.py`,
  `src/training`) – each a `main()` with pure helper functions that are unit-tested.
* Classes where state is essential: `EventCountVectorizer` (fit/transform/save/load),
  `LogSentinelModel` (MLflow `PythonModel`), `Settings` (backend config), Pydantic schemas.
