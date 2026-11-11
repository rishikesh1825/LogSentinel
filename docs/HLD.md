# LogSentinel – High-Level Design

## 1. Problem and goals

Distributed systems produce millions of unstructured log lines; operators cannot read them to spot
failures, so incidents are found late. LogSentinel reads raw HDFS logs, groups them into per-block
sessions, flags anomalous sessions and explains each verdict in plain language.

**Success metrics (acceptance criteria, see test_plan.md):** test F1 ≥ 0.90 and PR-AUC ≥ 0.90 for
the served model; p95 API latency < 2 s for 1,000 lines; pipeline throughput ≥ 3,500 lines/s
(EDA burst peak); full reproducibility from a Git commit + MLflow run ID; all unit tests pass.

## 2. Data

Loghub HDFS_v1: 11,175,629 lines (1.47 GB), 575,061 blocks, 2.93% anomalous, 38.7 hours.
Only the raw log and labels are used; Loghub's pre-computed features were deleted. Data is versioned
with DVC (local remote, no cloud). EDA findings and their design consequences are in
`docs/design_notes.md`.

## 3. Architecture

See `docs/architecture.md` (system diagram, DVC DAG, request sequence). Summary:

* **Offline (host):** a DVC pipeline of 9 stages; Spark (local mode, 8 cores) for parsing, matching
  and sessionizing; drain3 for template mining; scikit-learn for models; MLflow for tracking and the
  model registry.
* **Online (Docker Compose, 10 services):** nginx frontend (HTTPS) → FastAPI backend → MLflow model
  server; MLflow tracking server; Prometheus, Pushgateway, Alertmanager, Grafana, cAdvisor,
  node-exporter.

## 4. Key design choices and rationale

| Area | Choice | Why (evidence) | Alternatives considered |
|---|---|---|---|
| Session unit | HDFS block ID | Labels are per block; a block's events are its "story" | Fixed time windows (labels don't map to windows) |
| Split | Block-level, stratified 70/10/20, **before** template mining | Prevents leakage into templates, vocabulary and models; equal 2.93% anomaly ratio in all splits | Chronological split (more realistic, future work) |
| Parsing | drain3 with BLKS/BLK/IP/PATH/NUM masks, `sim_th` 0.85 | At 0.4 a 100%-anomalous event (`Interrupted.`) was merged with a normal one; 0.85 + PATH mask keeps them apart without fragmenting (52 templates) | Spell, IPLoM, regex-only templates |
| Distribution | Mine once on 1 core, broadcast frozen state, match in parallel (`mapInPandas`) | Drain is sequential/order-dependent; matching is read-only → parallel; 388k lines/s on 8 cores | Mine per partition and merge (inconsistent IDs) |
| Features | Event-count vector + `__unseen__` column | EDA: anomalies have missing/extra events; WARN-only rule fails (22.6% of normal blocks have WARN) | TF-IDF (param `use_tfidf`), sequence models (LSTM, future work) |
| Models | Logistic Regression (served) vs Isolation Forest | Supervised labels available; LR is fast, calibrated and explainable through coefficients; IF is an unsupervised baseline | Random forest / gradient boosting (less transparent) |
| Metrics | Precision, recall, F1, PR-AUC; threshold on validation | 97% accuracy is trivial on 3% positives | Accuracy, ROC-AUC alone |
| Packaging | One MLflow pyfunc model containing drain3 state + vectorizer + classifier + code | Parsing logic and model can never drift apart | Separate parser service |
| Serving | `mlflow models serve` behind a FastAPI backend | Model API standardised by MLflow; backend adds validation, auth, metrics, explanations | Model inside FastAPI process (tighter coupling) |
| Coupling | UI ↔ backend ↔ model only via configurable REST | Required by the rubric; services can be replaced or scaled independently | — |
| CI | DVC DAG (`dvc repro`) + pre-push hook (lint, unit tests, `dvc status`) | Only stages whose deps/params changed rerun; pushes are blocked if pipeline or tests are broken | Cloud CI (not allowed) |
| Monitoring | Prometheus (pull) + Pushgateway (batch pipeline) + cAdvisor/node-exporter + Grafana | Every component monitored; near-real-time 5 s dashboards | — |
| Drift | Unseen-template rate; alert > 1% → retraining | Baseline unseen rate on val/test is exactly 0%, so any rise is meaningful | Feature-distribution tests (KS/PSI) |
| Security | HTTPS, API key, webhook bearer token, secrets in `.env`, non-root containers | Rubric: encryption; local deployment | OAuth login (future) |
| Verdict display | Problem (score >= threshold) / **Borderline** (below threshold, risk >= 50%) / Normal | The validated threshold (0.9755) is strict; near-misses scored 0.967-0.975 - users should still look at them | Lower the threshold (more false alarms) |
| Docker builds | Wheels downloaded on the host, installed offline in the image | In-Docker downloads ran at ~40 kB/s; offline install takes ~40 s | Online `pip install` |

## 5. Reproducibility

Every training run is tagged in MLflow with the Git commit, a dirty-tree flag and the DVC hash of the
sessions data; `params.yaml` and `dvc.lock` are logged as artifacts. Reproduce any run with
`git checkout <commit> && dvc checkout && dvc repro`. Environments are pinned in `requirements*.txt`,
`python_env.yaml` (MLproject) and the Docker images.

## 6. Results

Test set: 115,013 blocks (3,367 anomalous), threshold chosen on validation (max F1).

| Model | Val PR-AUC | Test precision | Test recall | Test F1 | Test PR-AUC |
|---|---|---|---|---|---|
| Logistic Regression (served, C=10) | 0.9973 | 0.9955 | 0.9947 | **0.9951** | **0.9994** |
| Isolation Forest (best: 100 trees, 1,024 samples) | 0.1497 | 0.6228 | 0.2138 | 0.3184 | 0.1594 |

LR makes 15 false alarms and 18 misses on 115,013 test blocks. Isolation Forest fails because most
anomalies differ from normal blocks by a single extra event among ~19 - not easy to isolate in count
space - so the supervised model is served and IF is kept as the label-free baseline.

| Measure | Result |
|---|---|
| API latency (1,000 lines, backend -> model server) | 60 ms (target < 2 s) |
| Demo file of 40 complete unseen test blocks | 38/40 correct, 0 false alarms, 2 near-misses shown as *Borderline* |
| Spark parse / match throughput (8 cores) | ~477k / ~388k lines/s (target >= 3,500 lines/s) |
| Benchmark (1 -> 16 cores) | parse 5.6x, match 4.7x at 12 cores (`reports/figures/03_throughput_vs_cores.png`) |
| Unseen-template rate train / val / test | 0% / 0% / 0% (drift baseline) |
| Automated tests | 44/44 passed (`reports/tests/test_report.md`) |

## 7. Limitations and future work

Scores complete sessions only (a block cut off mid-lifecycle looks anomalous - the first 1,000 raw lines gave 98% "anomalies"); random (not chronological) split; HDFS-specific parsing regex; cAdvisor cannot resolve container names with Docker 29's containerd image store (containers are labelled by Docker ID); API key visible to the browser;
retraining reuses the same labelled data (new labelled data would be needed for real drift);
single-machine Spark. Future: sequence models (DeepLog-style), chronological evaluation, user
authentication, Kubernetes deployment.
