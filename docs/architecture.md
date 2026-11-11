# LogSentinel – Architecture

GitHub renders the Mermaid diagrams below. To export a PNG for slides, paste a diagram into
https://mermaid.live and download it (or take a screenshot of the GitHub page).

## 1. System architecture (runtime)

```mermaid
flowchart LR
    user([User<br/>browser])

    subgraph compose["Docker Compose stack (local, no cloud)"]
        direction LR
        nginx["frontend<br/>nginx + static UI<br/>HTTPS :8443"]
        backend["backend<br/>FastAPI :8000<br/>validation · auth · metrics"]
        model["model<br/>mlflow models serve :5001<br/>LogSentinel@champion"]
        mlflow["mlflow<br/>tracking + registry :5000"]
        prom["prometheus :9090"]
        push["pushgateway :9091"]
        am["alertmanager :9093"]
        graf["grafana :3000"]
        cadv["cadvisor"]
        node["node-exporter"]
    end

    subgraph host["Host (developer machine)"]
        dvc["DVC pipeline<br/>Spark · drain3 · sklearn"]
        watcher["retrain_watcher.py"]
        repo[("Git repo<br/>+ DVC remote")]
    end

    user -- "HTTPS" --> nginx
    nginx -- "/api/* REST" --> backend
    nginx -- "/grafana/*" --> graf
    backend -- "REST /invocations" --> model
    backend -- "REST (runs, model info)" --> mlflow
    model -- "load model artifacts" --> mlflow
    prom -- "scrape /metrics" --> backend
    prom -- scrape --> push
    prom -- scrape --> cadv
    prom -- scrape --> node
    prom -- "alerts" --> am
    am -- "webhook (drift)" --> backend
    graf -- "PromQL" --> prom
    dvc -- "stage metrics" --> push
    dvc -- "runs, models" --> mlflow
    backend -. "retrain request file" .-> watcher
    watcher -- "dvc repro" --> dvc
    dvc <--> repo
```

| Block | Responsibility |
|---|---|
| **frontend (nginx)** | Serves the static UI over HTTPS, forwards `/api/*` to the backend and `/grafana/*` to Grafana. The UI only knows the backend through the configurable `API_BASE_URL`. |
| **backend (FastAPI)** | Input validation and limits, API-key auth, calls the model server, builds the response, pipeline console data (DVC stages, run history, errors, benchmark), feedback, retraining queue, Prometheus metrics. |
| **model (MLflow serve)** | Serves the registered pyfunc model `LogSentinel@champion`: raw lines → drain3 match → per-block counts → vectorizer → classifier → score + explanation. |
| **mlflow** | Experiment tracking (params, metrics, artifacts) and model registry (versions, `champion`/`challenger` aliases). Data in `./mlflow_data`. |
| **prometheus / pushgateway / alertmanager** | Metrics collection (pull from services, push from batch pipeline), alert rules, alert routing (drift → retraining webhook). |
| **grafana** | Near-real-time dashboards (5 s refresh), provisioned from files. |
| **cadvisor / node-exporter** | Per-container and host resource metrics, so every component is monitored (containers labelled by short Docker ID – Docker 29's containerd image store hides names from cAdvisor). |
| **DVC pipeline (host)** | validate → parse → split → mine_templates → match → sessionize → featurize → train → package; Spark for heavy stages. |
| **retrain_watcher (host)** | Turns retraining requests into `dvc repro` runs and restarts the model service. |

## 2. ML pipeline (DVC DAG)

```mermaid
flowchart TD
    raw[(data/raw/HDFS_v1<br/>DVC-tracked)] --> validate
    validate --> parse["parse (Spark)"]
    validate --> split["split (stratified 70/10/20)"]
    parse --> mine["mine_templates (drain3, train only)"]
    split --> mine
    mine --> match["match (Spark broadcast)"]
    parse --> match
    match --> sessionize["sessionize (Spark)"]
    split --> sessionize
    sessionize --> featurize["featurize (vectorizer, train only)"]
    featurize --> train["train (LR + IF, MLflow)"]
    train --> package["package (pyfunc, register, promote)"]
    package --> registry[(MLflow registry<br/>LogSentinel@champion)]
```

## 3. Request flow (analyse logs)

```mermaid
sequenceDiagram
    actor U as User
    participant N as nginx (HTTPS)
    participant B as backend (FastAPI)
    participant M as model server
    U->>N: POST /api/v1/predict {logs}
    N->>B: forward
    B->>B: auth, size limits, keep HDFS lines with block IDs
    B->>M: POST /invocations {dataframe_split: log lines}
    M->>M: drain3 match → per-block counts → vectorizer → classifier
    M-->>B: per-block score, verdict, top events
    B->>B: sort, summary, Prometheus metrics
    B-->>U: sessions + summary (JSON)
```
