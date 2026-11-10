"""LogSentinel REST API (FastAPI).

The web frontend talks ONLY to this API; this API talks to the MLflow model server
over REST. Endpoints are documented in docs/LLD.md and live at /docs (Swagger UI).

Run locally:   uvicorn backend.app.main:app --reload --port 8000
"""

import asyncio
import json
import re
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import metrics as m
from . import model_client, pipeline_info
from .config import settings
from .logging_setup import get_logger
from .model_client import ModelServiceError
from .schemas import FeedbackRequest, PredictRequest, PredictResponse, RetrainRequest

logger = get_logger("backend")

LINE_RE = re.compile(r"^(\d{6}) (\d{6}) (\d+) (\w+) ([^:]+): (.*)$")
BLOCK_RE = re.compile(r"blk_-?\d+")
EXAMPLE_LINE = (
    "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 "
    "src: /10.250.19.102:54106 dest: /10.250.19.102:50010"
)


# ------------------------------------------------------------- lifespan
async def _ping_model_forever() -> None:
    """Keep the logsentinel_model_up gauge current for Prometheus alerts."""
    while True:
        m.MODEL_UP.set(1 if await run_in_threadpool(model_client.ping) else 0)
        await asyncio.sleep(settings.model_ping_interval_s)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Start the background model health check."""
    task = asyncio.create_task(_ping_model_forever())
    logger.info("LogSentinel API started; model server at %s", settings.model_url)
    yield
    task.cancel()


app = FastAPI(
    title="LogSentinel API",
    version="1.0.0",
    description="Anomaly detection for HDFS logs with per-session explanations.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key"],
)


# ------------------------------------------------------- middleware / errors
@app.middleware("http")
async def record_metrics(request: Request, call_next):
    """Count every request and time it, labelled by route template."""
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        m.ERRORS.labels("unhandled").inc()
        logger.exception("Unhandled error on %s", request.url.path)
        response = JSONResponse(status_code=500, content={"error": "Internal server error"})
    route = request.scope.get("route")
    endpoint = getattr(route, "path", None)
    if endpoint is None:  # fall back to the raw path, but only for known routes
        known = {getattr(r, "path", "") for r in app.routes}
        endpoint = request.url.path if request.url.path in known else "other"
    m.HTTP_REQUESTS.labels(endpoint, request.method, str(response.status_code)).inc()
    m.REQUEST_LATENCY.labels(endpoint).observe(time.perf_counter() - start)
    return response


@app.exception_handler(ModelServiceError)
async def model_error_handler(_request: Request, exc: ModelServiceError):
    """The model server is down or failing -> 503 with a clear message."""
    m.ERRORS.labels("model_service").inc()
    logger.error("Model service error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={
            "error": "The anomaly model is not available right now. Please try again shortly.",
            "detail": str(exc)[:300],
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_request: Request, exc: RequestValidationError):
    """Malformed request body -> 422 with a readable message."""
    m.ERRORS.labels("validation").inc()
    first = exc.errors()[0] if exc.errors() else {}
    field = ".".join(str(p) for p in first.get("loc", []) if p != "body")
    return JSONResponse(
        status_code=422,
        content={"error": f"Invalid request: {first.get('msg', 'bad input')}", "detail": field},
    )


@app.exception_handler(HTTPException)
async def http_error_handler(_request: Request, exc: HTTPException):
    """Uniform error shape: {"error": ..., "detail": ...}."""
    if exc.status_code >= 400:
        m.ERRORS.labels(f"http_{exc.status_code}").inc()
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


# ----------------------------------------------------------------- auth
def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Every /api/v1 endpoint needs the X-API-Key header."""
    if x_api_key is None or not secrets.compare_digest(x_api_key, settings.api_key):
        raise HTTPException(status_code=401, detail="Missing or invalid API key")


def require_webhook_token(authorization: str | None = Header(default=None)) -> None:
    """Alertmanager authenticates with a bearer token."""
    expected = f"Bearer {settings.alert_webhook_token}"
    if authorization is None or not secrets.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook token")


# ------------------------------------------------------------ analysis
def analyze(text: str) -> dict:
    """Validate raw log text, score it with the model and build the response."""
    start = time.perf_counter()
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise HTTPException(status_code=422, detail="No log lines found in the input.")
    if len(lines) > settings.max_lines:
        limit = settings.max_lines
        raise HTTPException(
            status_code=413, detail=f"Too many lines ({len(lines):,}). The limit is {limit:,}."
        )

    usable = [ln for ln in lines if LINE_RE.match(ln.strip()) and BLOCK_RE.search(ln)]
    skipped = len(lines) - len(usable)
    m.LINES_PROCESSED.inc(len(lines))
    m.LINES_SKIPPED.inc(skipped)
    if not usable:
        raise HTTPException(
            status_code=422,
            detail=(
                "None of the lines look like HDFS log lines with a block ID. "
                f"Expected a line like: {EXAMPLE_LINE}"
            ),
        )

    sessions = model_client.predict(usable)
    sessions.sort(key=lambda s: s["score"], reverse=True)
    anomalies = sum(1 for s in sessions if s["is_anomaly"])
    unseen = sum(s["n_unseen"] for s in sessions)
    anomaly_rate = anomalies / len(sessions) if sessions else 0.0
    unseen_ratio = unseen / len(usable)

    m.SESSIONS_SCORED.inc(len(sessions))
    m.ANOMALIES.inc(anomalies)
    m.UNSEEN_LINES.inc(unseen)
    m.LAST_ANOMALY_RATE.set(anomaly_rate)
    m.LAST_UNSEEN_RATIO.set(unseen_ratio)

    return {
        "summary": {
            "total_lines": len(lines),
            "parsed_lines": len(usable),
            "skipped_lines": skipped,
            "sessions": len(sessions),
            "anomalies": anomalies,
            "anomaly_rate": round(anomaly_rate, 4),
            "unseen_lines": unseen,
            "unseen_ratio": round(unseen_ratio, 4),
            "latency_ms": round((time.perf_counter() - start) * 1000, 1),
        },
        "sessions": sessions,
        "model": {"name": settings.registered_model, "alias": settings.model_alias},
    }


# ------------------------------------------------------------- endpoints
@app.get("/health", tags=["ops"])
def health() -> dict:
    """Liveness: the API process is running."""
    return {"status": "ok"}


@app.get("/ready", tags=["ops"])
def ready() -> JSONResponse:
    """Readiness: the API can reach the model server."""
    up = model_client.ping()
    m.MODEL_UP.set(1 if up else 0)
    return JSONResponse(
        status_code=200 if up else 503,
        content={"status": "ready" if up else "model_unavailable", "model_server": up},
    )


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    """Prometheus scrape endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post(
    "/api/v1/predict",
    response_model=PredictResponse,
    dependencies=[Depends(require_api_key)],
    tags=["analysis"],
)
def predict(body: PredictRequest) -> dict:
    """Analyse pasted log text."""
    return analyze(body.logs)


@app.post(
    "/api/v1/predict/file",
    response_model=PredictResponse,
    dependencies=[Depends(require_api_key)],
    tags=["analysis"],
)
async def predict_file(file: UploadFile = File(...)) -> dict:
    """Analyse an uploaded .log / .txt file."""
    raw = await file.read(settings.max_upload_bytes + 1)
    if len(raw) > settings.max_upload_bytes:
        limit_mb = settings.max_upload_bytes // (1024 * 1024)
        raise HTTPException(status_code=413, detail=f"File too large. The limit is {limit_mb} MB.")
    if not raw:
        raise HTTPException(status_code=422, detail="The uploaded file is empty.")
    return await run_in_threadpool(analyze, raw.decode("utf-8", errors="replace"))


@app.get("/api/v1/model/info", dependencies=[Depends(require_api_key)], tags=["model"])
def model_info() -> dict:
    """Registered model version currently served."""
    return pipeline_info.model_info()


@app.get("/api/v1/pipeline/status", dependencies=[Depends(require_api_key)], tags=["pipeline"])
def pipeline_status() -> dict:
    """Every DVC stage with its latest run and metrics."""
    return {"stages": pipeline_info.stage_status()}


@app.get("/api/v1/pipeline/runs", dependencies=[Depends(require_api_key)], tags=["pipeline"])
def pipeline_runs(limit: int = 30) -> dict:
    """Pipeline stage runs and MLflow training runs, newest first."""
    return {
        "stage_runs": pipeline_info.run_records(limit=limit),
        "mlflow_runs": pipeline_info.mlflow_runs(limit=min(limit, 50)),
    }


@app.get("/api/v1/pipeline/errors", dependencies=[Depends(require_api_key)], tags=["pipeline"])
def pipeline_errors() -> dict:
    """Failed stages, failed MLflow runs and recent ERROR log lines."""
    return pipeline_info.recent_errors()


@app.get("/api/v1/pipeline/benchmark", dependencies=[Depends(require_api_key)], tags=["pipeline"])
def pipeline_benchmark() -> dict:
    """Spark throughput benchmark (lines/s vs cores)."""
    return pipeline_info.benchmark() or {"results": []}


@app.post("/api/v1/feedback", dependencies=[Depends(require_api_key)], tags=["analysis"])
def feedback(body: FeedbackRequest) -> dict:
    """Store user feedback on a verdict (JSON lines file)."""
    settings.feedback_file.parent.mkdir(parents=True, exist_ok=True)
    record = {**body.model_dump(), "received_at": datetime.now(timezone.utc).isoformat()}
    with settings.feedback_file.open("a") as f:
        f.write(json.dumps(record) + "\n")
    m.FEEDBACK.labels(body.correct_label).inc()
    return {"status": "saved"}


def _write_retrain_request(source: str, payload: dict) -> str:
    settings.retrain_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    name = f"retrain_{stamp}_{source}.json"
    record = {"source": source, "requested_at": stamp, **payload}
    (settings.retrain_dir / name).write_text(json.dumps(record, indent=2) + "\n")
    m.RETRAIN_REQUESTS.labels(source).inc()
    logger.info("Retraining requested by %s -> %s", source, name)
    return name


@app.post("/api/v1/retrain", dependencies=[Depends(require_api_key)], tags=["pipeline"])
def retrain(body: RetrainRequest) -> dict:
    """Queue a retraining request (picked up by scripts/retrain_watcher.py)."""
    return {"status": "queued", "request": _write_retrain_request("manual", body.model_dump())}


@app.get("/api/v1/retrain/requests", dependencies=[Depends(require_api_key)], tags=["pipeline"])
def retrain_requests() -> dict:
    """Pending and processed retraining requests."""
    pending = sorted(p.name for p in settings.retrain_dir.glob("retrain_*.json"))
    processed_dir = settings.retrain_dir / "processed"
    processed = sorted(p.name for p in processed_dir.glob("retrain_*.json"))[-20:]
    return {"pending": pending, "processed": processed[::-1]}


@app.post(
    "/api/v1/webhooks/alertmanager",
    dependencies=[Depends(require_webhook_token)],
    tags=["pipeline"],
)
def alertmanager_webhook(payload: dict) -> dict:
    """Alertmanager calls this when a drift alert fires -> queue retraining."""
    firing = [
        a["labels"].get("alertname", "unknown")
        for a in payload.get("alerts", [])
        if a.get("status") == "firing" and a.get("labels", {}).get("action") == "retrain"
    ]
    if not firing:
        return {"status": "ignored"}
    name = _write_retrain_request("alertmanager", {"alerts": firing})
    return {"status": "queued", "request": name}
