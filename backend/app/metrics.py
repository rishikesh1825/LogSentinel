"""Prometheus metrics exposed by the backend at GET /metrics."""

from prometheus_client import Counter, Gauge, Histogram

BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30)

HTTP_REQUESTS = Counter(
    "logsentinel_http_requests_total",
    "HTTP requests by endpoint, method and status code",
    ["endpoint", "method", "status"],
)
REQUEST_LATENCY = Histogram(
    "logsentinel_request_latency_seconds",
    "End-to-end request latency",
    ["endpoint"],
    buckets=BUCKETS,
)
MODEL_LATENCY = Histogram(
    "logsentinel_model_latency_seconds",
    "Latency of calls to the MLflow model server",
    buckets=BUCKETS,
)
LINES_PROCESSED = Counter("logsentinel_lines_processed_total", "Log lines received for analysis")
LINES_SKIPPED = Counter(
    "logsentinel_lines_skipped_total", "Lines skipped (not HDFS format or no block ID)"
)
UNSEEN_LINES = Counter(
    "logsentinel_unseen_lines_total", "Lines that matched no known template (drift signal)"
)
SESSIONS_SCORED = Counter("logsentinel_sessions_scored_total", "Sessions (blocks) scored")
ANOMALIES = Counter("logsentinel_anomalies_total", "Sessions flagged as anomalous")
LAST_ANOMALY_RATE = Gauge(
    "logsentinel_last_anomaly_rate", "Share of anomalous sessions in the last request"
)
LAST_UNSEEN_RATIO = Gauge(
    "logsentinel_last_unseen_ratio", "Share of unseen-template lines in the last request"
)
ERRORS = Counter("logsentinel_errors_total", "Errors by type", ["error_type"])
MODEL_UP = Gauge("logsentinel_model_up", "1 if the model server answers /ping, else 0")
RETRAIN_REQUESTS = Counter(
    "logsentinel_retrain_requests_total", "Retraining requests by source", ["source"]
)
FEEDBACK = Counter("logsentinel_feedback_total", "User feedback items", ["correct_label"])
