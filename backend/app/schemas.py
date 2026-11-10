"""Request / response models of the LogSentinel REST API (see docs/LLD.md)."""

from typing import Literal

from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    """Raw HDFS log text pasted by the user (one log line per line)."""

    logs: str = Field(..., min_length=1, description="Raw log lines separated by newlines")


class EventContribution(BaseModel):
    """One log event that pushed a session towards (or away from) 'anomaly'."""

    template_id: int
    template: str
    count: int
    contribution: float


class SessionResult(BaseModel):
    """Verdict for one block (session)."""

    block_id: str
    n_lines: int
    n_unseen: int
    score: float
    risk: float
    threshold: float
    is_anomaly: bool
    top_events: list[EventContribution]


class Summary(BaseModel):
    """Totals for one analysis request."""

    total_lines: int
    parsed_lines: int
    skipped_lines: int
    sessions: int
    anomalies: int
    anomaly_rate: float
    unseen_lines: int
    unseen_ratio: float
    latency_ms: float


class PredictResponse(BaseModel):
    """Response of /api/v1/predict and /api/v1/predict/file."""

    summary: Summary
    sessions: list[SessionResult]
    model: dict


class FeedbackRequest(BaseModel):
    """User feedback that a verdict was wrong (stored for later retraining)."""

    block_id: str = Field(..., pattern=r"^blk_-?\d+$")
    correct_label: Literal["Normal", "Anomaly"]
    comment: str = Field("", max_length=500)


class RetrainRequest(BaseModel):
    """Manual retraining request from the pipeline console."""

    reason: str = Field("manual request from pipeline console", max_length=300)
