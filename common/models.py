"""Shared message schemas exchanged between agents and the central server."""
from __future__ import annotations

import time
from typing import List, Optional

from pydantic import BaseModel, Field


class AgentReport(BaseModel):
    """Payload an agent POSTs to the central server every report interval."""

    agent_id: str
    timestamp: float = Field(default_factory=time.time)
    window_seconds: float = 5.0
    request_rate: float            # requests / second observed in the window
    bytes_rate: float              # response bytes / second observed in the window
    active_connections: int        # in-flight requests right now
    error_rate: float              # share of >=400 responses in the window
    avg_latency_ms: float
    total_requests: int            # lifetime attempt counter
    local_status: str = "OK"       # OK | FLAGGED  (local detector verdict)
    local_flags: List[str] = Field(default_factory=list)
    local_confidence: float = 0.0


class Directive(BaseModel):
    """Traffic-shaping instruction returned to agents inside the report ACK.

    Agents translate the directive into a request-rate multiplier using the
    named curve (spike / ramp / flood), so no extra round-trips are needed.
    ``peak_mult`` is customized per agent by the central server (e.g. for the
    low-and-slow scenario every agent receives a multiplier that lands it just
    below its own local threshold).
    """

    event_id: int
    scenario: str
    label: str
    started_at: float
    duration: float
    curve: str                     # spike | ramp | flood
    peak_mult: float = 1.0
    # Optional absolute request-rate target (req/s). Used by the "ramp"
    # (low-and-slow) curve: the agent self-calibrates the multiplier against
    # its own configured baseline, so it lands exactly on this rate.
    target_rate_abs: Optional[float] = None
