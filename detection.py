"""
Anomaly-detection logic for the distributed traffic monitoring system.

Two complementary detectors live here:

* ``LocalDetector``       - runs inside every monitoring agent. It only sees the
  traffic of the network segment that the agent observes and compares it against
  fixed, configurable thresholds. It represents the "local-only" detection mode
  (each silo decides on its own, without global context).

* ``DistributedDetector`` - runs inside the central server. It correlates the
  aggregated view of ALL agents and applies global rules: aggregate thresholds,
  multi-agent coordination, and a statistical z-score spike detector.

Both detectors are pure (no I/O) so they are trivially unit-testable.
Every threshold can be overridden through environment variables (TM_*) or by
passing a custom ``DetectionConfig``.
"""
from __future__ import annotations

import math
import os
from collections import deque
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

STATUS_OK = "OK"
STATUS_FLAGGED = "FLAGGED"

LEVEL_NORMAL = "NORMAL"
LEVEL_WARNING = "WARNING"
LEVEL_CRITICAL = "CRITICAL"

SEVERITY_ORDER = {LEVEL_WARNING: 1, LEVEL_CRITICAL: 2}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


@dataclass
class DetectionConfig:
    """All tunable thresholds. Every value can be overridden via env vars."""

    # --- per-agent (local) thresholds --------------------------------------
    local_rate: float = _env_float("TM_LOCAL_RATE", 25.0)          # req/s per agent
    local_bytes: float = _env_float("TM_LOCAL_BYTES", 280_000.0)   # bytes/s per agent
    local_conns: float = _env_float("TM_LOCAL_CONNS", 8.0)         # in-flight per agent
    local_error: float = _env_float("TM_LOCAL_ERROR", 0.30)        # share of failed
    local_debounce: int = _env_int("TM_LOCAL_DEBOUNCE", 2)         # consecutive hits

    # --- global (central) thresholds ---------------------------------------
    global_rate: float = _env_float("TM_GLOBAL_RATE", 75.0)        # req/s aggregate
    global_bytes: float = _env_float("TM_GLOBAL_BYTES", 1_000_000.0)
    global_conns: float = _env_float("TM_GLOBAL_CONNS", 15.0)

    # --- distributed rules --------------------------------------------------
    coordination_min_agents: int = _env_int("TM_COORD_MIN_AGENTS", 2)
    zscore_window: int = _env_int("TM_ZSCORE_WINDOW", 30)          # samples (1 Hz)
    zscore_min_history: int = _env_int("TM_ZSCORE_MIN_HISTORY", 12)
    zscore_trigger: float = _env_float("TM_ZSCORE_TRIGGER", 3.0)


# ---------------------------------------------------------------------------
# Local (per-agent) detection
# ---------------------------------------------------------------------------

@dataclass
class LocalVerdict:
    status: str                 # OK | FLAGGED
    flags: List[str]            # human readable, e.g. "RATE 31.2 > 25.0 req/s"
    confidence: float           # 0..1
    streak: int                 # consecutive evaluations with flags


class LocalDetector:
    """Threshold-based detector working on a single agent's own window stats.

    A short debounce (default: 2 consecutive evaluations) prevents flicker
    caused by transient traffic jitter, so an agent only reports itself as
    FLAGGED once the anomaly is sustained.
    """

    def __init__(self, config: Optional[DetectionConfig] = None) -> None:
        self.cfg = config or DetectionConfig()
        self._streak = 0

    def reset(self) -> None:
        self._streak = 0

    def evaluate(self, request_rate: float, bytes_rate: float,
                 active_conns: int, error_rate: float) -> LocalVerdict:
        cfg = self.cfg
        flags: List[str] = []
        if request_rate > cfg.local_rate:
            flags.append(f"RATE {request_rate:.1f} > {cfg.local_rate:.0f} req/s")
        if bytes_rate > cfg.local_bytes:
            flags.append(f"BANDWIDTH {bytes_rate / 1000:.0f} > {cfg.local_bytes / 1000:.0f} KB/s")
        if active_conns > cfg.local_conns:
            flags.append(f"CONNECTIONS {active_conns} > {cfg.local_conns:.0f}")
        if error_rate > cfg.local_error:
            flags.append(f"ERRORS {error_rate:.2f} > {cfg.local_error:.2f}")

        self._streak = self._streak + 1 if flags else 0
        flagged = self._streak >= cfg.local_debounce
        status = STATUS_FLAGGED if flagged else STATUS_OK
        confidence = min(1.0, round(0.30 * len(flags) + (0.40 if flagged else 0.0), 2))
        return LocalVerdict(status=status, flags=flags, confidence=confidence,
                            streak=self._streak)


# ---------------------------------------------------------------------------
# Distributed (central) detection
# ---------------------------------------------------------------------------

@dataclass
class RuleHit:
    rule: str                   # e.g. "COORDINATION"
    severity: str               # WARNING | CRITICAL
    message: str
    agents: List[str] = field(default_factory=list)
    value: float = 0.0


@dataclass
class Decision:
    level: str                  # NORMAL | WARNING | CRITICAL
    hits: List[RuleHit]
    zscore: Optional[float] = None


class DistributedDetector:
    """Central detector correlating the aggregated view of all agents.

    Rules
    -----
    R1  AGGREGATE_RATE           aggregate req/s above the global threshold
    R2  AGGREGATE_BANDWIDTH      aggregate bytes/s above the global threshold
    R3  AGGREGATE_CONNECTIONS    sum of in-flight connections above threshold
    R4  COORDINATION             >= N agents flagged locally at the same time
    R5  RATE_SPIKE               z-score of aggregate rate over a rolling
                                 window exceeds a statistical trigger
    """

    def __init__(self, config: Optional[DetectionConfig] = None) -> None:
        self.cfg = config or DetectionConfig()
        self._rate_history: deque = deque(maxlen=self.cfg.zscore_window + 1)

    def reset(self) -> None:
        self._rate_history.clear()

    def ingest(self, aggregate_rate: float, aggregate_bytes: float,
               aggregate_conns: float,
               flagged_agents: Sequence[str]) -> Decision:
        cfg = self.cfg
        self._rate_history.append(float(aggregate_rate))
        hits: List[RuleHit] = []

        # R1 - aggregate request rate ----------------------------------------
        if aggregate_rate > cfg.global_rate:
            ratio = aggregate_rate / cfg.global_rate
            severity = LEVEL_CRITICAL if ratio >= 1.5 else LEVEL_WARNING
            hits.append(RuleHit(
                rule="AGGREGATE_RATE", severity=severity, value=aggregate_rate,
                message=(f"Aggregate request rate {aggregate_rate:.1f} req/s exceeds "
                         f"global threshold {cfg.global_rate:.0f} req/s ({ratio:.2f}x)")))

        # R2 - aggregate bandwidth --------------------------------------------
        if aggregate_bytes > cfg.global_bytes:
            ratio = aggregate_bytes / cfg.global_bytes
            hits.append(RuleHit(
                rule="AGGREGATE_BANDWIDTH", severity=LEVEL_WARNING, value=aggregate_bytes,
                message=(f"Aggregate bandwidth {aggregate_bytes / 1e6:.2f} MB/s exceeds "
                         f"global threshold {cfg.global_bytes / 1e6:.2f} MB/s ({ratio:.2f}x)")))

        # R3 - aggregate active connections -----------------------------------
        if aggregate_conns > cfg.global_conns:
            hits.append(RuleHit(
                rule="AGGREGATE_CONNECTIONS", severity=LEVEL_WARNING, value=aggregate_conns,
                message=(f"Active connections {aggregate_conns:.0f} exceed global "
                         f"threshold {cfg.global_conns:.0f}")))

        # R4 - coordination across agents --------------------------------------
        if len(flagged_agents) >= cfg.coordination_min_agents:
            hits.append(RuleHit(
                rule="COORDINATION", severity=LEVEL_CRITICAL,
                value=float(len(flagged_agents)), agents=list(flagged_agents),
                message=(f"Coordinated behavior: {len(flagged_agents)} agents flagged "
                         f"simultaneously ({', '.join(flagged_agents)})")))

        # R5 - statistical spike (z-score over rolling window) -----------------
        zscore = self._spike_zscore()
        if zscore is not None and zscore >= cfg.zscore_trigger:
            hits.append(RuleHit(
                rule="RATE_SPIKE", severity=LEVEL_WARNING, value=zscore,
                message=(f"Statistical spike: aggregate rate z-score {zscore:.1f} over "
                         f"{cfg.zscore_window}s rolling window")))

        if any(h.severity == LEVEL_CRITICAL for h in hits):
            level = LEVEL_CRITICAL
        elif hits:
            level = LEVEL_WARNING
        else:
            level = LEVEL_NORMAL
        return Decision(level=level, hits=hits, zscore=zscore)

    def _spike_zscore(self) -> Optional[float]:
        """z-score of the current sample vs the previous rolling window."""
        cfg = self.cfg
        history = list(self._rate_history)[:-1]           # exclude current sample
        if len(history) < cfg.zscore_min_history:
            return None
        window = history[-cfg.zscore_window:]
        mean = sum(window) / len(window)
        variance = sum((v - mean) ** 2 for v in window) / len(window)
        std = max(math.sqrt(variance), 0.75)              # floor avoids div-by-zero
        current = self._rate_history[-1]
        return (current - mean) / std
