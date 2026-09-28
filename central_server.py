"""
Central monitoring server.

Responsibilities
----------------
* receive periodic metrics from every agent (POST /api/metrics)
* aggregate request rate, bandwidth and active connections in a 1 Hz series
* run the DISTRIBUTED detector (aggregate thresholds, coordination across
  agents, statistical spike detection)
* maintain a live alert store (active + resolved history)
* manage demo scenarios: directives are returned to the agents inside the
  report ACK, so the whole choreography works over the existing reporting path
* compare local-only vs centralized detection per scenario event
* push a full snapshot to the dashboard over WebSocket every second
* poll the protected API's authoritative counters as a cross-check

Run standalone::

    python central_server.py --port 8001 --auto-demo
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import sys
import time
from collections import deque
from pathlib import Path
from typing import Deque, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx  # noqa: E402
from fastapi import FastAPI, WebSocket, WebSocketDisconnect  # noqa: E402
from fastapi.responses import FileResponse, JSONResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from common.models import AgentReport, Directive  # noqa: E402
from detection import (  # noqa: E402
    LEVEL_CRITICAL,
    LEVEL_NORMAL,
    SEVERITY_ORDER,
    DetectionConfig,
    DistributedDetector,
)

BASE_DIR = Path(__file__).resolve().parent
LOG = logging.getLogger("central")

STALE_S = 6.0          # agent without reports for this long is considered offline
SERIES_MAX = 180       # 1 Hz series retention (3 minutes of history)
EVENT_GRACE = 4.0      # keep collecting detections after a directive expires
ALERT_GRACE = 6.0      # resolve an active alert after no hits for this long
AUTO_COOLDOWN = 10.0   # quiet time between auto-demo scenarios

SCENARIOS = {
    "single_spike": {"label": "Single-source spike", "curve": "spike",
                     "peak_mult": 8.0, "duration": 18.0, "agents": "one"},
    "coordinated_spike": {"label": "Coordinated spike (all agents)", "curve": "spike",
                          "peak_mult": 5.0, "duration": 18.0, "agents": "all"},
    "low_slow": {"label": "Low & slow distributed ramp", "curve": "ramp",
                 "duration": 26.0, "agents": "all", "target_rate": 20.0},
    "flood": {"label": "Full distributed flood", "curve": "flood",
              "peak_mult": 10.0, "duration": 14.0, "agents": "all"},
}
ROTATION = ["single_spike", "coordinated_spike", "low_slow", "flood"]


# NOTE: request-body models must live at module level. Together with
# `from __future__ import annotations`, FastAPI cannot resolve function-local
# model annotations via get_type_hints() and would mis-read them as query
# parameters (-> HTTP 422).
class ScenarioBody(BaseModel):
    scenario: str


class AutoBody(BaseModel):
    enabled: bool


class Alert:
    __slots__ = ("id", "rule", "severity", "message", "agents", "started_at",
                 "last_seen", "peak_rate", "peak_bytes", "resolved_at")

    def __init__(self, seq: int, rule: str, severity: str, message: str,
                 agents: List[str], ts: float, rate: float, bytes_rate: float) -> None:
        self.id = f"AL-{seq:04d}"
        self.rule = rule
        self.severity = severity
        self.message = message
        self.agents = agents
        self.started_at = ts
        self.last_seen = ts
        self.peak_rate = rate
        self.peak_bytes = bytes_rate
        self.resolved_at: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id, "rule": self.rule, "severity": self.severity,
            "message": self.message, "agents": self.agents,
            "started_at": self.started_at, "last_seen": self.last_seen,
            "peak_rate": self.peak_rate, "peak_bytes": self.peak_bytes,
            "resolved": self.resolved_at is not None,
            "resolved_at": self.resolved_at,
        }


class EventRecord:
    """One scenario event and how each detection mode performed on it."""

    def __init__(self, seq: int, scenario: str, label: str,
                 started_at: float, duration: float) -> None:
        self.id = seq
        self.scenario = scenario
        self.label = label
        self.started_at = started_at
        self.duration = duration
        self.local_latency: Optional[float] = None
        self.local_agents: set = set()
        self.central_latency: Optional[float] = None
        self.central_severity: Optional[str] = None
        self.central_rules: set = set()

    @property
    def active_until(self) -> float:
        return self.started_at + self.duration + EVENT_GRACE

    def to_dict(self) -> dict:
        local_detected = self.local_latency is not None
        central_detected = self.central_latency is not None
        if local_detected and central_detected:
            verdict = "both"
        elif central_detected:
            verdict = "central_only"
        elif local_detected:
            verdict = "local_only"
        else:
            verdict = "none"
        return {
            "id": self.id, "scenario": self.scenario, "label": self.label,
            "started_at": self.started_at, "duration": self.duration,
            "active": time.time() <= self.active_until,
            "local": {"detected": local_detected,
                      "latency_s": round(self.local_latency, 2) if local_detected else None,
                      "agents": sorted(self.local_agents)},
            "central": {"detected": central_detected,
                        "latency_s": round(self.central_latency, 2) if central_detected else None,
                        "severity": self.central_severity,
                        "rules": sorted(self.central_rules)},
            "verdict": verdict,
        }


def _zero_totals() -> dict:
    return {"events": 0, "central": 0, "local": 0, "central_only": 0,
            "local_latency_sum": 0.0, "local_latency_n": 0,
            "central_latency_sum": 0.0, "central_latency_n": 0}


class Central:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.cfg = DetectionConfig(global_rate=args.global_rate,
                                   global_bytes=args.global_bytes,
                                   global_conns=args.global_conns)
        self.display_local_rate = args.local_rate
        self.detector = DistributedDetector(self.cfg)

        self.agents: Dict[str, dict] = {}      # id -> {metrics, last_seen, history, baseline}
        self.series_t: Deque = deque(maxlen=SERIES_MAX)
        self.series_rate: Deque = deque(maxlen=SERIES_MAX)
        self.series_bytes: Deque = deque(maxlen=SERIES_MAX)
        self.series_conns: Deque = deque(maxlen=SERIES_MAX)
        self.series_flagged: Deque = deque(maxlen=SERIES_MAX)
        self.series_status: Deque = deque(maxlen=SERIES_MAX)
        self.series_agents: Dict[str, Deque] = {}

        self.alerts_active: Dict[str, Alert] = {}
        self.alerts_history: Deque = deque(maxlen=200)
        self._alert_seq = 0

        self.events: Deque = deque(maxlen=12)
        self.event_seq = 0
        self.active_directive: Optional[dict] = None
        self.current_event: Optional[EventRecord] = None

        self.auto_demo = args.auto_demo
        self._rotation_idx = 0
        self._next_auto_action = time.time() + 8.0

        self.protected: Optional[dict] = None
        self.http: Optional[httpx.AsyncClient] = None
        self.totals = _zero_totals()

    # ------------------------------------------------------------------ agents

    async def on_report(self, report: AgentReport) -> dict:
        now = time.time()
        rec = self.agents.setdefault(report.agent_id, {"history": deque(maxlen=120),
                                                       "baseline": report.request_rate})
        rec["metrics"] = report
        rec["last_seen"] = now
        rec["history"].append((now, report.request_rate))

        # learn the quiet-traffic baseline (only when no directive targets this
        # agent) - used to customize low-and-slow multipliers per agent
        if self.directive_for(report.agent_id) is None:
            rec["baseline"] = round(0.85 * rec["baseline"] + 0.15 * report.request_rate, 2)

        if report.agent_id not in self.series_agents:
            self.series_agents[report.agent_id] = deque([None] * len(self.series_t),
                                                        maxlen=SERIES_MAX)

        ev = self.current_event
        if ev and now <= ev.active_until and report.local_status == "FLAGGED":
            ev.local_agents.add(report.agent_id)
            if ev.local_latency is None:
                ev.local_latency = max(0.0, now - ev.started_at)
                LOG.info("event #%d: local-only detection by %s after %.1fs",
                         ev.id, report.agent_id, ev.local_latency)

        return {"ok": True, "directive": self.directive_for(report.agent_id)}

    def baseline_estimate(self, agent_id: str) -> float:
        rec = self.agents.get(agent_id)
        if not rec:
            return 5.0
        return max(0.5, float(rec.get("baseline", 5.0)))

    # -------------------------------------------------------------- directives

    def directive_for(self, agent_id: str) -> Optional[dict]:
        base = self.active_directive
        if not base:
            return None
        if base["target"] != "all" and agent_id not in base["targets"]:
            return None
        peak = base["peak_mult"]
        target_abs = None
        if base["curve"] == "ramp" and base.get("target_rate"):
            # the agent self-calibrates against its own configured baseline
            # (target_rate_abs); peak_mult stays as a dashboard-side estimate
            peak = round(max(1.5, base["target_rate"] / self.baseline_estimate(agent_id)), 2)
            target_abs = base["target_rate"]
        return Directive(
            event_id=base["event_id"], scenario=base["scenario"], label=base["label"],
            started_at=base["started_at"], duration=base["duration"],
            curve=base["curve"], peak_mult=peak,
            target_rate_abs=target_abs).model_dump()

    def issue_scenario(self, name: str) -> dict:
        spec = SCENARIOS[name]
        now = time.time()
        self.event_seq += 1
        ev = EventRecord(self.event_seq, name, spec["label"], now, spec["duration"])
        self.events.appendleft(ev)
        self.current_event = ev

        targets: List[str] = []
        if spec["agents"] == "one":
            online = [aid for aid, rec in self.agents.items()
                      if now - rec.get("last_seen", 0) <= STALE_S]
            pool = online or list(self.agents) or ["edge-north"]
            targets = [max(pool, key=self.baseline_estimate)]

        self.active_directive = {
            "event_id": ev.id, "scenario": name, "label": spec["label"],
            "started_at": now, "duration": spec["duration"], "curve": spec["curve"],
            "peak_mult": spec.get("peak_mult", 1.0),
            "target_rate": spec.get("target_rate"),
            "target": spec["agents"], "targets": targets,
        }
        LOG.warning("SCENARIO issued: %s (#%d, %.0fs)%s", name, ev.id, spec["duration"],
                    f" target={targets}" if targets else "")
        return self.active_directive

    def clear_scenario(self) -> None:
        self.active_directive = None

    # ------------------------------------------------------------------ 1 Hz tick

    async def ticker(self) -> None:
        while True:
            now = time.time()
            fresh = {aid: rec for aid, rec in self.agents.items()
                     if now - rec.get("last_seen", 0) <= STALE_S}
            agg_rate = sum(rec["metrics"].request_rate for rec in fresh.values())
            agg_bytes = sum(rec["metrics"].bytes_rate for rec in fresh.values())
            agg_conns = sum(rec["metrics"].active_connections for rec in fresh.values())
            flagged = [aid for aid, rec in fresh.items()
                       if rec["metrics"].local_status == "FLAGGED"]

            self.series_t.append(now)
            self.series_rate.append(round(agg_rate, 2))
            self.series_bytes.append(round(agg_bytes, 1))
            self.series_conns.append(int(agg_conns))
            self.series_flagged.append(len(flagged))

            decision = self.detector.ingest(agg_rate, agg_bytes, agg_conns, flagged)
            self.series_status.append(decision.level)

            for aid, dq in self.series_agents.items():
                if aid in fresh:
                    dq.append(round(fresh[aid]["metrics"].request_rate, 2))
                else:
                    dq.append(None)

            self._update_alerts(decision, now, agg_rate, agg_bytes)

            ev = self.current_event
            if decision.level != LEVEL_NORMAL and ev and now <= ev.active_until:
                if ev.central_latency is None:
                    ev.central_latency = max(0.0, now - ev.started_at)
                    LOG.info("event #%d: centralized detection after %.1fs (%s)",
                             ev.id, ev.central_latency, decision.level)
                if ev.central_severity != LEVEL_CRITICAL:
                    ev.central_severity = decision.level
                for hit in decision.hits:
                    ev.central_rules.add(hit.rule)

            self._auto_demo_step(now)
            self._finalize_events(now)
            await asyncio.sleep(1.0)

    # ------------------------------------------------------------------ alerts

    def _update_alerts(self, decision, now: float, agg_rate: float, agg_bytes: float) -> None:
        seen = {hit.rule: hit for hit in decision.hits}
        for rule, hit in seen.items():
            alert = self.alerts_active.get(rule)
            if alert:
                alert.last_seen = now
                alert.peak_rate = max(alert.peak_rate, agg_rate)
                alert.peak_bytes = max(alert.peak_bytes, agg_bytes)
                if SEVERITY_ORDER.get(hit.severity, 0) > SEVERITY_ORDER.get(alert.severity, 0):
                    alert.severity = hit.severity
                alert.message = hit.message
                alert.agents = hit.agents or alert.agents
            else:
                self._alert_seq += 1
                alert = Alert(self._alert_seq, rule, hit.severity, hit.message,
                              list(hit.agents), now, agg_rate, agg_bytes)
                self.alerts_active[rule] = alert
                self.alerts_history.appendleft(alert)
                LOG.warning("ALERT %s [%s] %s", alert.id, rule, hit.message)
        for rule in [r for r in self.alerts_active if r not in seen]:
            alert = self.alerts_active[rule]
            if now - alert.last_seen > ALERT_GRACE:
                alert.resolved_at = now
                del self.alerts_active[rule]
                LOG.info("alert %s [%s] resolved after %.0fs",
                         alert.id, rule, now - alert.started_at)

    # ------------------------------------------------------------- event lifecycle

    def _finalize_events(self, now: float) -> None:
        ev = self.current_event
        if ev and now > ev.active_until:
            self.current_event = None
            self.active_directive = None
            t = self.totals
            t["events"] += 1
            central_detected = ev.central_latency is not None
            local_detected = ev.local_latency is not None
            if central_detected:
                t["central"] += 1
                t["central_latency_sum"] += ev.central_latency
                t["central_latency_n"] += 1
            if local_detected:
                t["local"] += 1
                t["local_latency_sum"] += ev.local_latency
                t["local_latency_n"] += 1
            if central_detected and not local_detected:
                t["central_only"] += 1
            LOG.info("event #%d (%s) closed -> local:%s central:%s",
                     ev.id, ev.scenario,
                     "detected" if ev.local_latency is not None else "MISSED",
                     "detected" if ev.central_latency is not None else "MISSED")

    def _auto_demo_step(self, now: float) -> None:
        if not self.auto_demo:
            return
        if self.active_directive:
            if now >= self.active_directive["started_at"] + self.active_directive["duration"]:
                self.clear_scenario()
                self._next_auto_action = now + AUTO_COOLDOWN
            return
        if now >= self._next_auto_action:
            name = ROTATION[self._rotation_idx % len(ROTATION)]
            self._rotation_idx += 1
            self.issue_scenario(name)

    # ------------------------------------------------------------- protected poll

    async def protected_poller(self) -> None:
        while True:
            try:
                assert self.http is not None
                resp = await self.http.get(self.args.protected + "/internal/metrics",
                                           timeout=2.5)
                self.protected = resp.json()
            except Exception:
                self.protected = None
            await asyncio.sleep(2.0)

    # ------------------------------------------------------------------ snapshot

    def current_level(self) -> str:
        if any(a.severity == LEVEL_CRITICAL for a in self.alerts_active.values()):
            return LEVEL_CRITICAL
        if self.alerts_active:
            return "WARNING"
        return LEVEL_NORMAL

    def snapshot(self) -> dict:
        now = time.time()
        agents_out = []
        for aid in sorted(self.agents):
            rec = self.agents[aid]
            m: AgentReport = rec["metrics"]
            age = now - rec.get("last_seen", 0)
            online = age <= STALE_S
            agents_out.append({
                "id": aid, "online": online, "age_s": round(age, 1),
                "rate": m.request_rate, "bytes_rate": m.bytes_rate,
                "conns": m.active_connections, "error_rate": m.error_rate,
                "latency_ms": m.avg_latency_ms, "total_requests": m.total_requests,
                "status": m.local_status if online else "OFFLINE",
                "flags": m.local_flags if online else [],
                "confidence": m.local_confidence,
                "baseline_estimate": round(self.baseline_estimate(aid), 2),
            })
        t = self.totals
        comparison = {
            "events_total": t["events"], "local_detections": t["local"],
            "central_detections": t["central"], "central_only_catches": t["central_only"],
            "avg_local_latency_s": round(t["local_latency_sum"] / t["local_latency_n"], 2)
            if t["local_latency_n"] else None,
            "avg_central_latency_s": round(t["central_latency_sum"] / t["central_latency_n"], 2)
            if t["central_latency_n"] else None,
        }
        active = sorted(self.alerts_active.values(),
                        key=lambda a: (SEVERITY_ORDER.get(a.severity, 0), -a.started_at),
                        reverse=True)
        current = None
        if self.active_directive:
            d = self.active_directive
            current = {"scenario": d["scenario"], "label": d["label"],
                       "started_at": d["started_at"],
                       "ends_at": d["started_at"] + d["duration"],
                       "curve": d["curve"], "peak_mult": d["peak_mult"]}
        return {
            "ts": now, "system_status": self.current_level(),
            "agents": agents_out,
            "series": {
                "t": list(self.series_t),
                "aggregate_rate": list(self.series_rate),
                "aggregate_bytes": list(self.series_bytes),
                "aggregate_conns": list(self.series_conns),
                "flagged_count": list(self.series_flagged),
                "status": list(self.series_status),
                "per_agent": {aid: list(dq) for aid, dq in self.series_agents.items()},
            },
            "thresholds": {"local_rate": self.display_local_rate,
                           "global_rate": self.cfg.global_rate,
                           "global_bytes": self.cfg.global_bytes,
                           "global_conns": self.cfg.global_conns},
            "protected": self.protected,
            "active_alerts": [a.to_dict() for a in active],
            "alerts_history": [a.to_dict() for a in list(self.alerts_history)[:25]],
            "alerts_count": {"active": len(self.alerts_active),
                             "total": len(self.alerts_history)},
            "comparison": comparison,
            "events": [ev.to_dict() for ev in list(self.events)[:10]],
            "current_scenario": current,
            "auto_demo": self.auto_demo,
        }

    def reset(self) -> None:
        self.active_directive = None
        self.current_event = None
        self.alerts_active.clear()
        self.alerts_history.clear()
        self.events.clear()
        self.detector.reset()
        self.totals = _zero_totals()
        for dq in (self.series_t, self.series_rate, self.series_bytes,
                   self.series_conns, self.series_flagged, self.series_status):
            dq.clear()
        for dq in self.series_agents.values():
            dq.clear()
        LOG.info("state reset")


def create_app(args: argparse.Namespace) -> FastAPI:
    central = Central(args)
    ws_clients: set = set()

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI):
        central.http = httpx.AsyncClient()
        tasks = [asyncio.create_task(central.ticker()),
                 asyncio.create_task(central.protected_poller()),
                 asyncio.create_task(broadcaster())]
        LOG.info("central server up (global thresholds: rate %.0f req/s, %.2f MB/s, "
                 "conns %.0f; coordination >= %d agents)",
                 central.cfg.global_rate, central.cfg.global_bytes / 1e6,
                 central.cfg.global_conns, central.cfg.coordination_min_agents)
        yield
        for task in tasks:
            task.cancel()
        await central.http.aclose()

    app = FastAPI(title="central-monitor", docs_url=None, redoc_url=None,
                  openapi_url=None, lifespan=lifespan)

    async def broadcaster() -> None:
        while True:
            if ws_clients:
                payload = json.dumps(central.snapshot())
                results = await asyncio.gather(
                    *[ws.send_text(payload) for ws in list(ws_clients)],
                    return_exceptions=True)
                for ws, res in zip(list(ws_clients), results):
                    if isinstance(res, Exception):
                        ws_clients.discard(ws)
            await asyncio.sleep(1.0)

    @app.post("/api/metrics")
    async def api_metrics(report: AgentReport):
        return await central.on_report(report)

    @app.get("/api/snapshot")
    async def api_snapshot():
        return JSONResponse(central.snapshot())

    @app.get("/api/comparison")
    async def api_comparison():
        snap = central.snapshot()
        return JSONResponse({"comparison": snap["comparison"],
                             "events": snap["events"]})

    @app.post("/api/control/scenario")
    async def api_control_scenario(body: ScenarioBody):
        if body.scenario not in SCENARIOS:
            return JSONResponse({"error": f"unknown scenario '{body.scenario}'",
                                 "available": sorted(SCENARIOS)}, status_code=400)
        central.auto_demo = False
        central.clear_scenario()
        directive = central.issue_scenario(body.scenario)
        return {"ok": True, "directive": directive}

    @app.post("/api/control/auto")
    async def api_control_auto(body: AutoBody):
        central.auto_demo = body.enabled
        if body.enabled:
            central._next_auto_action = time.time() + 3.0
            central.clear_scenario()
        return {"ok": True, "auto_demo": central.auto_demo}

    @app.post("/api/control/reset")
    async def api_control_reset():
        central.reset()
        return {"ok": True}

    @app.get("/")
    async def dashboard():
        return FileResponse(BASE_DIR / "templates" / "dashboard.html")

    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket):
        await websocket.accept()
        ws_clients.add(websocket)
        try:
            while True:
                await websocket.receive_text()  # keepalive pings from the client
        except WebSocketDisconnect:
            pass
        finally:
            ws_clients.discard(websocket)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Central traffic-monitoring server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--protected", default="http://127.0.0.1:8000")
    parser.add_argument("--global-rate", type=float, default=75.0,
                        help="aggregate req/s threshold")
    parser.add_argument("--global-bytes", type=float, default=1_000_000.0)
    parser.add_argument("--global-conns", type=float, default=15.0)
    parser.add_argument("--local-rate", type=float, default=25.0,
                        help="per-agent threshold, shown on the dashboard for reference")
    parser.add_argument("--auto-demo", action="store_true",
                        help="cycle through demo scenarios automatically")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="[central] %(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")

    import uvicorn
    uvicorn.run(create_app(args), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
