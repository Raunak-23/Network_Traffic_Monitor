"""
Simulated traffic-monitoring agent.

Each agent plays two roles at once:

1. Traffic generator - it drives a realistic mix of requests against the
   protected API (simulating the client population of one network segment,
   identified by a unique ``X-Simulated-IP`` header).

2. Local monitor     - it keeps rolling-window statistics (request rate,
   bandwidth, active connections, error rate, latency), runs the LOCAL
   detector against its own segment only, and periodically (default 2s)
   reports everything to the central monitoring server.

The report ACK may contain a traffic-shaping *directive* issued by the central
server (demo scenarios: single spike, coordinated spike, low-and-slow ramp,
flood). The agent translates the directive into a request-rate multiplier
locally, so no extra control channel is required.

Run standalone::

    python agent.py --id edge-north --baseline-rate 5.0
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import logging
import math
import random
import sys
import time
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx  # noqa: E402

from common.models import AgentReport  # noqa: E402
from detection import DetectionConfig, LocalDetector, STATUS_FLAGGED  # noqa: E402

TICK = 0.5            # traffic generation cadence (seconds)
WINDOW = 5.0          # rolling statistics window (seconds)

LOG = logging.getLogger("agent")

# endpoint mix: (method, path, weight)
ROUTES = [
    ("GET", "/api/data?count=50", 0.45),
    ("GET", "/api/report", 0.15),
    ("POST", "/api/echo", 0.15),
    ("GET", "/api/slow?ms=600", 0.25),
]


def poisson(lam: float) -> int:
    """Knuth sampler - keeps the agent dependency-free (no numpy)."""
    if lam <= 0:
        return 0
    lam = min(lam, 500.0)
    L = math.exp(-lam)
    k, p = 0, 1.0
    while True:
        p *= random.random()
        if p <= L:
            return k
        k += 1


def curve_multiplier(directive: dict | None, now: float, baseline: float) -> float:
    """Translate a directive into a request-rate multiplier at time *now*."""
    if not directive:
        return 1.0
    t = now - directive["started_at"]
    duration = directive["duration"]
    curve = directive["curve"]
    if t <= 0 or t >= duration:
        return 1.0
    if curve == "flood":
        return max(1.0, directive["peak_mult"])
    if curve == "ramp":
        frac = min(1.0, t / (duration * 0.6))
        target_abs = directive.get("target_rate_abs")
        if target_abs:
            # absolute-rate attack: self-calibrate against the agent's own
            # configured baseline so the ramp lands exactly on the target
            peak = max(1.2, target_abs / max(baseline, 0.5))
        else:
            peak = max(1.0, directive["peak_mult"])
        return 1.0 + (peak - 1.0) * frac
    # spike
    peak = max(1.0, directive["peak_mult"])
    up = min(1.0, t / 2.0)
    down = min(1.0, (duration - t) / 2.0)
    return 1.0 + (peak - 1.0) * up * down


class Agent:
    def __init__(self, args: argparse.Namespace) -> None:
        self.id = args.id
        self.api = args.api.rstrip("/")
        self.central = args.central.rstrip("/")
        self.baseline = args.baseline_rate
        self.report_interval = args.report_interval
        self.cfg = DetectionConfig(
            local_rate=args.local_rate,
            local_bytes=args.local_bytes,
            local_conns=args.local_conns,
            local_error=args.local_error,
        )
        self.detector = LocalDetector(self.cfg)
        self.client: httpx.AsyncClient | None = None
        self.window: collections.deque = collections.deque()    # (ts, bytes, is_error)
        self.latencies: collections.deque = collections.deque()  # (ts, seconds)
        self.total_requests = 0
        self.inflight = 0
        self.inflight_samples: collections.deque = collections.deque()  # (ts, gauge)
        self.directive: dict | None = None
        self.last_directive_event = None
        self.last_status = "OK"
        self._tasks: set[asyncio.Task] = set()
        self.ip = args.simulated_ip or self._derive_ip()

    def _derive_ip(self) -> str:
        h = zlib.crc32(self.id.encode())
        return f"10.{(h >> 8) % 50 + 10}.{h % 250 + 5}.{(h >> 4) % 250 + 3}"

    # -- traffic generation ---------------------------------------------------

    def _spawn_fire(self) -> None:
        task = asyncio.create_task(self.fire())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def fire(self) -> None:
        method, path, _ = random.choices(ROUTES, weights=[r[2] for r in ROUTES])[0]
        content = json.dumps({"ping": "x" * 900}) if method == "POST" else None
        self.inflight += 1
        self.total_requests += 1
        t0 = time.time()
        try:
            assert self.client is not None
            resp = await self.client.request(
                method, self.api + path, content=content,
                headers={"x-simulated-ip": self.ip})
            self.window.append((t0, len(resp.content), resp.status_code >= 400))
            self.latencies.append((t0, time.time() - t0))
        except Exception:
            self.window.append((t0, 0, True))
        finally:
            self.inflight -= 1

    async def traffic_loop(self) -> None:
        while True:
            now = time.time()
            self.inflight_samples.append((now, self.inflight))
            mult = curve_multiplier(self.directive, now, self.baseline)
            # low-and-slow attackers keep their rate deliberately smooth
            if self.directive and self.directive.get("curve") == "ramp":
                mult *= random.uniform(0.92, 1.08)
            else:
                mult *= random.uniform(0.85, 1.15)
            lam = self.baseline * max(0.0, mult) * TICK
            for _ in range(poisson(lam)):
                self._spawn_fire()
            await asyncio.sleep(TICK)

    # -- local monitoring -----------------------------------------------------

    def window_stats(self) -> tuple[float, float, float, float, int]:
        now = time.time()
        while self.window and self.window[0][0] < now - WINDOW:
            self.window.popleft()
        while self.latencies and self.latencies[0][0] < now - WINDOW:
            self.latencies.popleft()
        while self.inflight_samples and self.inflight_samples[0][0] < now - WINDOW:
            self.inflight_samples.popleft()
        n = len(self.window)
        rate = n / WINDOW
        bytes_rate = sum(w[1] for w in self.window) / WINDOW
        err_rate = (sum(1 for w in self.window if w[2]) / n) if n else 0.0
        lat_ms = (sum(l[1] for l in self.latencies) / len(self.latencies) * 1000.0) \
            if self.latencies else 0.0
        # window-smoothed mean of the in-flight gauge - far less bursty than a
        # single instantaneous sample, while sustained pressure still shows
        conns = (round(sum(s for _, s in self.inflight_samples) / len(self.inflight_samples))
                 if self.inflight_samples else 0)
        return rate, bytes_rate, err_rate, lat_ms, conns

    # -- reporting ------------------------------------------------------------

    async def report_loop(self) -> None:
        while True:
            rate, bytes_rate, err_rate, lat_ms, conns = self.window_stats()
            verdict = self.detector.evaluate(rate, bytes_rate, conns, err_rate)
            if verdict.status != self.last_status:
                if verdict.status == STATUS_FLAGGED:
                    LOG.warning("LOCAL DETECTOR FLAGGED: %s", "; ".join(verdict.flags))
                else:
                    LOG.info("local detector back to OK")
                self.last_status = verdict.status
            report = AgentReport(
                agent_id=self.id,
                timestamp=time.time(),
                window_seconds=WINDOW,
                request_rate=round(rate, 2),
                bytes_rate=round(bytes_rate, 1),
                active_connections=conns,
                error_rate=round(err_rate, 3),
                avg_latency_ms=round(lat_ms, 1),
                total_requests=self.total_requests,
                local_status=verdict.status,
                local_flags=verdict.flags,
                local_confidence=verdict.confidence,
            )
            try:
                assert self.client is not None
                resp = await self.client.post(self.central + "/api/metrics",
                                              json=report.model_dump())
                directive = resp.json().get("directive")
                if directive and directive.get("event_id") != self.last_directive_event:
                    self.last_directive_event = directive.get("event_id")
                    LOG.info("directive #%s: scenario=%s curve=%s peak=%.1fx for %.0fs",
                             directive.get("event_id"), directive["scenario"],
                             directive["curve"], directive["peak_mult"], directive["duration"])
                self.directive = directive
            except Exception as exc:  # central down - keep generating traffic
                LOG.warning("report to central failed: %s", exc)
                self.directive = None
            await asyncio.sleep(self.report_interval)


async def run_agent(args: argparse.Namespace) -> None:
    limits = httpx.Limits(max_connections=150, max_keepalive_connections=50)
    timeout = httpx.Timeout(connect=2.0, read=8.0, write=2.0, pool=8.0)
    agent = Agent(args)
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        agent.client = client
        LOG.info("agent up -> api=%s central=%s baseline=%.1f req/s simulated-ip=%s "
                 "(local thresholds: rate %.0f req/s, %.0f KB/s, err %.2f)",
                 agent.api, agent.central, agent.baseline, agent.ip,
                 agent.cfg.local_rate, agent.cfg.local_bytes / 1000, agent.cfg.local_error)
        try:
            await asyncio.gather(agent.traffic_loop(), agent.report_loop())
        except asyncio.CancelledError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulated traffic-monitoring agent")
    parser.add_argument("--id", default="edge-north")
    parser.add_argument("--api", default="http://127.0.0.1:8000",
                        help="base URL of the protected API")
    parser.add_argument("--central", default="http://127.0.0.1:8001",
                        help="base URL of the central monitoring server")
    parser.add_argument("--baseline-rate", type=float, default=5.0,
                        help="normal request rate (req/s) of the simulated clients")
    parser.add_argument("--report-interval", type=float, default=2.0)
    parser.add_argument("--simulated-ip", default=None)
    parser.add_argument("--local-rate", type=float, default=25.0)
    parser.add_argument("--local-bytes", type=float, default=280_000.0)
    parser.add_argument("--local-conns", type=float, default=8.0)
    parser.add_argument("--local-error", type=float, default=0.30)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=f"[agent:{args.id}] %(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S")
    try:
        asyncio.run(run_agent(args))
    except KeyboardInterrupt:
        LOG.info("stopped")


if __name__ == "__main__":
    main()
