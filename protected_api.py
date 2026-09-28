"""
Protected API - the service that the monitoring system defends.

A small JSON API (FastAPI + uvicorn) with an observability middleware that
tracks, per simulated source IP:

* request rate (sliding 10s window)
* bytes served
* in-flight connections
* status-code distribution (including 429s produced by the built-in
  token-bucket rate limiter)

Simulated clients identify themselves with an ``X-Simulated-IP`` header so that
every agent appears as a distinct source address (on localhost they would
otherwise share 127.0.0.1). The authoritative counters are exposed on
``/internal/metrics`` for the central server to cross-check the agents' view.
"""
from __future__ import annotations

import argparse
import collections
import random
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

RATE_LIMIT_CAPACITY = 45.0
RATE_LIMIT_REFILL = 25.0            # tokens per second, per source IP
EXEMPT_PATHS = {"/", "/health", "/internal/metrics"}

app = FastAPI(title="protected-api", docs_url=None, redoc_url=None, openapi_url=None)


class TokenBucket:
    __slots__ = ("tokens", "updated")

    def __init__(self) -> None:
        self.tokens = RATE_LIMIT_CAPACITY
        self.updated = time.time()

    def allow(self) -> bool:
        now = time.time()
        self.tokens = min(RATE_LIMIT_CAPACITY,
                          self.tokens + (now - self.updated) * RATE_LIMIT_REFILL)
        self.updated = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class Registry:
    """In-memory traffic registry maintained by the ASGI middleware."""

    def __init__(self) -> None:
        self.start = time.time()
        self.inflight = 0
        self.events: collections.deque = collections.deque(maxlen=20_000)  # (ts, ip, bytes, status)
        self.status_totals: collections.Counter = collections.Counter()
        self.total_requests = 0
        self.buckets: dict[str, TokenBucket] = {}


REG = Registry()


def client_key(request: Request) -> str:
    return request.headers.get("x-simulated-ip") or (
        request.client.host if request.client else "unknown")


def _record(key: str, status: int, nbytes: int) -> None:
    now = time.time()
    REG.total_requests += 1
    REG.status_totals[str(status)] += 1
    REG.events.append((now, key, nbytes, status))


@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    key = client_key(request)
    if request.url.path not in EXEMPT_PATHS:
        bucket = REG.buckets.setdefault(key, TokenBucket())
        if not bucket.allow():
            _record(key, 429, 89)
            return JSONResponse({"detail": "rate limit exceeded"}, status_code=429)
    REG.inflight += 1
    try:
        response = await call_next(request)
        cl = response.headers.get("content-length")
        nbytes = int(cl) if cl and cl.isdigit() else 0
        _record(key, response.status_code, nbytes)
        return response
    finally:
        REG.inflight -= 1


# ---------------------------------------------------------------------------
# Endpoints (the "business" API being protected)
# ---------------------------------------------------------------------------

@app.get("/")
async def root():
    return {"service": "protected-api", "status": "ok"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/data")
async def api_data(count: int = 50):
    """Returns a JSON document large enough to produce measurable bandwidth."""
    count = max(1, min(count, 200))
    now = time.time()
    items = [{
        "id": i,
        "ts": now,
        "sensor": f"sensor-{i % 7}",
        "value": round(random.random() * 100.0, 3),
        "blob": "x" * 400,
    } for i in range(count)]
    return {"count": count, "items": items}


@app.post("/api/echo")
async def echo(request: Request):
    body = await request.body()
    return {"received": len(body), "echo": body[:2048].decode("utf-8", "replace")}


@app.get("/api/report")
async def report():
    return {"node": "dc-1", "load": round(random.uniform(0.15, 0.55), 3),
            "queue": random.randint(0, 40)}


@app.get("/api/slow")
async def slow(ms: int = 600):
    """Simulated slow endpoint - keeps connections open to exercise the
    active-connections metric."""
    ms = max(50, min(ms, 1000))
    await _sleep(ms / 1000.0)
    return {"slept_ms": ms}


async def _sleep(seconds: float) -> None:
    import asyncio
    await asyncio.sleep(seconds)


# ---------------------------------------------------------------------------
# Authoritative metrics (polled by the central server)
# ---------------------------------------------------------------------------

@app.get("/internal/metrics")
async def internal_metrics():
    now = time.time()
    while REG.events and REG.events[0][0] < now - 10.0:
        REG.events.popleft()
    n = len(REG.events)
    per_ip: collections.Counter = collections.Counter()
    status_counts: collections.Counter = collections.Counter()
    for _, ip, _, status in REG.events:
        per_ip[ip] += 1
        status_counts[str(status)] += 1
    return {
        "available": True,
        "uptime_s": round(now - REG.start, 1),
        "request_rate": round(n / 10.0, 2),
        "bytes_rate": round(sum(e[2] for e in REG.events) / 10.0, 1),
        "active_connections": REG.inflight,
        "total_requests": REG.total_requests,
        "status_counts_10s": dict(status_counts),
        "top_ips": [{"ip": ip, "rate": round(c / 10.0, 2)} for ip, c in per_ip.most_common(6)],
        "rate_limit": {"capacity": RATE_LIMIT_CAPACITY, "refill_per_s": RATE_LIMIT_REFILL},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Protected API service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
