# Distributed Traffic Monitoring System

A working **defensive** monitoring system that detects *coordinated* abnormal
traffic coming from multiple simulated sources. Everything runs locally in
Python — no external services, no CDN dependencies.

```
                      metrics (every 2 s)        WebSocket snapshots (1 Hz)
  +-------------+  ------------------------->  +------------------+      +------------+
  |   agent     |   <------------------------- |  central server  | <--> | dashboard  |
  | edge-north  |      directive (ACK)        |  aggregation     | WS   | (browser)  |
  +-------------+                             |  distributed     |      +------------+
  +-------------+  metrics ^ / directive v    |  detection       |      +------------+
  |   agent     |  -------------------------> |  alerts          | <--> |  browser   |
  | edge-south  |                             +--------+---------+      +------------+
  +-------------+                                      | GET /internal/metrics (2 s)
  +-------------+                                      v
  |   agent     |      traffic (HTTP requests) +------------------+
  | edge-core   |  ==========================> |  protected API   |
  +-------------+                              |  (rate-limited)  |
                                               +------------------+
```

| Component          | File               | Default endpoint        | Role |
|--------------------|--------------------|-------------------------|------|
| Protected API      | `protected_api.py` | `http://127.0.0.1:8000` | The service being defended. Per-source-IP observability middleware, token-bucket rate limiter (429s), authoritative counters on `/internal/metrics`. |
| Central server     | `central_server.py`| `http://127.0.0.1:8001` | Aggregates agent metrics into 1 Hz series, runs distributed detection, stores alerts, manages scenarios, serves dashboard + WebSocket. |
| Monitoring agents  | `agent.py`         | (no listening port)     | Each agent simulates the client population of one network segment (unique `X-Simulated-IP`), measures its own traffic, runs a **local detector**, and reports every 2 s. |
| Dashboard          | `templates/`, `static/` | `http://127.0.0.1:8001/` | Real-time view: KPI cards, aggregate + per-agent charts, agent cards, alert feed, detection-comparison table. |
| Detection logic    | `detection.py`     | (library)               | Pure, unit-testable detector implementations. |
| Unit tests         | `tests/`           | (library)               | Tests for both detectors, runnable without pytest. |

## Quickstart

```bash
pip install -r requirements.txt
python run_all.py
# open the dashboard
#   http://127.0.0.1:8001/
```

`run_all.py` starts the protected API, the central server and 4 agents
(`edge-north`, `edge-south`, `edge-core`, `edge-exchange`), each with a slightly
different baseline rate (4.5 - 7 req/s). With the auto demo enabled (default)
the central server cycles through all four attack scenarios with quiet
cooldowns in between; you can also trigger any scenario manually from the
dashboard header. Use `--no-demo` for a purely manual experience.

Press `Ctrl+C` to stop the whole stack.

## Attack scenarios (demo scenarios)

| Scenario           | What the agents do | Local-only detectors | Centralized detector |
|--------------------|--------------------|----------------------|----------------------|
| **Single spike**   | One agent jumps to ~8x baseline (~56 req/s) | Flags that one agent (rate above 25 req/s) | `RATE_SPIKE` warning via z-score; correctly does **not** claim coordination |
| **Coordinated spike** | All agents jump to ~5x | 2 of 4 agents cross their local threshold | `COORDINATION` **critical** (>= 2 agents flagged simultaneously) + `AGGREGATE_RATE` critical |
| **Low & slow**     | Every agent ramps to ~20 req/s - **below** every local threshold | **Miss it entirely** | `AGGREGATE_RATE` + `RATE_SPIKE` - caught in the aggregate |
| **Flood**          | All agents attempt ~10x | All agents flag (rate + errors from 429s) | `AGGREGATE_RATE` critical + `COORDINATION` critical + errors/bandwidth |

The **Low & slow** row is the key demonstration: each silo looks normal in
isolation, so local-only detection is blind; the centralized view multiplies
the signals and catches the attack. The dashboard's *Detection comparison*
panel records this per event (verdicts: `both caught it`, `central only`,
`local only`, `missed by both`) together with the detection latency of each
mode.

## Detection rules

**Local (per agent, threshold-based, debounced over 2 evaluations):**

| Flag         | Default threshold        |
|--------------|--------------------------|
| `RATE`       | > 25 req/s               |
| `BANDWIDTH`  | > 280 KB/s               |
| `CONNECTIONS`| > 8 in-flight            |
| `ERRORS`     | > 30 % failed responses  |

**Central (correlated):**

| Rule                     | Trigger                                                    | Severity |
|--------------------------|------------------------------------------------------------|----------|
| `AGGREGATE_RATE`         | aggregate req/s > 75 (critical at >= 1.5x threshold)       | WARNING/CRITICAL |
| `AGGREGATE_BANDWIDTH`    | aggregate bytes/s > 1 MB/s                                 | WARNING  |
| `AGGREGATE_CONNECTIONS`  | sum of active connections > 15                             | WARNING  |
| `COORDINATION`           | >= 2 agents flagged locally at the same time               | CRITICAL |
| `RATE_SPIKE`             | z-score of aggregate rate > 3 over a 30 s rolling window   | WARNING  |

Active alerts auto-resolve after ~6 s without further hits. Every alert keeps
its peak metrics and is stored in the history shown on the dashboard.

### Configuring thresholds

Every threshold is configurable via CLI flags or environment variables:

| Setting                | CLI (via `run_all.py`)     | Env var              | Default   |
|------------------------|----------------------------|----------------------|-----------|
| Local rate threshold   | `--local-rate`             | `TM_LOCAL_RATE`      | 25 req/s  |
| Local bandwidth        | -                          | `TM_LOCAL_BYTES`     | 280 KB/s  |
| Local error ratio      | -                          | `TM_LOCAL_ERROR`     | 0.30      |
| Global rate threshold  | `--global-rate`            | `TM_GLOBAL_RATE`     | 75 req/s  |
| Global bandwidth       | -                          | `TM_GLOBAL_BYTES`    | 1 MB/s    |
| Global connections     | -                          | `TM_GLOBAL_CONNS`    | 15        |
| Coordination quorum    | -                          | `TM_COORD_MIN_AGENTS`| 2 agents  |
| Spike z-score trigger  | -                          | `TM_ZSCORE_TRIGGER`  | 3.0       |
| Spike window           | -                          | `TM_ZSCORE_WINDOW`   | 30 s      |

## Dashboard

- **KPI cards** - aggregate request rate, bandwidth, active connections, number
  of currently flagged agents.
- **Request rate chart** - 1 Hz aggregate series (thick cyan) plus one series
  per agent, with the global threshold as a dashed line and red background
  bands marking abnormal periods.
- **Protected API panel** - the authoritative server-side view (accepted rate,
  bandwidth, connections, HTTP 429 count in the last 10 s, top talkers), which
  cross-checks the agents' numbers; the difference between *attempted* (agent
  view) and *accepted* (server view) traffic becomes visible during floods.
- **Agent cards** - per-segment rate with sparkline, bandwidth, connections,
  error ratio, latency, local-detector status pill and the exact local flags.
- **Detection comparison** - live local vs central status, all-time counters,
  average detection latency, and a per-event verdict table.
- **Security alerts** - active alerts on top (rule, severity, message, involved
  agents, peak rate) followed by the resolved history.

Updates are pushed over a WebSocket at ~1 Hz; the connection pill in the header
shows `LIVE` / `RECONNECTING`.

## HTTP / WS API of the central server

| Endpoint                   | Method | Purpose |
|----------------------------|--------|---------|
| `/api/metrics`             | POST   | Agents submit `AgentReport` every 2 s; ACK carries an optional scenario `directive` |
| `/api/snapshot`            | GET    | Full state (same payload as the WebSocket pushes) |
| `/api/comparison`          | GET    | Local-vs-central comparison counters and event table |
| `/api/control/scenario`    | POST   | `{"scenario": "single_spike\|coordinated_spike\|low_slow\|flood"}` (disables auto demo) |
| `/api/control/auto`        | POST   | `{"enabled": true\|false}` - toggle the auto demo rotation |
| `/api/control/reset`       | POST   | Clear alerts, series, events and counters |
| `/ws`                      | WS     | Snapshot push every second |

## Running components individually

```bash
python protected_api.py --port 8000
python central_server.py --port 8001 --protected http://127.0.0.1:8000 --auto-demo
python agent.py --id edge-north --baseline-rate 6.0
```

Each agent accepts `--local-rate/--local-bytes/--local-conns/--local-error`
(so different segments can have different sensitivity) and `--simulated-ip`
(default: a stable pseudo-IP derived from the agent id, sent as
`X-Simulated-IP` so the protected API can rate-limit and rank sources
individually).

## Tests

```bash
python -m tests.test_detection     # dependency-free runner
# or
pytest tests/ -v
```

The tests cover: local threshold breach + debounce + recovery, the
coordination rule (fires at >= 2 agents, silent for 1), aggregate
warning/critical scaling, z-score spike detection (and its warm-up phase),
bandwidth/connection rules, and the low-and-slow story (local misses, central
catches).

## Implementation notes

- **FastAPI everywhere**: the whole stack is async (agents use `httpx.AsyncClient`),
  and the central server uses FastAPI's native WebSocket support for the
  real-time dashboard - the reason FastAPI was chosen over Flask.
- The **agent doubles as traffic generator and monitor** - the cleanest way to
  simulate "network segments" locally while keeping every number real (all
  metrics come from actual HTTP round-trips, nothing is faked).
- The protected API's **token-bucket rate limiter** (45 burst / 25 req/s refill
  per source) produces realistic 429 storms during floods, which feeds the
  error-rate signals.
- The **baseline EWMA** at the central server is only updated while no
  directive targets an agent, so scenario multipliers are computed against the
  agent's quiet-traffic level even if scenarios are triggered back-to-back.
- All state is in-memory and bounded (series: 180 samples, alerts: 200,
  events: 12) - it is a demo/teaching system, not a production SIEM.

## Troubleshooting

- **Port already in use** - pass `--api-port` / `--central-port` to `run_all.py`.
- **Agents show OFFLINE** - check the console output of the stack; the agents
  log failed reports (`report to central failed`).
- **No anomaly ever fires** - lower the thresholds, e.g.
  `python run_all.py --global-rate 60 --local-rate 20`.
- **Remote access** - components bind to `127.0.0.1` by default for safety;
  pass `--host 0.0.0.0` to expose them (only on trusted networks).
