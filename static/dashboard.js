/* Distributed Traffic Monitor - frontend logic
 * Real-time updates arrive over WebSocket (~1 Hz) as full snapshots.
 * Charts are rendered with a small dependency-free canvas engine. */
"use strict";

const $ = (id) => document.getElementById(id);

/* ---------------- formatting helpers ---------------- */
function fmtRate(v) { return v == null ? "-" : v.toFixed(1) + " req/s"; }
function fmtBytes(bps) {
  if (bps == null) return "-";
  if (bps >= 1e6) return (bps / 1e6).toFixed(2) + " MB/s";
  if (bps >= 1e3) return (bps / 1e3).toFixed(1) + " KB/s";
  return Math.round(bps) + " B/s";
}
function fmtCompact(v) {
  if (v >= 1e6) return (v / 1e6).toFixed(v >= 1e7 ? 0 : 1) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(v >= 1e4 ? 0 : 1) + "k";
  return Math.round(v).toString();
}
function fmtClock(ts) { return new Date(ts * 1000).toTimeString().slice(0, 8); }
function fmtAgo(s) { return s < 2 ? "now" : s < 60 ? Math.round(s) + "s ago" : Math.round(s / 60) + "m ago"; }
function last(arr) { return arr && arr.length ? arr[arr.length - 1] : null; }
function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

const AGENT_COLORS = ["#22d3ee", "#a78bfa", "#fbbf24", "#34d399", "#f472b6", "#60a5fa"];
function agentColor(i) { return AGENT_COLORS[i % AGENT_COLORS.length]; }

/* ---------------- canvas chart engine ---------------- */
class TSChart {
  constructor(canvas, opts) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d");
    this.opts = Object.assign({
      maxPoints: 180, padLeft: 46, padRight: 86, padTop: 10, padBottom: 18,
      fmtY: fmtCompact, thresholds: [], series: [],
    }, opts);
    this.t = [];
    this.status = [];
    this.data = {};
    if (typeof ResizeObserver !== "undefined") {
      new ResizeObserver(() => this.draw()).observe(canvas.parentElement);
    }
  }
  setSeriesDefs(defs) {
    this.opts.series = defs;
    this.data = {};
    defs.forEach((d) => { this.data[d.key] = []; });
  }
  setData(t, seriesMap, status) {
    const max = this.opts.maxPoints;
    this.t = (t || []).slice(-max);
    this.status = (status || []).slice(-max);
    for (const d of this.opts.series) {
      this.data[d.key] = (seriesMap[d.key] || []).slice(-max);
    }
    this.draw();
  }
  draw() {
    const c = this.canvas, ctx = this.ctx, o = this.opts;
    const dpr = window.devicePixelRatio || 1;
    const w = c.clientWidth || 600, h = c.clientHeight || 220;
    if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) {
      c.width = Math.round(w * dpr); c.height = Math.round(h * dpr);
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    const pl = o.padLeft, pr = o.padRight, pt = o.padTop, pb = o.padBottom;
    const iw = w - pl - pr, ih = h - pt - pb;
    if (iw < 10 || ih < 10) return;
    const n = this.t.length;

    let yMax = 0;
    for (const k in this.data) for (const v of this.data[k]) if (v != null && v > yMax) yMax = v;
    for (const th of o.thresholds) if (th.value > yMax) yMax = th.value;
    if (yMax <= 0) yMax = 10;
    yMax *= 1.15;

    const slot = iw / (o.maxPoints - 1);           // right-anchored scrolling
    const X = (i) => iw - (n - 1 - i) * slot;
    const Y = (v) => ih - (v / yMax) * ih;

    ctx.save();
    ctx.translate(pl, pt);

    // anomaly background bands (periods where system status != NORMAL)
    let s0 = null;
    ctx.fillStyle = "rgba(248,113,113,0.07)";
    for (let i = 0; i <= n; i++) {
      const bad = i < n && this.status[i] && this.status[i] !== "NORMAL";
      if (bad && s0 === null) s0 = i;
      if (!bad && s0 !== null) {
        const x0 = X(s0), x1 = X(i - 1) + slot;
        ctx.fillRect(x0, 0, Math.max(2, x1 - x0), ih);
        s0 = null;
      }
    }

    // grid + y labels
    ctx.font = "10px ui-monospace, Menlo, Consolas, monospace";
    ctx.strokeStyle = "rgba(148,163,184,0.13)";
    ctx.fillStyle = "rgba(148,163,184,0.8)";
    ctx.lineWidth = 1;
    for (let g = 0; g <= 4; g++) {
      const gy = Math.round((ih * g) / 4) + 0.5;
      ctx.beginPath(); ctx.moveTo(0, gy); ctx.lineTo(iw, gy); ctx.stroke();
      ctx.textAlign = "right";
      ctx.fillText(o.fmtY(yMax * (1 - g / 4)), -7, gy + 3);
    }

    // threshold lines
    for (const th of o.thresholds) {
      const ty = Y(Math.min(th.value, yMax));
      ctx.save();
      ctx.setLineDash([5, 4]);
      ctx.strokeStyle = th.color;
      ctx.beginPath(); ctx.moveTo(0, ty); ctx.lineTo(iw, ty); ctx.stroke();
      ctx.restore();
      ctx.fillStyle = th.color;
      ctx.textAlign = "left";
      ctx.fillText(th.label, iw + 6, ty - 4);
    }

    // series (area fill first, then stroke)
    for (const d of o.series) {
      const arr = this.data[d.key];
      if (!arr) continue;
      if (d.fillColor) {
        ctx.beginPath();
        let started = false, firstX = null, lastX = null;
        for (let i = 0; i < n; i++) {
          const v = arr[i];
          if (v == null) continue;
          const px = X(i), py = Y(v);
          if (!started) { ctx.moveTo(px, py); started = true; firstX = px; }
          else ctx.lineTo(px, py);
          lastX = px;
        }
        if (started) {
          ctx.lineTo(lastX, ih); ctx.lineTo(firstX, ih); ctx.closePath();
          ctx.fillStyle = d.fillColor; ctx.fill();
        }
      }
      ctx.beginPath();
      let pen = false;
      for (let i = 0; i < n; i++) {
        const v = arr[i];
        if (v == null) { pen = false; continue; }
        const px = X(i), py = Y(v);
        if (!pen) { ctx.moveTo(px, py); pen = true; } else ctx.lineTo(px, py);
      }
      ctx.strokeStyle = d.color;
      ctx.lineWidth = d.width || 1.3;
      ctx.globalAlpha = d.alpha || 1;
      ctx.lineJoin = "round";
      ctx.stroke();
      ctx.globalAlpha = 1;

      if (d.main) {  // last-value dot + label on the right edge
        for (let i = n - 1; i >= 0; i--) {
          if (arr[i] != null) {
            const px = X(i), py = Y(arr[i]);
            ctx.fillStyle = d.color;
            ctx.beginPath(); ctx.arc(px, py, 3, 0, Math.PI * 2); ctx.fill();
            ctx.font = "bold 11px ui-monospace, Menlo, monospace";
            ctx.textAlign = "left";
            ctx.fillText(o.fmtY(arr[i]), iw + 6, py + 4);
            break;
          }
        }
      }
    }

    // time hints
    if (n > 1) {
      ctx.fillStyle = "rgba(148,163,184,0.55)";
      ctx.font = "10px ui-monospace, monospace";
      ctx.textAlign = "left";
      ctx.fillText(fmtClock(this.t[0]), 0, ih + 13);
      ctx.textAlign = "right";
      ctx.fillText(fmtClock(this.t[n - 1]), iw, ih + 13);
    }
    ctx.restore();
  }
}

class Spark {
  constructor(canvas, color) {
    this.canvas = canvas; this.ctx = canvas.getContext("2d");
    this.color = color; this.vals = [];
  }
  push(v) {
    if (v == null) return;
    this.vals.push(v);
    if (this.vals.length > 40) this.vals.shift();
    this.draw();
  }
  draw() {
    const c = this.canvas, ctx = this.ctx, dpr = window.devicePixelRatio || 1;
    const w = 96, h = 26;
    if (c.width !== w * dpr) { c.width = w * dpr; c.height = h * dpr; c.style.width = w + "px"; c.style.height = h + "px"; }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    const vals = this.vals;
    if (vals.length < 2) return;
    const min = Math.min(...vals), max = Math.max(...vals);
    const span = Math.max(max - min, 0.001);
    ctx.beginPath();
    vals.forEach((v, i) => {
      const x = (i / (vals.length - 1)) * (w - 2) + 1;
      const y = h - 2 - ((v - min) / span) * (h - 5);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = this.color;
    ctx.lineWidth = 1.4;
    ctx.stroke();
  }
}

/* ---------------- application state ---------------- */
const state = { snap: null, ws: null, wsOpen: false, charts: {}, agentEls: new Map(), agentKey: "" };

function setConn(up) {
  state.wsOpen = up;
  const pill = $("connPill");
  pill.textContent = up ? "LIVE" : "RECONNECTING";
  pill.className = "conn-pill " + (up ? "live" : "down");
}

function setStatusBadge(level) {
  const b = $("statusBadge");
  b.textContent = level;
  b.className = "status-badge " + level.toLowerCase();
}

/* ---------------- renderers ---------------- */
function renderKpis(s) {
  const rate = last(s.series.aggregate_rate);
  const bytes = last(s.series.aggregate_bytes);
  const conns = last(s.series.aggregate_conns);
  const flagged = last(s.series.flagged_count);
  $("kpiRate").textContent = rate == null ? "-" : rate.toFixed(1);
  $("kpiRate").style.color = rate > s.thresholds.global_rate ? "var(--crit)" : "var(--accent)";
  $("kpiRateSub").textContent = `global threshold ${s.thresholds.global_rate} req/s`;
  $("kpiBytes").textContent = fmtBytes(bytes);
  $("kpiBytes").style.color = bytes > s.thresholds.global_bytes ? "var(--warn)" : "var(--accent)";
  $("kpiBytesSub").textContent = `global threshold ${fmtBytes(s.thresholds.global_bytes)}`;
  $("kpiConns").textContent = conns == null ? "-" : conns;
  $("kpiConnsSub").textContent = `global threshold ${s.thresholds.global_conns}`;
  $("kpiFlagged").textContent = flagged == null ? "-" : flagged;
  $("kpiFlagged").style.color = flagged > 0 ? "var(--crit)" : "var(--text)";
  $("kpiFlaggedSub").textContent = `coordination rule fires at >= 2`;
}

function renderScenario(s) {
  const banner = $("scenarioBanner");
  if (s.current_scenario) {
    banner.hidden = false;
    $("scenarioText").textContent = "Scenario running: " + s.current_scenario.label;
    const rem = Math.max(0, s.current_scenario.ends_at - s.ts);
    $("scenarioCountdown").textContent = rem.toFixed(0) + "s left";
  } else {
    banner.hidden = true;
  }
  const ab = $("autoBtn");
  ab.textContent = "Auto demo: " + (s.auto_demo ? "ON" : "OFF");
  ab.classList.toggle("on", s.auto_demo);
}

function renderAgents(s) {
  const grid = $("agentsGrid");
  const key = s.agents.map((a) => a.id).join(",");
  if (key !== state.agentKey) {
    state.agentKey = key;
    grid.innerHTML = "";
    state.agentEls.clear();
  }
  s.agents.forEach((a, i) => {
    let el = state.agentEls.get(a.id);
    if (!el) {
      const root = document.createElement("div");
      root.className = "agent-card";
      root.innerHTML = `
        <div class="agent-head">
          <span class="name">${esc(a.id)}</span><span class="ip mono"></span>
          <span class="pill ok">OK</span>
        </div>
        <div class="agent-rate"><span class="v mono">0.0</span><canvas></canvas></div>
        <div class="agent-stats">
          <span>bandwidth <b class="mono bw"></b></span>
          <span>conns <b class="mono cn"></b></span>
          <span>errors <b class="mono er"></b></span>
          <span>latency <b class="mono la"></b></span>
          <span>seen <b class="mono se"></b></span>
          <span>total req <b class="mono tr"></b></span>
        </div>
        <div class="flags"></div>`;
      grid.appendChild(root);
      el = {
        root,
        ip: root.querySelector(".ip"),
        pill: root.querySelector(".pill"),
        rate: root.querySelector(".agent-rate .v"),
        spark: new Spark(root.querySelector("canvas"), agentColor(i)),
        bw: root.querySelector(".bw"), cn: root.querySelector(".cn"),
        er: root.querySelector(".er"), la: root.querySelector(".la"),
        se: root.querySelector(".se"), tr: root.querySelector(".tr"),
        flags: root.querySelector(".flags"),
      };
      state.agentEls.set(a.id, el);
    }
    el.ip.textContent = a.baseline_estimate.toFixed(1) + " base";
    el.pill.textContent = a.status;
    el.pill.className = "pill " + a.status.toLowerCase();
    el.rate.textContent = a.rate.toFixed(1);
    el.spark.push(a.online ? a.rate : null);
    el.bw.textContent = fmtBytes(a.bytes_rate);
    el.cn.textContent = a.conns;
    el.er.textContent = (a.error_rate * 100).toFixed(0) + "%";
    el.la.textContent = a.latency_ms.toFixed(0) + "ms";
    el.se.textContent = a.online ? fmtAgo(a.age_s) : "offline";
    el.tr.textContent = a.total_requests.toLocaleString();
    el.root.classList.toggle("offline", !a.online);
    el.flags.innerHTML = a.flags.length
      ? a.flags.map((f) => `<div class="flag">${esc(f)}</div>`).join("")
      : '<div class="flag ok">no local flags</div>';
  });
}

function renderCharts(s) {
  if (!state.charts.rate) {
    state.charts.rate = new TSChart($("rateChart"));
    state.charts.bytes = new TSChart($("bytesChart"));
  }
  const agentDefs = s.agents.map((a, i) => ({
    key: a.id, label: a.id, color: agentColor(i), width: 1.1, alpha: 0.7,
  }));
  const defs = [{ key: "aggregate_rate", label: "aggregate", color: "#22d3ee",
                  width: 2.2, main: true, fillColor: "rgba(34,211,238,0.10)" }].concat(agentDefs);
  const rateChart = state.charts.rate;
  rateChart.setSeriesDefs(defs);
  rateChart.opts.thresholds = [{
    value: s.thresholds.global_rate, color: "rgba(251,191,36,0.85)",
    label: "global " + s.thresholds.global_rate,
  }];
  const map = { aggregate_rate: s.series.aggregate_rate };
  for (const k in s.series.per_agent) map[k] = s.series.per_agent[k];
  rateChart.setData(s.series.t, map, s.series.status);

  const bytesChart = state.charts.bytes;
  bytesChart.setSeriesDefs([{ key: "aggregate_bytes", label: "bandwidth", color: "#a78bfa",
                              width: 2.0, main: true, fillColor: "rgba(167,139,250,0.10)" }]);
  bytesChart.opts.thresholds = [{
    value: s.thresholds.global_bytes, color: "rgba(251,191,36,0.85)",
    label: fmtBytes(s.thresholds.global_bytes),
  }];
  bytesChart.setData(s.series.t, { aggregate_bytes: s.series.aggregate_bytes }, s.series.status);

  const aggregate = last(s.series.aggregate_rate);
  $("rateLegend").innerHTML =
    `<span class="chip"><i style="background:#22d3ee"></i>aggregate <b class="mono">${fmtRate(aggregate)}</b></span>` +
    s.agents.map((a, i) => {
      const arr = s.series.per_agent[a.id] || [];
      return `<span class="chip"><i style="background:${agentColor(i)}"></i>${esc(a.id)} <b class="mono">${fmtRate(last(arr))}</b></span>`;
    }).join("");
}

function renderProtected(s) {
  const box = $("protectedBox");
  const p = s.protected;
  if (!p) {
    box.innerHTML = '<div class="muted">protected API unreachable</div>';
    return;
  }
  const rl = (p.status_counts_10s && p.status_counts_10s["429"]) || 0;
  const cells = [
    { k: "accepted rate", v: fmtRate(p.request_rate) },
    { k: "bandwidth", v: fmtBytes(p.bytes_rate) },
    { k: "connections", v: p.active_connections },
    { k: "HTTP 429 (10s)", v: rl, alerting: rl > 0 },
    { k: "total requests", v: p.total_requests.toLocaleString() },
    { k: "top talker", v: p.top_ips && p.top_ips.length
        ? `${esc(p.top_ips[0].ip)} (${p.top_ips[0].rate.toFixed(1)}/s)` : "-" },
  ];
  box.innerHTML = cells.map((c) =>
    `<div class="pstat${c.alerting ? " alerting" : ""}"><div class="v mono">${c.v}</div><div class="k">${c.k}</div></div>`
  ).join("");
}

function renderComparison(s) {
  const cmp = s.comparison;
  const flaggedNow = s.agents.filter((a) => a.status === "FLAGGED").length;
  $("compareTiles").innerHTML = `
    <div class="tile">
      <h3>Local-only (per-silo)</h3>
      <div class="tile-big">${flaggedNow}<span>/ ${s.agents.length} agents flagged now</span></div>
      <div class="tile-stats">events detected: <b>${cmp.local_detections}</b>${cmp.avg_local_latency_s != null ? " &middot; avg " + cmp.avg_local_latency_s + "s" : ""}</div>
    </div>
    <div class="tile">
      <h3>Centralized (correlated)</h3>
      <div class="tile-big ${s.system_status.toLowerCase()}">${s.system_status}</div>
      <div class="tile-stats">events detected: <b>${cmp.central_detections}</b>${cmp.avg_central_latency_s != null ? " &middot; avg " + cmp.avg_central_latency_s + "s" : ""}</div>
    </div>`;

  const body = $("eventsBody");
  if (!s.events.length) {
    body.innerHTML = '<tr><td colspan="5" class="muted">no scenario events yet</td></tr>';
    return;
  }
  const verdictText = { both: "both caught it", central_only: "central only",
                        local_only: "local only", none: "missed by both" };
  body.innerHTML = s.events.slice(0, 8).map((ev) => {
    const localCell = ev.local.detected
      ? `<span class="hit">detected ${ev.local.latency_s.toFixed(1)}s</span> (${ev.local.agents.length} agent${ev.local.agents.length === 1 ? "" : "s"})`
      : '<span class="miss">missed</span>';
    const centralCell = ev.central.detected
      ? `<span class="hit">${esc((ev.central.severity || "").toLowerCase())} ${ev.central.latency_s.toFixed(1)}s</span>`
      : '<span class="miss">missed</span>';
    return `<tr>
      <td class="mono">${fmtClock(ev.started_at)}</td>
      <td>${esc(ev.label)}</td>
      <td>${localCell}</td>
      <td>${centralCell}</td>
      <td><span class="verdict ${ev.verdict}">${verdictText[ev.verdict]}</span></td>
    </tr>`;
  }).join("");
}

function renderAlerts(s) {
  $("alertCount").textContent = `${s.alerts_count.active} active / ${s.alerts_count.total} total`;
  const rows = [];
  for (const a of s.active_alerts) rows.push(alertRow(a, true));
  const resolved = s.alerts_history.filter((a) => a.resolved).slice(0, 12);
  for (const a of resolved) rows.push(alertRow(a, false));
  $("alertsBox").innerHTML = rows.join("") ||
    '<div class="alert empty">No alerts recorded yet.</div>';
}

function alertRow(a, active) {
  const agents = a.agents && a.agents.length
    ? " &middot; agents: " + a.agents.map(esc).join(", ") : "";
  return `<div class="alert ${a.severity.toLowerCase()}">
    <div class="alert-top">
      <span class="alert-rule mono">${esc(a.rule)}</span>
      <span class="alert-sev">${a.severity}</span>
      <span class="alert-time mono">${fmtClock(a.started_at)}</span>
    </div>
    <div class="alert-msg">${esc(a.message)}${agents}</div>
    <div class="alert-meta">${active ? "ongoing" : "resolved " + fmtClock(a.resolved_at)}
      &middot; peak ${fmtRate(a.peak_rate)} &middot; ${a.id}</div>
  </div>`;
}

function render(s) {
  state.snap = s;
  document.body.dataset.status = s.system_status;
  setStatusBadge(s.system_status);
  renderKpis(s);
  renderScenario(s);
  renderAgents(s);
  renderCharts(s);
  renderProtected(s);
  renderComparison(s);
  renderAlerts(s);
  $("footInfo").textContent =
    `local threshold ${s.thresholds.local_rate} req/s/agent - global ${s.thresholds.global_rate} req/s - ${s.agents.length} agents`;
}

/* ---------------- networking ---------------- */
function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  state.ws = ws;
  ws.onopen = () => { setConn(true); };
  ws.onmessage = (e) => {
    try { render(JSON.parse(e.data)); } catch (err) { console.error("bad snapshot", err); }
  };
  ws.onclose = () => {
    setConn(false);
    setTimeout(connectWS, 2000);
  };
  ws.onerror = () => ws.close();
}
setInterval(() => {
  if (state.ws && state.ws.readyState === 1) state.ws.send("ping");
}, 15000);

async function post(path, body) {
  const r = await fetch(path, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  return r.json();
}
async function refreshNow() {
  try {
    const r = await fetch("/api/snapshot");
    render(await r.json());
  } catch (e) { /* WS will recover */ }
}

/* ---------------- controls ---------------- */
document.querySelectorAll("button[data-scenario]").forEach((btn) => {
  btn.addEventListener("click", async () => {
    await post("/api/control/scenario", { scenario: btn.dataset.scenario });
    refreshNow();
  });
});
$("resetBtn").addEventListener("click", async () => {
  await post("/api/control/reset");
  refreshNow();
});
$("autoBtn").addEventListener("click", async () => {
  const enabled = !(state.snap && state.snap.auto_demo);
  await post("/api/control/auto", { enabled });
  refreshNow();
});

/* ---------------- boot ---------------- */
(async function boot() {
  await refreshNow();
  connectWS();
})();
