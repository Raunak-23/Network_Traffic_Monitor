"""Unit tests for the pure detection logic (no network required).

Run either with pytest (``pytest tests/``) or directly::

    python -m tests.test_detection
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from detection import (  # noqa: E402
    LEVEL_CRITICAL,
    LEVEL_NORMAL,
    LEVEL_WARNING,
    DetectionConfig,
    DistributedDetector,
    LocalDetector,
)

CFG = DetectionConfig()  # default thresholds


def _feed(det: DistributedDetector, history, current):
    for rate in history:
        det.ingest(rate, 0.0, 0, [])
    return det.ingest(current, 0.0, 0, [])


# --- LocalDetector ----------------------------------------------------------

def test_local_ok_at_baseline():
    v = LocalDetector(CFG).evaluate(5.0, 40_000, 2, 0.0)
    assert v.status == "OK"
    assert v.flags == []


def test_local_requires_sustained_breach():
    det = LocalDetector(CFG)
    first = det.evaluate(31.0, 40_000, 2, 0.0)      # streak 1 -> debounce not met
    assert first.status == "OK" and first.flags
    second = det.evaluate(32.0, 41_000, 2, 0.0)     # streak 2 -> flagged
    assert second.status == "FLAGGED"
    assert any(f.startswith("RATE") for f in second.flags)


def test_local_error_flag():
    det = LocalDetector(CFG)
    det.evaluate(5.0, 40_000, 2, 0.5)
    v = det.evaluate(5.0, 40_000, 2, 0.6)
    assert v.status == "FLAGGED"
    assert any(f.startswith("ERRORS") for f in v.flags)


def test_local_recovers():
    det = LocalDetector(CFG)
    det.evaluate(40.0, 40_000, 2, 0.0)
    det.evaluate(40.0, 40_000, 2, 0.0)
    v = det.evaluate(5.0, 40_000, 2, 0.0)
    assert v.status == "OK" and v.streak == 0


# --- DistributedDetector ----------------------------------------------------

def test_quite_baseline_is_normal():
    d = _feed(DistributedDetector(CFG), [22.0] * 30, 23.0)
    assert d.level == LEVEL_NORMAL and d.hits == []


def test_coordination_rule_is_critical():
    d = DistributedDetector(CFG)
    dec = d.ingest(20.0, 0.0, 0, ["edge-a", "edge-b"])
    hit = next(h for h in dec.hits if h.rule == "COORDINATION")
    assert hit.severity == LEVEL_CRITICAL
    assert dec.level == LEVEL_CRITICAL


def test_no_coordination_with_single_agent():
    d = DistributedDetector(CFG)
    dec = d.ingest(20.0, 0.0, 0, ["edge-a"])
    assert not any(h.rule == "COORDINATION" for h in dec.hits)


def test_aggregate_rate_warning_and_critical():
    d = _feed(DistributedDetector(CFG), [20.0] * 20, 80.0)   # 1.07x -> WARNING
    assert any(h.rule == "AGGREGATE_RATE" for h in d.hits)
    d2 = _feed(DistributedDetector(CFG), [20.0] * 20, 120.0)  # 1.6x -> CRITICAL
    hit = next(h for h in d2.hits if h.rule == "AGGREGATE_RATE")
    assert hit.severity == LEVEL_CRITICAL


def test_zscore_spike_detection():
    d = _feed(DistributedDetector(CFG), [20.0] * 25, 60.0)
    assert d.zscore is not None and d.zscore > CFG.zscore_trigger
    assert any(h.rule == "RATE_SPIKE" for h in d.hits)
    assert d.level == LEVEL_WARNING


def test_zscore_needs_history():
    d = DistributedDetector(CFG)
    dec = d.ingest(90.0, 0.0, 0, [])                # no history yet
    assert not any(h.rule == "RATE_SPIKE" for h in dec.hits)
    assert any(h.rule == "AGGREGATE_RATE" for h in dec.hits)  # absolute rule still fires


def test_bandwidth_and_connections_rules():
    d = DistributedDetector(CFG)
    dec = d.ingest(10.0, 2_000_000.0, 40, [])
    rules = {h.rule for h in dec.hits}
    assert "AGGREGATE_BANDWIDTH" in rules and "AGGREGATE_CONNECTIONS" in rules


def test_low_and_slow_story():
    """Each agent stays under its local threshold, but the aggregate breaches."""
    d = _feed(DistributedDetector(CFG), [22.0] * 30, 80.0)
    assert d.level != LEVEL_NORMAL                     # central catches it
    local = LocalDetector(CFG)
    assert local.evaluate(19.0, 240_000, 3, 0.0).status == "OK"   # local misses it


# --- simple runner -----------------------------------------------------------

def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except AssertionError as exc:
            failed += 1
            print(f"FAIL  {name}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} tests passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
