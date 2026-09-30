"""
Process orchestrator - launches the full distributed traffic monitoring stack:

    protected API  ->  central server (+ dashboard)  ->  N simulated agents

Usage::

    python run_all.py                 # 4 agents, auto demo on
    python run_all.py --agents 5 --no-demo
    python run_all.py --global-rate 60 --local-rate 20

Stop everything with Ctrl+C (SIGTERM is handled as well).
"""
from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))

AGENT_NAMES = ["edge-north", "edge-south", "edge-core", "edge-exchange",
               "edge-metro", "edge-coast"]
BASELINES = [4.5, 6.0, 5.0, 7.0, 5.5, 6.5]


def wait_port(host: str, port: int, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the monitoring demo stack")
    parser.add_argument("--agents", type=int, default=4,
                        help="number of simulated monitoring agents (3-6)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--api-port", type=int, default=8000)
    parser.add_argument("--central-port", type=int, default=8001)
    parser.add_argument("--local-rate", type=float, default=25.0)
    parser.add_argument("--global-rate", type=float, default=75.0)
    parser.add_argument("--no-demo", action="store_true",
                        help="disable the automatic scenario rotation")
    args = parser.parse_args()

    n = max(3, min(args.agents, len(AGENT_NAMES)))
    procs: list = []
    stopping = {"flag": False}

    def spawn(name: str, script: str, argv: list) -> subprocess.Popen:
        cmd = [sys.executable, os.path.join(ROOT, script)] + argv
        proc = subprocess.Popen(cmd, cwd=ROOT)
        procs.append(proc)
        print(f"[run_all] started {name:<14} (pid {proc.pid})")
        return proc

    def shutdown(*_) -> None:
        if stopping["flag"]:
            return
        stopping["flag"] = True
        print("\n[run_all] shutting down...")
        for proc in procs:
            try:
                proc.terminate()
            except Exception:
                pass
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    spawn("protected-api", "protected_api.py",
          ["--host", args.host, "--port", str(args.api_port)])
    if not wait_port(args.host, args.api_port):
        print("[run_all] ERROR: protected API did not start")
        return shutdown()

    central_argv = ["--host", args.host, "--port", str(args.central_port),
                    "--protected", f"http://{args.host}:{args.api_port}",
                    "--global-rate", str(args.global_rate),
                    "--local-rate", str(args.local_rate)]
    if not args.no_demo:
        central_argv.append("--auto-demo")
    spawn("central-server", "central_server.py", central_argv)
    if not wait_port(args.host, args.central_port):
        print("[run_all] ERROR: central server did not start")
        return shutdown()

    for i in range(n):
        spawn(f"agent:{AGENT_NAMES[i]}", "agent.py", [
            "--id", AGENT_NAMES[i],
            "--api", f"http://{args.host}:{args.api_port}",
            "--central", f"http://{args.host}:{args.central_port}",
            "--baseline-rate", str(BASELINES[i % len(BASELINES)]),
            "--local-rate", str(args.local_rate),
        ])

    print()
    print("=" * 70)
    print(f"  Dashboard      : http://{args.host}:{args.central_port}/")
    print(f"  Protected API  : http://{args.host}:{args.api_port}/")
    print(f"  Agents         : {n} simulated monitors reporting every 2s")
    print(f"  Auto demo      : {'off' if args.no_demo else 'on (cycles scenarios)'}")
    print("=" * 70)
    print("  Press Ctrl+C to stop the whole stack.\n")

    try:
        while True:
            time.sleep(1)
            for proc in list(procs):
                code = proc.poll()
                if code is not None:
                    print(f"[run_all] warning: child process exited (code {code})")
                    procs.remove(proc)
            if not procs:
                break
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
