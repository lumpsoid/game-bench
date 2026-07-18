#!/usr/bin/env python3
"""
Benchmark runner: sequential, fair, reproducible.

For each (server, connection-count, trial) it:
  1. builds the server (once) and the loadgen,
  2. starts the server pinned to the SERVER cores, with that runtime's parallelism
     set to match the core budget (GOMAXPROCS / TOKIO_WORKER_THREADS / -domains /
     +S schedulers / N python processes),
  3. waits until the port actually accepts (no blind sleeps),
  4. runs the loadgen pinned to disjoint CLIENT cores (so the client can never
     steal the server's CPU — the whole point),
  5. samples the server's RSS + CPU from /proc for the whole run,
  6. records one CSV row: throughput, latency percentiles, idle/peak RSS, CPU cores.

Only ONE server runs at a time. Client and server never share cores.

Usage:
  python3 runner/run.py --servers go,rust,ocaml,elixir,python \
      --conns 500,1000,2000,5000,10000 --trials 3 --dur 20 --warmup 5
See METHODOLOGY.md.
"""

import argparse
import csv
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLK = os.sysconf("SC_CLK_TCK")
HAVE_TASKSET = subprocess.run(["sh", "-c", "command -v taskset"],
                              capture_output=True).returncode == 0
PORT = 9100


# ----------------------------------------------------------------------------
# Server specs. Each returns a build command (or None) and a launch plan:
# a list of (argv, env, cpuspec) — one entry per OS process. Single-process
# servers pin one process to ALL server cores; Python pins N processes, one core
# each (its multi-core story, since asyncio is single-core per process).
# ----------------------------------------------------------------------------

def cpuspec(cores):
    return ",".join(str(c) for c in cores)


def spec_go(port, tick, cores):
    exe = os.path.join(ROOT, "servers/go/gosrv")
    env = {"GOMAXPROCS": str(len(cores))}
    return [([exe, "-addr", f":{port}", "-tick", str(tick)], env, cpuspec(cores))]


def spec_rust(port, tick, cores):
    exe = os.path.join(ROOT, "servers/rust/target/release/server-rust")
    env = {"TOKIO_WORKER_THREADS": str(len(cores))}
    return [([exe, "-addr", f":{port}", "-tick", str(tick)], env, cpuspec(cores))]


def spec_ocaml(port, tick, cores):
    exe = os.path.join(ROOT, "servers/ocaml/_build/default/main.exe")
    argv = [exe, "-addr", f":{port}", "-tick", str(tick), "-domains", str(len(cores))]
    return [(argv, {}, cpuspec(cores))]


def spec_odin(port, tick, cores):
    exe = os.path.join(ROOT, "servers/odin/server-odin")
    # One process, N worker threads (thread-per-core sharded reactor), pinned to
    # all server cores — SO_REUSEPORT lets the kernel load-balance across workers.
    argv = [exe, "-addr", f":{port}", "-tick", str(tick), "-workers", str(len(cores))]
    return [(argv, {}, cpuspec(cores))]


def spec_elixir(port, tick, cores):
    script = os.path.join(ROOT, "servers/elixir/server.exs")
    n = len(cores)
    argv = ["elixir", "--erl", f"+S {n}:{n}", script, "-addr", f":{port}", "-tick", str(tick)]
    return [(argv, {}, cpuspec(cores))]


def spec_python(port, tick, cores):
    script = os.path.join(ROOT, "servers/python/server.py")
    # One process per server core (SO_REUSEPORT shares the port); each pinned to
    # a single core. This is Python's multi-core story.
    return [
        ([sys.executable, script, "-addr", f":{port}", "-tick", str(tick)], {}, str(c))
        for c in cores
    ]


SERVERS = {
    "go":     {"build": (["go", "build", "-o", "gosrv", "."], "servers/go"),               "spec": spec_go},
    "rust":   {"build": (["cargo", "build", "--release"], "servers/rust"),                  "spec": spec_rust},
    "ocaml":  {"build": (["opam", "exec", "--", "dune", "build", "--profile", "release"], "servers/ocaml"), "spec": spec_ocaml},
    "odin":   {"build": (["odin", "build", ".", "-out:server-odin", "-o:speed"], "servers/odin"), "spec": spec_odin},
    "elixir": {"build": None,                                                                "spec": spec_elixir},
    "python": {"build": None,                                                                "spec": spec_python},
}


# ----------------------------------------------------------------------------
# /proc sampling (by process group, so it covers thread pools and child procs
# like BEAM's erl). CPU ticks in /proc/<pid>/stat already aggregate all threads.
# ----------------------------------------------------------------------------

def pids_in_groups(pgids):
    out = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as f:
                data = f.read()
            # field 5 (pgrp) — parse safely around the (comm) which may hold spaces
            rparen = data.rfind(")")
            fields = data[rparen + 2:].split()
            pgrp = int(fields[2])  # fields after comm: state, ppid, pgrp,...
            if pgrp in pgids:
                out.append(int(entry))
        except (FileNotFoundError, ProcessLookupError, IndexError, ValueError):
            continue
    return out


def sample_cpu_ticks(pids):
    total = 0
    for pid in pids:
        try:
            with open(f"/proc/{pid}/stat") as f:
                data = f.read()
            fields = data[data.rfind(")") + 2:].split()
            total += int(fields[11]) + int(fields[12])  # utime + stime
        except (FileNotFoundError, ProcessLookupError, IndexError, ValueError):
            continue
    return total


def sample_rss_kb(pids):
    total = 0
    for pid in pids:
        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        total += int(line.split()[1])
                        break
        except (FileNotFoundError, ProcessLookupError):
            continue
    return total


class Sampler(threading.Thread):
    """Samples RSS peak and CPU ticks for a set of process groups until stopped."""

    def __init__(self, pgids):
        super().__init__(daemon=True)
        self.pgids = set(pgids)
        self.stop_flag = threading.Event()
        self.rss_peak_kb = 0
        self.rss_first_kb = 0
        self.cpu_start = None
        self.cpu_end = None

    def run(self):
        pids = pids_in_groups(self.pgids)
        self.cpu_start = sample_cpu_ticks(pids)
        self.rss_first_kb = sample_rss_kb(pids)
        while not self.stop_flag.is_set():
            pids = pids_in_groups(self.pgids)
            rss = sample_rss_kb(pids)
            if rss > self.rss_peak_kb:
                self.rss_peak_kb = rss
            self.cpu_end = sample_cpu_ticks(pids)
            time.sleep(0.1)

    def stop(self):
        self.stop_flag.set()
        self.join(timeout=2)


# ----------------------------------------------------------------------------
# Process lifecycle
# ----------------------------------------------------------------------------

def taskset_wrap(argv, cpu):
    if HAVE_TASKSET:
        return ["taskset", "-c", cpu] + argv
    return argv


def start_server(plan):
    procs, pgids = [], []
    for argv, env_over, cpu in plan:
        env = dict(os.environ)
        env.update(env_over)
        env["ULIMIT_HINT"] = "raise nofile via the parent shell"
        p = subprocess.Popen(
            taskset_wrap(argv, cpu),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # own process group => clean group kill
        )
        procs.append(p)
        pgids.append(os.getpgid(p.pid))
    return procs, pgids


def wait_ready(port, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def stop_server(procs):
    for p in procs:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        except (ProcessLookupError, OSError):
            pass
    time.sleep(0.4)
    for p in procs:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except (ProcessLookupError, OSError):
            pass
        try:
            p.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def run_loadgen(loadgen, port, conns, room_size, rate, warmup, dur, client_cores):
    argv = [
        loadgen, "-addr", f"127.0.0.1:{port}", "-conns", str(conns),
        "-room-size", str(room_size), "-rate", str(rate),
        "-warmup", f"{warmup}s", "-dur", f"{dur}s", "-json",
    ]
    r = subprocess.run(taskset_wrap(argv, cpuspec(client_cores)),
                       capture_output=True, text=True)
    line = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        sys.stderr.write(f"  ! loadgen produced no JSON. stderr:\n{r.stderr}\n")
        return None


# ----------------------------------------------------------------------------
# Build
# ----------------------------------------------------------------------------

def build(cmd_cwd, label):
    if cmd_cwd is None:
        return True
    cmd, rel = cmd_cwd
    cwd = os.path.join(ROOT, rel)
    print(f"  building {label} ...", flush=True)
    r = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(f"  ! build failed for {label}:\n{r.stdout}\n{r.stderr}\n")
        return False
    return True


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def parse_cores(s, default):
    if not s:
        return default
    out = []
    for part in s.split(","):
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


def main():
    ncpu = os.cpu_count()
    half = ncpu // 2
    ap = argparse.ArgumentParser()
    ap.add_argument("--servers", default="go,rust,ocaml,odin,elixir,python")
    ap.add_argument("--conns", default="500,1000,2000,5000,10000")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--room-size", type=int, default=50)
    ap.add_argument("--rate", type=int, default=20)
    ap.add_argument("--tick", type=int, default=30)
    ap.add_argument("--warmup", type=int, default=5)
    ap.add_argument("--dur", type=int, default=20)
    ap.add_argument("--server-cores", default="", help=f"default 0-{half-1}")
    ap.add_argument("--client-cores", default="", help=f"default {half}-{ncpu-1}")
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "results.csv"))
    ap.add_argument("--cooldown", type=float, default=1.0)
    args = ap.parse_args()

    server_cores = parse_cores(args.server_cores, list(range(0, half)))
    client_cores = parse_cores(args.client_cores, list(range(half, ncpu)))
    if set(server_cores) & set(client_cores):
        sys.stderr.write("! server and client core sets overlap — results will be unfair.\n")
    servers = [s.strip() for s in args.servers.split(",") if s.strip()]
    conns_list = [int(c) for c in args.conns.split(",")]

    print(f"cores: server={cpuspec(server_cores)}  client={cpuspec(client_cores)}  "
          f"taskset={'yes' if HAVE_TASKSET else 'NO (unpinned!)'}")
    print(f"matrix: {servers} x conns={conns_list} x trials={args.trials}  "
          f"(room={args.room_size}, rate={args.rate}Hz, tick={args.tick}Hz, "
          f"warmup={args.warmup}s, dur={args.dur}s)\n")

    loadgen = os.path.join(ROOT, "loadgen", "loadgen")
    if not build((["go", "build", "-o", "loadgen", "."], "loadgen"), "loadgen"):
        sys.exit(1)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    fields = ["server", "conns", "trial", "moves_sent", "snaps_recv", "measured",
              "moves_per_s", "snaps_per_s", "p50_ms", "p90_ms", "p99_ms", "p999_ms",
              "max_ms", "rss_idle_mb", "rss_peak_mb", "cpu_cores"]
    fh = open(args.out, "w", newline="")
    writer = csv.DictWriter(fh, fieldnames=fields)
    writer.writeheader()

    for name in servers:
        if name not in SERVERS:
            sys.stderr.write(f"! unknown server '{name}', skipping\n")
            continue
        spec = SERVERS[name]
        if not build(spec["build"], name):
            continue
        print(f"\n=== {name} ===")
        for conns in conns_list:
            for trial in range(1, args.trials + 1):
                plan = spec["spec"](PORT, args.tick, server_cores)
                procs, pgids = start_server(plan)
                if not wait_ready(PORT):
                    sys.stderr.write(f"  ! {name} @ {conns} conns did not become ready\n")
                    stop_server(procs)
                    time.sleep(args.cooldown)
                    continue

                sampler = Sampler(pgids)
                sampler.start()
                res = run_loadgen(loadgen, PORT, conns, args.room_size, args.rate,
                                  args.warmup, args.dur, client_cores)
                sampler.stop()
                stop_server(procs)

                if res is None:
                    time.sleep(args.cooldown)
                    continue

                wall = args.warmup + args.dur
                cpu_cores = (sampler.cpu_end - sampler.cpu_start) / (CLK * wall) \
                    if sampler.cpu_end is not None else 0.0
                row = {
                    "server": name, "conns": conns, "trial": trial,
                    "moves_sent": res["moves_sent"], "snaps_recv": res["snaps_recv"],
                    "measured": res["measured"],
                    "moves_per_s": round(res["moves_per_s"], 1),
                    "snaps_per_s": round(res["snaps_per_s"], 1),
                    "p50_ms": round(res["p50_ms"], 2), "p90_ms": round(res["p90_ms"], 2),
                    "p99_ms": round(res["p99_ms"], 2), "p999_ms": round(res["p999_ms"], 2),
                    "max_ms": round(res["max_ms"], 2),
                    "rss_idle_mb": round(sampler.rss_first_kb / 1024, 1),
                    "rss_peak_mb": round(sampler.rss_peak_kb / 1024, 1),
                    "cpu_cores": round(cpu_cores, 2),
                }
                writer.writerow(row)
                fh.flush()
                print(f"  conns={conns:<6} trial={trial}  "
                      f"snaps/s={row['snaps_per_s']:<9} p50={row['p50_ms']:<6} "
                      f"p99={row['p99_ms']:<7} rss_peak={row['rss_peak_mb']}MB "
                      f"cpu={row['cpu_cores']}", flush=True)
                time.sleep(args.cooldown)

    fh.close()
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
