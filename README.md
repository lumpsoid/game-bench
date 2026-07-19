# game-bench

A realistic throughput/latency benchmark for request handling across six runtimes,
using a **mini authoritative game server** + an automated **load-generator client**.

Servers to compare: **Go, Rust, OCaml, Odin, Zig, Dart, Elixir (bare BEAM), Python, Lua (LuaJIT).**
Wire protocol: raw TCP, `u32` length-prefixed frames — see [PROTOCOL.md](PROTOCOL.md).
Fairness rules: see [METHODOLOGY.md](METHODOLOGY.md).

## Layout

```
PROTOCOL.md          wire spec — the single source of truth
METHODOLOGY.md       fairness rules + how to read results
loadgen/             automated client (Go), open-loop, built-in histogram
loadgen-odin/        default automated client (Odin) — epoll reactor, won't self-saturate
servers/
  go/     <- reference server, DONE + smoke-tested
  rust/   ocaml/  odin/  elixir/  python/   <- ported from the spec
runner/              orchestration (build, pin cores, ramp, sample RSS/CPU) — TODO
results/             raw output + plots
```

## Status

- [x] Protocol spec
- [x] Load generators: **Odin** (`loadgen-odin`, default — epoll reactor, stays lean at 10k+
      conns) and **Go** (`loadgen`); select with `runner/run.py --loadgen odin|go`. Both are
      open-loop, share the wire protocol + JSON output, and self-report client CPU / send-rate
- [x] **Go** server (`servers/go`) — built + smoke-tested
- [x] **Rust** server (`servers/rust`, tokio) — built + smoke-tested
- [x] **Elixir** server (`servers/elixir`, bare BEAM / `:gen_tcp`, `{packet,4}`) — smoke-tested;
      100-acceptor pool (was accept-starved at 10k — see [FINDINGS.md](FINDINGS.md))
- [x] **Python** server (`servers/python`, asyncio; uvloop optional) — smoke-tested
- [x] **OCaml** server (`servers/ocaml`, Eio 1.3 / OCaml 5.5) — built + smoke-tested,
      **multicore** via `Eio.Net.run_server ~additional_domains` (`-domains N` flag).
      Verified real parallelism: under 120k moves/s offered load, domains=1 used
      0.84 cores / 30k moves/s (p50 86ms, saturated) vs domains=8 used 3.59 cores /
      117k moves/s (p50 18ms). Ceiling is the central tick loop + per-room mutex
      (single-domain serialization); see header in `main.ml`.
- [x] **Odin** server (`servers/odin`, raw epoll) — built + smoke-tested.
      No green threads/async runtime, so it uses a **thread-per-core sharded
      reactor**: `-workers N` independent single-threaded epoll loops, each owning
      its own connections and rooms (no locks on game state), with the listener
      opened per worker via `SO_REUSEPORT` so the kernel load-balances accepts.
      Same shard-the-rooms multi-core model as the Python server, but with OS
      threads instead of processes. Idle RSS is tiny (~3 MB). See header in `main.odin`.
- [x] **Zig** server (`servers/zig`, raw epoll) — built + smoke-tested. Same
      architecture as Odin: no async runtime, so a **thread-per-core sharded
      reactor** — `-workers N` independent single-threaded epoll loops, each owning
      its own connections and rooms (no locks on game state), listener opened per
      worker via `SO_REUSEPORT`. Uses `std.heap.smp_allocator` and raw `std.os.linux`
      syscalls (Zig 0.16 dropped the blocking socket wrappers from `std.posix`). Idle
      RSS ~3 MB. See header in `server.zig`.
- [x] **Dart** server (`servers/dart`, AOT-compiled) — built + smoke-tested. A GC'd,
      single-threaded-event-loop-per-isolate runtime (like Python's asyncio), but
      isolates are shared-nothing and run in **real parallel** on OS threads, so
      Dart gets multi-core inside ONE process: `-workers N` isolates each bind the
      port with `shared:true` and the runtime load-balances accepts across them
      (SO_REUSEPORT equivalent). Same shard-the-rooms model as Odin/Zig, but with
      GC and no locks (isolates share no memory). Idle RSS ~47 MB. See header in
      `server.dart`.
- [x] **Lua** server (`servers/lua`, LuaJIT + cqueues) — smoke-tested + short runner
      run. Lua has no stdlib networking and no in-state parallelism, so it uses the
      **same multi-core story as Python**: one cqueues event loop per process, N
      processes sharding rooms via `SO_REUSEPORT`. Within a process it mirrors the Go
      reference model (reader + writer coroutine per conn, room owns state, shed the
      freshest snapshot when a client is backed up). Idle RSS ~18 MB at a few hundred
      conns. cqueues install + notes in [servers/lua/README.md](servers/lua/README.md);
      see header in `server.lua`.
- [x] `runner/` orchestration + RSS/CPU sampling — `runner/run.py`, validated on all 7
- [x] plots — `runner/plot.py` renders `results.csv` into a self-contained,
      theme-aware HTML report of saturation curves (no deps)

## Running the full benchmark

```sh
ulimit -n 100000
eval $(opam env)          # so the runner can build the OCaml server
python3 runner/run.py \
    --servers go,rust,ocaml,odin,elixir,python \
    --conns 500,1000,2000,5000,10000 \
    --trials 3 --dur 20 --warmup 5
```

The runner (stdlib Python, no deps) runs ONE server at a time and, for each
`(server, conns, trial)`:
- pins the server to the **server cores** and the loadgen to disjoint **client
  cores** (`taskset`) so the client can never steal the server's CPU;
- sets each runtime's parallelism to the core budget (`GOMAXPROCS` /
  `TOKIO_WORKER_THREADS` / `-domains` / `-workers` / `+S N:N` / N python procs via
  SO_REUSEPORT);
- waits for the port to actually accept (no blind sleeps);
- samples server RSS + CPU from `/proc` (by process group, so BEAM's children count);
- writes one CSV row to `results/results.csv`.

Core split defaults to first-half / second-half of the machine; override with
`--server-cores 0-7 --client-cores 8-15`. Best results come from running the
loadgen on a SEPARATE machine (`--client-cores` then irrelevant) — see METHODOLOGY.md.

CSV columns: `server,conns,trial,moves_sent,snaps_recv,measured,moves_per_s,
snaps_per_s,p50_ms,p90_ms,p99_ms,p999_ms,max_ms,rss_idle_mb,rss_peak_mb,cpu_cores`.

## Plotting

```sh
python3 runner/plot.py [results/results.csv] [-o results/report.html]
```

Produces a self-contained, theme-aware HTML report (stdlib only, no external
assets) with five saturation curves — throughput, p99, p50, peak RSS, CPU cores —
x = offered load, one line per server (median over trials). Colorblind-safe
palette (validated), every line direct-labelled, full data table included. Open
`results/report.html` in a browser.

All four verified servers produce statistically identical numbers at low load
(~15k snap/s, p50 ~27 ms) because latency is pinned to the tick floor — this
confirms the harness is fair/uniform. Real differences require the saturation
ramp (see METHODOLOGY.md), which the `runner/` will drive.

### Run any server (all take the same flags)

```sh
./servers/go/gosrv                    -addr :9000 -tick 30    # go build first
./servers/rust/target/release/server-rust  -addr :9000 -tick 30
elixir servers/elixir/server.exs      -addr :9000 -tick 30
python3 servers/python/server.py      -addr :9000 -tick 30
./servers/odin/server-odin            -addr :9000 -tick 30 -workers 4   # odin build . -out:server-odin -o:speed first
./servers/zig/server-zig              -addr :9000 -tick 30 -workers 4   # zig build-exe server.zig -O ReleaseFast -femit-bin=server-zig first
./servers/dart/server-dart            -addr :9000 -tick 30 -workers 4   # dart compile exe server.dart -o server-dart first
luajit servers/lua/server.lua         -addr :9000 -tick 30   # install cqueues first — see servers/lua/README.md
# ocaml: dune build --profile release && ./_build/default/main.exe -addr :9000 -tick 30
```

Odin, Zig, and Dart take `-workers N` (default 1) to set their core budget, the way
the others take `GOMAXPROCS` / `-domains` / `+S`; the runner passes `-workers = #server cores`.

## Quick start (single box, smoke test)

```sh
# terminal 1
cd servers/go && go build -o /tmp/gosrv . && /tmp/gosrv -addr :9000 -tick 30

# terminal 2
cd loadgen && go build -o /tmp/loadgen .
/tmp/loadgen -addr 127.0.0.1:9000 -conns 1000 -room-size 50 -rate 20 -warmup 5s -dur 30s
```

Output: total moves/snapshots, throughput, and latency p50/p90/p99/p99.9/max.
Remember (see METHODOLOGY): absolute low-load latency is dominated by the tick
period; the signal is how latency climbs above that floor under load.

## Porting checklist (per new server)

Each server MUST:
1. Accept raw TCP, set `TCP_NODELAY`.
2. Frame every message `u32-BE length | payload`; reject frames > 1 MiB.
3. Handle JOIN (`0x01`) → reply JOINED (`0x81`) with a unique player id.
4. Handle MOVE (`0x02`) → set player velocity + `last_seq`.
5. Run a per-room tick loop at `-tick` Hz; on each tick integrate positions and
   broadcast one SNAPSHOT (`0x82`) to every connection in the room.
6. Use the reference architecture: per-conn read path, per-conn write path (a slow
   client must not stall a room), room owns its state (no shared-state locks on the
   hot path). Adapt idiomatically per language — do NOT hand-optimize beyond that.
7. Take `-addr` and `-tick` flags/env with the same defaults.

Validate a new server by pointing the existing Go `loadgen` at it and confirming
sane latency percentiles, then run the scenarios in METHODOLOGY.md.
```
