# Methodology

The point of this benchmark is a fair, realistic comparison of how five runtimes
(Go, Rust, OCaml, Elixir, Python) handle sustained, fan-out request processing.
Getting the *methodology* right matters more than the code. Read this before trusting
any number.

## The instrument vs. the subject

- **Subject:** the server, written once per language. This is what we compare.
- **Instrument:** the load generator — `loadgen-odin/` (Odin, default) or `loadgen/` (Go),
  selected per run and identical across every server in that run. It must never be the
  bottleneck and must never steal CPU from the subject, or every number is contaminated —
  so each run self-reports `client_cpu_cores` and `send_rate_pct` to prove it didn't. The
  Odin client is an epoll reactor (one worker thread per client core) precisely so it stays
  lean at high connection counts where a goroutine-per-conn client would burn CPU.

## Non-negotiable rules

1. **`TCP_NODELAY` on every socket** (server and client). Nagle adds ~40 ms latency
   spikes and would dwarf real differences. Already set in the reference code.
2. **Open-loop load.** The client sends on a fixed wall-clock schedule and does NOT
   wait for responses. A closed-loop generator hides stalls (coordinated omission).
   Already implemented this way.
3. **Run the client on a separate machine** over a quiet LAN if at all possible.
   If you must co-locate, pin server and client to **disjoint CPU cores**:
   ```
   taskset -c 0-3   ./server ...     # server gets cores 0-3
   taskset -c 4-7   ./loadgen ...    # client gets cores 4-7
   ```
4. **Equal core budget.** Give every server the same cores, and match each runtime's
   parallelism to that count:
   - Go:      `GOMAXPROCS=N`
   - Rust:    tokio worker threads = N
   - OCaml:   N domains (Eio) — but Elixir choice here is bare BEAM
   - Elixir:  `+S N:N` schedulers (or `ERL_...`); bare Thousand Island, no Phoenix
   - Python:  asyncio is single-core; to use N cores run N processes sharding rooms
5. **Warm up** before measuring (`-warmup`), especially BEAM and Python.
6. **Raise fd limits:** `ulimit -n 100000` on both sides before high connection counts.
7. **Optimized builds:** `cargo build --release`, OCaml `dune build --profile release`
   (flambda on), Elixir `MIX_ENV=prod`, Python with `uvloop`. Go is optimized by default.
8. **Multiple trials.** Report median + spread across >=3 runs, never a single run.

## Reading the numbers (important)

The server is **tick-based**, so at low load the end-to-end latency is dominated by
the tick period (~33 ms at 30 Hz), NOT by runtime speed. Do not compare absolute
low-load latencies and conclude anything.

The real signal is **how far latency climbs above the tick floor as offered load
rises**. The comparison you want is the saturation curve, not a single point.

## Scenarios

1. **Fixed-load latency** — fixed `-conns` and `-rate`; report p50/p99/p99.9 + msg/s.
2. **Saturation ramp** — increase `-conns` until p99 breaches an SLA (e.g. 50 ms above
   the tick floor). That connection count = "max concurrent players" for the runtime.
3. **Fan-out stress** — vary `-room-size` (10 / 50 / 200 / 500) to amplify broadcast cost.
4. **Soak** — `-dur 15m` to catch GC drift, latency creep, and leaks.
5. **Memory** — sample server RSS idle and under load (see `runner/`), tying back to
   the idle-footprint question that motivated this.

## What we are deliberately NOT doing

- No per-language micro-optimization heroics. Every server uses the same architecture
  (per-conn read + per-conn write + room-owns-state) and idiomatic code. If one server
  is hand-tuned, the comparison is meaningless.
- Note honestly where a design choice favors a runtime — e.g. the room-owns-state model
  fits Go channels and BEAM processes naturally, and idiomatic tokio pays locking cost
  unless written thread-per-core. Report that as a finding, not a fix.
