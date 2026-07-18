# Findings

## Elixir "collapse" at 10k connections (investigated & fixed)

**Symptom.** In the first ramp (4 server cores), Elixir throughput read 11.5k
snaps/s at 10k connections — a cliff from 106k at 6k — while Go/Rust/Python stayed
in the 160–280k range.

**It was not what it looked like.** The reported rate is `total ÷ wall-clock`, and
the Elixir 10k run took **77 s of wall clock** instead of ~8 s. So the low number
was mostly a *measurement artifact*: connection establishment stalled so badly that
many clients blocked ~70 s waiting for their `JOINED` reply, stretching the wall
clock and cratering the rate. The server actually processed ~884k snapshots — over
77 s, with a catastrophic join ramp.

**Root cause (found by server-side DIAG instrumentation, gated on `DIAG=1`).**
Three hypotheses were falsified before landing on the real one:

1. ~~GenServer.call join timeout killing conns~~ — `join_fail=0`.
2. ~~`:ok = controlling_process` MatchError killing acceptors~~ — `cp_fail=0`.
3. ~~Mailbox / GC overload spiral~~ — `max_mailbox` and `run_queue` stayed ~0.

The real cause: **accept starvation.** The hand-rolled acceptor used only
`schedulers_online` = **4 acceptor loops**. Four acceptors cannot drain a 10k
connection burst, so the kernel backlog (1024) overflowed → ~1000 connections
refused, the rest accepted in a slow trickle (DIAG showed `accepts` crawling
5679 → 8995 over ~20 s, then frozen). This is exactly the problem a real acceptor
pool (**ranch / Thousand Island**) exists to solve — and the cost of the decision
to hand-roll the acceptor instead of using Thousand Island.

**Fixes applied to `servers/elixir/server.exs`:**

1. **Acceptor pool** — 100 acceptors (was 4), `ACCEPTORS=N` to override; backlog
   1024 → 4096. Wall time 77 s → 8.1 s; accepts reach 10000 immediately;
   throughput 11k → 128k snaps/s.
2. **Robust `controlling_process`** — handle `{:error,_}` instead of crashing the
   acceptor (latent bug that would bite under connection resets).
3. **Snapshot coalescing** — the broadcast path now sheds load like the
   Go/Rust/OCaml servers (which drop on a full buffer): a backed-up connection
   drops stale snapshots and sends only the freshest state. Bounds latency under
   overload: p99 at 10k improved 4977 ms → 3236 ms.

**Residual (genuine, not a bug).** After the fixes, at 10k conns on 4 cores Elixir
sustains ~103k snaps/s at p99 ≈ 3.2 s — functional but still the weakest of the
five here. That remaining gap is real: process-per-connection + GenServer-per-room
with immutable per-tick state rebuilds is heavier under a broadcast-heavy load on
only 4 cores than the Go/Rust/OCaml designs. Report it as a characteristic, not a
defect.

**Cross-cutting note (affects all servers).** The loadgen computes throughput over
total wall-clock, which includes connection establishment. When establishment is
slow, this pollutes both the rate and the "measured window." A hardening TODO:
measure throughput only over the steady-state window, and/or fail a run whose wall
time exceeds warmup+dur by more than a small margin (establishment did not settle).

## OCaml latency collapse at 10k connections (investigated & fixed)

**Symptom.** On 8 server cores, OCaml's latency exploded at 10k connections while
every other axis looked fine below it. p50 jumped from ~33 ms at 5k to **~3000 ms**
at 10k (p99 ~7000 ms), and throughput stalled at ~136k snaps/s where Go/Rust/Odin
delivered ~295k. The server never crashed — it just fell seconds behind.

**Root cause (found by server-side DIAG instrumentation, gated on `DIAG=1`).** Two
distinct bottlenecks stacked, and both had to be removed:

1. **Centralized single-domain tick.** The original design ran per-connection IO
   across N domains but funneled the *entire* simulation + broadcast through one
   central `tick_loop` on the main domain (`List.iter step rooms`). At 10k conns /
   200 rooms that one core had to build 200 snapshots and perform **10,000
   cross-domain `Eio.Stream` enqueues every tick**. DIAG measured the sweep at
   **~23 ms on a single core**, so with `sleep(33ms)` *then* the sweep the effective
   tick rate fell from 30 Hz to **~17 Hz**. This is the deviation the source header
   already flagged ("snapshot construction is centralized on the main domain").

2. **Per-frame write path.** Even after fixing (1), DIAG showed the tick running a
   clean 30 Hz/domain (~2 ms sweeps) but the mailboxes *still* pinned at the
   `drop_at=64` threshold (~60 deep, **~36% dropped**). The writer did one
   `Eio.Flow.copy_string` — a fiber-suspend + io_uring round-trip, ~50 µs — *per
   frame*, capping outbound throughput at ~160k/s. A ~60-deep standing queue draining
   at ~16 frames/s/conn is exactly the ~3 s of latency observed.

**Fixes applied to `servers/ocaml/main.ml` (idiomatic Eio rewrite).**

1. **Shared-nothing per domain.** One independent Eio event loop per domain, each
   opening the listen socket with **`SO_REUSEPORT`** so the kernel load-balances
   connections; a connection and its room live entirely on the domain that accepted
   it. Because fibers within a domain are cooperative (never parallel), domain-local
   room/player/conn state needs **no locks at all** — the `Eio.Mutex` and the shared
   registry are gone; the only cross-domain value is the atomic player-id counter.
   This mirrors the Odin thread-per-core reactor, reached by following Eio's own model
   (confine mutable state to a domain) rather than sharing it behind mutexes. Result:
   tick 17 Hz → **30 Hz/domain**, sweep 23 ms → **~2 ms**.
2. **Coalescing writer.** The per-connection writer now drains every frame already
   queued in its mailbox and writes them in **one syscall per wakeup**. Mailbox depth
   dropped from ~60 (36% dropped) to **~0.3 (zero dropped)**.

Together: at 10k conns p50 **3213 ms → ~107 ms**, p99 **7096 ms → ~182 ms**,
throughput **136k → ~233k snaps/s**; the 5k point also rose 99k → **148k** (now at
the theoretical 150k). OCaml went from dead-last to mid-pack — beating Elixir on
every axis and bettering Python's p99 (182 vs 340 ms).

**Residual (genuine, not a bug).** ~233k snaps/s is still below the 290–295k leaders
(Go/Rust/Odin/Python). At this point the server keeps up with zero mailbox backlog,
and the loadgen only drove ~154k of the 200k target moves/s, so the remaining gap
looks partly client-side rather than a server ceiling. Note also that, like Python's
multi-process and Odin's multi-worker designs, `SO_REUSEPORT` **shards a room's
members across domains** (accepted by METHODOLOGY.md) — a client sees only its
co-domain roommates, not all 50. Report the number as a characteristic of this
sharded design, not a defect.

**Lesson (cross-cutting).** The original 10k number measured *this implementation's*
centralized-tick + per-frame-write design, not OCaml/Eio's ceiling. The winning
servers all share one property regardless of async-vs-threads: the broadcast fan-out
is distributed across cores and each room owns its state (Go/Rust: one task per room;
Odin: one shard per core). Centralizing the tick on one domain threw away exactly the
multicore the additional domains were meant to provide.
