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

## Reading the loadgen self-check (`send%` / `clientcpu`)

The Odin loadgen (`loadgen-odin/main.odin`) is **open-loop**: each worker thread
fires MOVEs on a fixed 20 Hz schedule and never waits for replies, so server stalls
surface as latency instead of being hidden. Two mechanics govern what `send%`
(`send_rate_pct`) means, and both are load-bearing for interpreting a run:

1. A MOVE is counted the instant it is **enqueued** — `send_move` does
   `moves_sent += 1` right after `enqueue`, *even if the socket returns EAGAIN and
   the byte sits in `wbuf`*. **Socket backpressure alone therefore does not lower
   `send%`.**
2. The only thing that lowers `send%` is the **snap-forward** in the send loop: if a
   connection's deadline has slipped more than one full interval (>50 ms), the
   loadgen *drops* the missed ticks rather than bursting to catch up
   (`if now - head.next_send > interval_ns { head.next_send = now + interval_ns }`).
   That dropped tick is the shortfall — the deliberate anti-coordinated-omission move.

So **`send% < 100` means exactly one thing: a worker thread was too busy to revisit
its send schedule for >50 ms** — either CPU-bound draining recv work, or drowning in
`EPOLLOUT` flush events because the *server's read side* is backpressuring the
client's writes. And because `send_target = conns × rate × (warmup + dur)` bills
every connection for the full window, **connection-establishment ramp is a permanent
floor tax**: a connection only starts sending after TCP-connect + JOIN→JOINED.

Three observations from the Odin-loadgen matrix, each a distinct mechanism:

**Rust's ~1–2.7% `send%` dip is establishment ramp, not omission.** The tell: Rust is
already at 99.0% at 500 conns, where the client is 99.99% idle (`clientcpu≈0.22`) —
no CPU pressure and no backpressure is possible, so the only thing that can cost
sends is connections entering the schedule late. Tokio's accept-and-spawn path is a
touch heavier per connection than Go's/Odin's accept loops, so Rust's connections
join marginally later; that lost lead-in is a ~1% constant tax that grows to ~2.7% at
10k as accept contention rises. It is **not** a mid-run stall — Rust has the cleanest
latencies of the field at 10k (p99 ≈ 54 ms, best of all) and normal `clientcpu`. The
metric was slightly *unfair* to Rust here: it was the one number that made the
healthiest server look worst, and it was really just measuring Tokio's accept warm-up.

**Fixed (both loadgens).** `send%` now measures only the steady-state (post-warmup)
window — a warmup-gated `moves_measured` counter (mirroring the existing latency gate
`(now - start) > warm`) over `target = conns × rate × dur`, dropping `warmup` from the
denominator. A connection only enters the send schedule after JOIN, so the ramp no
longer counts against it; by the measured window every conn is joined and sending at
full rate. Effect: Rust @ 2k rose 98.1% → **100.0%**, and a dip now unambiguously means
a genuine mid-run sender stall. Elixir's real 82% stall at 10k is unaffected — its
shortfall is *inside* the measured window. See `moves_measured` in
`loadgen-odin/main.odin` and `movesMeasured` in `loadgen/main.go`.

**A missing omission warning does not mean the server is healthy.** At 10k:

- **OCaml**: p99 ≈ **8,800 ms** but `send=98.9%`, *no warning*. Its bottleneck is
  snapshot *production* (the tick/coalescing writer stalls; `snaps/s` ≈ 146k, half of
  Go's ~296k), but it still drains the client's tiny 13-byte MOVEs fine, so there is
  no write-backpressure. The client keeps cadence → the 8.8 s tail is **real and
  honestly measured**.
- **Elixir**: p99 ≈ 350–515 ms but `send=82%`, *warning fires*. Its **read** side
  falls behind (at 10k BEAM processes, a connection isn't scheduled to drain its
  socket promptly) → the client's writes back up → the worker floods with `EPOLLOUT`
  flushes → it can't revisit the send schedule within 50 ms → ~18% of ticks dropped.
  So the 350 ms p99 is **optimistic** — ~18% of intended load was never offered.

The warning tells you whether to *trust* the number, not whether the server is good.
OCaml's is **trustworthy-and-catastrophic**; Elixir's is **untrustworthy-and-bad** —
OCaml is arguably the worse server despite triggering no warning.

**The old Go loadgen was self-saturating at 10k; the Odin rewrite proves the Go
server never saturates.** Same Go server, two loadgens:

| loadgen | Go @ 10k p99 | Go server CPU | clientcpu |
|---|---|---|---|
| old Go (goroutine-per-conn, ~20k goroutines) | ~140 ms | 5.26 | (not measured) |
| new Odin (8-worker epoll reactor) | ~65 ms | 4.38 | 2.57 |

The Go loadgen ran two goroutines per connection over 8 client cores, so a chunk of
that 140 ms p99 was the *client's own* scheduler latency — and it had no
`send%`/`clientcpu` instrumentation to reveal it. The lean Odin reactor costs only
2.57 client cores at 10k, so the Go server's true p99 (~65 ms, 4.38/8 cores, headroom
to spare) shows through. The sharpest demonstration is OCaml: the old loadgen reported
**p50 = 3,147 ms**, the Odin loadgen **p50 ≈ 90 ms** with p99 ≈ 8,800 ms — the old
client was so starved it corrupted even the *median*, while the lean client keeps the
median honest and exposes the real server tail. That 35× p50 drop is entirely a
loadgen artifact, which is exactly why the rewrite (with `send%`/`clientcpu`) was
worth doing.

## Latency-stability fingerprints — odin, go, rust (top three)

Across the top three the *median* is nearly identical (~28–36 ms) and p99 is
comparable, so the interesting engineering signal is entirely in the **jitter**
(per-1 s-window p99 coefficient of variation). Each language's jitter has a completely
different fingerprint:

| server | stability failure mode | worst at |
|---|---|---|
| **odin** | timer-granularity beat vs 20 Hz sends when *idle* | 5k (recovers at 10k) |
| **go** | periodic GC pauses; scale with alloc volume | 10k |
| **rust** | none — no GC, work-stealing balances tails | flat |

**Odin is bumpier at 5k than at 10k (counterintuitive).** The tick loop is
**timer-clocked** via the epoll timeout, with an integer-millisecond floor:

```odin
wait_ns := next_tick._nsec - now._nsec
if wait_ns > 0 { timeout_ms = i32(wait_ns / 1_000_000) }   // floor to whole ms
nev := epoll_wait(w.epfd, ..., timeout_ms)
...
if now._nsec >= next_tick._nsec { tick_room(...) }          // fire tick
```

At **5k** the worker is mostly idle (`cpu≈1.5/8`) and sleeps in `epoll_wait` between
ticks, so cadence is set by that timeout — but 1/30 Hz = 33.3 ms floors to a 33 ms
timeout, so ticks land at 33–34 ms. That beats against the client's 50 ms (20 Hz)
send interval, and the p99 at 5k sits *right at the 50 ms beat knee* (49–56 ms); tiny
tick-timing wobble shoves a fraction of requests across it → sawtooth tail
(max/p99.9 swinging 48↔67 ms) and a CV spike to **0.043**. At **10k** the worker is
busy: `epoll_wait` returns immediately full of events, the loop spins, and the tick is
**work-clocked** rather than timer-clocked. Latency rises to a steady ~68 ms (*above*
the beat knee) and the jitter washes out → CV back to ~0.004, bands tight. More load
buys stability here.

**Go is worst at 10k — the GC signature.** The server allocates on every tick and
every frame:

```go
payload := make([]byte, 7+n*16)   // fresh snapshot buffer, per room, per tick
b := make([]byte, 4+len(payload)) // fresh framing buffer, per broadcast
```

At 10k conns / 50 per room = ~200 room goroutines each `time.NewTicker`-ing at 30 Hz,
this is a firehose of short-lived garbage → periodic GC, whose assist + brief STW work
lands on the tail. The go@10k timeline shows exactly this: a ~65 ms baseline with
**regular ~85 ms spikes every ~4–6 s**. The tick *timing* is fine (real
`time.NewTicker`); it's the collector perturbing the tail, and because allocation
volume scales with connections the CV climbs monotonically to **0.018** at 10k.

**Rust is flat everywhere.** No GC → no periodic pause to inject a tail spike, and
Tokio's work-stealing scheduler keeps per-worker load balanced so no single worker's
tail runs away. CV pinned at ~0.003–0.004 at every load, and it **wins the tail at
10k**: p99 ≈ 54 ms flat while Odin and Go rise to ~68 and ~65 ms. Rust trades a hair
more per-request cost at low load (p50 ≈ 29 ms, from Tokio task overhead) for a tail
that simply does not move.

**Lesson.** All three top languages have essentially the same median and comparable
p99; the differentiating signal is jitter, and each fingerprint is distinct: Odin's is
a scheduling artifact that *disappears* under load, Go's is the runtime (GC) and
*grows* with load, Rust's is absent by construction. To confirm directly: run Go with
`GODEBUG=gctrace=1` (or `GOGC=off` for a short run) and check the spike timestamps
line up with GC cycles; flatten the Odin 5k sawtooth by moving its tick loop to a
self-clocked / higher-resolution timer instead of the ms-granular epoll timeout.
