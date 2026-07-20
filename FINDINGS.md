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

## `sync.Pool` vs. the memory ramp — how much of Go's RSS is churn vs. per-conn fixed cost?

**Question.** The Go server allocates two fresh buffers per room per tick (the
snapshot `payload` + its framing buffer — see the churn note above). `sync.Pool` is
the idiomatic "allocate-per-request, reclaim-at-end" fix: return buffers to a pool
instead of letting the GC chase them. How much of Go's RSS ramp with connection count
does that actually remove?

**Variant.** `servers/go-pool/` is byte-for-byte identical to `servers/go/` except the
per-tick snapshot path: one pooled buffer holds the whole frame (`[u32 len | payload]`,
so two allocs → zero once warm). Because the snapshot is **broadcast** — one buffer
handed to every connection's writer goroutine — it outlives `step()`, so a naive
`pool.Put` at end-of-tick would recycle a buffer writers still hold. The buffer is
therefore **reference-counted**: `ref = #recipients`, each writer calls `release()`
after `nc.Write`, last one back to the pool. This is the broadcast analog of the
per-request pattern: reclaim-when-done, not free-at-request-end. Verified race-free
(`go build -race`, 200-conn load, clean) and wire-identical to the reference server.

**Result** (median of 3 trials, 8 server cores, Odin loadgen, same session as `go`):

| conns  | go peak RSS | go-pool peak RSS | saved |
|-------:|------------:|-----------------:|------:|
| 500    | 14.9 MB     | 13.1 MB          | 12%   |
| 1000   | 21.8 MB     | 18.8 MB          | 14%   |
| 2000   | 36.1 MB     | 30.0 MB          | 17%   |
| 5000   | 78.7 MB     | 63.4 MB          | 19%   |
| 10000  | 150.1 MB    | 118.9 MB         | 21%   |

Throughput, CPU and p50 are unchanged (identical within noise). Idle RSS is identical
(~5.5 MB) — pooling changes nothing until traffic flows.

**Interpretation — pooling flattens the ramp but does not bend it.**

- The saving *grows with load* (12% → 21%) because it removes **transient churn**: the
  per-tick snapshot garbage that `GOGC=100` lets accumulate to ~2× live between
  collections. More connections → higher snapshot broadcast rate → bigger transient
  set → bigger high-water mark. Pool it and that whole term drops out. This is a real,
  free ~20% peak-RSS win with no throughput/CPU/latency cost.
- But the **ramp is still steep**: the per-connection slope only falls from **14.2 to
  11.1 MB per 1000 conns** (~22%). Even fully pooled, go-pool at 10k (119 MB) is ~25×
  Rust's footprint at the same load. That residual ramp is **per-connection fixed cost
  the pool cannot touch**: 2 goroutine stacks per conn (reader + writer, ~8 KB min each
  → the dominant term: ~20000 stacks at 10k), the 64-slot send channel, and runtime
  bookkeeping. `sync.Pool` recycles *heap objects*; it does nothing for *goroutine
  stacks*, and this architecture spends two of them per connection.

- Secondary win: **tail jitter halves**. p99 CV at 10k drops from 0.064 (go) to 0.028
  (go-pool). Fewer/cheaper GC cycles → less assist + STW work landing on the tail —
  consistent with the GC-tail-spike finding above. p99 itself is within noise
  (51.9 vs 53.6 ms).

**Lesson.** `sync.Pool` is the right tool for the allocation it targets (broadcast
snapshot churn: ~20% peak RSS, halved tail jitter, zero cost) — but it is not why Go
uses more memory than Rust here. Go's connection-count ramp is dominated by the
**goroutine-per-connection-times-two** model, not by heap churn. To actually bend the
ramp you'd change the *architecture* (one goroutine per conn instead of two; or an
epoll reactor like the Odin/Python servers), not add more pooling. Data:
`results/pool/results.csv`; report: `results/pool/report.html`.

## The epoll reactor bends the ramp flat — Go's memory ramp *is* the goroutine model

**Follow-on to the `sync.Pool` result above.** Pooling the snapshot buffers only
shaved ~20% off peak RSS and barely moved the per-connection slope (14.2 → 10.8 MB
per 1000 conns), which pointed the finger at per-connection *fixed* cost — two
goroutine stacks per connection (reader + writer) — not heap churn. `servers/go-epoll/`
tests that directly: same wire protocol, same load-shedding, but **zero goroutines per
connection**. It is a thread-per-core sharded epoll reactor (a direct port of
`servers/odin`): N single-threaded event loops, one per core, each owning its
connections and rooms, SO_REUSEPORT spreading accepts. A connection is now a `Conn`
struct + its rbuf/wbuf (~1 KB warm) instead of two ~8 KB stacks; snapshot churn also
vanishes for free (single-threaded worker builds into one reused scratch buffer — no
allocation, no `sync.Pool` needed).

**Peak RSS under load** (median of 3, 8 server cores, Odin loadgen, same session):

| conns  | go       | go-pool  | go-epoll | epoll vs go |
|-------:|---------:|---------:|---------:|------------:|
| 500    | 14.8 MB  | 13.2 MB  | 6.2 MB   | −58%        |
| 1000   | 21.8 MB  | 18.6 MB  | 6.4 MB   | −71%        |
| 2000   | 35.6 MB  | 29.9 MB  | 6.6 MB   | −81%        |
| 5000   | 78.8 MB  | 62.6 MB  | 7.8 MB   | −90%        |
| 10000  | 149.6 MB | 115.8 MB | **8.6 MB** | **−94%**  |

**Per-connection ramp slope: go 14.2 → go-pool 10.8 → go-epoll 0.3 MB per 1000 conns.**
The reactor's slope is essentially zero: 10k connections cost it +3 MB over idle. This
is the whole answer to the original question — Go's memory ramp with connection count
was the goroutine-per-connection-times-two model, and `sync.Pool` couldn't touch it
because it recycles heap objects, not goroutine stacks. Remove the goroutines and the
ramp disappears; go-epoll now sits in the same tiny-footprint class as the Odin reactor.

**It's also ~40% cheaper on CPU** — at 10k conns, 2.33 cores vs go's 4.04. No goroutine
scheduling, no per-conn channel sends, no GC: the reactor just does syscalls.
Throughput is identical (296k vs 295k snaps/s).

**The tradeoff is tail latency at high fan-out.** p99 (ms):

| conns | go   | go-epoll |
|------:|-----:|---------:|
| 500   | 50.6 | 40.0     |
| 2000  | 50.1 | 47.3     |
| 5000  | 49.8 | 59.8     |
| 10000 | 55.5 | 64.4     |

Below ~2k conns the reactor wins the tail; above it, it loses ~10 ms. The cause is
structural: each worker steps **all** its rooms in one `onTick`, so at 10k/8 workers
that's ~25 rooms × 50 conns = ~1250 snapshots built and written **serially** at each
tick boundary. The goroutine server instead fans each room's broadcast out to per-conn
writer goroutines that the scheduler spreads across cores — more parallel, lower tail,
at the cost of all those stacks. Notably the reactor's tail is *more stable* (p99 CV
0.005 vs go's 0.032 at 10k): no GC pauses to inject jitter, just a higher steady floor.

**Lesson.** Three points on one curve, same language, same protocol: `go` (2 goroutines
+ GC churn), `go-pool` (2 goroutines, churn pooled away), `go-epoll` (no goroutines).
Memory tracks the goroutine count, not the allocation rate — pooling bought 20%, ditching
the goroutines bought 94%. The reactor is the right tool when footprint and CPU per
connection matter (many idle/slow conns); the goroutine model is simpler and keeps the
tail flatter under heavy per-room fan-out. Data: `results/pool/results.csv`; report:
`results/pool/report.html`.

## The Dart 10k `send%` dip — single-thread GC starves the tick (investigated; latency mitigated, send% not)

**Symptom.** At 10k connections the Dart server (`servers/dart`) trips the loadgen's
coordinated-omission check: steady-state `send%` sits in the high-80s/low-90s (6-trial
mean **91%**, range 87–96%) while Zig/Clojure hold ~100%. It happens at only ~5.5/8
server cores — **nowhere near CPU saturation.** Latency is also poor: p50 ≈ 39 ms,
p99 ≈ 73 ms.

**Why `send%` is the signal (not throughput).** `send%` can only drop one way: the
loadgen's fixed-rate send loop finds a connection's deadline has slipped past by more
than a full interval and coalesces the missed sends away (see
`## Reading the loadgen self-check`). Write backpressure does *not* lower it (moves are
counted unconditionally). So a dip means the client's loop stalled — and since the
client uses *less* CPU during Dart runs than during the (faster) Zig runs, it is not
client overload. It is the pattern of what Dart puts on the wire.

**Seven experiments, four dead ends.**

| # | intervention | result | ruled out |
|---|---|---|---|
| 1 | `--new_gen_semi_max_size` bump | no change | GC *frequency* tuning |
| 2 | `RawSocket` (drop `dart:io` high-level `Socket`) | −12% CPU, −17% RSS, dip persists | the IOSink write abstraction |
| 3 | staggered tick (phase-slot rooms vs one `Timer.periodic` batch) | **−20% latency**, dip persists | within-tick emission burst |
| 4 | isolate oversubscription (8→16→32 workers / 8 cores) | no change | conns-per-isolate / CPU |
| 5 | tick instrumentation (`TICK_LOG`) | 8–12 ms per-fire blocks, gaps to 20 ms, at ~57% CPU/isolate | *(measured the cause)* |
| 6 | self-paced chunked async tick (`await` between room chunks) | a single ≤4-room chunk still blocks ~6 ms | that async can hide it |
| 7 | allocation-pooled hot paths | latency win holds; send% ~unchanged | *(see below)* |

**Root cause.** On a single-threaded isolate the tick timer, socket reads, and writes
all serialize on one thread. Periodic **stop-the-world GC pauses (~6–8 ms)** land inside
a tick, blow the tick budget, and bunch snapshot emission into gap-then-burst. The client
receives irregular bursts and its fixed-rate send loop occasionally overruns a deadline →
`send%` dips. The spare cores can't help: one isolate is one thread and GC freezes it —
which is exactly why it dips *while unsaturated*. Experiment 6 is the clincher: chunking
to ≤4 rooms and yielding to I/O between chunks left the longest block at ~6 ms, and a
handful of writes cannot take 6 ms — only a stop-the-world pause can, and `await` cannot
run during one. This is why the failure is Dart-specific: Zig/Odin do cheap GC-free work
that fits the tick budget and `epoll` inline; OCaml/Elixir fail into a *different* regime
(uniform high tail / clean CPU saturation) rather than bursty gaps.

**What allocation pooling changed — and didn't (`servers/dart-pool`).** The refined
server strips the removable per-event allocations (ByteData views → direct big-endian
byte-math; the inbound copy → parse-in-place from the read buffer; JOINED ack and
per-pass room list → reused buffers). GC pause *frequency* drops (allocation rate down)
but *magnitude* does not (set by the live-set), and a hard floor remains: **`RawSocket.read()`
allocates a fresh `Uint8List` per read (~200k/s at 10k) and `dart:io` exposes no
read-into API.** Net over 6 trials at 10k:

- **`send%`: not robustly improved** — pool mean 93% vs baseline 91%, both high-variance
  (pool range 85–99%), both still dip below the 97% threshold on most trials. The
  read()-driven GC-pause floor persists.
- **latency: robustly and substantially improved** — p50 **38.6 → 25.9 ms (−33%)**, p99
  **73.2 → 51.8 ms (−29%)** on *every* trial, at lower CPU (5.5 → 5.2) and RSS
  (119 → 103 MB). This win is from the RawSocket reactor + spread tick, retained in the
  pooled server.

**Residual (genuine characteristic, not a bug).** The 10k `send%` dip is a real property
of Dart's single-threaded-isolate model: a fine-grained game tick cannot be held on time
under fan-out because stop-the-world GC on the one thread bunches delivery. It is **not
fixable in idiomatic Dart** — closing it fully would mean dropping below `dart:io`'s
socket API (FFI `epoll`+`recv` into reused buffers), at which point it is no longer a Dart
program in any meaningful sense. Practical outcome: `servers/dart-pool` supersedes
`servers/dart` for its ~30% latency and lower CPU/RSS; treat the residual few-percent
`send%` dip as Dart's cost of a GC'd single-threaded event loop — the analogue of OCaml's
tail-latency regime and Elixir's CPU cost, a characteristic to report, not a defect.

**Artifacts.** `servers/dart-pool/server.dart` (RawSocket + self-paced chunked tick +
pooled hot paths). `TICK_LOG=<path>` enables per-isolate tick-timing logs
(`maxChunkMs` = longest contiguous on-loop block; dormant unless set). Intermediate
variants (raw / staggered-tick / async) were rungs on the ladder — their deltas are the
table above.
