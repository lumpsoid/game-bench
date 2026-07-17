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
