// Load generator (Odin) — a lean, epoll-driven port of the Go loadgen.
//
// Odin has no green threads, so we can't run two goroutines per connection like
// the Go client (20k goroutines at 10k conns). Instead this mirrors the Odin
// SERVER's design: a THREAD-PER-CORE SHARDED REACTOR. Each worker thread owns a
// disjoint block of connections and runs one single-threaded epoll loop — so a
// worker touches only its own connections and histograms, with zero locking on
// the hot path (the only shared state is a merge under a mutex at the very end).
//
// Each connection simulates a player exactly like the Go client: JOIN a room,
// send MOVE at a fixed rate (OPEN-LOOP — sends never wait for replies, so server
// stalls surface as latency), read SNAPSHOTs, and record end-to-end latency via
// the echoed seq. Output (stderr logs + one JSON line on stdout) is byte-for-byte
// compatible with the Go loadgen, so runner/run.py can use either.
//
// Open-loop timing without per-connection timers: because every connection sends
// at the same rate, a connection's send deadlines are strictly periodic, so a
// simple FIFO ring ordered by next-send stays sorted with O(1) push/pop — we only
// ever look at the head. See PROTOCOL.md for the wire format.
package main

import "core:fmt"
import "core:math"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:thread"
import "core:time"
import linux "core:sys/linux"

MSG_JOIN :: 0x01
MSG_MOVE :: 0x02
MSG_JOINED :: 0x81
MSG_SNAPSHOT :: 0x82

RING :: 256 // seq -> send time; window = RING/rate seconds (must exceed max RTT)
READ_CHUNK :: 65536
MAX_EVENTS :: 1024
WINDOW_NS :: i64(1_000_000_000) // 1 s latency-timeline windows

// ---- histogram: log-linear, 1 µs .. 100 s, 1000 sub-buckets/decade ----

DECADES :: 8
PER_DECADE :: 1000
NBUCKETS :: DECADES * PER_DECADE

Hist :: struct {
	counts: [NBUCKETS]u64,
}

idx_for :: proc(us: f64) -> int {
	x := us
	if x < 1 {x = 1}
	e := math.log10(x)
	if e < 0 {e = 0}
	i := int(e * PER_DECADE)
	if i >= NBUCKETS {i = NBUCKETS - 1}
	return i
}

val_for :: proc(i: int) -> f64 {return math.pow(10, f64(i) / PER_DECADE)}

hist_record :: proc(h: ^Hist, us: f64) {h.counts[idx_for(us)] += 1}

hist_total :: proc(h: ^Hist) -> u64 {
	t: u64
	for i in 0 ..< NBUCKETS {t += h.counts[i]}
	return t
}

hist_percentile :: proc(h: ^Hist, p: f64) -> f64 {
	total := hist_total(h)
	if total == 0 {return 0}
	target := u64(p * f64(total))
	c: u64
	for i in 0 ..< NBUCKETS {
		c += h.counts[i]
		if c >= target {return val_for(i)}
	}
	return val_for(NBUCKETS - 1)
}

hist_merge :: proc(dst, src: ^Hist) {
	for i in 0 ..< NBUCKETS {dst.counts[i] += src.counts[i]}
}

// stability_stats: worst (max) window value + coefficient of variation (sample
// stdev / mean) across the supplied per-window p99 values.
stability_stats :: proc(xs: []f64) -> (worst, cv: f64) {
	if len(xs) == 0 {return 0, 0}
	sum: f64
	for x in xs {
		if x > worst {worst = x}
		sum += x
	}
	mean := sum / f64(len(xs))
	if len(xs) < 2 || mean == 0 {return worst, 0}
	ss: f64
	for x in xs {
		d := x - mean
		ss += d * d
	}
	return worst, math.sqrt(ss / f64(len(xs) - 1)) / mean
}

// ---- wire helpers ----

put_u32 :: proc(b: []u8, v: u32) {
	b[0] = u8(v >> 24);b[1] = u8(v >> 16);b[2] = u8(v >> 8);b[3] = u8(v)
}
put_u16 :: proc(b: []u8, v: u16) {b[0] = u8(v >> 8);b[1] = u8(v)}
get_u32 :: proc(b: []u8) -> u32 {
	return u32(b[0]) << 24 | u32(b[1]) << 16 | u32(b[2]) << 8 | u32(b[3])
}
get_u16 :: proc(b: []u8) -> u16 {return u16(b[0]) << 8 | u16(b[1])}

// ---- connection & worker state ----

CConn :: struct {
	fd:        linux.Fd,
	room:      u32,
	my_id:     u32,
	connected: bool, // TCP connect completed
	joined:    bool, // JOINED received; participating in the send schedule
	dead:      bool,
	want_out:  bool, // EPOLLOUT currently armed
	rbuf:      [dynamic]u8,
	wbuf:      [dynamic]u8,
	seq:       u32,
	send_at:   [RING]i64, // seq%RING -> send timestamp (monotonic ns)
	next_send: i64, // when this conn should send its next MOVE
}

// Sched: FIFO ring of joined conns ordered by next_send. Equal send interval
// keeps it sorted, so the head is always the soonest deadline.
Sched :: struct {
	buf:   []^CConn,
	head:  int,
	count: int,
}

sched_push :: proc(s: ^Sched, c: ^CConn) {
	s.buf[(s.head + s.count) % len(s.buf)] = c
	s.count += 1
}
sched_pop :: proc(s: ^Sched) -> ^CConn {
	c := s.buf[s.head]
	s.head = (s.head + 1) % len(s.buf)
	s.count -= 1
	return c
}

Worker :: struct {
	id:          int,
	epfd:        linux.Fd,
	conns:       [dynamic]^CConn,
	sched:       Sched,
	// config
	ip:          [4]u8,
	port:        u16,
	room_size:   int,
	interval_ns: i64,
	start_ns:    i64,
	warm_ns:     i64,
	deadline_ns: i64, // start + warm + dur (sending stops here)
	nwin:        int,
	// index range [lo, hi) of the GLOBAL connection ids this worker owns
	lo, hi:      int,
	// results (worker-local; merged under a mutex at the end)
	hist:        Hist,
	windows:     []Hist,
	moves_sent:  u64,
	moves_measured: u64, // moves fired in the post-warmup window (send% numerator)
	snaps_recv:  u64,
	measured:    u64,
}

now_ns :: proc() -> i64 {return time.tick_now()._nsec}

// ---- outbound ----

client_mod :: proc(w: ^Worker, c: ^CConn, want_out: bool) {
	ev: linux.EPoll_Event
	ev.events = {.IN}
	if want_out {ev.events |= {.OUT}}
	ev.data.ptr = c
	linux.epoll_ctl(w.epfd, .MOD, c.fd, &ev)
	c.want_out = want_out
}

// flush writes as much of wbuf as the socket accepts; arms/disarms EPOLLOUT to
// match the remaining backlog. Returns false if the conn must be closed.
flush :: proc(w: ^Worker, c: ^CConn) -> bool {
	sent := 0
	for sent < len(c.wbuf) {
		n, err := linux.send(c.fd, c.wbuf[sent:], {.NOSIGNAL})
		if err == .EAGAIN || err == .EWOULDBLOCK {break}
		if err == .EINTR {continue}
		if err != .NONE || n <= 0 {return false}
		sent += n
	}
	if sent > 0 {
		remaining := len(c.wbuf) - sent
		if remaining > 0 {copy(c.wbuf[:], c.wbuf[sent:])}
		resize(&c.wbuf, remaining)
	}
	want := len(c.wbuf) > 0
	if want != c.want_out {client_mod(w, c, want)}
	return true
}

enqueue :: proc(w: ^Worker, c: ^CConn, data: []u8) {
	append(&c.wbuf, ..data)
	flush(w, c)
}

send_join :: proc(w: ^Worker, c: ^CConn) {
	frame: [9]u8
	put_u32(frame[0:], 5)
	frame[4] = MSG_JOIN
	put_u32(frame[5:], c.room)
	enqueue(w, c, frame[:])
}

// send_move stamps the send time (open-loop: fire regardless of replies).
send_move :: proc(w: ^Worker, c: ^CConn, now: i64) {
	c.seq += 1
	c.send_at[c.seq % RING] = now
	frame: [13]u8
	put_u32(frame[0:], 9)
	frame[4] = MSG_MOVE
	put_u32(frame[5:], c.seq)
	put_u16(frame[9:], 1) // dx
	put_u16(frame[11:], 0) // dy
	enqueue(w, c, frame[:])
	w.moves_sent += 1
	// send% is measured over the steady-state window only, matching the latency
	// gate below: warmup ramp (conns still JOINing) must not count against it.
	if now - w.start_ns > w.warm_ns {w.moves_measured += 1}
}

// ---- inbound ----

client_handle_frame :: proc(w: ^Worker, c: ^CConn, payload: []u8, now: i64) {
	switch payload[0] {
	case MSG_JOINED:
		if len(payload) < 5 {return}
		c.my_id = get_u32(payload[1:])
		if !c.joined {
			c.joined = true
			c.next_send = now + w.interval_ns
			sched_push(&w.sched, c)
		}
	case MSG_SNAPSHOT:
		if !c.joined {return}
		w.snaps_recv += 1
		if len(payload) < 7 {return}
		count := int(get_u16(payload[5:]))
		off := 7
		for k in 0 ..< count {
			if off + 16 > len(payload) {break}
			if get_u32(payload[off:]) == c.my_id {
				last_seq := get_u32(payload[off + 12:])
				if last_seq != 0 {
					st := c.send_at[last_seq % RING]
					if st != 0 {
						us := f64(now - st) / 1000.0
						if us > 0 && (now - w.start_ns) > w.warm_ns {
							hist_record(&w.hist, us)
							wi := int((now - w.start_ns - w.warm_ns) / WINDOW_NS)
							if wi >= 0 && wi < len(w.windows) {
								hist_record(&w.windows[wi], us)
							}
							w.measured += 1
						}
					}
				}
				break
			}
			off += 16
		}
	}
}

// client_read drains the socket and processes every complete frame.
client_read :: proc(w: ^Worker, c: ^CConn, now: i64) -> bool {
	tmp: [READ_CHUNK]u8
	for {
		n, err := linux.read(c.fd, tmp[:])
		if err == .EAGAIN || err == .EWOULDBLOCK {break}
		if err == .EINTR {continue}
		if err != .NONE || n == 0 {return false}
		append(&c.rbuf, ..tmp[:n])
		if n < len(tmp) {break}
	}
	off := 0
	for len(c.rbuf) - off >= 4 {
		length := int(get_u32(c.rbuf[off:]))
		if length <= 0 || length > (1 << 20) {return false}
		if len(c.rbuf) - off - 4 < length {break}
		client_handle_frame(w, c, c.rbuf[off + 4:off + 4 + length], now)
		off += 4 + length
	}
	if off > 0 {
		remaining := len(c.rbuf) - off
		if remaining > 0 {copy(c.rbuf[:], c.rbuf[off:])}
		resize(&c.rbuf, remaining)
	}
	return true
}

// ---- connection lifecycle ----

kill_conn :: proc(w: ^Worker, c: ^CConn) {
	if c.dead {return}
	c.dead = true
	linux.epoll_ctl(w.epfd, .DEL, c.fd, nil)
	linux.close(c.fd)
}

// dial creates a non-blocking socket, starts the connect, and registers for
// EPOLLOUT (writable == connect completed). Returns nil on immediate failure.
dial :: proc(w: ^Worker, room: u32) -> ^CConn {
	fd, err := linux.socket(.INET, .STREAM, {.NONBLOCK}, .TCP)
	if err != .NONE {return nil}
	one: i32 = 1
	linux.setsockopt(fd, linux.SOL_TCP, linux.Socket_TCP_Option.NODELAY, &one)

	addr: linux.Sock_Addr_In
	addr.sin_family = .INET
	addr.sin_port = u16be(w.port)
	addr.sin_addr = w.ip
	cerr := linux.connect(fd, &addr)
	if cerr != .NONE && cerr != .EINPROGRESS {
		linux.close(fd)
		return nil
	}
	c := new(CConn)
	c.fd = fd
	c.room = room
	ev: linux.EPoll_Event
	ev.events = {.OUT} // wait for connect to complete
	ev.data.ptr = c
	if linux.epoll_ctl(w.epfd, .ADD, fd, &ev) != .NONE {
		linux.close(fd)
		free(c)
		return nil
	}
	return c
}

// ---- worker event loop ----

worker_run :: proc(w: ^Worker) {
	epfd, err := linux.epoll_create1({})
	if err != .NONE {
		fmt.eprintfln("loadgen worker %d: epoll_create failed: %v", w.id, err)
		return
	}
	w.epfd = epfd

	// Open all connections for this worker, lightly staggered to avoid a SYN
	// thundering herd (mirrors the Go client's connect ramp).
	local := 0
	for gi in w.lo ..< w.hi {
		c := dial(w, u32(gi / w.room_size))
		if c != nil {append(&w.conns, c)}
		local += 1
		if local % 200 == 0 {time.sleep(5 * time.Millisecond)}
	}
	w.sched.buf = make([]^CConn, len(w.conns) + 1) // +1 so head/tail never collide

	events: [MAX_EVENTS]linux.EPoll_Event
	for {
		now := now_ns()
		if now >= w.deadline_ns {break}

		// Timeout = time until the next scheduled send (or a short poll while we
		// are still waiting for connections to JOIN), clamped to the deadline.
		// Round the wait UP to whole ms: a sub-ms wait must not floor to 0, or the
		// worker spins on timeout=0 instead of blocking. Sending up to ~1 ms late
		// is harmless — send_at is stamped at the real send, so latency stays exact.
		timeout_ms: i32 = 50
		if w.sched.count > 0 {
			wait := w.sched.buf[w.sched.head].next_send - now
			timeout_ms = wait <= 0 ? 0 : i32((wait + 999_999) / 1_000_000)
		}
		until := (w.deadline_ns - now) / 1_000_000
		if i64(timeout_ms) > until {timeout_ms = i32(max(until, 0))}

		nev, werr := linux.epoll_wait(w.epfd, &events[0], MAX_EVENTS, timeout_ms)
		if werr != .NONE && werr != .EINTR {break}

		now = now_ns()
		for i in 0 ..< int(nev) {
			ev := events[i]
			c := cast(^CConn)ev.data.ptr
			if c.dead {continue}
			if .ERR in ev.events || .HUP in ev.events {
				kill_conn(w, c)
				continue
			}
			if !c.connected {
				c.connected = true
				client_mod(w, c, false) // switch OUT->IN; flush re-arms OUT if needed
				send_join(w, c)
				continue
			}
			if .IN in ev.events {
				if !client_read(w, c, now) {kill_conn(w, c);continue}
			}
			if .OUT in ev.events {
				if !flush(w, c) {kill_conn(w, c);continue}
			}
		}

		// Fire every send whose deadline has passed. Advance by one interval to
		// hold cadence; if we've fallen more than an interval behind (worker was
		// starved), snap forward instead of bursting — that shortfall is exactly
		// what send_rate_pct should reveal.
		now = now_ns()
		for w.sched.count > 0 {
			head := w.sched.buf[w.sched.head]
			if head.next_send > now {break}
			sched_pop(&w.sched)
			if head.dead {continue}
			send_move(w, head, now)
			head.next_send += w.interval_ns
			if now - head.next_send > w.interval_ns {head.next_send = now + w.interval_ns}
			sched_push(&w.sched, head)
		}
	}

	for c in w.conns {
		if !c.dead {linux.close(c.fd)}
	}
}

worker_thread_proc :: proc(t: ^thread.Thread) {
	worker_run(cast(^Worker)t.data)
}

// ---- argument parsing ----

parse_addr :: proc(addr: string) -> (ip: [4]u8, port: u16) {
	ip = {127, 0, 0, 1}
	port = 9000
	host := addr
	if idx := strings.last_index_byte(addr, ':'); idx >= 0 {
		host = addr[:idx]
		if v, ok := strconv.parse_int(addr[idx + 1:]); ok && v > 0 && v <= 65535 {
			port = u16(v)
		}
	}
	if host == "" || host == "localhost" {return}
	parts := strings.split(host, ".")
	defer delete(parts)
	if len(parts) == 4 {
		for p, i in parts {
			if v, ok := strconv.parse_int(p); ok && v >= 0 && v <= 255 {ip[i] = u8(v)}
		}
	}
	return
}

// parse_dur understands the "<int>s" / "<int>ms" forms runner/run.py emits.
parse_dur_ns :: proc(s: string) -> i64 {
	if strings.has_suffix(s, "ms") {
		if v, ok := strconv.parse_int(s[:len(s) - 2]); ok {return i64(v) * 1_000_000}
	} else if strings.has_suffix(s, "s") {
		if v, ok := strconv.parse_int(s[:len(s) - 1]); ok {return i64(v) * 1_000_000_000}
	} else if v, ok := strconv.parse_int(s); ok {
		return i64(v) * 1_000_000_000
	}
	return 0
}

arg_str :: proc(args: []string, i: ^int, cur: string) -> string {
	if i^ + 1 < len(args) {i^ += 1;return args[i^]}
	return cur
}
arg_int :: proc(args: []string, i: ^int, cur: int) -> int {
	if i^ + 1 < len(args) {
		i^ += 1
		if v, ok := strconv.parse_int(args[i^]); ok {return v}
	}
	return cur
}

// ---- self-check: is the loadgen itself the bottleneck? ----

self_cpu_cores :: proc(wall_s: f64) -> f64 {
	ru: linux.RUsage
	if wall_s <= 0 || linux.getrusage(.SELF, &ru) != .NONE {return 0}
	cpu :=
		f64(ru.utime.seconds) +
		f64(ru.utime.microseconds) / 1e6 +
		f64(ru.stime.seconds) +
		f64(ru.stime.microseconds) / 1e6
	return cpu / wall_s
}

main :: proc() {
	addr := "127.0.0.1:9000"
	conns := 1000
	room_size := 50
	rate := 20
	dur_ns := i64(30) * 1_000_000_000
	warm_ns := i64(5) * 1_000_000_000
	json_out := false
	nworkers := 4

	args := os.args
	i := 1
	for i < len(args) {
		switch args[i] {
		case "-addr", "--addr":
			addr = arg_str(args, &i, addr)
		case "-conns", "--conns":
			conns = arg_int(args, &i, conns)
		case "-room-size", "--room-size":
			room_size = arg_int(args, &i, room_size)
		case "-rate", "--rate":
			rate = arg_int(args, &i, rate)
		case "-dur", "--dur":
			dur_ns = parse_dur_ns(arg_str(args, &i, ""))
		case "-warmup", "--warmup":
			warm_ns = parse_dur_ns(arg_str(args, &i, ""))
		case "-workers", "--workers":
			nworkers = arg_int(args, &i, nworkers)
		case "-json", "--json":
			json_out = true
		}
		i += 1
	}
	if rate <= 0 {rate = 20}
	if room_size <= 0 {room_size = 50}
	if nworkers <= 0 {nworkers = 1}
	if nworkers > conns {nworkers = max(conns, 1)}

	ip, port := parse_addr(addr)
	nwin := int(dur_ns / WINDOW_NS)
	if nwin < 1 {nwin = 1}
	interval_ns := i64(1_000_000_000 / rate)

	start_ns := now_ns()
	deadline_ns := start_ns + warm_ns + dur_ns

	workers := make([]^Worker, nworkers)
	threads := make([]^thread.Thread, nworkers)
	base := conns / nworkers
	extra := conns % nworkers
	next := 0
	for wi in 0 ..< nworkers {
		n := base + (wi < extra ? 1 : 0)
		w := new(Worker)
		w.id = wi
		w.ip = ip;w.port = port
		w.room_size = room_size
		w.interval_ns = interval_ns
		w.start_ns = start_ns;w.warm_ns = warm_ns;w.deadline_ns = deadline_ns
		w.nwin = nwin
		w.windows = make([]Hist, nwin)
		w.lo = next;w.hi = next + n
		next += n
		workers[wi] = w
		t := thread.create(worker_thread_proc)
		t.data = w
		threads[wi] = t
		thread.start(t)
	}
	for t in threads {thread.join(t)}

	// merge worker-local results
	total: Hist
	windows := make([]Hist, nwin)
	moves, moves_measured, snaps, measured: u64
	for w in workers {
		hist_merge(&total, &w.hist)
		for k in 0 ..< nwin {hist_merge(&windows[k], &w.windows[k])}
		moves += w.moves_sent
		moves_measured += w.moves_measured
		snaps += w.snaps_recv
		measured += w.measured
	}

	secs := f64(now_ns() - start_ns) / 1e9

	// per-window p99 timeline -> stability scalars (empty windows skipped)
	p99s := make([dynamic]f64)
	defer delete(p99s)
	tl := strings.builder_make()
	defer strings.builder_destroy(&tl)
	strings.write_byte(&tl, '[')
	first := true
	for k in 0 ..< nwin {
		if hist_total(&windows[k]) == 0 {continue}
		p99 := hist_percentile(&windows[k], 0.99) / 1000
		append(&p99s, p99)
		if !first {strings.write_byte(&tl, ',')}
		first = false
		fmt.sbprintf(
			&tl,
			"[%d,%f,%f,%f,%f,%f]",
			k,
			hist_percentile(&windows[k], 0.50) / 1000,
			hist_percentile(&windows[k], 0.90) / 1000,
			p99,
			hist_percentile(&windows[k], 0.999) / 1000,
			hist_percentile(&windows[k], 1.0) / 1000,
		)
	}
	strings.write_byte(&tl, ']')
	p99_worst, p99_cv := stability_stats(p99s[:])

	client_cpu := self_cpu_cores(secs)
	// send% covers ONLY the steady-state (post-warmup) window. A connection enters
	// the send schedule after JOIN, so charging it for the warmup ramp understated
	// healthy runs and penalized servers with a slightly heavier accept path (e.g.
	// Tokio's accept+spawn). By the measured window every conn is joined and should
	// send at full rate, so a dip here now means a genuine mid-run sender stall
	// (real coordinated omission), not connection-establishment slack.
	send_target := f64(conns) * f64(rate) * (f64(dur_ns) / 1e9)
	send_rate_pct := send_target > 0 ? 100.0 * f64(moves_measured) / send_target : 100.0

	// human-readable lines on stderr; machine JSON on stdout (matches Go loadgen)
	fmt.eprintfln("conns=%d moves=%d snaps=%d measured=%d", conns, moves, snaps, measured)
	fmt.eprintfln("throughput: moves=%.0f/s snaps=%.0f/s", f64(moves) / secs, f64(snaps) / secs)
	fmt.eprintfln(
		"latency ms: p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f max=%.2f",
		hist_percentile(&total, 0.50) / 1000,
		hist_percentile(&total, 0.90) / 1000,
		hist_percentile(&total, 0.99) / 1000,
		hist_percentile(&total, 0.999) / 1000,
		hist_percentile(&total, 1.0) / 1000,
	)
	fmt.eprintfln(
		"latency stability: p99_worst_1s=%.2f ms p99_cv=%.3f (%d windows)",
		p99_worst,
		p99_cv,
		len(p99s),
	)
	fmt.eprintfln(
		"loadgen self-check: client_cpu=%.2f cores  send_rate=%.1f%% of target",
		client_cpu,
		send_rate_pct,
	)

	if json_out {
		// Assemble by hand: Odin's fmt treats literal { } as directives, so we
		// only ever hand it brace-free format fragments.
		b := strings.builder_make()
		defer strings.builder_destroy(&b)
		strings.write_byte(&b, '{')
		fmt.sbprintf(&b, `"conns":%d,`, conns)
		fmt.sbprintf(&b, `"moves_sent":%d,`, moves)
		fmt.sbprintf(&b, `"snaps_recv":%d,`, snaps)
		fmt.sbprintf(&b, `"measured":%d,`, measured)
		fmt.sbprintf(&b, `"moves_per_s":%f,`, f64(moves) / secs)
		fmt.sbprintf(&b, `"snaps_per_s":%f,`, f64(snaps) / secs)
		fmt.sbprintf(&b, `"p50_ms":%f,`, hist_percentile(&total, 0.50) / 1000)
		fmt.sbprintf(&b, `"p90_ms":%f,`, hist_percentile(&total, 0.90) / 1000)
		fmt.sbprintf(&b, `"p99_ms":%f,`, hist_percentile(&total, 0.99) / 1000)
		fmt.sbprintf(&b, `"p999_ms":%f,`, hist_percentile(&total, 0.999) / 1000)
		fmt.sbprintf(&b, `"max_ms":%f,`, hist_percentile(&total, 1.0) / 1000)
		fmt.sbprintf(&b, `"p99_worst_1s_ms":%f,`, p99_worst)
		fmt.sbprintf(&b, `"p99_cv":%f,`, p99_cv)
		fmt.sbprintf(&b, `"client_cpu_cores":%f,`, client_cpu)
		fmt.sbprintf(&b, `"send_rate_pct":%f,`, send_rate_pct)
		strings.write_string(&b, `"timeline":`)
		strings.write_string(&b, strings.to_string(tl))
		strings.write_byte(&b, '}')
		fmt.println(strings.to_string(b))
	}
}
