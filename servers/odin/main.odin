// Game server (Odin). Odin has no green threads / async runtime, so the idiomatic
// high-throughput design is a THREAD-PER-CORE SHARDED REACTOR:
//   - N worker threads (one per server core, set via -workers), each an independent
//     single-threaded epoll event loop that OWNS its connections and its rooms —
//     so there are no locks on game state (the only shared atomic is the global
//     player-id counter).
//   - the listen socket is opened once per worker with SO_REUSEPORT, and the kernel
//     load-balances new connections across the workers. A room therefore lives
//     entirely on whichever worker accepted its members; with the same room_id
//     landing on different workers, the room is sharded across them.
//
// This mirrors the Python server's multi-core story (N processes sharding rooms via
// SO_REUSEPORT) — accepted by METHODOLOGY.md — but with OS threads instead of
// processes, and it keeps the "room owns its state, no locks on the hot path"
// architecture of the Go/Rust/OCaml servers. Load is shed exactly as they do: a
// backed-up client's outbound buffer stops accepting new snapshots (drop the
// freshest tick rather than block the whole worker).
//
// See ../../PROTOCOL.md for the wire format.
package main

import "base:intrinsics"
import "core:fmt"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:thread"
import "core:time"
import linux "core:sys/linux"

MSG_JOIN :: 0x01 // client -> server
MSG_MOVE :: 0x02 // client -> server
MSG_JOINED :: 0x81 // server -> client
MSG_SNAPSHOT :: 0x82 // server -> client

MAX_FRAME :: 1 << 20 // reject/close frames larger than 1 MiB (protocol cap)
WBUF_CAP :: 1 << 20 // shed snapshots once a client is this far backed up
BACKLOG :: 4096
MAX_EVENTS :: 1024
READ_CHUNK :: 65536

// Server-unique player ids, handed out across all workers.
next_pid: u32

next_player_id :: proc() -> u32 {
	return intrinsics.atomic_add(&next_pid, 1) + 1
}

Conn :: struct {
	fd:       linux.Fd,
	rbuf:     [dynamic]u8, // accumulated inbound bytes, parsed from the front
	wbuf:     [dynamic]u8, // pending outbound bytes (partial writes / backpressure)
	room:     ^Room,
	midx:     int, // index of this conn within room.members (for O(1) removal)
	pid:      u32,
	// player state (owned by the worker that owns this conn's room)
	x, y:     i32,
	vx, vy:   i16,
	last_seq: u32,
	want_out: bool, // EPOLLOUT currently armed
}

Room :: struct {
	id:      u32,
	members: [dynamic]^Conn,
	tick:    u32,
}

Worker :: struct {
	id:       int,
	epfd:     linux.Fd,
	listener: linux.Fd,
	rooms:    map[u32]^Room,
	tick_ns:  i64,
	port:     u16,
	scratch:  [dynamic]u8, // reused per-tick snapshot payload buffer
}

// ---------------------------------------------------------------------------
// wire helpers
// ---------------------------------------------------------------------------

put_u32 :: proc(b: []u8, v: u32) {
	b[0] = u8(v >> 24)
	b[1] = u8(v >> 16)
	b[2] = u8(v >> 8)
	b[3] = u8(v)
}

get_u32 :: proc(b: []u8) -> u32 {
	return u32(b[0]) << 24 | u32(b[1]) << 16 | u32(b[2]) << 8 | u32(b[3])
}

get_u16 :: proc(b: []u8) -> u16 {
	return u16(b[0]) << 8 | u16(b[1])
}

// ---------------------------------------------------------------------------
// outbound: enqueue a full frame (4-byte length prefix + payload) and try to flush
// ---------------------------------------------------------------------------

// enqueue_frame appends length-prefixed `payload` to the conn's write buffer and
// attempts an immediate flush. `sheddable` snapshots are dropped when the buffer
// is already saturated, so one slow client never stalls the worker.
enqueue_frame :: proc(w: ^Worker, c: ^Conn, payload: []u8, sheddable: bool) {
	if sheddable && len(c.wbuf) >= WBUF_CAP {
		return
	}
	hdr: [4]u8
	put_u32(hdr[:], u32(len(payload)))
	append(&c.wbuf, ..hdr[:])
	append(&c.wbuf, ..payload)
	flush(w, c)
}

// flush writes as much of wbuf as the socket accepts. On EAGAIN it arms EPOLLOUT;
// when fully drained it disarms EPOLLOUT. Returns false if the conn must be closed.
flush :: proc(w: ^Worker, c: ^Conn) -> bool {
	sent := 0
	for sent < len(c.wbuf) {
		n, err := linux.send(c.fd, c.wbuf[sent:], {.NOSIGNAL})
		if err == .EAGAIN || err == .EWOULDBLOCK {
			break
		}
		if err == .EINTR {
			continue
		}
		if err != .NONE || n <= 0 {
			return false
		}
		sent += n
	}
	if sent > 0 {
		remaining := len(c.wbuf) - sent
		if remaining > 0 {
			copy(c.wbuf[:], c.wbuf[sent:])
		}
		resize(&c.wbuf, remaining)
	}
	want := len(c.wbuf) > 0
	if want != c.want_out {
		mod_conn(w, c, want)
	}
	return true
}

// ---------------------------------------------------------------------------
// epoll registration
// ---------------------------------------------------------------------------

mod_conn :: proc(w: ^Worker, c: ^Conn, want_out: bool) {
	ev: linux.EPoll_Event
	ev.events = {.IN}
	if want_out {
		ev.events |= {.OUT}
	}
	ev.data.ptr = c
	linux.epoll_ctl(w.epfd, .MOD, c.fd, &ev)
	c.want_out = want_out
}

// ---------------------------------------------------------------------------
// room management (all within one worker — no locking)
// ---------------------------------------------------------------------------

get_room :: proc(w: ^Worker, id: u32) -> ^Room {
	if r, ok := w.rooms[id]; ok {
		return r
	}
	r := new(Room)
	r.id = id
	w.rooms[id] = r
	return r
}

room_add :: proc(r: ^Room, c: ^Conn) {
	c.midx = len(r.members)
	append(&r.members, c)
}

room_remove :: proc(r: ^Room, c: ^Conn) {
	i := c.midx
	last := len(r.members) - 1
	if i < 0 || i > last {
		return
	}
	r.members[i] = r.members[last]
	r.members[i].midx = i
	pop(&r.members)
}

// ---------------------------------------------------------------------------
// per-tick simulation + broadcast
// ---------------------------------------------------------------------------

tick_room :: proc(w: ^Worker, r: ^Room) {
	if len(r.members) == 0 {
		return
	}
	r.tick += 1
	for c in r.members {
		c.x += i32(c.vx)
		c.y += i32(c.vy)
	}
	n := len(r.members)
	size := 7 + n * 16
	resize(&w.scratch, size)
	p := w.scratch[:]
	p[0] = MSG_SNAPSHOT
	put_u32(p[1:], r.tick)
	p[5] = u8(u16(n) >> 8)
	p[6] = u8(u16(n))
	off := 7
	for c in r.members {
		put_u32(p[off:], c.pid)
		put_u32(p[off + 4:], u32(c.x))
		put_u32(p[off + 8:], u32(c.y))
		put_u32(p[off + 12:], c.last_seq)
		off += 16
	}
	for c in r.members {
		enqueue_frame(w, c, p, true)
	}
}

// ---------------------------------------------------------------------------
// inbound frame handling
// ---------------------------------------------------------------------------

// handle_conn_read reads available bytes and processes every complete frame.
// Returns false if the connection must be closed.
handle_conn_read :: proc(w: ^Worker, c: ^Conn) -> bool {
	tmp: [READ_CHUNK]u8
	for {
		n, err := linux.read(c.fd, tmp[:])
		if err == .EAGAIN || err == .EWOULDBLOCK {
			break
		}
		if err == .EINTR {
			continue
		}
		if err != .NONE || n == 0 {
			return false // error or peer closed
		}
		append(&c.rbuf, ..tmp[:n])
		if n < len(tmp) {
			break // socket drained
		}
	}

	// parse as many whole frames as we have
	off := 0
	for len(c.rbuf) - off >= 4 {
		length := int(get_u32(c.rbuf[off:]))
		if length == 0 || length > MAX_FRAME {
			return false
		}
		if len(c.rbuf) - off - 4 < length {
			break // frame incomplete; wait for more
		}
		payload := c.rbuf[off + 4:off + 4 + length]
		if !handle_frame(w, c, payload) {
			return false
		}
		off += 4 + length
	}
	if off > 0 {
		remaining := len(c.rbuf) - off
		if remaining > 0 {
			copy(c.rbuf[:], c.rbuf[off:])
		}
		resize(&c.rbuf, remaining)
	}
	return true
}

handle_frame :: proc(w: ^Worker, c: ^Conn, payload: []u8) -> bool {
	switch payload[0] {
	case MSG_JOIN:
		if len(payload) < 5 {
			return false
		}
		room_id := get_u32(payload[1:])
		if c.room != nil {
			room_remove(c.room, c)
		}
		r := get_room(w, room_id)
		c.pid = next_player_id()
		c.room = r
		room_add(r, c)
		jp: [9]u8
		jp[0] = MSG_JOINED
		put_u32(jp[1:], c.pid)
		put_u32(jp[5:], room_id)
		enqueue_frame(w, c, jp[:], false)
	case MSG_MOVE:
		if len(payload) < 9 {
			return true // ignore malformed move, keep the connection
		}
		if c.room != nil {
			c.last_seq = get_u32(payload[1:])
			c.vx = i16(get_u16(payload[5:]))
			c.vy = i16(get_u16(payload[7:]))
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// connection lifecycle
// ---------------------------------------------------------------------------

close_conn :: proc(w: ^Worker, c: ^Conn) {
	linux.epoll_ctl(w.epfd, .DEL, c.fd, nil)
	linux.close(c.fd)
	if c.room != nil {
		room_remove(c.room, c)
	}
	delete(c.rbuf)
	delete(c.wbuf)
	free(c)
}

accept_loop :: proc(w: ^Worker) {
	for {
		addr: linux.Sock_Addr_In
		fd, err := linux.accept(w.listener, &addr, {.NONBLOCK})
		if err == .EAGAIN || err == .EWOULDBLOCK {
			return
		}
		if err == .EINTR {
			continue
		}
		if err != .NONE {
			return
		}
		// TCP_NODELAY — mandatory for latency fairness
		one: i32 = 1
		linux.setsockopt(fd, linux.SOL_TCP, linux.Socket_TCP_Option.NODELAY, &one)

		c := new(Conn)
		c.fd = fd
		c.midx = -1
		ev: linux.EPoll_Event
		ev.events = {.IN}
		ev.data.ptr = c
		if linux.epoll_ctl(w.epfd, .ADD, fd, &ev) != .NONE {
			linux.close(fd)
			free(c)
			continue
		}
	}
}

// ---------------------------------------------------------------------------
// worker event loop
// ---------------------------------------------------------------------------

open_listener :: proc(port: u16) -> (linux.Fd, bool) {
	fd, err := linux.socket(.INET, .STREAM, {.NONBLOCK}, .TCP)
	if err != .NONE {
		return 0, false
	}
	one: i32 = 1
	linux.setsockopt(fd, linux.SOL_SOCKET, linux.Socket_Option.REUSEADDR, &one)
	linux.setsockopt(fd, linux.SOL_SOCKET, linux.Socket_Option.REUSEPORT, &one)
	addr: linux.Sock_Addr_In
	addr.sin_family = .INET
	addr.sin_port = u16be(port)
	addr.sin_addr = {0, 0, 0, 0} // INADDR_ANY
	if linux.bind(fd, &addr) != .NONE {
		linux.close(fd)
		return 0, false
	}
	if linux.listen(fd, BACKLOG) != .NONE {
		linux.close(fd)
		return 0, false
	}
	return fd, true
}

worker_run :: proc(w: ^Worker) {
	epfd, err := linux.epoll_create1({})
	if err != .NONE {
		fmt.eprintfln("worker %d: epoll_create failed: %v", w.id, err)
		return
	}
	w.epfd = epfd

	lfd, ok := open_listener(w.port)
	if !ok {
		fmt.eprintfln("worker %d: could not open listener on port %d", w.id, w.port)
		return
	}
	w.listener = lfd
	lev: linux.EPoll_Event
	lev.events = {.IN}
	lev.data.ptr = nil // nil ptr marks the listener
	linux.epoll_ctl(w.epfd, .ADD, w.listener, &lev)

	events: [MAX_EVENTS]linux.EPoll_Event
	next_tick := time.tick_now()
	next_tick._nsec += w.tick_ns

	for {
		now := time.tick_now()
		wait_ns := next_tick._nsec - now._nsec
		timeout_ms: i32 = 0
		if wait_ns > 0 {
			timeout_ms = i32(wait_ns / 1_000_000)
		}

		nev, werr := linux.epoll_wait(w.epfd, &events[0], MAX_EVENTS, timeout_ms)
		if werr != .NONE && werr != .EINTR {
			fmt.eprintfln("worker %d: epoll_wait failed: %v", w.id, werr)
			return
		}

		for i in 0 ..< int(nev) {
			ev := events[i]
			if ev.data.ptr == nil {
				accept_loop(w)
				continue
			}
			c := cast(^Conn)ev.data.ptr
			if .ERR in ev.events || .HUP in ev.events {
				close_conn(w, c)
				continue
			}
			if .IN in ev.events {
				if !handle_conn_read(w, c) {
					close_conn(w, c)
					continue
				}
			}
			if .OUT in ev.events {
				if !flush(w, c) {
					close_conn(w, c)
					continue
				}
			}
		}

		// run every tick whose deadline has passed (catch up if we fell behind)
		now = time.tick_now()
		if now._nsec >= next_tick._nsec {
			for r in w.rooms {
				tick_room(w, w.rooms[r])
			}
			next_tick._nsec += w.tick_ns
			// if we're badly behind, don't spiral: snap forward
			if now._nsec - next_tick._nsec > w.tick_ns {
				next_tick._nsec = now._nsec + w.tick_ns
			}
		}
	}
}

worker_thread_proc :: proc(t: ^thread.Thread) {
	w := cast(^Worker)t.data
	worker_run(w)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

parse_port :: proc(addr: string) -> u16 {
	s := addr
	if idx := strings.last_index_byte(s, ':'); idx >= 0 {
		s = s[idx + 1:]
	}
	v, ok := strconv.parse_int(s)
	if !ok || v <= 0 || v > 65535 {
		return 9000
	}
	return u16(v)
}

main :: proc() {
	addr := ":9000"
	tick_hz := 30
	workers := 1

	args := os.args
	i := 1
	for i < len(args) {
		switch args[i] {
		case "-addr", "--addr":
			if i + 1 < len(args) {i += 1;addr = args[i]}
		case "-tick", "--tick":
			if i + 1 < len(args) {
				i += 1
				if v, ok := strconv.parse_int(args[i]); ok {tick_hz = v}
			}
		case "-workers", "--workers":
			if i + 1 < len(args) {
				i += 1
				if v, ok := strconv.parse_int(args[i]); ok {workers = v}
			}
		}
		i += 1
	}
	if tick_hz <= 0 {tick_hz = 30}
	if workers <= 0 {workers = 1}

	port := parse_port(addr)
	tick_ns := i64(1_000_000_000 / tick_hz)

	fmt.printfln("odin game server on :%d, tick=%dHz, workers=%d", port, tick_hz, workers)

	threads := make([]^thread.Thread, workers)
	for wi in 0 ..< workers {
		w := new(Worker)
		w.id = wi
		w.port = port
		w.tick_ns = tick_ns
		t := thread.create(worker_thread_proc)
		t.data = w
		threads[wi] = t
		thread.start(t)
	}
	for t in threads {
		thread.join(t)
	}
}
