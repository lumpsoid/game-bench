// Game server (Zig). Zig has no green threads / async runtime, so the idiomatic
// high-throughput design is a THREAD-PER-CORE SHARDED REACTOR — the same model as
// the Odin server:
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
const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const Allocator = std.mem.Allocator;

const MSG_JOIN: u8 = 0x01; // client -> server
const MSG_MOVE: u8 = 0x02; // client -> server
const MSG_JOINED: u8 = 0x81; // server -> client
const MSG_SNAPSHOT: u8 = 0x82; // server -> client

const MAX_FRAME: usize = 1 << 20; // reject/close frames larger than 1 MiB (protocol cap)
const WBUF_CAP: usize = 1 << 20; // shed snapshots once a client is this far backed up
const BACKLOG: u31 = 4096;
const MAX_EVENTS: usize = 1024;
const READ_CHUNK: usize = 65536;

// Fast multi-threaded allocator; each worker uses it without extra locking on the
// hot path (it keeps per-thread arenas internally).
const gpa: Allocator = std.heap.smp_allocator;

// Server-unique player ids, handed out across all workers.
var next_pid: u32 = 0;

fn nextPlayerId() u32 {
    return @atomicRmw(u32, &next_pid, .Add, 1, .monotonic) + 1;
}

// ---------------------------------------------------------------------------
// tiny growable containers (kept local to avoid depending on churny std APIs)
// ---------------------------------------------------------------------------

fn List(comptime T: type) type {
    return struct {
        items: []T = &.{},
        len: usize = 0,
        const Self = @This();

        fn ensure(self: *Self, need: usize) void {
            if (self.items.len >= need) return;
            var newcap: usize = if (self.items.len == 0) 8 else self.items.len * 2;
            while (newcap < need) newcap *= 2;
            self.items = if (self.items.len == 0)
                gpa.alloc(T, newcap) catch @panic("oom")
            else
                gpa.realloc(self.items, newcap) catch @panic("oom");
        }
        fn append(self: *Self, v: T) void {
            self.ensure(self.len + 1);
            self.items[self.len] = v;
            self.len += 1;
        }
        fn appendSlice(self: *Self, s: []const T) void {
            self.ensure(self.len + s.len);
            @memcpy(self.items[self.len..][0..s.len], s);
            self.len += s.len;
        }
        fn slice(self: *Self) []T {
            return self.items[0..self.len];
        }
        // set logical length to n, growing the backing store if needed
        fn resizeUp(self: *Self, n: usize) []T {
            self.ensure(n);
            self.len = n;
            return self.items[0..n];
        }
        // drop the first `front` elements, shifting the remainder to the start
        fn consume(self: *Self, front: usize) void {
            const remaining = self.len - front;
            if (remaining > 0) {
                std.mem.copyForwards(T, self.items[0..remaining], self.items[front..self.len]);
            }
            self.len = remaining;
        }
        fn deinit(self: *Self) void {
            if (self.items.len > 0) gpa.free(self.items);
        }
    };
}

const Buf = List(u8);

// ---------------------------------------------------------------------------
// wire helpers (all multi-byte integers big-endian, per PROTOCOL.md)
// ---------------------------------------------------------------------------

fn putU32(b: []u8, v: u32) void {
    b[0] = @intCast(v >> 24 & 0xff);
    b[1] = @intCast(v >> 16 & 0xff);
    b[2] = @intCast(v >> 8 & 0xff);
    b[3] = @intCast(v & 0xff);
}

fn getU32(b: []const u8) u32 {
    return @as(u32, b[0]) << 24 | @as(u32, b[1]) << 16 | @as(u32, b[2]) << 8 | @as(u32, b[3]);
}

fn getU16(b: []const u8) u16 {
    return @as(u16, b[0]) << 8 | @as(u16, b[1]);
}

// ---------------------------------------------------------------------------
// types
// ---------------------------------------------------------------------------

const Conn = struct {
    fd: posix.socket_t,
    rbuf: Buf = .{}, // accumulated inbound bytes, parsed from the front
    wbuf: Buf = .{}, // pending outbound bytes (partial writes / backpressure)
    room: ?*Room = null,
    midx: isize = -1, // index of this conn within room.members (for O(1) removal)
    pid: u32 = 0,
    // player state (owned by the worker that owns this conn's room)
    x: i32 = 0,
    y: i32 = 0,
    vx: i16 = 0,
    vy: i16 = 0,
    last_seq: u32 = 0,
    want_out: bool = false, // EPOLLOUT currently armed
};

const Room = struct {
    id: u32,
    members: List(*Conn) = .{},
    tick: u32 = 0,
};

const Worker = struct {
    id: usize,
    epfd: i32 = -1,
    listener: posix.socket_t = -1,
    rooms: std.AutoHashMap(u32, *Room),
    tick_ns: i128,
    port: u16,
    scratch: Buf = .{}, // reused per-tick snapshot payload buffer
};

// ---------------------------------------------------------------------------
// outbound: enqueue a full frame (4-byte length prefix + payload) and try to flush
// ---------------------------------------------------------------------------

// enqueueFrame appends length-prefixed `payload` to the conn's write buffer and
// attempts an immediate flush. `sheddable` snapshots are dropped when the buffer
// is already saturated, so one slow client never stalls the worker.
fn enqueueFrame(w: *Worker, c: *Conn, payload: []const u8, sheddable: bool) void {
    if (sheddable and c.wbuf.len >= WBUF_CAP) return;
    var hdr: [4]u8 = undefined;
    putU32(hdr[0..], @intCast(payload.len));
    c.wbuf.appendSlice(hdr[0..]);
    c.wbuf.appendSlice(payload);
    _ = flush(w, c);
}

// flush writes as much of wbuf as the socket accepts. On EAGAIN it arms EPOLLOUT;
// when fully drained it disarms EPOLLOUT. Returns false if the conn must be closed.
fn flush(w: *Worker, c: *Conn) bool {
    var sent: usize = 0;
    const data = c.wbuf.slice();
    while (sent < data.len) {
        const n = sysSend(c.fd, data[sent..]) catch |err| switch (err) {
            error.WouldBlock => break,
            else => return false,
        };
        if (n == 0) return false;
        sent += n;
    }
    if (sent > 0) c.wbuf.consume(sent);
    const want = c.wbuf.len > 0;
    if (want != c.want_out) modConn(w, c, want);
    return true;
}

// ---------------------------------------------------------------------------
// epoll: the wrappers live in std.os.linux now (raw syscalls returning errno).
// ---------------------------------------------------------------------------

fn epollCreate() ?i32 {
    const rc = linux.epoll_create1(0);
    return switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        else => null,
    };
}

// best-effort, matching the Odin server which ignores epoll_ctl errors
fn epollCtl(epfd: i32, op: u32, fd: i32, ev: ?*linux.epoll_event) void {
    _ = linux.epoll_ctl(epfd, op, fd, ev);
}

fn epollWait(epfd: i32, events: []linux.epoll_event, timeout: i32) usize {
    const rc = linux.epoll_wait(epfd, events.ptr, @intCast(events.len), timeout);
    return switch (linux.errno(rc)) {
        .SUCCESS => rc,
        else => 0, // EINTR etc. — treat as "no events this iteration"
    };
}

// ---------------------------------------------------------------------------
// socket syscalls: Zig 0.16 removed the blocking-oriented wrappers from
// std.posix, so we call the raw linux syscalls and map errno ourselves. The
// semantics match what the Odin server gets from core:sys/linux.
// ---------------------------------------------------------------------------

const IoError = error{ WouldBlock, Failed };

fn sysClose(fd: posix.socket_t) void {
    _ = linux.close(fd);
}

fn sysSend(fd: posix.socket_t, buf: []const u8) IoError!usize {
    while (true) {
        const rc = linux.sendto(fd, buf.ptr, buf.len, posix.MSG.NOSIGNAL, null, 0);
        switch (linux.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            else => return error.Failed,
        }
    }
}

fn sysAccept(fd: posix.socket_t, flags: u32) IoError!posix.socket_t {
    while (true) {
        const rc = linux.accept4(fd, null, null, flags);
        switch (linux.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            else => return error.Failed,
        }
    }
}

// ---------------------------------------------------------------------------
// epoll registration
// ---------------------------------------------------------------------------

fn modConn(w: *Worker, c: *Conn, want_out: bool) void {
    var ev = linux.epoll_event{
        .events = linux.EPOLL.IN | (if (want_out) @as(u32, linux.EPOLL.OUT) else 0),
        .data = .{ .ptr = @intFromPtr(c) },
    };
    epollCtl(w.epfd, linux.EPOLL.CTL_MOD, c.fd, &ev);
    c.want_out = want_out;
}

// ---------------------------------------------------------------------------
// room management (all within one worker — no locking)
// ---------------------------------------------------------------------------

fn getRoom(w: *Worker, id: u32) *Room {
    if (w.rooms.get(id)) |r| return r;
    const r = gpa.create(Room) catch @panic("oom");
    r.* = .{ .id = id };
    w.rooms.put(id, r) catch @panic("oom");
    return r;
}

fn roomAdd(r: *Room, c: *Conn) void {
    c.midx = @intCast(r.members.len);
    r.members.append(c);
}

fn roomRemove(r: *Room, c: *Conn) void {
    const i = c.midx;
    const last: isize = @as(isize, @intCast(r.members.len)) - 1;
    if (i < 0 or i > last) return;
    const iu: usize = @intCast(i);
    const lu: usize = @intCast(last);
    r.members.items[iu] = r.members.items[lu];
    r.members.items[iu].midx = i;
    r.members.len -= 1;
    c.midx = -1;
}

// ---------------------------------------------------------------------------
// per-tick simulation + broadcast
// ---------------------------------------------------------------------------

fn tickRoom(w: *Worker, r: *Room) void {
    if (r.members.len == 0) return;
    r.tick += 1;
    for (r.members.slice()) |c| {
        c.x +%= c.vx;
        c.y +%= c.vy;
    }
    const n = r.members.len;
    const size = 7 + n * 16;
    const p = w.scratch.resizeUp(size);
    p[0] = MSG_SNAPSHOT;
    putU32(p[1..], r.tick);
    const nn: u16 = @intCast(n);
    p[5] = @intCast(nn >> 8);
    p[6] = @intCast(nn & 0xff);
    var off: usize = 7;
    for (r.members.slice()) |c| {
        putU32(p[off..], c.pid);
        putU32(p[off + 4 ..], @bitCast(c.x));
        putU32(p[off + 8 ..], @bitCast(c.y));
        putU32(p[off + 12 ..], c.last_seq);
        off += 16;
    }
    for (r.members.slice()) |c| {
        enqueueFrame(w, c, p, true);
    }
}

// ---------------------------------------------------------------------------
// inbound frame handling
// ---------------------------------------------------------------------------

// handleConnRead reads available bytes and processes every complete frame.
// Returns false if the connection must be closed.
fn handleConnRead(w: *Worker, c: *Conn) bool {
    var tmp: [READ_CHUNK]u8 = undefined;
    while (true) {
        const n = posix.read(c.fd, tmp[0..]) catch |err| switch (err) {
            error.WouldBlock => break,
            else => return false,
        };
        if (n == 0) return false; // peer closed
        c.rbuf.appendSlice(tmp[0..n]);
        if (n < tmp.len) break; // socket drained
    }

    // parse as many whole frames as we have
    var off: usize = 0;
    const rb = c.rbuf.slice();
    while (rb.len - off >= 4) {
        const length = getU32(rb[off..]);
        if (length == 0 or length > MAX_FRAME) return false;
        if (rb.len - off - 4 < length) break; // frame incomplete; wait for more
        const payload = rb[off + 4 .. off + 4 + length];
        if (!handleFrame(w, c, payload)) return false;
        off += 4 + length;
    }
    if (off > 0) c.rbuf.consume(off);
    return true;
}

fn handleFrame(w: *Worker, c: *Conn, payload: []const u8) bool {
    switch (payload[0]) {
        MSG_JOIN => {
            if (payload.len < 5) return false;
            const room_id = getU32(payload[1..]);
            if (c.room) |old| roomRemove(old, c);
            const r = getRoom(w, room_id);
            c.pid = nextPlayerId();
            c.room = r;
            roomAdd(r, c);
            var jp: [9]u8 = undefined;
            jp[0] = MSG_JOINED;
            putU32(jp[1..], c.pid);
            putU32(jp[5..], room_id);
            enqueueFrame(w, c, jp[0..], false);
        },
        MSG_MOVE => {
            if (payload.len < 9) return true; // ignore malformed move, keep the connection
            if (c.room != null) {
                c.last_seq = getU32(payload[1..]);
                c.vx = @bitCast(getU16(payload[5..]));
                c.vy = @bitCast(getU16(payload[7..]));
            }
        },
        else => {},
    }
    return true;
}

// ---------------------------------------------------------------------------
// connection lifecycle
// ---------------------------------------------------------------------------

fn closeConn(w: *Worker, c: *Conn) void {
    epollCtl(w.epfd, linux.EPOLL.CTL_DEL, c.fd, null);
    sysClose(c.fd);
    if (c.room) |r| roomRemove(r, c);
    c.rbuf.deinit();
    c.wbuf.deinit();
    gpa.destroy(c);
}

fn setSockOpt(fd: posix.socket_t, level: i32, name: u32, val: c_int) void {
    var v = val;
    _ = linux.setsockopt(fd, level, name, @ptrCast(&v), @sizeOf(c_int));
}

fn acceptLoop(w: *Worker) void {
    while (true) {
        const fd = sysAccept(w.listener, posix.SOCK.NONBLOCK) catch return;
        // TCP_NODELAY — mandatory for latency fairness
        setSockOpt(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, 1);

        const c = gpa.create(Conn) catch {
            sysClose(fd);
            continue;
        };
        c.* = .{ .fd = fd };
        var ev = linux.epoll_event{
            .events = linux.EPOLL.IN,
            .data = .{ .ptr = @intFromPtr(c) },
        };
        if (linux.errno(linux.epoll_ctl(w.epfd, linux.EPOLL.CTL_ADD, fd, &ev)) != .SUCCESS) {
            sysClose(fd);
            gpa.destroy(c);
            continue;
        }
    }
}

// ---------------------------------------------------------------------------
// worker event loop
// ---------------------------------------------------------------------------

fn openListener(port: u16) ?posix.socket_t {
    const rc = linux.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    if (linux.errno(rc) != .SUCCESS) return null;
    const fd: posix.socket_t = @intCast(rc);
    setSockOpt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, 1);
    setSockOpt(fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, 1);
    var addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = 0, // INADDR_ANY
    };
    if (linux.errno(linux.bind(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.in))) != .SUCCESS) {
        sysClose(fd);
        return null;
    }
    if (linux.errno(linux.listen(fd, BACKLOG)) != .SUCCESS) {
        sysClose(fd);
        return null;
    }
    return fd;
}

fn nowNs() i128 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(posix.CLOCK.MONOTONIC, &ts);
    return @as(i128, ts.sec) * 1_000_000_000 + ts.nsec;
}

fn workerRun(w: *Worker) void {
    w.epfd = epollCreate() orelse {
        std.debug.print("worker {d}: epoll_create failed\n", .{w.id});
        return;
    };

    w.listener = openListener(w.port) orelse {
        std.debug.print("worker {d}: could not open listener on port {d}\n", .{ w.id, w.port });
        return;
    };
    var lev = linux.epoll_event{
        .events = linux.EPOLL.IN,
        .data = .{ .ptr = 0 }, // 0 ptr marks the listener
    };
    epollCtl(w.epfd, linux.EPOLL.CTL_ADD, w.listener, &lev);

    var events: [MAX_EVENTS]linux.epoll_event = undefined;
    var next_tick = nowNs() + w.tick_ns;

    while (true) {
        const now = nowNs();
        var timeout_ms: i32 = 0;
        const wait_ns = next_tick - now;
        if (wait_ns > 0) timeout_ms = @intCast(@divFloor(wait_ns, 1_000_000));

        const nev = epollWait(w.epfd, events[0..], timeout_ms);

        for (events[0..nev]) |ev| {
            if (ev.data.ptr == 0) {
                acceptLoop(w);
                continue;
            }
            const c: *Conn = @ptrFromInt(ev.data.ptr);
            if (ev.events & (linux.EPOLL.ERR | linux.EPOLL.HUP) != 0) {
                closeConn(w, c);
                continue;
            }
            if (ev.events & linux.EPOLL.IN != 0) {
                if (!handleConnRead(w, c)) {
                    closeConn(w, c);
                    continue;
                }
            }
            if (ev.events & linux.EPOLL.OUT != 0) {
                if (!flush(w, c)) {
                    closeConn(w, c);
                    continue;
                }
            }
        }

        // run every tick whose deadline has passed (catch up if we fell behind)
        const now2 = nowNs();
        if (now2 >= next_tick) {
            var it = w.rooms.valueIterator();
            while (it.next()) |rp| tickRoom(w, rp.*);
            next_tick += w.tick_ns;
            // if we're badly behind, don't spiral: snap forward
            if (now2 - next_tick > w.tick_ns) next_tick = now2 + w.tick_ns;
        }
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn copyInto(buf: []u8, s: []const u8) []const u8 {
    const n = @min(buf.len, s.len);
    @memcpy(buf[0..n], s[0..n]);
    return buf[0..n];
}

fn parsePort(addr: []const u8) u16 {
    var s = addr;
    if (std.mem.lastIndexOfScalar(u8, s, ':')) |idx| s = s[idx + 1 ..];
    const v = std.fmt.parseInt(i64, s, 10) catch return 9000;
    if (v <= 0 or v > 65535) return 9000;
    return @intCast(v);
}

pub fn main(init: std.process.Init.Minimal) void {
    var addr_buf: [256]u8 = undefined;
    var addr: []const u8 = ":9000";
    var tick_hz: i64 = 30;
    var workers: usize = 1;

    // Collect argv into a slice (the iterator's buffer is not guaranteed stable
    // across next() on every platform, so copy the addr string we keep).
    var it = init.args.iterate();
    _ = it.skip(); // argv[0]
    while (it.next()) |a| {
        if ((std.mem.eql(u8, a, "-addr") or std.mem.eql(u8, a, "--addr"))) {
            if (it.next()) |v| addr = copyInto(addr_buf[0..], v);
        } else if ((std.mem.eql(u8, a, "-tick") or std.mem.eql(u8, a, "--tick"))) {
            if (it.next()) |v| tick_hz = std.fmt.parseInt(i64, v, 10) catch tick_hz;
        } else if ((std.mem.eql(u8, a, "-workers") or std.mem.eql(u8, a, "--workers"))) {
            if (it.next()) |v| workers = std.fmt.parseInt(usize, v, 10) catch workers;
        }
    }
    if (tick_hz <= 0) tick_hz = 30;
    if (workers == 0) workers = 1;

    const port = parsePort(addr);
    const tick_ns: i128 = @divTrunc(1_000_000_000, @as(i128, tick_hz));

    std.debug.print("zig game server on :{d}, tick={d}Hz, workers={d}\n", .{ port, tick_hz, workers });

    const threads = gpa.alloc(std.Thread, workers) catch @panic("oom");
    for (0..workers) |wi| {
        const w = gpa.create(Worker) catch @panic("oom");
        w.* = .{
            .id = wi,
            .rooms = std.AutoHashMap(u32, *Room).init(gpa),
            .tick_ns = tick_ns,
            .port = port,
        };
        threads[wi] = std.Thread.spawn(.{}, workerRun, .{w}) catch @panic("spawn");
    }
    for (threads) |t| t.join();
}
