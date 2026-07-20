// Game server (Dart, RAW socket + self-paced async tick + ALLOCATION-POOLED variant).
// Same reactor and self-paced chunked tick loop as ../dart-async, but the hot paths
// are stripped of per-event allocation to reduce GC pressure — the ../dart-async
// instrumentation proved the residual send% dip is stop-the-world GC pauses (a single
// ≤4-room chunk still blocked ~5-8ms), which no async restructuring can hide. The
// allocations removed here:
//   - ByteData views: all read-parse and frame-build now use direct big-endian byte
//     math (be32/putBe32/…) instead of ByteData.sublistView (~200k views/s gone).
//   - the inbound copy: when nothing is buffered we parse straight from the read
//     buffer in place, stashing only a trailing partial frame (no rappend memcpy).
//   - the JOINED ack and the per-pass room list are reused buffers, not fresh allocs.
// The one allocation that CANNOT be removed is RawSocket.read() itself — dart:io has
// no read-into API — so this variant also measures whether that residual ~200k
// Uint8List/s floor still drives the GC pauses (watch maxChunkMs). Original rationale:
//
// Same isolate-per-core, shard-the-rooms
// architecture as ../dart/server.dart, but it drops dart:io's high-level `Socket`
// (a Stream/IOSink abstraction that allocates and schedules an event per write) in
// favour of the low-level `RawSocket` readiness API — the same raw-reactor design
// the Zig/Odin/go-epoll servers use, expressed inside a Dart isolate.
//
// Why this exists: the high-level Socket costs ~2x the CPU-per-snapshot of the raw
// servers and its per-operation churn produces GC + event-loop jitter that, under
// fan-out, occasionally stalls the load generator (coordinated-omission dips). Two
// concrete wins here:
//   1. RawSocket.write(buf, off, count) writes synchronously and RETURNS the number
//      of bytes the kernel accepted — real send() semantics. No IOSink queue, no
//      per-write Future. Backpressure is a partial-write count, handled by a
//      per-conn pending buffer + RawSocketEvent.write readiness (EPOLLOUT).
//   2. Because write() copies out synchronously, the snapshot frame buffer can be
//      REUSED per worker (rebuilt in place each tick) instead of allocating a fresh
//      Uint8List every room every tick. Only a backpressured conn pays a copy (into
//      its own pending buffer); the common path allocates nothing.
//
// Everything else (strided per-isolate pid space, shared:true accept load-balancing,
// per-room tick that can't be stalled by one slow client) matches the sibling server.
// See ../../PROTOCOL.md for the wire format.
import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

const int msgJoin = 0x01; // client -> server
const int msgMove = 0x02; // client -> server
const int msgJoined = 0x81; // server -> client
const int msgSnapshot = 0x82; // server -> client

const int maxFrame = 1 << 20; // reject/close frames larger than 1 MiB (protocol cap)

// wrap an arbitrary int into the signed 32-bit range, matching the +%/bit-cast
// wraparound the compiled servers get for free on i32 position accumulation.
int wrap32(int v) {
  v &= 0xFFFFFFFF;
  return v >= 0x80000000 ? v - 0x100000000 : v;
}

// Big-endian byte math directly on Uint8List — avoids allocating a ByteData view on
// every read event / frame build (the churn that feeds GC). Read helpers:
@pragma('vm:prefer-inline')
int be32(Uint8List b, int o) =>
    (b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3];
@pragma('vm:prefer-inline')
int be16(Uint8List b, int o) => (b[o] << 8) | b[o + 1];
@pragma('vm:prefer-inline')
int beI16(Uint8List b, int o) {
  final v = be16(b, o);
  return v >= 0x8000 ? v - 0x10000 : v;
}

// Write helpers. putBe32 writes the low 32 bits two's-complement, so it serialises
// both u32 and i32 (negative positions) correctly for the big-endian wire format.
@pragma('vm:prefer-inline')
void putBe32(Uint8List b, int o, int v) {
  b[o] = (v >> 24) & 0xFF;
  b[o + 1] = (v >> 16) & 0xFF;
  b[o + 2] = (v >> 8) & 0xFF;
  b[o + 3] = v & 0xFF;
}

@pragma('vm:prefer-inline')
void putBe16(Uint8List b, int o, int v) {
  b[o] = (v >> 8) & 0xFF;
  b[o + 1] = v & 0xFF;
}

// ---------------------------------------------------------------------------
// types
// ---------------------------------------------------------------------------

class Conn {
  final RawSocket socket;
  Uint8List rbuf = Uint8List(4096); // accumulated inbound bytes, parsed from front
  int rlen = 0;

  // pending outbound bytes the kernel has NOT accepted yet (partial writes). Empty
  // in the common case; grows only under backpressure. wstart..wlen is the unsent
  // slice; we compact/reset it once fully drained.
  Uint8List wbuf = Uint8List(0);
  int wstart = 0;
  int wlen = 0;

  Room? room;
  int midx = -1; // index within room.members (for O(1) swap-removal)
  int pid = 0;
  // player state (owned by the isolate that owns this conn's room)
  int x = 0, y = 0, vx = 0, vy = 0, lastSeq = 0;
  bool closed = false;

  Conn(this.socket);

  void rappend(Uint8List src, int n) {
    final need = rlen + n;
    if (need > rbuf.length) {
      var cap = rbuf.length * 2;
      while (cap < need) cap *= 2;
      final nb = Uint8List(cap);
      nb.setRange(0, rlen, rbuf);
      rbuf = nb;
    }
    rbuf.setRange(rlen, need, src);
    rlen = need;
  }

  // stash `count` bytes of src[off..] as the buffered remainder (partial frame left
  // over after an in-place parse of a fresh read buffer).
  void rappendFrom(Uint8List src, int off, int count) {
    final need = rlen + count;
    if (need > rbuf.length) {
      var cap = rbuf.length * 2;
      while (cap < need) cap *= 2;
      final nb = Uint8List(cap);
      nb.setRange(0, rlen, rbuf);
      rbuf = nb;
    }
    rbuf.setRange(rlen, need, src, off);
    rlen = need;
  }

  // drop the first `front` bytes, shifting the remainder to the start
  void rconsume(int front) {
    final remaining = rlen - front;
    if (remaining > 0) rbuf.setRange(0, remaining, rbuf, front);
    rlen = remaining;
  }

  // queue `count` bytes from src[off..] into the pending buffer (compacting first
  // if the already-sent prefix has grown large), so they flush on the next
  // RawSocketEvent.write.
  void wappend(Uint8List src, int off, int count) {
    if (wstart > 0 && wstart == wlen) {
      wstart = 0;
      wlen = 0;
    } else if (wstart > 4096 && wstart * 2 > wlen) {
      final remaining = wlen - wstart;
      wbuf.setRange(0, remaining, wbuf, wstart);
      wstart = 0;
      wlen = remaining;
    }
    final need = wlen + count;
    if (need > wbuf.length) {
      var cap = wbuf.length == 0 ? 4096 : wbuf.length * 2;
      while (cap < need) cap *= 2;
      final nb = Uint8List(cap);
      nb.setRange(0, wlen, wbuf);
      wbuf = nb;
    }
    wbuf.setRange(wlen, need, src, off);
    wlen = need;
  }
}

class Room {
  final int id;
  int tickNo = 0;
  final List<Conn> members = [];
  Room(this.id);
}

class Worker {
  final int workerId;
  final int numWorkers;
  final Map<int, Room> rooms = {};
  int _nextPid;

  // reused snapshot scratch buffer (rebuilt in place each tick). Grows to the
  // largest frame seen; never freed. Safe to reuse because RawSocket.write copies
  // synchronously — nothing retains a reference past the write() call.
  Uint8List scratch = Uint8List(4096);
  // reused JOINED-ack buffer (fixed 13 bytes) and per-pass room list — both avoid a
  // per-event / per-pass allocation. joinBuf is safe to reuse for the same reason as
  // scratch; tickList is refilled each pass without reallocating its backing store.
  final Uint8List joinBuf = Uint8List(13);
  final List<Room> tickList = [];

  Worker(this.workerId, this.numWorkers) : _nextPid = workerId + 1;

  // strided across isolates so ids stay server-unique with no shared counter
  int newPid() {
    final p = _nextPid;
    _nextPid += numWorkers;
    return p & 0xFFFFFFFF;
  }
}

// ---------------------------------------------------------------------------
// room management (all within one isolate — no locking)
// ---------------------------------------------------------------------------

Room getRoom(Worker w, int id) {
  return w.rooms.putIfAbsent(id, () => Room(id));
}

void roomAdd(Room r, Conn c) {
  c.midx = r.members.length;
  r.members.add(c);
}

void roomRemove(Room r, Conn c) {
  final i = c.midx;
  final last = r.members.length - 1;
  if (i < 0 || i > last) return;
  final moved = r.members[last];
  r.members[i] = moved;
  moved.midx = i;
  r.members.removeLast();
  c.midx = -1;
}

// ---------------------------------------------------------------------------
// outbound
// ---------------------------------------------------------------------------

// _flush drains a conn's pending buffer to the kernel. RawSocket.write returns the
// bytes accepted (0 == would-block); we arm writeEvents when bytes remain and
// disarm once fully drained. Returns false if the socket errored (caller closes).
bool _flush(Conn c) {
  while (c.wstart < c.wlen) {
    int n;
    try {
      n = c.socket.write(c.wbuf, c.wstart, c.wlen - c.wstart);
    } catch (_) {
      return false;
    }
    if (n <= 0) break; // kernel buffer full — wait for RawSocketEvent.write
    c.wstart += n;
  }
  final drained = c.wstart >= c.wlen;
  if (drained) {
    c.wstart = 0;
    c.wlen = 0;
  }
  c.socket.writeEventsEnabled = !drained;
  return true;
}

// _send writes a frame (frame[0..len]) straight to the socket. Anything the kernel
// won't accept right now is copied into the conn's own pending buffer, so `frame`
// (the shared per-worker scratch) is free to reuse the instant this returns. If a
// backlog already exists we must append to preserve ordering rather than write.
void _send(Conn c, Uint8List frame, int len) {
  if (c.closed) return;
  if (c.wstart < c.wlen) {
    // backlog present: never write out of order, just queue behind it
    c.wappend(frame, 0, len);
    return;
  }
  int off = 0;
  try {
    off = c.socket.write(frame, 0, len);
  } catch (_) {
    _kill(c);
    return;
  }
  if (off < len) {
    c.wappend(frame, off, len - off);
    c.socket.writeEventsEnabled = true;
  }
}

// destroy the socket without touching room membership (used from the write path,
// where the read-side onDone/closeConn will do the room removal as a later event).
void _kill(Conn c) {
  if (c.closed) return;
  c.closed = true;
  try {
    c.socket.close();
  } catch (_) {}
}

// ---------------------------------------------------------------------------
// per-tick simulation + broadcast
// ---------------------------------------------------------------------------

void tickRoom(Worker w, Room r) {
  final members = r.members;
  if (members.isEmpty) return;
  r.tickNo = (r.tickNo + 1) & 0xFFFFFFFF;

  final n = members.length;
  final total = 4 + 7 + n * 16;
  if (w.scratch.length < total) {
    var cap = w.scratch.length;
    while (cap < total) cap *= 2;
    w.scratch = Uint8List(cap);
  }
  final frame = w.scratch;
  putBe32(frame, 0, 7 + n * 16); // length prefix
  frame[4] = msgSnapshot;
  putBe32(frame, 5, r.tickNo);
  putBe16(frame, 9, n);
  var off = 11;
  for (final c in members) {
    c.x = wrap32(c.x + c.vx);
    c.y = wrap32(c.y + c.vy);
    putBe32(frame, off, c.pid);
    putBe32(frame, off + 4, c.x); // i32 via two's-complement low bits
    putBe32(frame, off + 8, c.y);
    putBe32(frame, off + 12, c.lastSeq);
    off += 16;
  }
  for (final c in members) {
    _send(c, frame, total);
  }
}

// ---------------------------------------------------------------------------
// inbound frame handling
// ---------------------------------------------------------------------------

// parseFrames consumes as many whole frames as `buf[0..len)` holds, dispatching each,
// and returns the number of bytes consumed (the caller keeps/stashes the remainder).
// Byte-math only — no ByteData view is allocated. Sets c.closed on protocol error.
int parseFrames(Worker w, Conn c, Uint8List buf, int len) {
  var off = 0;
  while (len - off >= 4) {
    final flen = be32(buf, off);
    if (flen == 0 || flen > maxFrame) {
      closeConn(w, c);
      return off;
    }
    if (len - off - 4 < flen) break; // frame incomplete; keep remainder
    handleFrame(w, c, buf, off + 4, flen);
    if (c.closed) return off;
    off += 4 + flen;
  }
  return off;
}

void handleFrame(Worker w, Conn c, Uint8List buf, int pos, int len) {
  final type = buf[pos];
  if (type == msgJoin) {
    if (len < 5) return;
    final roomId = be32(buf, pos + 1);
    if (c.room != null) roomRemove(c.room!, c);
    final r = getRoom(w, roomId);
    c.pid = w.newPid();
    c.room = r;
    roomAdd(r, c);
    final jb = w.joinBuf; // reused 13-byte ack buffer
    putBe32(jb, 0, 9);
    jb[4] = msgJoined;
    putBe32(jb, 5, c.pid);
    putBe32(jb, 9, roomId);
    _send(c, jb, 13);
  } else if (type == msgMove) {
    if (len < 9) return; // ignore malformed move, keep the connection
    if (c.room != null) {
      c.lastSeq = be32(buf, pos + 1);
      c.vx = beI16(buf, pos + 5);
      c.vy = beI16(buf, pos + 7);
    }
  }
}

// ---------------------------------------------------------------------------
// connection lifecycle
// ---------------------------------------------------------------------------

void closeConn(Worker w, Conn c) {
  if (c.closed) return;
  c.closed = true;
  if (c.room != null) {
    roomRemove(c.room!, c);
    c.room = null;
  }
  try {
    c.socket.close();
  } catch (_) {}
}

// Drain the socket and parse. Fast path (steady state): rbuf is empty, so we parse
// straight from the freshly read buffer with no copy, stashing only a trailing
// partial frame. Only when a remainder is already buffered do we fall back to the
// append-then-parse path. The single unavoidable allocation is RawSocket.read()
// itself — dart:io exposes no read-into-buffer API.
void onReadable(Worker w, Conn c) {
  while (true) {
    Uint8List? data;
    try {
      data = c.socket.read();
    } catch (_) {
      closeConn(w, c);
      return;
    }
    if (data == null || data.isEmpty) break;
    final n = data.length;
    if (c.rlen == 0) {
      final used = parseFrames(w, c, data, n);
      if (c.closed) return;
      if (used < n) c.rappendFrom(data, used, n - used);
    } else {
      c.rappend(data, n);
      final used = parseFrames(w, c, c.rbuf, c.rlen);
      if (c.closed) return;
      if (used > 0) c.rconsume(used);
    }
    if (n < 1024) break; // likely drained; more read events will come
  }
}

void handleSocket(Worker w, RawSocket socket) {
  socket.setOption(SocketOption.tcpNoDelay, true); // TCP_NODELAY — latency fairness
  socket.writeEventsEnabled = false; // only arm EPOLLOUT when a write actually blocks
  final c = Conn(socket);
  socket.listen(
    (event) {
      switch (event) {
        case RawSocketEvent.read:
          onReadable(w, c);
          break;
        case RawSocketEvent.write:
          if (!_flush(c)) closeConn(w, c);
          break;
        case RawSocketEvent.readClosed:
          closeConn(w, c);
          break;
        case RawSocketEvent.closed:
          closeConn(w, c);
          break;
      }
    },
    onError: (_, __) => closeConn(w, c),
    onDone: () => closeConn(w, c),
    cancelOnError: true,
  );
}

// ---------------------------------------------------------------------------
// worker isolate
// ---------------------------------------------------------------------------

Future<void> workerMain((int, int, int, int) args) async {
  final (port, tickHz, workerId, numWorkers) = args;
  final w = Worker(workerId, numWorkers);

  // shared:true lets every isolate bind the same port; the runtime load-balances
  // accepted connections across them (the SO_REUSEPORT equivalent).
  final server =
      await RawServerSocket.bind(InternetAddress.anyIPv4, port, shared: true);
  server.listen((socket) => handleSocket(w, socket));

  // Self-paced, chunked, I/O-interleaving tick loop (instead of Timer.periodic).
  // Two differences from the timer variants:
  //   1. self-pacing: we do the work, measure elapsed, and await only the REMAINDER
  //      of the period — so we never demand a cadence the loop can't meet, and an
  //      overrun doesn't compound into unbounded drift (it just resyncs).
  //   2. chunking: we tick `chunk` rooms, then `await Future.delayed(Duration.zero)`
  //      to yield to the event loop so queued inbound reads / outbound flushes run
  //      BETWEEN chunks, instead of all being blocked behind one monolithic tick.
  // The point is to distribute emission + I/O evenly across the period rather than
  // firing the whole batch at once. Instrumentation (TICK_LOG) records maxChunkMs —
  // the longest contiguous block on the loop — which tells us whether the blocks are
  // work (chunking shrinks them) or GC (chunking can't, since a GC pause stops the
  // isolate mid-chunk regardless).
  final periodUs = 1000000 ~/ tickHz;
  const chunk = 4; // rooms to tick before yielding to the event loop

  final tickLog = Platform.environment['TICK_LOG'];
  final logPath = tickLog == null ? null : '$tickLog.w$workerId';
  final sw = Stopwatch()..start();
  var reportAtUs = 1000000;
  var passes = 0, maxPeriodUs = 0, maxChunkUs = 0;
  var lastPassUs = 0;

  var nextUs = sw.elapsedMicroseconds;
  while (true) {
    final passStart = sw.elapsedMicroseconds;
    if (logPath != null && lastPassUs != 0) {
      final realPeriodUs = passStart - lastPassUs;
      if (realPeriodUs > maxPeriodUs) maxPeriodUs = realPeriodUs;
    }
    lastPassUs = passStart;

    var i = 0;
    var chunkStartUs = logPath == null ? 0 : sw.elapsedMicroseconds;
    // Snapshot the room list into the REUSED tickList (no per-pass allocation): we
    // await mid-pass, and an inbound JOIN during an await mutates w.rooms
    // (putIfAbsent). Iterating the live map across awaits would throw
    // ConcurrentModificationError; a new room created mid-pass just ticks next pass.
    final roomList = w.tickList
      ..clear()
      ..addAll(w.rooms.values);
    for (final r in roomList) {
      tickRoom(w, r);
      if (++i % chunk == 0) {
        if (logPath != null) {
          final d = sw.elapsedMicroseconds - chunkStartUs;
          if (d > maxChunkUs) maxChunkUs = d;
        }
        await Future.delayed(Duration.zero); // yield: let queued I/O events run
        if (logPath != null) chunkStartUs = sw.elapsedMicroseconds;
      }
    }
    if (logPath != null) {
      final d = sw.elapsedMicroseconds - chunkStartUs;
      if (d > maxChunkUs) maxChunkUs = d;
    }
    passes++;

    // self-pace to the next period boundary; if we overran, resync instead of drifting
    nextUs += periodUs;
    var sleepUs = nextUs - sw.elapsedMicroseconds;
    if (sleepUs < 0) {
      nextUs = sw.elapsedMicroseconds;
      sleepUs = 0;
    }
    await Future.delayed(Duration(microseconds: sleepUs));

    if (logPath != null && sw.elapsedMicroseconds >= reportAtUs) {
      final line = '${reportAtUs ~/ 1000000}s w$workerId passes=$passes '
          'maxPeriodMs=${(maxPeriodUs / 1000).toStringAsFixed(1)} '
          'maxChunkMs=${(maxChunkUs / 1000).toStringAsFixed(2)} '
          '(idealPeriodMs=${(periodUs / 1000).toStringAsFixed(1)})\n';
      try {
        File(logPath).writeAsStringSync(line, mode: FileMode.append, flush: true);
      } catch (_) {}
      passes = 0;
      maxPeriodUs = 0;
      maxChunkUs = 0;
      reportAtUs += 1000000;
    }
  }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int parsePort(String addr) {
  final s = addr.substring(addr.lastIndexOf(':') + 1);
  final v = int.tryParse(s) ?? 9000;
  return (v <= 0 || v > 65535) ? 9000 : v;
}

Future<void> main(List<String> argv) async {
  var addr = ':9000';
  var tickHz = 30;
  var workers = 1;

  for (var i = 0; i < argv.length; i++) {
    final a = argv[i];
    if ((a == '-addr' || a == '--addr') && i + 1 < argv.length) {
      addr = argv[++i];
    } else if ((a == '-tick' || a == '--tick') && i + 1 < argv.length) {
      tickHz = int.tryParse(argv[++i]) ?? tickHz;
    } else if ((a == '-workers' || a == '--workers') && i + 1 < argv.length) {
      workers = int.tryParse(argv[++i]) ?? workers;
    }
  }
  if (tickHz <= 0) tickHz = 30;
  if (workers <= 0) workers = 1;

  final port = parsePort(addr);
  print('dart-pool game server on :$port, tick=${tickHz}Hz, workers=$workers');

  for (var i = 0; i < workers; i++) {
    await Isolate.spawn(workerMain, (port, tickHz, i, workers));
  }
  // Keep the main isolate alive (workers keep the process alive regardless, but
  // an open receive port makes that explicit and independent of them).
  ReceivePort();
}
