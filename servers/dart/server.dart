// Game server (Dart). Dart's concurrency model is a single-threaded event loop
// per ISOLATE, with garbage collection — much like Python's asyncio. The twist
// is that isolates are shared-nothing but run in PARALLEL on real OS threads, so
// Dart gets true multi-core inside ONE process (unlike Python, whose GIL forces
// N processes). That makes the multi-core story a hybrid:
//   - N worker isolates (one per server core, set via -workers), each an
//     independent event loop that OWNS its connections and its rooms. Isolates
//     share no memory, so there are no locks on game state at all.
//   - every worker binds the same port with `shared: true`; the Dart runtime
//     load-balances accepted connections across the isolates bound to that port
//     (the SO_REUSEPORT-equivalent). A room therefore lives entirely on whichever
//     isolate accepted its members; rooms shard across isolates.
//
// This is the Python/Lua "shard the rooms" multi-core model (blessed by
// METHODOLOGY.md) but with in-process isolates instead of OS processes. Within an
// isolate it mirrors the reference architecture: a per-connection read handler and
// a per-room tick that writes each snapshot straight to the (buffered, non-blocking)
// socket, so one slow client can't stall the room — the same approach as the
// Python server. Player ids are made server-unique by giving each isolate a
// disjoint, strided id space (no shared counter is possible across isolates).
//
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

// ---------------------------------------------------------------------------
// types
// ---------------------------------------------------------------------------

class Conn {
  final Socket socket;
  Uint8List rbuf = Uint8List(4096); // accumulated inbound bytes, parsed from front
  int rlen = 0;
  Room? room;
  int midx = -1; // index within room.members (for O(1) swap-removal)
  int pid = 0;
  // player state (owned by the isolate that owns this conn's room)
  int x = 0, y = 0, vx = 0, vy = 0, lastSeq = 0;
  bool closed = false;

  Conn(this.socket);

  void rappend(Uint8List src) {
    final need = rlen + src.length;
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

  // drop the first `front` bytes, shifting the remainder to the start
  void rconsume(int front) {
    final remaining = rlen - front;
    if (remaining > 0) rbuf.setRange(0, remaining, rbuf, front);
    rlen = remaining;
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

// _send writes a complete frame to the socket's buffered sink. On failure it just
// marks the conn dead and destroys the socket; the actual room removal happens in
// the socket's onDone callback (a separate event), so we never mutate a room's
// member list while the tick loop is iterating it.
void _send(Conn c, Uint8List frame) {
  if (c.closed) return;
  try {
    c.socket.add(frame);
  } catch (_) {
    c.closed = true;
    try {
      c.socket.destroy();
    } catch (_) {}
  }
}

// ---------------------------------------------------------------------------
// per-tick simulation + broadcast
// ---------------------------------------------------------------------------

void tickRoom(Room r) {
  final members = r.members;
  if (members.isEmpty) return;
  r.tickNo = (r.tickNo + 1) & 0xFFFFFFFF;

  final n = members.length;
  final frame = Uint8List(4 + 7 + n * 16);
  final bd = ByteData.sublistView(frame);
  bd.setUint32(0, 7 + n * 16, Endian.big); // length prefix
  frame[4] = msgSnapshot;
  bd.setUint32(5, r.tickNo, Endian.big);
  bd.setUint16(9, n, Endian.big);
  var off = 11;
  for (final c in members) {
    c.x = wrap32(c.x + c.vx);
    c.y = wrap32(c.y + c.vy);
    bd.setUint32(off, c.pid, Endian.big);
    bd.setInt32(off + 4, c.x, Endian.big);
    bd.setInt32(off + 8, c.y, Endian.big);
    bd.setUint32(off + 12, c.lastSeq, Endian.big);
    off += 16;
  }
  for (final c in members) {
    _send(c, frame);
  }
}

// ---------------------------------------------------------------------------
// inbound frame handling
// ---------------------------------------------------------------------------

void onData(Worker w, Conn c, Uint8List chunk) {
  if (c.closed) return;
  c.rappend(chunk);
  var off = 0;
  final bd = ByteData.sublistView(c.rbuf, 0, c.rlen);
  while (c.rlen - off >= 4) {
    final flen = bd.getUint32(off, Endian.big);
    if (flen == 0 || flen > maxFrame) {
      closeConn(w, c);
      return;
    }
    if (c.rlen - off - 4 < flen) break; // frame incomplete; wait for more
    handleFrame(w, c, bd, off + 4, flen);
    if (c.closed) return;
    off += 4 + flen;
  }
  if (off > 0) c.rconsume(off);
}

void handleFrame(Worker w, Conn c, ByteData bd, int pos, int len) {
  final type = bd.getUint8(pos);
  if (type == msgJoin) {
    if (len < 5) return;
    final roomId = bd.getUint32(pos + 1, Endian.big);
    if (c.room != null) roomRemove(c.room!, c);
    final r = getRoom(w, roomId);
    c.pid = w.newPid();
    c.room = r;
    roomAdd(r, c);
    final f = Uint8List(4 + 9);
    final fb = ByteData.sublistView(f);
    fb.setUint32(0, 9, Endian.big);
    f[4] = msgJoined;
    fb.setUint32(5, c.pid, Endian.big);
    fb.setUint32(9, roomId, Endian.big);
    _send(c, f);
  } else if (type == msgMove) {
    if (len < 9) return; // ignore malformed move, keep the connection
    if (c.room != null) {
      c.lastSeq = bd.getUint32(pos + 1, Endian.big);
      c.vx = bd.getInt16(pos + 5, Endian.big);
      c.vy = bd.getInt16(pos + 7, Endian.big);
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
    c.socket.destroy();
  } catch (_) {}
}

void handleSocket(Worker w, Socket socket) {
  socket.setOption(SocketOption.tcpNoDelay, true); // TCP_NODELAY — latency fairness
  final c = Conn(socket);
  socket.listen(
    (data) => onData(w, c, data),
    onError: (_, __) => closeConn(w, c),
    onDone: () => closeConn(w, c),
    cancelOnError: true,
  );
  // Socket WRITE failures (broken pipe / reset by a departing client) surface on
  // the sink's `done` future, NOT the read stream above. Observe it so a slow or
  // gone client can never raise an unhandled exception that kills the isolate.
  unawaited(socket.done.whenComplete(() => closeConn(w, c)).catchError((_) {}));
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
      await ServerSocket.bind(InternetAddress.anyIPv4, port, shared: true);
  server.listen((socket) => handleSocket(w, socket));

  final period = Duration(microseconds: 1000000 ~/ tickHz);
  Timer.periodic(period, (_) {
    for (final r in w.rooms.values) {
      tickRoom(r);
    }
  });
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
  print('dart game server on :$port, tick=${tickHz}Hz, workers=$workers');

  for (var i = 0; i < workers; i++) {
    await Isolate.spawn(workerMain, (port, tickHz, i, workers));
  }
  // Keep the main isolate alive (workers keep the process alive regardless, but
  // an open receive port makes that explicit and independent of them).
  ReceivePort();
}
