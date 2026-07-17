#!/usr/bin/env python3
# Game server (Python / asyncio). Mirrors the reference architecture within the
# limits of one event loop:
#   - one coroutine per connection for reads
#   - the room tick writes snapshots directly to each connection's transport
#     (writer.write is non-blocking / buffered, so a slow client can't stall the room)
#   - a Room object owns its state; there are no threads, so no locks needed
#
# NOTE ON FAIRNESS: asyncio is single-core (GIL). To use N cores you must run N
# processes and shard rooms across them (e.g. by room_id % N behind SO_REUSEPORT).
# This single-process version is the honest "one core" baseline. Install `uvloop`
# for a large speedup; it falls back to the stdlib loop automatically.
#
# Run:  python3 server.py -addr :9000 -tick 30
# See ../../PROTOCOL.md.

import asyncio
import socket
import struct
import sys

try:
    import uvloop
    uvloop.install()
    LOOP = "uvloop"
except ImportError:
    LOOP = "asyncio"

MSG_JOIN = 0x01
MSG_MOVE = 0x02
MSG_JOINED = 0x81
MSG_SNAPSHOT = 0x82

_next_pid = 0


def new_pid():
    global _next_pid
    _next_pid = (_next_pid + 1) & 0xFFFFFFFF
    return _next_pid


class Player:
    __slots__ = ("id", "x", "y", "vx", "vy", "last_seq")

    def __init__(self, pid):
        self.id = pid
        self.x = self.y = self.vx = self.vy = self.last_seq = 0


class Room:
    def __init__(self, room_id, tick):
        self.id = room_id
        self.tick = tick
        self.tick_no = 0
        self.players = {}  # pid -> Player
        self.conns = {}    # pid -> StreamWriter
        self._task = asyncio.create_task(self.run())

    def join(self, writer):
        pid = new_pid()
        self.players[pid] = Player(pid)
        self.conns[pid] = writer
        return pid

    def move(self, pid, seq, dx, dy):
        p = self.players.get(pid)
        if p is not None:
            p.vx, p.vy, p.last_seq = dx, dy, seq

    def leave(self, pid):
        self.players.pop(pid, None)
        self.conns.pop(pid, None)

    async def run(self):
        loop = asyncio.get_event_loop()
        nxt = loop.time()
        while True:
            nxt += self.tick
            delay = nxt - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self.step()

    def step(self):
        self.tick_no = (self.tick_no + 1) & 0xFFFFFFFF
        players = self.players
        for p in players.values():
            p.x += p.vx
            p.y += p.vy
        n = len(players)
        buf = bytearray(7 + n * 16)
        buf[0] = MSG_SNAPSHOT
        struct.pack_into(">IH", buf, 1, self.tick_no, n)
        off = 7
        for p in players.values():
            struct.pack_into(">IiiI", buf, off, p.id, p.x, p.y, p.last_seq)
            off += 16
        frame = struct.pack(">I", len(buf)) + bytes(buf)
        for w in self.conns.values():
            try:
                w.write(frame)
            except Exception:
                pass


rooms = {}


def get_room(room_id, tick):
    r = rooms.get(room_id)
    if r is None:
        r = Room(room_id, tick)
        rooms[room_id] = r
    return r


async def handle(reader, writer, tick):
    sock = writer.get_extra_info("socket")
    if sock is not None:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # TCP_NODELAY
    room = None
    my_pid = 0
    try:
        while True:
            header = await reader.readexactly(4)
            n = struct.unpack(">I", header)[0]
            if n == 0 or n > (1 << 20):
                break
            payload = await reader.readexactly(n)
            t = payload[0]
            if t == MSG_JOIN and n >= 5:
                room_id = struct.unpack_from(">I", payload, 1)[0]
                room = get_room(room_id, tick)
                my_pid = room.join(writer)
                jp = bytes([MSG_JOINED]) + struct.pack(">II", my_pid, room_id)
                writer.write(struct.pack(">I", len(jp)) + jp)
            elif t == MSG_MOVE and n >= 9:
                if room is not None:
                    seq, dx, dy = struct.unpack_from(">Ihh", payload, 1)
                    room.move(my_pid, seq, dx, dy)
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        if room is not None and my_pid:
            room.leave(my_pid)
        try:
            writer.close()
        except Exception:
            pass


def parse_args():
    port, tick = 9000, 30
    a = sys.argv[1:]
    i = 0
    while i < len(a):
        if a[i] in ("-addr", "--addr") and i + 1 < len(a):
            port = int(a[i + 1].split(":")[-1])
            i += 2
        elif a[i] in ("-tick", "--tick") and i + 1 < len(a):
            tick = int(a[i + 1])
            i += 2
        else:
            i += 1
    return port, tick


async def main():
    port, tick_hz = parse_args()
    tick = 1.0 / tick_hz
    # reuse_port lets the runner launch N single-core processes sharing this port
    # (asyncio is one core per process; the kernel load-balances connections).
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, tick), "0.0.0.0", port, reuse_port=True
    )
    print(f"python game server on :{port}, tick={tick_hz}Hz, loop={LOOP}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
