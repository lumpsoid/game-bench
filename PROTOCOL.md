# Wire Protocol (v1)

The single source of truth. Every server and the load generator implement exactly this.
Transport: **raw TCP**, no TLS. Every socket sets `TCP_NODELAY` (Nagle OFF).

## Framing

Every message is a length-prefixed frame:

```
+------------------+-------------------------+
| length: u32 (BE) | payload: <length> bytes |
+------------------+-------------------------+
```

`length` is the number of payload bytes (not including the 4 length bytes).
All multi-byte integers are **big-endian**. `payload[0]` is the message type tag.
Max accepted frame: 1 MiB (reject/close the connection otherwise).

## Messages

### Client → Server

**JOIN** (`0x01`) — join (or lazily create) a room. Payload = 5 bytes.
```
0x01 | room_id: u32
```

**MOVE** (`0x02`) — set this player's velocity. Payload = 9 bytes.
```
0x02 | seq: u32 | dx: i16 | dy: i16
```
`seq` is a per-connection monotonic counter the client stamps and the server echoes
back (see SNAPSHOT.last_seq) — this is how end-to-end latency is measured.

### Server → Client

**JOINED** (`0x81`) — ack a JOIN, assigning a server-unique player id. Payload = 9 bytes.
```
0x81 | player_id: u32 | room_id: u32
```

**SNAPSHOT** (`0x82`) — broadcast once per tick to every client in the room.
Payload = 7 + count*16 bytes.
```
0x82 | tick: u32 | count: u16 | entry[count]

entry = player_id: u32 | x: i32 | y: i32 | last_seq: u32
```
`last_seq` for each player is the highest MOVE.seq the server has applied for that
player. A client finds its own `player_id` in the entry list, reads `last_seq`, and
computes latency = now − (time it sent that seq).

## Server simulation (identical across languages)

- A **room** owns its players' state. Players join via JOIN.
- On **MOVE**: set `player.vx,vy = dx,dy` and `player.last_seq = seq`.
- On each **tick** (default 30 Hz): for every player `x += vx; y += vy`, then
  build one SNAPSHOT frame and send it to every connection in the room.
- Keep the simulation this trivial on purpose — we are measuring the runtime's
  I/O + scheduling + GC under fan-out, not physics.

## Latency semantics

- The client sends MOVE at a fixed rate (default 20 Hz), open-loop.
- The server echoes each player's `last_seq` in every SNAPSHOT.
- Latency sample = receipt time of a SNAPSHOT − send time of the MOVE whose `seq`
  equals the `last_seq` in that snapshot, taken only for the client's own player.
- Only record after the warmup window has elapsed.

## Defaults (override via flags)

| Param        | Default | Notes                            |
|--------------|---------|----------------------------------|
| server port  | 9000    |                                  |
| tick rate    | 30 Hz   | server broadcast frequency       |
| input rate   | 20 Hz   | client MOVE frequency per conn   |
| room size    | 50      | players per room                 |
| warmup       | 5 s     | not measured                     |
| duration     | 30 s    | measured window                  |
