// Game server (Swift / SwiftNIO). See ../../PROTOCOL.md for the wire format.
//
// Architecture — the idiomatic Swift server story, an EVENT-LOOP-PER-CORE reactor
// (the direct parallel to Rust/tokio and Dart's isolates, but in ONE process with
// shared memory):
//
//   - SwiftNIO's MultiThreadedEventLoopGroup gives `-workers N` OS threads, each
//     running one event loop. One ServerBootstrap listens; NIO distributes the
//     accepted child channels across the N loops (work is spread, not sharded by
//     SO_REUSEPORT the way Odin/Zig/Dart do it).
//   - Each ROOM is pinned to one event loop and OWNS its state there — no locks on
//     game state, exactly like the reference's one-goroutine-per-room. A room's tick
//     is a repeated task on its loop; MOVE/JOIN/LEAVE from a connection hop onto the
//     room's loop via `loop.execute`.
//   - Per-connection writes go through the Channel, which serializes writes per
//     socket. `Channel.writeAndFlush` is thread-safe and hops to the channel's own
//     loop, so a room on loop A can safely broadcast to a connection on loop B.
//   - Slow-client safety: child channels carry a write-buffer high-water mark, so a
//     backed-up client goes `!isWritable` and the room DROPS its snapshot rather than
//     buffering unboundedly — same semantics as the reference's full-send-buffer drop.
//
// GC/runtime notes: Swift is ARC (ref-counted), not tracing-GC. No stop-the-world
// pauses; per-frame snapshot ByteBuffers are allocated from a pooling allocator.

import Foundation
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers

// ---- protocol tags --------------------------------------------------------
let msgJoin: UInt8 = 0x01     // client -> server
let msgMove: UInt8 = 0x02     // client -> server
let msgJoined: UInt8 = 0x81   // server -> client
let msgSnapshot: UInt8 = 0x82 // server -> client
let maxFrame = 1 << 20        // 1 MiB

// Server-unique player ids.
let nextPlayerID = NIOLockedValueBox<UInt32>(0)
@inline(__always) func allocPID() -> UInt32 {
    nextPlayerID.withLockedValue { $0 &+= 1; return $0 }
}

// ---- room state (confined to one event loop) ------------------------------

final class Player {
    let id: UInt32
    var x: Int32 = 0
    var y: Int32 = 0
    var vx: Int16 = 0
    var vy: Int16 = 0
    var lastSeq: UInt32 = 0
    init(id: UInt32) { self.id = id }
}

/// A room owns its players' state. Every method here MUST run on `loop`.
final class Room {
    let id: UInt32
    let loop: EventLoop
    let alloc = ByteBufferAllocator()
    var players: [UInt32: Player] = [:]
    var conns: [UInt32: Channel] = [:]
    var tick: UInt32 = 0
    var drops: UInt64 = 0

    init(id: UInt32, loop: EventLoop, tickDur: TimeAmount) {
        self.id = id
        self.loop = loop
        // The room's tick runs on its own loop — no lock needed against join/move,
        // which are also hopped onto this loop.
        loop.scheduleRepeatedTask(initialDelay: tickDur, delay: tickDur) { [weak self] _ in
            self?.step()
        }
    }

    func addPlayer(_ pid: UInt32, _ channel: Channel) {
        players[pid] = Player(id: pid)
        conns[pid] = channel
    }

    func applyMove(_ pid: UInt32, seq: UInt32, dx: Int16, dy: Int16) {
        if let p = players[pid] {
            p.vx = dx
            p.vy = dy
            p.lastSeq = seq
        }
    }

    func removePlayer(_ pid: UInt32) {
        players[pid] = nil
        conns[pid] = nil
    }

    func step() {
        tick &+= 1
        for p in players.values {
            p.x = p.x &+ Int32(p.vx)
            p.y = p.y &+ Int32(p.vy)
        }
        let n = players.count
        let payloadLen = 7 + n * 16
        var buf = alloc.buffer(capacity: 4 + payloadLen)
        buf.writeInteger(UInt32(payloadLen))       // frame length prefix
        buf.writeInteger(msgSnapshot)
        buf.writeInteger(tick)
        buf.writeInteger(UInt16(n))
        for p in players.values {
            buf.writeInteger(p.id)
            buf.writeInteger(UInt32(bitPattern: p.x))
            buf.writeInteger(UInt32(bitPattern: p.y))
            buf.writeInteger(p.lastSeq)
        }
        for ch in conns.values {
            // Never block the room on one slow client: if its write buffer is over
            // the high-water mark, drop this snapshot. `buf` is COW — each write
            // gets its own reader view over shared storage.
            if ch.isWritable {
                ch.writeAndFlush(buf, promise: nil)
            } else {
                drops &+= 1
            }
        }
    }
}

// ---- room registry (touched from any loop) --------------------------------

final class Registry {
    let group: EventLoopGroup
    let tickDur: TimeAmount
    let lock = NIOLock()
    var rooms: [UInt32: Room] = [:]

    init(group: EventLoopGroup, tickDur: TimeAmount) {
        self.group = group
        self.tickDur = tickDur
    }

    /// Get (or lazily create) a room, pinning new rooms round-robin across loops.
    func get(_ id: UInt32) -> Room {
        lock.withLock {
            if let r = rooms[id] { return r }
            let r = Room(id: id, loop: group.next(), tickDur: tickDur)
            rooms[id] = r
            return r
        }
    }
}

// ---- per-connection handler -----------------------------------------------

final class GameHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    let registry: Registry
    var cumulation: ByteBuffer?
    var room: Room?
    var pid: UInt32 = 0
    var joined = false

    init(registry: Registry) { self.registry = registry }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var incoming = unwrapInboundIn(data)
        if cumulation == nil {
            cumulation = incoming
        } else {
            cumulation!.writeBuffer(&incoming)
        }

        while true {
            guard var buf = cumulation, buf.readableBytes >= 4 else { break }
            let len = Int(buf.getInteger(at: buf.readerIndex, as: UInt32.self)!)
            if len == 0 || len > maxFrame {
                context.close(promise: nil)
                return
            }
            guard buf.readableBytes >= 4 + len else { break }
            let payloadStart = buf.readerIndex + 4
            let tag = buf.getInteger(at: payloadStart, as: UInt8.self)!

            switch tag {
            case msgJoin where len >= 5:
                let roomID = buf.getInteger(at: payloadStart + 1, as: UInt32.self)!
                handleJoin(context: context, roomID: roomID)
            case msgMove where len >= 9:
                let seq = buf.getInteger(at: payloadStart + 1, as: UInt32.self)!
                let dx = buf.getInteger(at: payloadStart + 5, as: Int16.self)!
                let dy = buf.getInteger(at: payloadStart + 7, as: Int16.self)!
                if let room = room, joined {
                    let p = pid
                    room.loop.execute { room.applyMove(p, seq: seq, dx: dx, dy: dy) }
                }
            default:
                break
            }

            buf.moveReaderIndex(forwardBy: 4 + len)
            cumulation = buf
        }
        cumulation?.discardReadBytes()
    }

    private func handleJoin(context: ChannelHandlerContext, roomID: UInt32) {
        let room = registry.get(roomID)
        self.room = room
        // Assign the player id here (on this connection's loop) so `pid` is only ever
        // read/written on one loop; then hand the channel to the room's loop.
        let p = allocPID()
        pid = p
        joined = true
        let channel = context.channel
        room.loop.execute { room.addPlayer(p, channel) }

        // Ack immediately — we are on the channel's own loop.
        var jb = context.channel.allocator.buffer(capacity: 13)
        jb.writeInteger(UInt32(9))
        jb.writeInteger(msgJoined)
        jb.writeInteger(p)
        jb.writeInteger(roomID)
        context.writeAndFlush(NIOAny(jb), promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        if joined, let room = room {
            let p = pid
            room.loop.execute { room.removePlayer(p) }
        }
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        context.close(promise: nil)
    }
}

// ---- CLI + bootstrap ------------------------------------------------------

func parseArgs() -> (host: String, port: Int, tick: Int, workers: Int) {
    var host = "0.0.0.0"
    var port = 9000
    var tick = 30
    var workers = 1
    let a = CommandLine.arguments
    var i = 1
    while i < a.count {
        switch a[i] {
        case "-addr":
            i += 1
            if i < a.count {
                // Accept ":9000" or "host:9000".
                let s = a[i]
                if let colon = s.lastIndex(of: ":") {
                    let h = String(s[s.startIndex..<colon])
                    if !h.isEmpty { host = h }
                    port = Int(s[s.index(after: colon)...]) ?? port
                } else {
                    port = Int(s) ?? port
                }
            }
        case "-tick":
            i += 1; if i < a.count { tick = Int(a[i]) ?? tick }
        case "-workers":
            i += 1; if i < a.count { workers = Int(a[i]) ?? workers }
        default:
            break
        }
        i += 1
    }
    return (host, port, tick, max(1, workers))
}

let cfg = parseArgs()
let group = MultiThreadedEventLoopGroup(numberOfThreads: cfg.workers)
let tickDur = TimeAmount.nanoseconds(Int64(1_000_000_000 / cfg.tick))
let registry = Registry(group: group, tickDur: tickDur)

let bootstrap = ServerBootstrap(group: group)
    .serverChannelOption(ChannelOptions.backlog, value: 1024)
    .childChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1) // Nagle OFF (IPPROTO_TCP)
    .childChannelOption(
        ChannelOptions.writeBufferWaterMark,
        value: ChannelOptions.Types.WriteBufferWaterMark(low: 32 * 1024, high: 256 * 1024)
    )
    .childChannelInitializer { channel in
        return channel.pipeline.addHandler(GameHandler(registry: registry))
    }

do {
    let ch = try bootstrap.bind(host: cfg.host, port: cfg.port).wait()
    FileHandle.standardError.write(
        "swift game server on \(cfg.host):\(cfg.port), tick=\(cfg.tick)Hz, workers=\(cfg.workers)\n"
            .data(using: .utf8)!)
    try ch.closeFuture.wait()
} catch {
    FileHandle.standardError.write("bind failed: \(error)\n".data(using: .utf8)!)
    exit(1)
}
