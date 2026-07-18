// Pooled variant of the reference Go server. IDENTICAL to servers/go/main.go in
// every respect EXCEPT the per-tick snapshot buffer, which is drawn from a
// sync.Pool and reference-counted so it can be reused instead of allocated fresh
// each tick. This isolates one variable: does recycling the broadcast buffer
// change the RSS ramp with connection count, or is that ramp dominated by
// per-connection fixed cost (goroutine stacks + send channels) that pooling
// cannot touch? See FINDINGS.md.
//
// Why reference counting (and not a plain Get/Put around step)?
//   The snapshot frame is BROADCAST: one buffer is handed to every connection's
//   writer goroutine. Its lifetime therefore outlives step() — a naive
//   pool.Put(fb) at the end of the tick would recycle a buffer the writers are
//   still reading (a data race / torn frame). So each buffer carries a ref count
//   of "outstanding holders"; the last writer to finish with it returns it to
//   the pool. This is the broadcast analog of the classic per-request
//   sync.Pool: reclaim-when-done rather than free-at-request-end.
package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	msgJoin     = 0x01 // client -> server
	msgMove     = 0x02 // client -> server
	msgJoined   = 0x81 // server -> client
	msgSnapshot = 0x82 // server -> client
)

var nextPlayerID uint32 // atomic

type Player struct {
	id      uint32
	x, y    int32
	vx, vy  int16
	lastSeq uint32
}

// buf is a reference-counted, pooled byte buffer holding a complete wire frame
// (length prefix + payload). ref is the number of holders that still need to
// read b; release() returns the buffer to the pool when that reaches zero.
type buf struct {
	b   []byte
	ref int32 // atomic
}

var bufPool = sync.Pool{New: func() any { return new(buf) }}

// getBuf returns a buffer of exactly n bytes, growing the backing array only
// when a recycled one is too small (the "reuse, not free" shape).
func getBuf(n int) *buf {
	fb := bufPool.Get().(*buf)
	if cap(fb.b) < n {
		fb.b = make([]byte, n)
	} else {
		fb.b = fb.b[:n]
	}
	return fb
}

func (fb *buf) release() {
	if atomic.AddInt32(&fb.ref, -1) == 0 {
		bufPool.Put(fb) // <- back to the pool, not the OS
	}
}

type Conn struct {
	id    uint32
	send  chan *buf
	done  chan struct{}
	drops uint64 // atomic; snapshots dropped because this client's buffer was full
}

// Room commands (the only way to touch room state).
type cmdJoin struct {
	c     *Conn
	reply chan uint32
}
type cmdMove struct {
	pid    uint32
	seq    uint32
	dx, dy int16
}
type cmdLeave struct{ pid uint32 }

type Room struct {
	id      uint32
	inbox   chan interface{}
	players map[uint32]*Player
	conns   map[uint32]*Conn
	tickDur time.Duration
	tick    uint32
}

func newRoom(id uint32, tickDur time.Duration) *Room {
	r := &Room{
		id:      id,
		inbox:   make(chan interface{}, 1024),
		players: make(map[uint32]*Player),
		conns:   make(map[uint32]*Conn),
		tickDur: tickDur,
	}
	go r.run()
	return r
}

func (r *Room) run() {
	t := time.NewTicker(r.tickDur)
	defer t.Stop()
	for {
		select {
		case m := <-r.inbox:
			switch c := m.(type) {
			case cmdJoin:
				pid := atomic.AddUint32(&nextPlayerID, 1)
				r.players[pid] = &Player{id: pid}
				r.conns[pid] = c.c
				c.reply <- pid
			case cmdMove:
				if p := r.players[c.pid]; p != nil {
					p.vx, p.vy, p.lastSeq = c.dx, c.dy, c.seq
				}
			case cmdLeave:
				delete(r.players, c.pid)
				delete(r.conns, c.pid)
			}
		case <-t.C:
			r.step()
		}
	}
}

func (r *Room) step() {
	r.tick++
	for _, p := range r.players {
		p.x += int32(p.vx)
		p.y += int32(p.vy)
	}
	nconns := len(r.conns)
	if nconns == 0 {
		return
	}
	n := len(r.players)
	payloadLen := 7 + n*16

	// One pooled buffer holds the whole frame: [u32 length | payload]. In the
	// reference server this was two make() calls per tick (payload, then frame);
	// here it is zero allocations once the pool is warm.
	fb := getBuf(4 + payloadLen)
	b := fb.b
	binary.BigEndian.PutUint32(b, uint32(payloadLen))
	b[4] = msgSnapshot
	binary.BigEndian.PutUint32(b[5:], r.tick)
	binary.BigEndian.PutUint16(b[9:], uint16(n))
	off := 11
	for _, p := range r.players {
		binary.BigEndian.PutUint32(b[off:], p.id)
		binary.BigEndian.PutUint32(b[off+4:], uint32(p.x))
		binary.BigEndian.PutUint32(b[off+8:], uint32(p.y))
		binary.BigEndian.PutUint32(b[off+12:], p.lastSeq)
		off += 16
	}

	// Set the ref count BEFORE any enqueue: once a writer can observe fb it must
	// see a correct, nonzero count, or two holders could each drive it to zero
	// and double-Put. Each writer that receives fb calls release() exactly once;
	// for a full send channel we drop the snapshot and release our own ref here.
	atomic.StoreInt32(&fb.ref, int32(nconns))
	for _, c := range r.conns {
		select {
		case c.send <- fb:
		default:
			atomic.AddUint64(&c.drops, 1) // never block the room on one slow client
			fb.release()
		}
	}
}

type Registry struct {
	mu    sync.Mutex
	rooms map[uint32]*Room
	tick  time.Duration
}

func (reg *Registry) get(id uint32) *Room {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	r := reg.rooms[id]
	if r == nil {
		r = newRoom(id, reg.tick)
		reg.rooms[id] = r
	}
	return r
}

func handle(nc net.Conn, reg *Registry) {
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true) // TCP_NODELAY — mandatory for latency fairness
	}
	c := &Conn{send: make(chan *buf, 64), done: make(chan struct{})}
	go func() { // writer
		for {
			select {
			case fb := <-c.send:
				_, err := nc.Write(fb.b)
				fb.release()
				if err != nil {
					return
				}
			case <-c.done:
				return
			}
		}
	}()

	var room *Room
	header := make([]byte, 4)
	for {
		if _, err := io.ReadFull(nc, header); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(header)
		if n == 0 || n > 1<<20 {
			break
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(nc, payload); err != nil {
			break
		}
		switch payload[0] {
		case msgJoin:
			roomID := binary.BigEndian.Uint32(payload[1:])
			room = reg.get(roomID)
			reply := make(chan uint32, 1)
			room.inbox <- cmdJoin{c: c, reply: reply}
			c.id = <-reply
			jf := getBuf(4 + 9)
			binary.BigEndian.PutUint32(jf.b, 9)
			jf.b[4] = msgJoined
			binary.BigEndian.PutUint32(jf.b[5:], c.id)
			binary.BigEndian.PutUint32(jf.b[9:], roomID)
			atomic.StoreInt32(&jf.ref, 1)
			c.send <- jf
		case msgMove:
			if room != nil && n >= 9 {
				seq := binary.BigEndian.Uint32(payload[1:])
				dx := int16(binary.BigEndian.Uint16(payload[5:]))
				dy := int16(binary.BigEndian.Uint16(payload[7:]))
				room.inbox <- cmdMove{pid: c.id, seq: seq, dx: dx, dy: dy}
			}
		}
	}

	if room != nil && c.id != 0 {
		room.inbox <- cmdLeave{pid: c.id}
	}
	close(c.done)
	nc.Close()
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	tickHz := flag.Int("tick", 30, "tick rate (Hz)")
	flag.Parse()

	reg := &Registry{rooms: map[uint32]*Room{}, tick: time.Second / time.Duration(*tickHz)}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("go-pool game server on %s, tick=%dHz", *addr, *tickHz)
	for {
		nc, err := ln.Accept()
		if err != nil {
			continue
		}
		go handle(nc, reg)
	}
}
