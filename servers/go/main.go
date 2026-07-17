// Reference game server (Go). Architecture the other languages mirror:
//   - one goroutine per connection for reads
//   - one goroutine per connection for writes (so a slow client can't stall a room)
//   - one goroutine per room that OWNS that room's state (no locks on game state)
// See ../../PROTOCOL.md for the wire format.
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

type Conn struct {
	id    uint32
	send  chan []byte
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
	n := len(r.players)
	payload := make([]byte, 7+n*16)
	payload[0] = msgSnapshot
	binary.BigEndian.PutUint32(payload[1:], r.tick)
	binary.BigEndian.PutUint16(payload[5:], uint16(n))
	off := 7
	for _, p := range r.players {
		binary.BigEndian.PutUint32(payload[off:], p.id)
		binary.BigEndian.PutUint32(payload[off+4:], uint32(p.x))
		binary.BigEndian.PutUint32(payload[off+8:], uint32(p.y))
		binary.BigEndian.PutUint32(payload[off+12:], p.lastSeq)
		off += 16
	}
	frame := frameOf(payload)
	for _, c := range r.conns {
		select {
		case c.send <- frame:
		default:
			atomic.AddUint64(&c.drops, 1) // never block the room on one slow client
		}
	}
}

func frameOf(payload []byte) []byte {
	b := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(b, uint32(len(payload)))
	copy(b[4:], payload)
	return b
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
	c := &Conn{send: make(chan []byte, 64), done: make(chan struct{})}
	go func() { // writer
		for {
			select {
			case b := <-c.send:
				if _, err := nc.Write(b); err != nil {
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
			jp := make([]byte, 9)
			jp[0] = msgJoined
			binary.BigEndian.PutUint32(jp[1:], c.id)
			binary.BigEndian.PutUint32(jp[5:], roomID)
			c.send <- frameOf(jp)
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
	log.Printf("go game server on %s, tick=%dHz", *addr, *tickHz)
	for {
		nc, err := ln.Accept()
		if err != nil {
			continue
		}
		go handle(nc, reg)
	}
}
