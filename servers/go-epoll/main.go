// Epoll-reactor variant of the Go server (Linux/amd64). Same wire protocol and
// same shed-load-on-a-slow-client behaviour as servers/go, but a COMPLETELY
// different concurrency model, to isolate one question: how much of Go's RSS
// ramp with connection count is the goroutine-per-connection model itself?
//
// The reference server (servers/go) spends TWO goroutines per connection (a
// reader and a writer) plus one per room — each an ~8 KB-minimum stack — so its
// memory grows with connections mostly as goroutine stacks, which no amount of
// buffer pooling can reclaim (see FINDINGS.md, the sync.Pool experiment).
//
// This server spends ZERO goroutines per connection. It is a THREAD-PER-CORE
// SHARDED REACTOR, a direct port of servers/odin:
//   - N worker threads (one per core, -workers N), each a single-threaded epoll
//     event loop that OWNS its connections and its rooms — so there are no locks
//     on game state (the only shared atomic is the global player-id counter).
//   - each worker opens its own listen socket with SO_REUSEPORT; the kernel
//     load-balances new connections across workers. A room lives on whichever
//     worker accepted its members, so the same room_id can shard across workers.
//   - sockets are non-blocking; reads accumulate into a per-conn rbuf and are
//     parsed frame-by-frame; writes go through a per-conn wbuf with EPOLLOUT
//     arming for backpressure. A backed-up client (wbuf past wbufCap) drops the
//     freshest snapshot rather than stalling the whole worker.
//   - the tick is a per-worker timerfd in the same epoll set, so ticks are as
//     precise as the reference server's time.Ticker (not the ms-granular
//     epoll_wait timeout the Odin server uses).
//
// A connection now costs a Conn struct + its rbuf/wbuf slices (~1-2 KB warm),
// not two goroutine stacks. Per-tick snapshot churn is also gone for free: the
// worker is single-threaded, so it builds every snapshot into one reused scratch
// buffer and copies the bytes into each recipient's wbuf — no per-tick allocation
// and no sync.Pool needed. See ../../PROTOCOL.md for the wire format.
package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	msgJoin     = 0x01 // client -> server
	msgMove     = 0x02 // client -> server
	msgJoined   = 0x81 // server -> client
	msgSnapshot = 0x82 // server -> client

	maxFrame  = 1 << 20 // reject/close frames larger than 1 MiB (protocol cap)
	wbufCap   = 1 << 20 // shed snapshots once a client is this far backed up
	maxEvents = 1024
	readChunk = 65536

	clockMonotonic = 1
	soReusePort    = 15 // SO_REUSEPORT (Linux); not exported by stdlib syscall
)

var nextPID uint32 // atomic; server-unique player ids across all workers

// Conn holds everything for one connection AND that player's state — owned
// exclusively by the worker that accepted it, so no synchronization is needed.
type Conn struct {
	fd      int32
	rbuf    []byte // accumulated inbound bytes, parsed from the front
	wbuf    []byte // pending outbound bytes (partial writes / backpressure)
	room    *Room
	midx    int // index of this conn within room.members (for O(1) removal)
	pid     uint32
	x, y    int32
	vx, vy  int16
	lastSeq uint32
	wantOut bool // EPOLLOUT currently armed
	closed  bool
}

type Room struct {
	id      uint32
	members []*Conn
	tick    uint32
}

// Worker is one epoll event loop pinned to one OS thread (one core).
type Worker struct {
	epfd    int
	lnfd    int
	tfd     int
	rooms   map[uint32]*Room
	conns   map[int32]*Conn
	scratch []byte // reused snapshot build buffer (single-threaded => safe)
	rchunk  []byte // reused read buffer
}

// itimerspec mirrors the kernel struct for timerfd_settime.
type itimerspec struct {
	interval syscall.Timespec
	value    syscall.Timespec
}

func setupListener(ta *net.TCPAddr) (int, error) {
	fd, err := syscall.Socket(syscall.AF_INET,
		syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, soReusePort, 1)
	sa := &syscall.SockaddrInet4{Port: ta.Port}
	if ip4 := ta.IP.To4(); ip4 != nil {
		copy(sa.Addr[:], ip4) // else 0.0.0.0 (INADDR_ANY)
	}
	if err := syscall.Bind(fd, sa); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	if err := syscall.Listen(fd, 4096); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

func setupTimer(period time.Duration) (int, error) {
	r, _, e := syscall.Syscall(syscall.SYS_TIMERFD_CREATE,
		clockMonotonic, uintptr(syscall.O_NONBLOCK), 0)
	if e != 0 {
		return -1, e
	}
	fd := int(r)
	var its itimerspec
	its.interval.Sec = int64(period / time.Second)
	its.interval.Nsec = int64(period % time.Second)
	its.value = its.interval // first fire one period from now
	_, _, e = syscall.Syscall6(syscall.SYS_TIMERFD_SETTIME,
		uintptr(fd), 0, uintptr(unsafe.Pointer(&its)), 0, 0, 0)
	if e != 0 {
		syscall.Close(fd)
		return -1, e
	}
	return fd, nil
}

func newWorker(ta *net.TCPAddr, period time.Duration) (*Worker, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	lnfd, err := setupListener(ta)
	if err != nil {
		return nil, err
	}
	tfd, err := setupTimer(period)
	if err != nil {
		return nil, err
	}
	w := &Worker{
		epfd:   epfd,
		lnfd:   lnfd,
		tfd:    tfd,
		rooms:  map[uint32]*Room{},
		conns:  map[int32]*Conn{},
		rchunk: make([]byte, readChunk),
	}
	w.ctl(syscall.EPOLL_CTL_ADD, lnfd, syscall.EPOLLIN)
	w.ctl(syscall.EPOLL_CTL_ADD, tfd, syscall.EPOLLIN)
	return w, nil
}

func (w *Worker) ctl(op, fd int, events uint32) error {
	ev := syscall.EpollEvent{Events: events, Fd: int32(fd)}
	return syscall.EpollCtl(w.epfd, op, fd, &ev)
}

func (w *Worker) run() {
	runtime.LockOSThread() // dedicate this OS thread to this reactor
	events := make([]syscall.EpollEvent, maxEvents)
	for {
		n, err := syscall.EpollWait(w.epfd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			log.Printf("epoll_wait: %v", err)
			return
		}
		for i := 0; i < n; i++ {
			fd := int(events[i].Fd)
			ev := events[i].Events
			switch fd {
			case w.lnfd:
				w.accept()
			case w.tfd:
				w.onTick()
			default:
				c := w.conns[int32(fd)]
				if c == nil {
					continue
				}
				if ev&(syscall.EPOLLHUP|syscall.EPOLLERR) != 0 {
					w.closeConn(c)
					continue
				}
				if ev&syscall.EPOLLIN != 0 {
					w.onReadable(c)
				}
				if !c.closed && ev&syscall.EPOLLOUT != 0 {
					w.flush(c)
				}
			}
		}
	}
}

func (w *Worker) accept() {
	for {
		nfd, _, err := syscall.Accept4(w.lnfd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
		if err != nil {
			return // EAGAIN => drained
		}
		syscall.SetsockoptInt(nfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		c := &Conn{fd: int32(nfd)}
		w.conns[int32(nfd)] = c
		w.ctl(syscall.EPOLL_CTL_ADD, nfd, syscall.EPOLLIN)
	}
}

func (w *Worker) onReadable(c *Conn) {
	for {
		n, err := syscall.Read(int(c.fd), w.rchunk)
		if n > 0 {
			c.rbuf = append(c.rbuf, w.rchunk[:n]...)
		}
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			if err == syscall.EAGAIN {
				break
			}
			w.closeConn(c)
			return
		}
		if n == 0 {
			w.closeConn(c) // peer closed
			return
		}
		if n < len(w.rchunk) {
			break // socket drained
		}
	}
	w.parse(c)
}

func (w *Worker) parse(c *Conn) {
	off := 0
	buf := c.rbuf
	for {
		if len(buf)-off < 4 {
			break
		}
		n := binary.BigEndian.Uint32(buf[off:])
		if n == 0 || n > maxFrame {
			w.closeConn(c)
			return
		}
		if len(buf)-off < 4+int(n) {
			break
		}
		w.handle(c, buf[off+4:off+4+int(n)])
		if c.closed {
			return
		}
		off += 4 + int(n)
	}
	if off > 0 { // compact consumed bytes off the front
		c.rbuf = c.rbuf[:copy(c.rbuf, c.rbuf[off:])]
	}
}

func (w *Worker) handle(c *Conn, payload []byte) {
	switch payload[0] {
	case msgJoin:
		if len(payload) < 5 {
			return
		}
		roomID := binary.BigEndian.Uint32(payload[1:])
		r := w.rooms[roomID]
		if r == nil {
			r = &Room{id: roomID}
			w.rooms[roomID] = r
		}
		c.pid = atomic.AddUint32(&nextPID, 1)
		c.room = r
		c.midx = len(r.members)
		r.members = append(r.members, c)
		var jp [13]byte
		binary.BigEndian.PutUint32(jp[0:], 9)
		jp[4] = msgJoined
		binary.BigEndian.PutUint32(jp[5:], c.pid)
		binary.BigEndian.PutUint32(jp[9:], roomID)
		w.enqueue(c, jp[:])
	case msgMove:
		if len(payload) < 9 {
			return
		}
		c.lastSeq = binary.BigEndian.Uint32(payload[1:])
		c.vx = int16(binary.BigEndian.Uint16(payload[5:]))
		c.vy = int16(binary.BigEndian.Uint16(payload[7:]))
	}
}

func (w *Worker) onTick() {
	var b [8]byte
	syscall.Read(w.tfd, b[:]) // drain the expiration count
	for _, r := range w.rooms {
		w.step(r)
	}
}

func (w *Worker) step(r *Room) {
	n := len(r.members)
	if n == 0 {
		return
	}
	r.tick++
	payloadLen := 7 + n*16
	size := 4 + payloadLen
	if cap(w.scratch) < size {
		w.scratch = make([]byte, size)
	}
	b := w.scratch[:size]
	binary.BigEndian.PutUint32(b, uint32(payloadLen)) // frame length prefix
	b[4] = msgSnapshot
	binary.BigEndian.PutUint32(b[5:], r.tick)
	binary.BigEndian.PutUint16(b[9:], uint16(n))
	off := 11
	for _, c := range r.members {
		c.x += int32(c.vx)
		c.y += int32(c.vy)
		binary.BigEndian.PutUint32(b[off:], c.pid)
		binary.BigEndian.PutUint32(b[off+4:], uint32(c.x))
		binary.BigEndian.PutUint32(b[off+8:], uint32(c.y))
		binary.BigEndian.PutUint32(b[off+12:], c.lastSeq)
		off += 16
	}
	for _, c := range r.members {
		w.enqueue(c, b)
	}
}

// enqueue writes b to c, buffering only what the socket won't take right now.
func (w *Worker) enqueue(c *Conn, b []byte) {
	if len(c.wbuf) == 0 {
		nn, err := syscall.Write(int(c.fd), b)
		if nn < 0 {
			nn = 0
		}
		if err != nil && err != syscall.EAGAIN && err != syscall.EINTR {
			w.closeConn(c)
			return
		}
		if nn == len(b) {
			return // fully sent, nothing to buffer
		}
		c.wbuf = append(c.wbuf, b[nn:]...)
		w.armOut(c)
		return
	}
	// Already backed up. Shed load like the other servers: drop the freshest
	// snapshot rather than let one slow client grow memory without bound.
	if len(c.wbuf) >= wbufCap {
		return
	}
	c.wbuf = append(c.wbuf, b...)
	w.armOut(c)
}

func (w *Worker) flush(c *Conn) {
	for len(c.wbuf) > 0 {
		nn, err := syscall.Write(int(c.fd), c.wbuf)
		if nn > 0 {
			c.wbuf = c.wbuf[:copy(c.wbuf, c.wbuf[nn:])]
		}
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			if err == syscall.EAGAIN {
				break
			}
			w.closeConn(c)
			return
		}
		if nn == 0 {
			break
		}
	}
	if len(c.wbuf) == 0 && c.wantOut {
		c.wantOut = false
		w.ctl(syscall.EPOLL_CTL_MOD, int(c.fd), syscall.EPOLLIN)
	}
}

func (w *Worker) armOut(c *Conn) {
	if !c.wantOut {
		c.wantOut = true
		w.ctl(syscall.EPOLL_CTL_MOD, int(c.fd), syscall.EPOLLIN|syscall.EPOLLOUT)
	}
}

func (w *Worker) closeConn(c *Conn) {
	if c.closed {
		return
	}
	c.closed = true
	w.ctl(syscall.EPOLL_CTL_DEL, int(c.fd), 0)
	syscall.Close(int(c.fd))
	delete(w.conns, c.fd)
	if r := c.room; r != nil {
		last := len(r.members) - 1
		if c.midx <= last && r.members[c.midx] == c {
			r.members[c.midx] = r.members[last]
			r.members[c.midx].midx = c.midx
			r.members = r.members[:last]
		}
		c.room = nil
	}
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	tickHz := flag.Int("tick", 30, "tick rate (Hz)")
	workers := flag.Int("workers", 1, "epoll worker threads (one per core)")
	flag.Parse()

	ta, err := net.ResolveTCPAddr("tcp4", *addr)
	if err != nil {
		log.Fatal(err)
	}
	period := time.Second / time.Duration(*tickHz)
	if *workers > runtime.GOMAXPROCS(0) {
		runtime.GOMAXPROCS(*workers)
	}
	log.Printf("go-epoll game server on %s, tick=%dHz, workers=%d", *addr, *tickHz, *workers)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := newWorker(ta, period)
			if err != nil {
				log.Fatal(err)
			}
			w.run()
		}()
	}
	wg.Wait()
}
