// Load generator = the automated client. ONE program, pointed at each server in turn.
// Every connection simulates a player: it JOINs a room, sends MOVE at a fixed rate
// (open-loop — it does NOT wait for replies, so server stalls show up as latency),
// reads SNAPSHOTs, and records end-to-end latency via the echoed seq.
//
// Zero external dependencies: latency goes into a built-in log-linear histogram
// (~0.23%/bucket). Swap in HdrHistogram before publishing final numbers.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	msgJoin     = 0x01
	msgMove     = 0x02
	msgJoined   = 0x81
	msgSnapshot = 0x82
)

// ---- histogram: log-linear, 1 µs .. 100 s, 1000 sub-buckets/decade ----

const (
	decades   = 8
	perDecade = 1000
	nbuckets  = decades * perDecade
)

type Hist struct{ counts [nbuckets]uint64 }

func idxFor(us float64) int {
	if us < 1 {
		us = 1
	}
	e := math.Log10(us)
	if e < 0 {
		e = 0
	}
	i := int(e * perDecade)
	if i >= nbuckets {
		i = nbuckets - 1
	}
	return i
}
func (h *Hist) record(us float64) { atomic.AddUint64(&h.counts[idxFor(us)], 1) }
func valFor(i int) float64        { return math.Pow(10, float64(i)/perDecade) }
func (h *Hist) total() uint64 {
	var t uint64
	for i := range h.counts {
		t += atomic.LoadUint64(&h.counts[i])
	}
	return t
}
func (h *Hist) percentile(p float64) float64 {
	total := h.total()
	if total == 0 {
		return 0
	}
	target := uint64(p * float64(total))
	var c uint64
	for i := range h.counts {
		c += atomic.LoadUint64(&h.counts[i])
		if c >= target {
			return valFor(i)
		}
	}
	return valFor(nbuckets - 1)
}

// ---- global counters ----

var (
	movesSent     uint64
	movesMeasured uint64 // moves fired in the post-warmup window (send% numerator)
	snapsRecv     uint64
	measured      uint64
)

// ---- per-window latency (stability over time) ----
// One histogram per fixed wall-clock window of the measured phase; per-window p99
// then yields "worst single window" and a scale-free steadiness score (CV).

const windowMS = 1000 // 1 s windows

var windows []Hist // sized in main(); indexed by (elapsed-warmup)/windowMS

func recordWindow(sinceStart, warm time.Duration, us float64) {
	if w := int((sinceStart - warm).Milliseconds()) / windowMS; w >= 0 && w < len(windows) {
		windows[w].record(us)
	}
}

// stabilityStats returns the worst (max) window value and the coefficient of
// variation (sample stdev / mean) across non-empty windows.
func stabilityStats(xs []float64) (worst, cv float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, x := range xs {
		if x > worst {
			worst = x
		}
		sum += x
	}
	mean := sum / float64(len(xs))
	if len(xs) < 2 || mean == 0 {
		return worst, 0
	}
	var ss float64
	for _, x := range xs {
		d := x - mean
		ss += d * d
	}
	return worst, math.Sqrt(ss/float64(len(xs)-1)) / mean
}

// ---- self-instrumentation (is the loadgen itself the bottleneck?) ----

func tvSec(tv syscall.Timeval) float64 { return float64(tv.Sec) + float64(tv.Usec)/1e6 }

// selfCPUCores returns this process's CPU usage (all threads) in cores over wall.
func selfCPUCores(wall float64) float64 {
	var ru syscall.Rusage
	if wall <= 0 || syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	return (tvSec(ru.Utime) + tvSec(ru.Stime)) / wall
}

// ---- wire helpers ----

func writeFrame(nc net.Conn, payload []byte) error {
	b := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(b, uint32(len(payload)))
	copy(b[4:], payload)
	_, err := nc.Write(b)
	return err
}
func joinPayload(room uint32) []byte {
	p := make([]byte, 5)
	p[0] = msgJoin
	binary.BigEndian.PutUint32(p[1:], room)
	return p
}
func movePayload(seq uint32, dx, dy int16) []byte {
	p := make([]byte, 9)
	p[0] = msgMove
	binary.BigEndian.PutUint32(p[1:], seq)
	binary.BigEndian.PutUint16(p[5:], uint16(dx))
	binary.BigEndian.PutUint16(p[7:], uint16(dy))
	return p
}
func readFrame(nc net.Conn, header []byte) ([]byte, error) {
	if _, err := io.ReadFull(nc, header); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header)
	payload := make([]byte, n)
	if _, err := io.ReadFull(nc, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func runConn(addr string, roomID uint32, rate int, dur, warm time.Duration, h *Hist, wg *sync.WaitGroup, start time.Time) {
	defer wg.Done()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer nc.Close()
	if tc, ok := nc.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}

	if err := writeFrame(nc, joinPayload(roomID)); err != nil {
		return
	}
	header := make([]byte, 4)
	// A snapshot can race ahead of JOINED; skip until we get our id.
	var myID uint32
	for {
		p, err := readFrame(nc, header)
		if err != nil {
			return
		}
		if p[0] == msgJoined {
			myID = binary.BigEndian.Uint32(p[1:])
			break
		}
	}

	const ring = 256 // seq -> send time; window = ring/rate seconds (must exceed max RTT)
	var sendAt [ring]int64

	// reader
	go func() {
		hdr := make([]byte, 4)
		for {
			p, err := readFrame(nc, hdr)
			if err != nil {
				return
			}
			if p[0] != msgSnapshot {
				continue
			}
			atomic.AddUint64(&snapsRecv, 1)
			count := int(binary.BigEndian.Uint16(p[5:]))
			off := 7
			for k := 0; k < count; k++ {
				if binary.BigEndian.Uint32(p[off:]) == myID {
					lastSeq := binary.BigEndian.Uint32(p[off+12:])
					if lastSeq != 0 {
						if st := atomic.LoadInt64(&sendAt[lastSeq%ring]); st != 0 {
							us := float64(time.Now().UnixNano()-st) / 1000.0
							if sinceStart := time.Since(start); us > 0 && sinceStart > warm {
								h.record(us)
								recordWindow(sinceStart, warm, us)
								atomic.AddUint64(&measured, 1)
							}
						}
					}
					break
				}
				off += 16
			}
		}
	}()

	// sender (open-loop, fixed rate)
	t := time.NewTicker(time.Second / time.Duration(rate))
	defer t.Stop()
	deadline := start.Add(warm + dur)
	var seq uint32
	for now := range t.C {
		if now.After(deadline) {
			return
		}
		seq++
		atomic.StoreInt64(&sendAt[seq%ring], time.Now().UnixNano())
		if writeFrame(nc, movePayload(seq, 1, 0)) != nil {
			return
		}
		atomic.AddUint64(&movesSent, 1)
		// send% is measured over the steady-state window only, matching the latency
		// gate above: warmup ramp (conns still JOINing) must not count against it.
		if now.Sub(start) > warm {
			atomic.AddUint64(&movesMeasured, 1)
		}
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	conns := flag.Int("conns", 1000, "concurrent connections (players)")
	roomSize := flag.Int("room-size", 50, "players per room")
	rate := flag.Int("rate", 20, "moves/sec per connection")
	dur := flag.Duration("dur", 30*time.Second, "measured duration")
	warm := flag.Duration("warmup", 5*time.Second, "warmup (not measured)")
	jsonOut := flag.Bool("json", false, "emit one JSON metrics object to stdout (logs stay on stderr)")
	flag.Parse()

	h := &Hist{}
	nWin := int(dur.Milliseconds()) / windowMS
	if nWin < 1 {
		nWin = 1
	}
	windows = make([]Hist, nWin)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		room := uint32(i / *roomSize)
		go runConn(*addr, room, *rate, *dur, *warm, h, &wg, start)
		if i%200 == 199 {
			time.Sleep(5 * time.Millisecond) // avoid a connect thundering herd
		}
	}
	wg.Wait()

	secs := time.Since(start).Seconds()
	ms := atomic.LoadUint64(&movesSent)
	sn := atomic.LoadUint64(&snapsRecv)
	me := atomic.LoadUint64(&measured)
	// Human-readable log lines always go to stderr (log package default).
	log.Printf("conns=%d moves=%d snaps=%d measured=%d", *conns, ms, sn, me)
	log.Printf("throughput: moves=%.0f/s snaps=%.0f/s", float64(ms)/secs, float64(sn)/secs)
	log.Printf("latency ms: p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f max=%.2f",
		h.percentile(0.50)/1000, h.percentile(0.90)/1000, h.percentile(0.99)/1000,
		h.percentile(0.999)/1000, h.percentile(1.0)/1000)

	// per-window latency timeline (full spread) + p99 stability scalars.
	// timeline[i] = [window_start_s, p50, p90, p99, p99.9, max] in ms; empty
	// windows are skipped so the client can't mistake "no data" for "0 ms".
	timeline := make([][]float64, 0, len(windows))
	p99s := make([]float64, 0, len(windows))
	for i := range windows {
		if windows[i].total() == 0 {
			continue
		}
		p99 := windows[i].percentile(0.99) / 1000
		p99s = append(p99s, p99)
		timeline = append(timeline, []float64{
			float64(i),
			windows[i].percentile(0.50) / 1000,
			windows[i].percentile(0.90) / 1000,
			p99,
			windows[i].percentile(0.999) / 1000,
			windows[i].percentile(1.0) / 1000,
		})
	}
	p99Worst, p99CV := stabilityStats(p99s)
	log.Printf("latency stability: p99_worst_1s=%.2f ms p99_cv=%.3f (%d windows)",
		p99Worst, p99CV, len(p99s))

	// Loadgen self-checks: if the client is CPU-bound or can't keep its send
	// schedule, the latency above measures the client, not the server.
	clientCPU := selfCPUCores(secs)
	// send% covers ONLY the steady-state (post-warmup) window. A connection enters
	// the send schedule after JOIN, so charging it for the warmup ramp understated
	// healthy runs and penalized servers with a slightly heavier accept path. By the
	// measured window every conn is joined and should send at full rate, so a dip
	// here means a genuine mid-run sender stall, not establishment slack.
	mm := atomic.LoadUint64(&movesMeasured)
	sendTarget := float64(*conns) * float64(*rate) * dur.Seconds()
	sendRatePct := 100.0
	if sendTarget > 0 {
		sendRatePct = 100.0 * float64(mm) / sendTarget
	}
	log.Printf("loadgen self-check: client_cpu=%.2f cores  send_rate=%.1f%% of target",
		clientCPU, sendRatePct)

	if *jsonOut {
		out := map[string]any{
			"conns":       *conns,
			"moves_sent":  ms,
			"snaps_recv":  sn,
			"measured":    me,
			"moves_per_s": float64(ms) / secs,
			"snaps_per_s": float64(sn) / secs,
			"p50_ms":      h.percentile(0.50) / 1000,
			"p90_ms":      h.percentile(0.90) / 1000,
			"p99_ms":      h.percentile(0.99) / 1000,
			"p999_ms":     h.percentile(0.999) / 1000,
			"max_ms":      h.percentile(1.0) / 1000,

			"p99_worst_1s_ms": p99Worst,
			"p99_cv":          p99CV,
			"timeline":        timeline,

			"client_cpu_cores": clientCPU,
			"send_rate_pct":    sendRatePct,
		}
		b, _ := json.Marshal(out)
		fmt.Println(string(b)) // machine-readable, on stdout
	}
}
