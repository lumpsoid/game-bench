;; Game server (Clojure, JVM + virtual threads / Project Loom).
;;
;; This is the first JVM entrant, and it uses the JVM's answer to goroutines:
;; VIRTUAL THREADS (Loom, JDK 21+). That puts it in the SAME camp as the Go
;; reference server — shared-memory, real parallelism, garbage-collected, many
;; cheap threads — so it ports the reference actor architecture directly:
;;   - one virtual thread per connection for reads
;;   - one virtual thread per connection for writes (a bounded, shed-on-full send
;;     queue, so one slow client can never stall a room)
;;   - one virtual thread per ROOM that OWNS that room's state. Because the room
;;     thread is the sole owner, the state is just an immutable Clojure map threaded
;;     through loop/recur — no atoms, no locks, no mutation on the hot path. Reader
;;     threads mutate a room only by putting commands ([:join] / [:move] / [:leave])
;;     on its inbox queue, exactly like the Go server's channel of commands.
;;
;; The only genuinely shared state is the room registry (a ConcurrentHashMap, since
;; many reader threads look up / create rooms) and the global player-id counter
;; (an AtomicInteger). Multi-core is the JVM's: virtual threads run on a carrier
;; ForkJoinPool whose parallelism we pin to -workers (== the server core budget),
;; the GOMAXPROCS equivalent.
;;
;; Idiomatic-Clojure note: rebuilding the players map as an immutable value every
;; tick allocates — that GC pressure is the honest cost of the idiom, and exactly
;; the runtime characteristic this benchmark exists to surface. We do NOT hand-tune
;; it into mutable arrays.
;;
;; See ../../PROTOCOL.md for the wire format.
(ns server
  (:import [java.net ServerSocket Socket InetSocketAddress]
           [java.io OutputStream BufferedOutputStream BufferedInputStream DataInputStream]
           [java.nio ByteBuffer]
           [java.util.concurrent ArrayBlockingQueue LinkedBlockingQueue BlockingQueue TimeUnit ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicInteger]
           [java.util.function Function]))

(set! *warn-on-reflection* true)

(def ^:const MSG-JOIN 0x01)     ; client -> server
(def ^:const MSG-MOVE 0x02)     ; client -> server
(def ^:const MSG-JOINED 0x81)   ; server -> client
(def ^:const MSG-SNAPSHOT 0x82) ; server -> client

(def ^:const MAX-FRAME (bit-shift-left 1 20)) ; reject/close frames > 1 MiB (protocol cap)
(def ^:const SEND-CAP 64)    ; per-conn outbound queue; drop snapshots once full (shed)
(def ^:const INBOX-CAP 1024) ; per-room command queue

(def ^AtomicInteger next-pid (AtomicInteger. 0))

;; ---------------------------------------------------------------------------
;; wire helpers (all multi-byte integers big-endian, per PROTOCOL.md). We write
;; positions with unchecked-int so an i32 that has wrapped truncates to its low 32
;; bits — matching the +%/bit-cast wraparound the compiled servers get for free.
;; ---------------------------------------------------------------------------

(defn frame-of ^bytes [^bytes payload]
  (let [n (alength payload)
        b (byte-array (+ 4 n))
        bb (ByteBuffer/wrap b)]
    (.putInt bb n)
    (.put bb payload)
    b))

(defn joined-payload ^bytes [^long pid ^long room-id]
  (let [b (byte-array 9)
        bb (ByteBuffer/wrap b)]
    (.put bb (unchecked-byte MSG-JOINED))
    (.putInt bb (unchecked-int pid))
    (.putInt bb (unchecked-int room-id))
    b))

(defn build-snapshot ^bytes [^long tick players]
  (let [n (count players)
        payload-len (+ 7 (* n 16))
        b (byte-array (+ 4 payload-len))
        bb (ByteBuffer/wrap b)]
    (.putInt bb payload-len) ; length prefix
    (.put bb (unchecked-byte MSG-SNAPSHOT))
    (.putInt bb (unchecked-int tick))
    (.putShort bb (unchecked-short n))
    (doseq [p players]
      (.putInt bb (unchecked-int (long (:id p))))
      (.putInt bb (unchecked-int (long (:x p))))
      (.putInt bb (unchecked-int (long (:y p))))
      (.putInt bb (unchecked-int (long (:seq p)))))
    b))

;; ---------------------------------------------------------------------------
;; room: a single virtual thread owning immutable state threaded through loop/recur
;; ---------------------------------------------------------------------------

(defn apply-cmd [state cmd]
  (case (long (nth cmd 0))
    0 ; :join  [0 pid conn]
    (let [pid (nth cmd 1) conn (nth cmd 2)]
      (-> state
          (assoc-in [:players pid] {:id pid :x 0 :y 0 :vx 0 :vy 0 :seq 0})
          (assoc-in [:conns pid] conn)))
    1 ; :move  [1 pid seq dx dy]
    (let [pid (nth cmd 1)]
      (if (get-in state [:players pid])
        (update-in state [:players pid] assoc
                   :vx (nth cmd 3) :vy (nth cmd 4) :seq (nth cmd 2))
        state))
    2 ; :leave [2 pid]
    (let [pid (nth cmd 1)]
      (-> state (update :players dissoc pid) (update :conns dissoc pid)))
    state))

(defn step
  "Integrate every player, build one snapshot, offer it to each conn's send queue
   (dropping it for any client whose queue is full), and return the new state."
  [state]
  (let [tick (unchecked-int (inc (long (:tick state))))
        players' (reduce-kv (fn [m pid p]
                              (assoc m pid (assoc p
                                                  :x (+ (long (:x p)) (long (:vx p)))
                                                  :y (+ (long (:y p)) (long (:vy p))))))
                            {} (:players state))
        frame (build-snapshot tick (vals players'))]
    (doseq [conn (vals (:conns state))]
      (.offer ^BlockingQueue (:send conn) frame))
    (assoc state :players players' :tick tick)))

(defn room-loop [^BlockingQueue inbox ^long tick-ns]
  (loop [state {:players {} :conns {} :tick 0}
         next-tick (+ (System/nanoTime) tick-ns)]
    (let [wait (- next-tick (System/nanoTime))
          cmd (when (pos? wait) (.poll inbox wait TimeUnit/NANOSECONDS))
          state (if cmd (apply-cmd state cmd) state)]
      (if (>= (System/nanoTime) next-tick)
        (let [state (step state)
              nt (+ next-tick tick-ns)
              now (System/nanoTime)]
          ;; if we fell badly behind, snap forward rather than spiral
          (recur state (if (> (- now nt) tick-ns) (+ now tick-ns) nt)))
        (recur state next-tick)))))

(defn make-room [^long tick-ns]
  (let [inbox (LinkedBlockingQueue. (int INBOX-CAP))]
    (Thread/startVirtualThread
     (reify Runnable (run [_] (room-loop inbox tick-ns))))
    {:inbox inbox}))

(defn get-room [^ConcurrentHashMap registry ^long id ^long tick-ns]
  (.computeIfAbsent registry id
                    (reify Function
                      (apply [_ _k] (make-room tick-ns)))))

;; ---------------------------------------------------------------------------
;; per-connection writer: drain the send queue and write frames to the socket
;; ---------------------------------------------------------------------------

(defn writer-loop [^OutputStream out ^BlockingQueue q ^Socket socket]
  (try
    (loop []
      (let [^bytes f (.take q)] ; blocks; interrupted on cleanup
        (.write out f 0 (alength f))
        ;; coalesce any frames already queued into one flush
        (loop [g (.poll q)]
          (when g
            (.write out ^bytes g 0 (alength ^bytes g))
            (recur (.poll q))))
        (.flush out)
        (recur)))
    (catch Exception _ nil)
    (finally (try (.close socket) (catch Exception _ nil)))))

;; ---------------------------------------------------------------------------
;; per-connection reader
;; ---------------------------------------------------------------------------

(defn handle [^Socket socket ^ConcurrentHashMap registry ^long tick-ns]
  (.setTcpNoDelay socket true) ; TCP_NODELAY — mandatory for latency fairness
  (let [in (DataInputStream. (BufferedInputStream. (.getInputStream socket)))
        out (BufferedOutputStream. (.getOutputStream socket))
        send-q (ArrayBlockingQueue. (int SEND-CAP))
        conn {:send send-q}
        writer (Thread/startVirtualThread
                (reify Runnable (run [_] (writer-loop out send-q socket))))
        ;; [room pid] of the current membership, so the finally block can leave
        cur (volatile! [nil 0])]
    (try
      (loop []
        (let [n (.readInt in)]
          (when (and (> n 0) (<= n MAX-FRAME))
            (let [payload (byte-array n)]
              (.readFully in payload)
              (let [bb (ByteBuffer/wrap payload)
                    t (bit-and (long (.get bb 0)) 0xff)]
                (cond
                  (and (= t MSG-JOIN) (>= n 5))
                  (let [room-id (long (.getInt bb 1))
                        pid (long (.incrementAndGet next-pid))
                        room (get-room registry room-id tick-ns)]
                    (.put ^BlockingQueue (:inbox room) [0 pid conn])
                    (.offer send-q (frame-of (joined-payload pid room-id)))
                    (vreset! cur [room pid])
                    (recur))

                  (and (= t MSG-MOVE) (>= n 9))
                  (let [[room pid] @cur]
                    (when room
                      (.put ^BlockingQueue (:inbox room)
                            [1 pid (long (.getInt bb 1)) (long (.getShort bb 5)) (long (.getShort bb 7))]))
                    (recur))

                  :else (recur)))))))
      (catch Exception _ nil)
      (finally
        (let [[room pid] @cur]
          (when (and room (pos? (long pid)))
            (try (.put ^BlockingQueue (:inbox room) [2 pid]) (catch Exception _ nil))))
        (.interrupt writer)
        (try (.close socket) (catch Exception _ nil))))))

;; ---------------------------------------------------------------------------
;; main
;; ---------------------------------------------------------------------------

(defn parse-port ^long [^String addr]
  (let [s (subs addr (inc (.lastIndexOf addr ":")))]
    (or (parse-long s) 9000)))

(defn parse-args [args]
  (loop [a args m {:port 9000 :tick 30 :workers 1}]
    (if (empty? a)
      m
      (let [k (first a) v (second a)]
        (case k
          ("-addr" "--addr")    (recur (drop 2 a) (assoc m :port (parse-port v)))
          ("-tick" "--tick")    (recur (drop 2 a) (assoc m :tick (or (parse-long (str v)) 30)))
          ("-workers" "--workers") (recur (drop 2 a) (assoc m :workers (or (parse-long (str v)) 1)))
          (recur (rest a) m))))))

(defn -main [& args]
  (let [{:keys [port tick workers]} (parse-args args)]
    ;; Pin the virtual-thread carrier pool to the core budget (the GOMAXPROCS
    ;; equivalent). Set BEFORE any virtual thread is created so the scheduler
    ;; reads it on first init.
    (System/setProperty "jdk.virtualThreadScheduler.parallelism" (str workers))
    (let [tick-ns (long (/ 1000000000 (long tick)))
          registry (ConcurrentHashMap.)
          ss (ServerSocket.)]
      (.setReuseAddress ss true)
      (.bind ss (InetSocketAddress. (int port)) 4096)
      (println (str "clojure game server on :" port ", tick=" tick "Hz, workers=" workers
                    " (Loom virtual threads)"))
      (loop []
        (let [socket (.accept ss)]
          (Thread/startVirtualThread
           (reify Runnable (run [_] (handle socket registry tick-ns))))
          (recur))))))

;; script-mode entrypoint (run via: clojure -M server.clj -addr :9000 -tick 30)
(apply -main *command-line-args*)
