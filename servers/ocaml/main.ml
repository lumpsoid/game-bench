(* ============================================================================
   Game server (OCaml 5 / Eio) — IDIOMATIC EDITION. Verified on OCaml 5.5.0 + Eio 1.3.

   WHY THIS VERSION EXISTS (fairness note):
     This is the OCaml server an Eio engineer writes FIRST — the direct-style,
     library-default version, with NO hand-rolled performance engineering. It is
     held to the same standard as the idiomatic Elixir (Thousand Island) server:
     reach for the built-in primitives, don't optimize the hot path by hand.

     A hand-optimized variant (shared-nothing per-domain via SO_REUSEPORT + a
     coalescing writer) lives in git history — commit 8ae294f, "Fix OCaml 10k
     latency collapse". That version survives the 10k phase; THIS one is expected
     to show the same latency collapse the naive Elixir server did, because it
     does NOT coalesce writes or shard state to dodge cross-domain locks. That is
     the honest idiomatic datapoint. Keep both when reporting: "idiomatic Eio"
     (this file) and "hand-tuned Eio" (git 8ae294f) are two points on one ladder.

   WHAT MAKES THIS THE IDIOMATIC CHOICE:
     - Multicore via Eio.Net.run_server ~additional_domains — the built-in
       one-liner for running accept loops + per-connection fibers across N domains
       (cores). We do NOT hand-roll N independent event loops with SO_REUSEPORT.
     - Shared state reached only through Eio's thread-safe primitives (Eio.Mutex,
       Eio.Stream, Atomic), exactly as run_server's cross-domain model requires.
     - Direct-style structured concurrency: Switch.run / Fiber.fork / fork_daemon.
     - The per-connection writer sends each frame with one Eio.Flow.copy_string —
       NO manual buffering/coalescing of queued frames into a single syscall.
     - Safe stdlib string reads (String.get_int32_be etc.), not Bytes.unsafe_* casts.

   Architecture:
     - one fiber per connection for reads (in whichever domain accepted it)
     - one daemon fiber per connection for writes, draining an Eio.Stream mailbox
       to the socket (its own write path; a slow client can't stall a room)
     - rooms hold state behind an Eio.Mutex (cross-domain safe)
     - a single central tick loop on the main domain steps every room each tick and
       pushes snapshots into each connection's mailbox (drop-on-full so one slow
       client never blocks ticking — the bounded Eio.Stream IS the backpressure)

   Deviations from the Go/Rust reference (note when reporting):
     - room state is mutex-protected rather than owned by a command-inbox fiber
       (Eio has no clean select-over-stream-or-timeout); moves mutate under the lock
     - snapshot construction is centralized on the main domain (fibers don't run in
       parallel within a domain anyway); the parallel win is the per-connection IO

   Flags: -addr :PORT  -tick HZ  -domains N   (N defaults to recommended core count)
   Build: dune build --profile release && ./_build/default/main.exe -addr :9000
   See ../../PROTOCOL.md.
   ============================================================================ *)

let msg_join = 0x01
let msg_move = 0x02
let msg_joined = 0x81
let msg_snapshot = 0x82

let next_pid = Atomic.make 0
let new_pid () = (Atomic.fetch_and_add next_pid 1 + 1) land 0xFFFFFFFF

type player = {
  pid : int;
  mutable x : int;
  mutable y : int;
  mutable vx : int;
  mutable vy : int;
  mutable last_seq : int;
}

type conn = { mailbox : string Eio.Stream.t }

type room = {
  mutex : Eio.Mutex.t;
  players : (int, player) Hashtbl.t;
  conns : (int, conn) Hashtbl.t;
  mutable tick_no : int;
}

(* A connection's mailbox is created large; we drop once DROP_AT items are pending
   so the central tick never blocks on a slow client. This is not an optimization
   so much as backpressure: a bounded Eio.Stream is the idiomatic queue. *)
let mailbox_cap = 4096
let drop_at = 64

(* ---- binary helpers (big-endian). Idiomatic safe stdlib reads on the string
   directly — String.get_int32_be exists since OCaml 4.13, so no Bytes.unsafe
   casts are needed. ---- *)
let get_u32 s off = Int32.to_int (String.get_int32_be s off) land 0xFFFFFFFF
let get_i16 s off = String.get_int16_be s off

(* Building a fresh Bytes we solely own and handing it out as a string is the
   one blessed use of unsafe_to_string (no aliasing) — the stdlib docs sanction it. *)
let frame_of payload =
  let n = String.length payload in
  let b = Bytes.create (4 + n) in
  Bytes.set_int32_be b 0 (Int32.of_int n);
  Bytes.blit_string payload 0 b 4 n;
  Bytes.unsafe_to_string b

(* ---- room registry (shared across domains) ---- *)
let registry : (int, room) Hashtbl.t = Hashtbl.create 256
let registry_mutex = Eio.Mutex.create ()

let get_room room_id =
  Eio.Mutex.use_rw ~protect:true registry_mutex (fun () ->
      match Hashtbl.find_opt registry room_id with
      | Some r -> r
      | None ->
          let r =
            {
              mutex = Eio.Mutex.create ();
              players = Hashtbl.create 64;
              conns = Hashtbl.create 64;
              tick_no = 0;
            }
          in
          Hashtbl.replace registry room_id r;
          r)

let step room =
  Eio.Mutex.use_rw ~protect:true room.mutex (fun () ->
      room.tick_no <- (room.tick_no + 1) land 0xFFFFFFFF;
      Hashtbl.iter (fun _ p -> p.x <- p.x + p.vx; p.y <- p.y + p.vy) room.players;
      let n = Hashtbl.length room.players in
      let payload = Bytes.create (7 + (n * 16)) in
      Bytes.set_uint8 payload 0 msg_snapshot;
      Bytes.set_int32_be payload 1 (Int32.of_int room.tick_no);
      Bytes.set_uint16_be payload 5 n;
      let off = ref 7 in
      Hashtbl.iter
        (fun _ p ->
          Bytes.set_int32_be payload !off (Int32.of_int p.pid);
          Bytes.set_int32_be payload (!off + 4) (Int32.of_int p.x);
          Bytes.set_int32_be payload (!off + 8) (Int32.of_int p.y);
          Bytes.set_int32_be payload (!off + 12) (Int32.of_int p.last_seq);
          off := !off + 16)
        room.players;
      let frame = frame_of (Bytes.unsafe_to_string payload) in
      Hashtbl.iter
        (fun _ c ->
          if Eio.Stream.length c.mailbox < drop_at then Eio.Stream.add c.mailbox frame)
        room.conns)

let tick_loop clock tick_s =
  let rec loop () =
    Eio.Time.sleep clock tick_s;
    let rooms =
      Eio.Mutex.use_rw ~protect:true registry_mutex (fun () ->
          Hashtbl.fold (fun _ r acc -> r :: acc) registry [])
    in
    List.iter step rooms;
    loop ()
  in
  loop ()

(* ---- per-connection ---- *)
let set_nodelay flow =
  match Eio_unix.Resource.fd_opt flow with
  | Some fd ->
      Eio_unix.Fd.use_exn "nodelay" fd (fun ufd ->
          Unix.setsockopt ufd Unix.TCP_NODELAY true)
  | None -> ()

let handle flow =
  set_nodelay flow;
  Eio.Switch.run @@ fun sw ->
  let r = Eio.Buf_read.of_flow flow ~max_size:(1 lsl 21) in
  let mailbox = Eio.Stream.create mailbox_cap in
  let conn = { mailbox } in
  (* writer: drains the mailbox to the socket; cancelled when the read loop ends.
     Idiomatic: one frame, one copy_string. No coalescing of queued frames. *)
  Eio.Fiber.fork_daemon ~sw (fun () ->
      try
        let rec w () =
          let frame = Eio.Stream.take mailbox in
          Eio.Flow.copy_string frame flow;
          w ()
        in
        w ()
      with _ -> `Stop_daemon);
  let room = ref None in
  let my_pid = ref 0 in
  let read_frame () =
    match Eio.Buf_read.take 4 r with
    | exception _ -> None
    | hdr ->
        let n = get_u32 hdr 0 in
        if n <= 0 || n > 1 lsl 20 then None
        else ( match Eio.Buf_read.take n r with exception _ -> None | p -> Some p )
  in
  (try
     let continue = ref true in
     while !continue do
       match read_frame () with
       | None -> continue := false
       | Some payload ->
           let tag = Char.code payload.[0] in
           if tag = msg_join && String.length payload >= 5 then begin
             let room_id = get_u32 payload 1 in
             let rm = get_room room_id in
             let pid = new_pid () in
             Eio.Mutex.use_rw ~protect:true rm.mutex (fun () ->
                 Hashtbl.replace rm.players pid
                   { pid; x = 0; y = 0; vx = 0; vy = 0; last_seq = 0 };
                 Hashtbl.replace rm.conns pid conn);
             room := Some rm;
             my_pid := pid;
             let jp = Bytes.create 9 in
             Bytes.set_uint8 jp 0 msg_joined;
             Bytes.set_int32_be jp 1 (Int32.of_int pid);
             Bytes.set_int32_be jp 5 (Int32.of_int room_id);
             Eio.Stream.add mailbox (frame_of (Bytes.unsafe_to_string jp))
           end
           else if tag = msg_move && String.length payload >= 9 then begin
             match !room with
             | Some rm ->
                 let seq = get_u32 payload 1 in
                 let dx = get_i16 payload 5 in
                 let dy = get_i16 payload 7 in
                 Eio.Mutex.use_rw ~protect:true rm.mutex (fun () ->
                     match Hashtbl.find_opt rm.players !my_pid with
                     | Some p -> p.vx <- dx; p.vy <- dy; p.last_seq <- seq
                     | None -> ())
             | None -> ()
           end
     done
   with _ -> ());
  match !room with
  | Some rm ->
      Eio.Mutex.use_rw ~protect:true rm.mutex (fun () ->
          Hashtbl.remove rm.players !my_pid;
          Hashtbl.remove rm.conns !my_pid)
  | None -> ()

(* ---- args + main ---- *)
let parse_args () =
  let port = ref 9000 and tick = ref 30 and domains = ref (Domain.recommended_domain_count ()) in
  let i = ref 1 in
  let argv = Sys.argv in
  while !i < Array.length argv do
    (match argv.(!i) with
    | "-addr" | "--addr" when !i + 1 < Array.length argv ->
        let a = argv.(!i + 1) in
        let parts = String.split_on_char ':' a in
        port := int_of_string (List.nth parts (List.length parts - 1));
        incr i
    | "-tick" | "--tick" when !i + 1 < Array.length argv ->
        tick := int_of_string argv.(!i + 1);
        incr i
    | "-domains" | "--domains" when !i + 1 < Array.length argv ->
        domains := int_of_string argv.(!i + 1);
        incr i
    | _ -> ());
    incr i
  done;
  (!port, !tick, max 1 !domains)

let () =
  let port, tick_hz, domains = parse_args () in
  let tick_s = 1.0 /. float_of_int tick_hz in
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let dm = Eio.Stdenv.domain_mgr env in
  Eio.Switch.run @@ fun sw ->
  let addr = `Tcp (Eio.Net.Ipaddr.V4.any, port) in
  (* backlog matched to the other servers (4096) so connection-burst admission is
     not a confound — the comparison is about architecture, not accept-queue size. *)
  let sock = Eio.Net.listen ~backlog:4096 ~reuse_addr:true ~sw net addr in
  let on_error = function
    | End_of_file -> ()
    | Eio.Io _ -> ()
    | ex -> Printf.eprintf "conn error: %s\n%!" (Printexc.to_string ex)
  in
  Printf.printf "ocaml game server on :%d, tick=%dHz, domains=%d (idiomatic Eio)\n%!"
    port tick_hz domains;
  Eio.Fiber.fork ~sw (fun () -> tick_loop clock tick_s);
  (* The idiomatic multicore primitive: run_server spreads accept + per-connection
     fibers across `domains` cores for us. *)
  Eio.Net.run_server sock
    (fun flow _addr -> handle flow)
    ~on_error
    ~additional_domains:(dm, domains - 1)
