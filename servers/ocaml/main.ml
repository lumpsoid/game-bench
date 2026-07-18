(* ============================================================================
   Game server (OCaml 5 / Eio) — MULTICORE, SHARED-NOTHING PER DOMAIN.
   Verified on OCaml 5.5.0 + Eio 1.3.

   Design: one independent Eio event loop per domain (one per core). Each domain
   opens the listen socket with SO_REUSEPORT, so the kernel load-balances incoming
   connections across domains. A connection — and the room it joins — lives entirely
   on the domain that accepted it. Nothing on the hot path is shared across domains,
   so there are NO locks on game state: within a single Eio domain fibers are
   cooperative (never parallel), so plain Hashtbl access from the tick fiber and the
   connection fibers needs no synchronisation. The only cross-domain value is the
   global player-id counter (an Atomic).

   Architecture (per domain):
     - one accept loop (Eio.Net.run_server, single domain) forking a fiber per conn
     - each connection: a reader fiber (parses frames, mutates room state directly)
       and a writer daemon fiber draining a bounded mailbox to the socket, so a slow
       client can't stall the domain's tick (drop-on-full sheds stale snapshots). The
       writer coalesces every frame already queued into ONE write per wakeup: without
       this, a per-frame fiber-suspend + io_uring round-trip caps outbound throughput
       and the mailbox backs up to the drop threshold (seconds of standing latency).
     - one tick fiber per domain steps that domain's own rooms and enqueues snapshots

   This mirrors the Odin thread-per-core sharded reactor and the Go/Rust "room owns
   its state, no locks on the hot path" architecture — reached here by following
   Eio's own model (confine mutable state to a domain) rather than sharing it behind
   mutexes. Like Python's multi-process and Odin's multi-worker designs, a room's
   members are sharded across domains by SO_REUSEPORT (accepted by METHODOLOGY.md).

   Flags: -addr :PORT  -tick HZ  -domains N   (N defaults to recommended core count)
   Build: dune build --profile release && ./_build/default/main.exe -addr :9000
   See ../../PROTOCOL.md.
   ============================================================================ *)

let msg_join = 0x01
let msg_move = 0x02
let msg_joined = 0x81
let msg_snapshot = 0x82

(* The only value shared across domains: server-unique player ids. *)
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

(* A room is owned entirely by one domain — no lock. *)
type room = {
  players : (int, player) Hashtbl.t;
  conns : (int, conn) Hashtbl.t;
  mutable tick_no : int;
}

(* A connection's mailbox is created large; we drop once DROP_AT items are pending
   so the tick never blocks on a slow client. *)
let mailbox_cap = 4096
let drop_at = 64

(* ---- binary helpers (big-endian), reading from strings ---- *)
let get_u32 s off = Int32.to_int (Bytes.get_int32_be (Bytes.unsafe_of_string s) off) land 0xFFFFFFFF
let get_i16 s off = Bytes.get_int16_be (Bytes.unsafe_of_string s) off

let frame_of payload =
  let n = String.length payload in
  let b = Bytes.create (4 + n) in
  Bytes.set_int32_be b 0 (Int32.of_int n);
  Bytes.blit_string payload 0 b 4 n;
  Bytes.unsafe_to_string b

(* ---- room lookup (domain-local, no lock) ---- *)
let get_room rooms room_id =
  match Hashtbl.find_opt rooms room_id with
  | Some r -> r
  | None ->
      let r = { players = Hashtbl.create 64; conns = Hashtbl.create 64; tick_no = 0 } in
      Hashtbl.replace rooms room_id r;
      r

(* Step one room: advance players, build one snapshot, fan it out to each conn's
   mailbox. Touches only domain-local state and never blocks (add is a no-op once a
   mailbox is full), so the whole tick runs without yielding to other fibers. *)
let step room =
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
    room.conns

(* One tick fiber per domain, stepping that domain's rooms. Schedule on an absolute
   deadline so per-tick work overlaps the sleep instead of adding to it; snap forward
   if we ever fall behind so we don't spiral. *)
let tick_loop clock tick_s rooms =
  let next = ref (Eio.Time.now clock +. tick_s) in
  let rec loop () =
    Eio.Time.sleep_until clock !next;
    Hashtbl.iter (fun _ r -> step r) rooms;
    next := !next +. tick_s;
    let now = Eio.Time.now clock in
    if !next < now then next := now +. tick_s;
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

let handle flow rooms =
  set_nodelay flow;
  Eio.Switch.run @@ fun sw ->
  let r = Eio.Buf_read.of_flow flow ~max_size:(1 lsl 21) in
  let mailbox = Eio.Stream.create mailbox_cap in
  let conn = { mailbox } in
  (* writer: drains the mailbox to the socket; cancelled when the read loop ends *)
  Eio.Fiber.fork_daemon ~sw (fun () ->
      let buf = Buffer.create 8192 in
      try
        let rec w () =
          let first = Eio.Stream.take mailbox in
          Buffer.clear buf;
          Buffer.add_string buf first;
          (* coalesce every frame already queued into one write syscall *)
          let rec drain () =
            match Eio.Stream.take_nonblocking mailbox with
            | Some f -> Buffer.add_string buf f; drain ()
            | None -> ()
          in
          drain ();
          Eio.Flow.copy_string (Buffer.contents buf) flow;
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
             let rm = get_room rooms room_id in
             let pid = new_pid () in
             Hashtbl.replace rm.players pid
               { pid; x = 0; y = 0; vx = 0; vy = 0; last_seq = 0 };
             Hashtbl.replace rm.conns pid conn;
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
                 (match Hashtbl.find_opt rm.players !my_pid with
                  | Some p -> p.vx <- dx; p.vy <- dy; p.last_seq <- seq
                  | None -> ())
             | None -> ()
           end
     done
   with _ -> ());
  match !room with
  | Some rm ->
      Hashtbl.remove rm.players !my_pid;
      Hashtbl.remove rm.conns !my_pid
  | None -> ()

(* ---- one independent server loop per domain ---- *)
let run_domain env port tick_s =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.any, port) in
  (* SO_REUSEPORT: every domain binds the same port; the kernel load-balances. *)
  let sock =
    Eio.Net.listen ~backlog:4096 ~reuse_addr:true ~reuse_port:true ~sw net addr
  in
  let rooms : (int, room) Hashtbl.t = Hashtbl.create 256 in
  let on_error = function
    | End_of_file -> ()
    | Eio.Io _ -> ()
    | ex -> Printf.eprintf "conn error: %s\n%!" (Printexc.to_string ex)
  in
  Eio.Fiber.fork ~sw (fun () -> tick_loop clock tick_s rooms);
  Eio.Net.run_server sock (fun flow _addr -> handle flow rooms) ~on_error

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
  let dm = Eio.Stdenv.domain_mgr env in
  Printf.printf "ocaml game server on :%d, tick=%dHz, domains=%d (shared-nothing/SO_REUSEPORT)\n%!"
    port tick_hz domains;
  Eio.Switch.run @@ fun sw ->
  (* Additional domains, each running its own independent server loop. *)
  for _ = 2 to domains do
    Eio.Fiber.fork ~sw (fun () ->
        Eio.Domain_manager.run dm (fun () -> run_domain env port tick_s))
  done;
  (* The main domain runs one too. *)
  run_domain env port tick_s
