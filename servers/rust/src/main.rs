// Game server (Rust / tokio). Mirrors the reference architecture:
//   - one task per connection for reads
//   - one task per connection for writes (mpsc -> socket), so a slow client
//     cannot stall a room's broadcast
//   - one task per room that OWNS its state; all mutation goes through its mpsc
//     inbox (no locks on the hot path). The only lock is the room registry,
//     touched on JOIN, never on the tick/broadcast path.
// See ../../PROTOCOL.md.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration};

const MSG_JOIN: u8 = 0x01;
const MSG_MOVE: u8 = 0x02;
const MSG_JOINED: u8 = 0x81;
const MSG_SNAPSHOT: u8 = 0x82;

static NEXT_PID: AtomicU32 = AtomicU32::new(0);

struct Player {
    id: u32,
    x: i32,
    y: i32,
    vx: i16,
    vy: i16,
    last_seq: u32,
}

// Frames sent to a connection are shared read-only across a whole room, so we
// pass Arc<Vec<u8>> and clone the pointer (not the bytes) per recipient.
type Frame = Arc<Vec<u8>>;

enum Cmd {
    Join {
        tx: mpsc::Sender<Frame>,
        reply: oneshot::Sender<u32>,
    },
    Move {
        pid: u32,
        seq: u32,
        dx: i16,
        dy: i16,
    },
    Leave {
        pid: u32,
    },
}

type Registry = Arc<Mutex<HashMap<u32, mpsc::Sender<Cmd>>>>;

fn room_sender(reg: &Registry, id: u32, tick: Duration) -> mpsc::Sender<Cmd> {
    let mut map = reg.lock().unwrap();
    if let Some(s) = map.get(&id) {
        return s.clone();
    }
    let (tx, rx) = mpsc::channel(1024);
    tokio::spawn(run_room(rx, tick));
    map.insert(id, tx.clone());
    tx
}

async fn run_room(mut rx: mpsc::Receiver<Cmd>, tick: Duration) {
    let mut players: HashMap<u32, Player> = HashMap::new();
    let mut conns: HashMap<u32, mpsc::Sender<Frame>> = HashMap::new();
    let mut ticker = interval(tick);
    let mut tick_no: u32 = 0;

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                None => return,
                Some(Cmd::Join { tx, reply }) => {
                    let pid = NEXT_PID.fetch_add(1, Ordering::Relaxed) + 1;
                    players.insert(pid, Player { id: pid, x: 0, y: 0, vx: 0, vy: 0, last_seq: 0 });
                    conns.insert(pid, tx);
                    let _ = reply.send(pid);
                }
                Some(Cmd::Move { pid, seq, dx, dy }) => {
                    if let Some(p) = players.get_mut(&pid) {
                        p.vx = dx; p.vy = dy; p.last_seq = seq;
                    }
                }
                Some(Cmd::Leave { pid }) => {
                    players.remove(&pid);
                    conns.remove(&pid);
                }
            },
            _ = ticker.tick() => {
                tick_no = tick_no.wrapping_add(1);
                for p in players.values_mut() {
                    p.x += p.vx as i32;
                    p.y += p.vy as i32;
                }
                let n = players.len();
                let mut payload = Vec::with_capacity(7 + n * 16);
                payload.push(MSG_SNAPSHOT);
                payload.extend_from_slice(&tick_no.to_be_bytes());
                payload.extend_from_slice(&(n as u16).to_be_bytes());
                for p in players.values() {
                    payload.extend_from_slice(&p.id.to_be_bytes());
                    payload.extend_from_slice(&(p.x as u32).to_be_bytes());
                    payload.extend_from_slice(&(p.y as u32).to_be_bytes());
                    payload.extend_from_slice(&p.last_seq.to_be_bytes());
                }
                let frame: Frame = Arc::new(frame_of(&payload));
                for tx in conns.values() {
                    let _ = tx.try_send(frame.clone()); // drop on full — never block the room
                }
            }
        }
    }
}

fn frame_of(payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(4 + payload.len());
    b.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    b.extend_from_slice(payload);
    b
}

async fn handle(stream: TcpStream, reg: Registry, tick: Duration) {
    let _ = stream.set_nodelay(true); // TCP_NODELAY
    let (mut rd, mut wr) = stream.into_split();

    let (out_tx, mut out_rx) = mpsc::channel::<Frame>(64);
    tokio::spawn(async move {
        while let Some(buf) = out_rx.recv().await {
            if wr.write_all(&buf).await.is_err() {
                break;
            }
        }
    });

    let mut my_pid: u32 = 0;
    let mut room: Option<mpsc::Sender<Cmd>> = None;
    let mut header = [0u8; 4];

    loop {
        if rd.read_exact(&mut header).await.is_err() {
            break;
        }
        let n = u32::from_be_bytes(header) as usize;
        if n == 0 || n > (1 << 20) {
            break;
        }
        let mut payload = vec![0u8; n];
        if rd.read_exact(&mut payload).await.is_err() {
            break;
        }
        match payload[0] {
            MSG_JOIN if n >= 5 => {
                let room_id = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
                let r = room_sender(&reg, room_id, tick);
                let (reply_tx, reply_rx) = oneshot::channel();
                if r.send(Cmd::Join { tx: out_tx.clone(), reply: reply_tx }).await.is_err() {
                    break;
                }
                my_pid = reply_rx.await.unwrap_or(0);
                room = Some(r);
                let mut jp = Vec::with_capacity(9);
                jp.push(MSG_JOINED);
                jp.extend_from_slice(&my_pid.to_be_bytes());
                jp.extend_from_slice(&room_id.to_be_bytes());
                let _ = out_tx.send(Arc::new(frame_of(&jp))).await;
            }
            MSG_MOVE if n >= 9 => {
                if let Some(r) = &room {
                    let seq = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
                    let dx = i16::from_be_bytes([payload[5], payload[6]]);
                    let dy = i16::from_be_bytes([payload[7], payload[8]]);
                    // .send().await gives backpressure, matching the Go reference.
                    let _ = r.send(Cmd::Move { pid: my_pid, seq, dx, dy }).await;
                }
            }
            _ => {}
        }
    }

    if let Some(r) = room {
        if my_pid != 0 {
            let _ = r.try_send(Cmd::Leave { pid: my_pid });
        }
    }
}

fn normalize_addr(a: &str) -> String {
    if let Some(rest) = a.strip_prefix(':') {
        format!("0.0.0.0:{rest}")
    } else {
        a.to_string()
    }
}

#[tokio::main]
async fn main() {
    let mut addr = "0.0.0.0:9000".to_string();
    let mut tick_hz: u64 = 30;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-addr" | "--addr" if i + 1 < args.len() => {
                i += 1;
                addr = normalize_addr(&args[i]);
            }
            "-tick" | "--tick" if i + 1 < args.len() => {
                i += 1;
                tick_hz = args[i].parse().unwrap_or(30);
            }
            _ => {}
        }
        i += 1;
    }
    let tick = Duration::from_nanos(1_000_000_000 / tick_hz);

    let reg: Registry = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind(&addr).await.expect("bind");
    println!("rust game server on {addr}, tick={tick_hz}Hz");
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let reg = reg.clone();
                tokio::spawn(handle(stream, reg, tick));
            }
            Err(_) => continue,
        }
    }
}
