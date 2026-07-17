# Game server (Elixir / bare BEAM, no Phoenix). Mirrors the reference architecture:
#   - one BEAM process per connection (owns the socket = its own write path)
#   - one GenServer per room that OWNS its state; all mutation via cast/call
#   - a slow client's process can't stall the room: the room only send()s to it
# Uses OTP's {packet, 4} framing (4-byte big-endian length prefix), the idiomatic
# equivalent of the manual length prefix in the Go/Rust servers.
#
# Run:  elixir server.exs -addr :9000 -tick 30
#       elixir --erl "+S 4:4" server.exs -addr :9000   # pin schedulers for fairness
# See ../../PROTOCOL.md. (Thousand Island could replace the acceptor pool below.)

defmodule PidGen do
  import Bitwise
  def init, do: :persistent_term.put({PidGen, :ref}, :atomics.new(1, signed: false))
  def next do
    ref = :persistent_term.get({PidGen, :ref})
    :atomics.add_get(ref, 1, 1) |> band(0xFFFFFFFF)
  end
end

defmodule Room do
  use GenServer

  def start(tick_ms), do: GenServer.start(__MODULE__, tick_ms)

  @impl true
  def init(tick_ms) do
    Process.send_after(self(), :tick, tick_ms)
    {:ok, %{tick_ms: tick_ms, tick_no: 0, players: %{}, conns: %{}, mons: %{}}}
  end

  @impl true
  def handle_call({:join, conn_pid}, _from, s) do
    pid = PidGen.next()
    ref = Process.monitor(conn_pid)
    s = %{
      s
      | players: Map.put(s.players, pid, %{x: 0, y: 0, vx: 0, vy: 0, last_seq: 0}),
        conns: Map.put(s.conns, pid, conn_pid),
        mons: Map.put(s.mons, ref, pid)
    }
    {:reply, pid, s}
  end

  @impl true
  def handle_cast({:move, pid, seq, dx, dy}, s) do
    case s.players do
      %{^pid => p} ->
        {:noreply, %{s | players: %{s.players | pid => %{p | vx: dx, vy: dy, last_seq: seq}}}}
      _ ->
        {:noreply, s}
    end
  end

  @impl true
  def handle_info(:tick, s) do
    Process.send_after(self(), :tick, s.tick_ms)
    tick_no = s.tick_no + 1
    players = Map.new(s.players, fn {id, p} -> {id, %{p | x: p.x + p.vx, y: p.y + p.vy}} end)
    n = map_size(players)

    # One shared binary (>64 bytes => refc binary, shared to every mailbox, not copied).
    entries =
      for {id, p} <- players, into: <<>> do
        <<id::32, p.x::signed-32, p.y::signed-32, p.last_seq::32>>
      end

    payload = <<0x82, tick_no::32, n::16, entries::binary>>
    for {_id, cpid} <- s.conns, do: send(cpid, {:snapshot, payload})
    {:noreply, %{s | tick_no: tick_no, players: players}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, s) do
    case Map.pop(s.mons, ref) do
      {nil, _} ->
        {:noreply, s}
      {pid, mons} ->
        {:noreply, %{s | players: Map.delete(s.players, pid), conns: Map.delete(s.conns, pid), mons: mons}}
    end
  end
end

defmodule Rooms do
  use GenServer

  def start_link(tick_ms), do: GenServer.start_link(__MODULE__, tick_ms, name: __MODULE__)
  def get(room_id), do: GenServer.call(__MODULE__, {:get, room_id})

  @impl true
  def init(tick_ms) do
    :ets.new(:rooms, [:named_table, :set, :protected, read_concurrency: true])
    {:ok, %{tick_ms: tick_ms}}
  end

  @impl true
  def handle_call({:get, room_id}, _from, s) do
    pid =
      case :ets.lookup(:rooms, room_id) do
        [{^room_id, p}] ->
          p
        [] ->
          {:ok, p} = Room.start(s.tick_ms)
          :ets.insert(:rooms, {room_id, p})
          p
      end
    {:reply, pid, s}
  end
end

defmodule Stats do
  # DIAG-only counters: 1=join_ok, 2=join_fail
  def init, do: :persistent_term.put({Stats, :ref}, :atomics.new(4, signed: false))
  def inc(slot), do: :atomics.add(:persistent_term.get({Stats, :ref}), slot, 1)
  def get(slot), do: :atomics.get(:persistent_term.get({Stats, :ref}), slot)
end

defmodule Conn do
  @diag System.get_env("DIAG") != nil

  def start(socket) do
    :inet.setopts(socket, [:binary, packet: 4, nodelay: true, active: :once])
    loop(socket, nil, nil)
  end

  defp loop(socket, room, my_pid) do
    receive do
      {:tcp, ^socket, <<0x01, room_id::32>>} ->
        room = Rooms.get(room_id)
        pid =
          if @diag do
            try do
              r = GenServer.call(room, {:join, self()})
              Stats.inc(1)
              r
            catch
              :exit, _ -> Stats.inc(2); exit(:normal)
            end
          else
            GenServer.call(room, {:join, self()})
          end
        :gen_tcp.send(socket, <<0x81, pid::32, room_id::32>>)
        :inet.setopts(socket, active: :once)
        loop(socket, room, pid)

      {:tcp, ^socket, <<0x02, seq::32, dx::signed-16, dy::signed-16>>} ->
        if room, do: GenServer.cast(room, {:move, my_pid, seq, dx, dy})
        :inet.setopts(socket, active: :once)
        loop(socket, room, my_pid)

      {:tcp, ^socket, _other} ->
        :inet.setopts(socket, active: :once)
        loop(socket, room, my_pid)

      {:snapshot, bin} ->
        # Shed load like the Go/Rust/OCaml servers do, but better: if this client
        # has fallen behind and several snapshots are queued, drop the stale ones
        # and send only the freshest state. Bounds latency under overload.
        :gen_tcp.send(socket, drain_snapshots(bin))
        loop(socket, room, my_pid)

      {:tcp_closed, ^socket} ->
        :ok

      {:tcp_error, ^socket, _} ->
        :ok
    end
  end

  # Non-blocking: keep the newest snapshot, discard any older ones already queued.
  defp drain_snapshots(cur) do
    receive do
      {:snapshot, newer} -> drain_snapshots(newer)
    after
      0 -> cur
    end
  end
end

defmodule Server do
  @diag System.get_env("DIAG") != nil

  def start(port, acceptors) do
    {:ok, lsock} =
      :gen_tcp.listen(port, [
        :binary,
        packet: 4,
        active: false,
        reuseaddr: true,
        nodelay: true,
        backlog: 4096
      ])

    for _ <- 1..acceptors, do: spawn(fn -> accept_loop(lsock) end)
    lsock
  end

  defp accept_loop(lsock) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        if @diag, do: Stats.inc(3)
        pid = spawn(fn ->
          receive do
            :go -> Conn.start(sock)
          end
        end)
        # Robust: a client that reset between accept and here makes
        # controlling_process return {:error,_}. The old `:ok = ...` match RAISED,
        # which killed this acceptor for good (no supervisor) — a few resets took
        # out all acceptors and accepts stopped. Handle it instead of crashing.
        case :gen_tcp.controlling_process(sock, pid) do
          :ok ->
            send(pid, :go)
          {:error, _} ->
            if @diag, do: Stats.inc(4)
            :gen_tcp.close(sock)
            Process.exit(pid, :kill)
        end
        accept_loop(lsock)

      _ ->
        accept_loop(lsock)
    end
  end
end

defmodule Diag do
  # Gated on DIAG=1. Prints establishment progress + backpressure signals to stderr.
  def start, do: spawn(fn -> loop(0) end)

  defp loop(n) do
    Process.sleep(1000)
    procs = :erlang.system_info(:process_count)
    mem = div(:erlang.memory(:total), 1024 * 1024)
    maxq =
      Process.list()
      |> Enum.reduce(0, fn p, mx ->
        case Process.info(p, :message_queue_len) do
          {:message_queue_len, q} when q > mx -> q
          _ -> mx
        end
      end)
    rq = :erlang.statistics(:run_queue)
    IO.puts(:stderr, "DIAG t=#{n + 1}s procs=#{procs} mem=#{mem}MB max_mailbox=#{maxq} " <>
      "run_queue=#{rq} accepts=#{Stats.get(3)} cp_fail=#{Stats.get(4)} " <>
      "join_ok=#{Stats.get(1)} join_fail=#{Stats.get(2)}")
    loop(n + 1)
  end
end

defmodule Args do
  def parse(argv), do: parse(argv, 9000, 30)
  defp parse([], port, tick), do: {port, tick}
  defp parse(["-addr", a | rest], _p, tick), do: parse(rest, port_of(a), tick)
  defp parse(["--addr", a | rest], _p, tick), do: parse(rest, port_of(a), tick)
  defp parse(["-tick", t | rest], port, _tk), do: parse(rest, port, String.to_integer(t))
  defp parse(["--tick", t | rest], port, _tk), do: parse(rest, port, String.to_integer(t))
  defp parse([_ | rest], port, tick), do: parse(rest, port, tick)
  defp port_of(a), do: a |> String.split(":") |> List.last() |> String.to_integer()
end

{port, tick_hz} = Args.parse(System.argv())
tick_ms = max(1, div(1000, tick_hz))

PidGen.init()
if System.get_env("DIAG"), do: Stats.init()
{:ok, _} = Rooms.start_link(tick_ms)
# A POOL of acceptors (like ranch/Thousand Island), not one-per-scheduler. Four
# acceptors cannot drain a 10k connection burst; the backlog overflows and
# connections are refused. Default 100; override with ACCEPTORS=N.
acceptors = String.to_integer(System.get_env("ACCEPTORS") || "100")
Server.start(port, acceptors)
IO.puts("elixir game server on :#{port}, tick=#{tick_hz}Hz (#{tick_ms}ms), schedulers=#{System.schedulers_online()}")
if System.get_env("DIAG"), do: Diag.start()
Process.sleep(:infinity)
