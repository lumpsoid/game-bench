# Game server (Elixir / bare BEAM, no Phoenix) — idiomatic OTP edition.
#
# This is what an Elixir team would actually ship: standard libraries, no
# hand-rolled socket loops or optimizations.
#   - Thousand Island: the acceptor pool + per-connection handler process
#     (the same library that powers Bandit/Phoenix). It owns the accept loop,
#     connection supervision, and socket flow control.
#   - Registry + DynamicSupervisor: room lookup and on-demand, race-safe room
#     creation — the canonical "process registry" pattern.
#   - one GenServer per room that OWNS its state; all mutation via cast/call.
#   - one handler process per connection that OWNS its socket (its write path);
#     a slow client can't stall the room — the room only send()s to it.
# OTP {packet, 4} framing (4-byte big-endian length prefix) matches the other
# servers. See ../../PROTOCOL.md.
#
# Run:  elixir server.exs -addr :9000 -tick 30
#       elixir --erl "+S 4:4" server.exs -addr :9000   # pin schedulers for fairness
Mix.install([{:thousand_island, "~> 1.3"}])

defmodule Room do
  # One GenServer per room. Owns all player state; mutated only via cast/call.
  use GenServer

  def start_link(room_id), do: GenServer.start_link(__MODULE__, room_id, name: via(room_id))
  defp via(id), do: {:via, Registry, {RoomRegistry, id}}

  @impl true
  def init(_room_id) do
    tick_ms = :persistent_term.get(:tick_ms)
    Process.send_after(self(), :tick, tick_ms)
    {:ok, %{tick_ms: tick_ms, tick_no: 0, players: %{}, conns: %{}, mons: %{}, next_id: 1}}
  end

  @impl true
  def handle_call({:join, conn_pid}, _from, s) do
    id = s.next_id
    ref = Process.monitor(conn_pid)
    s = %{
      s
      | players: Map.put(s.players, id, %{x: 0, y: 0, vx: 0, vy: 0, last_seq: 0}),
        conns: Map.put(s.conns, id, conn_pid),
        mons: Map.put(s.mons, ref, id),
        next_id: id + 1
    }
    {:reply, id, s}
  end

  @impl true
  def handle_cast({:move, id, seq, dx, dy}, s) do
    case s.players do
      %{^id => p} ->
        {:noreply, %{s | players: %{s.players | id => %{p | vx: dx, vy: dy, last_seq: seq}}}}
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
    for {_id, pid} <- s.conns, do: send(pid, {:snapshot, payload})
    {:noreply, %{s | tick_no: tick_no, players: players}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, s) do
    case Map.pop(s.mons, ref) do
      {nil, _} ->
        {:noreply, s}
      {id, mons} ->
        {:noreply, %{s | players: Map.delete(s.players, id), conns: Map.delete(s.conns, id), mons: mons}}
    end
  end
end

defmodule Conn do
  # One handler process per connection (Thousand Island manages the lifecycle).
  # It owns the socket, so it's the only process that writes to this client.
  use ThousandIsland.Handler

  @impl ThousandIsland.Handler
  def handle_connection(_socket, _state), do: {:continue, %{room: nil, id: nil}}

  @impl ThousandIsland.Handler
  def handle_data(<<0x01, room_id::32>>, socket, state) do
    room = room_pid(room_id)
    id = GenServer.call(room, {:join, self()})
    ThousandIsland.Socket.send(socket, <<0x81, id::32, room_id::32>>)
    {:continue, %{state | room: room, id: id}}
  end

  def handle_data(<<0x02, seq::32, dx::signed-16, dy::signed-16>>, _socket, %{room: room, id: id} = state)
      when room != nil do
    GenServer.cast(room, {:move, id, seq, dx, dy})
    {:continue, state}
  end

  def handle_data(_other, _socket, state), do: {:continue, state}

  # Snapshots pushed by the room arrive as normal messages; the handler is a
  # GenServer, so we handle them and write to our socket. State is {socket, state}.
  @impl GenServer
  def handle_info({:snapshot, bin}, {socket, state}) do
    ThousandIsland.Socket.send(socket, bin)
    {:noreply, {socket, state}}
  end

  # Canonical Registry + DynamicSupervisor lookup-or-start (race-safe).
  defp room_pid(room_id) do
    case Registry.lookup(RoomRegistry, room_id) do
      [{pid, _}] ->
        pid
      [] ->
        case DynamicSupervisor.start_child(RoomSup, {Room, room_id}) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
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
:persistent_term.put(:tick_ms, tick_ms)

children = [
  {Registry, keys: :unique, name: RoomRegistry},
  {DynamicSupervisor, name: RoomSup, strategy: :one_for_one},
  {ThousandIsland,
   port: port,
   handler_module: Conn,
   num_acceptors: 100,
   transport_options: [packet: 4, nodelay: true, backlog: 4096]}
]

{:ok, _} = Supervisor.start_link(children, strategy: :one_for_one)
IO.puts("elixir game server on :#{port}, tick=#{tick_hz}Hz (#{tick_ms}ms), " <>
  "schedulers=#{System.schedulers_online()}")
Process.sleep(:infinity)
