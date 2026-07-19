#!/usr/bin/env luajit
-- Game server (LuaJIT + cqueues). Lua has no networking in its stdlib and a Lua
-- state is single-threaded with no in-state parallelism, so the idiomatic design
-- is the SAME as the Python server, not the Odin/Go-epoll reactor:
--   - one cqueues event loop per PROCESS = one core;
--   - to use N cores the runner launches N processes, each pinned to one core and
--     sharing the port via SO_REUSEPORT, so the kernel load-balances connections
--     and a room lives entirely on whichever process accepted its members (rooms
--     shard across processes). This is exactly the Python multi-core story that
--     METHODOLOGY.md blesses as fair.
--
-- Within a process the concurrency model mirrors the Go *reference* server
-- (servers/go): TWO coroutines per connection — a reader and a writer — plus one
-- global tick coroutine, with a Room owning its players' state. Because the loop
-- is cooperative and single-threaded, room state needs no locks.
--
--   - reader coroutine: blocks on the socket, parses length-prefixed frames,
--     applies JOIN/MOVE to the player it owns;
--   - writer coroutine: drains that connection's outbound buffer to the socket.
--     A slow client only stalls ITS OWN writer coroutine (it yields on a full
--     socket buffer) — never the room tick;
--   - tick coroutine: every 1/tick s, for every room, integrate positions, build
--     ONE snapshot frame, and append it to each member's outbound buffer. Load is
--     shed exactly as the other servers do: if a member is already WBUF_CAP bytes
--     backed up, drop the freshest snapshot rather than grow memory without bound.
--
-- We deliberately build each snapshot with ordinary Lua string concatenation
-- rather than an FFI scratch buffer: the per-tick allocation + GC that creates is
-- a real, honest characteristic of an idiomatic Lua server under fan-out, which
-- is precisely what this benchmark measures. No micro-optimization heroics
-- (METHODOLOGY.md, "What we are deliberately NOT doing").
--
-- Run:  luajit server.lua -addr :9000 -tick 30
-- See ../../PROTOCOL.md for the wire format.

-- ---------------------------------------------------------------------------
-- self-bootstrap the local rocks tree (cqueues) relative to this script, so the
-- runner can invoke us as a bare `luajit server.lua` with no env setup.
-- ---------------------------------------------------------------------------
local here = (arg[0] or "server.lua"):match("^(.*)/") or "."
package.path  = here .. "/rocks/share/lua/5.1/?.lua;" ..
                here .. "/rocks/share/lua/5.1/?/init.lua;" .. package.path
package.cpath = here .. "/rocks/lib/lua/5.1/?.so;" .. package.cpath

local cqueues   = require("cqueues")
local socket    = require("cqueues.socket")
local condition = require("cqueues.condition")
local bit       = require("bit")
local ffi       = require("ffi")

-- FFI setsockopt: cqueues does NOT propagate the listener's nodelay option to
-- accepted sockets (verified), and TCP_NODELAY on every socket is non-negotiable
-- for latency fairness (METHODOLOGY.md rule 1). Set it directly on each fd.
ffi.cdef [[ int setsockopt(int fd, int level, int optname, const void *optval, unsigned int optlen); ]]
local IPPROTO_TCP, TCP_NODELAY = 6, 1
local NODELAY_ON = ffi.new("int[1]", 1)

local MSG_JOIN     = 0x01 -- client -> server
local MSG_MOVE     = 0x02 -- client -> server
local MSG_JOINED   = 0x81 -- server -> client
local MSG_SNAPSHOT = 0x82 -- server -> client

local MAX_FRAME = 1048576 -- 1 MiB: reject/close larger frames (protocol cap)
local WBUF_CAP  = 1048576 -- shed snapshots once a client is this far backed up

local band, rshift = bit.band, bit.rshift
local char, byte, concat = string.char, string.byte, table.concat

-- ---------------------------------------------------------------------------
-- wire helpers (LuaJIT is Lua 5.1 — no string.pack, no bitwise operators)
-- ---------------------------------------------------------------------------

-- wr_u32 encodes v big-endian. rshift coerces to uint32 (mod 2^32), so this is
-- correct for signed i32 too (two's complement) — used for x/y as well as ids.
local function wr_u32(v)
	return char(band(rshift(v, 24), 0xFF), band(rshift(v, 16), 0xFF),
	            band(rshift(v, 8), 0xFF), band(v, 0xFF))
end

-- rd_u32 decodes big-endian as an unsigned Lua number in [0, 2^32) via
-- arithmetic (a double holds this range exactly), avoiding int32 sign issues.
local function rd_u32(s, i)
	local a, b, c, d = byte(s, i, i + 3)
	return a * 16777216 + b * 65536 + c * 256 + d
end

local function rd_i16(s, i)
	local a, b = byte(s, i, i + 1)
	local v = a * 256 + b
	if v >= 32768 then v = v - 65536 end
	return v
end

-- ---------------------------------------------------------------------------
-- per-PROCESS state. Player ids are unique within this process, which is all the
-- protocol needs: a client finds its own id inside its own room's snapshot, and
-- rooms never span processes. (The Python server makes the same per-process
-- choice; the threaded Odin/Go-epoll servers instead share one atomic counter.)
-- ---------------------------------------------------------------------------

local rooms = {}   -- room_id -> Room
local next_pid = 0

local function new_pid()
	next_pid = (next_pid + 1) % 4294967296
	return next_pid
end

local function get_room(id)
	local r = rooms[id]
	if not r then
		r = { id = id, members = {}, tick = 0 }
		rooms[id] = r
	end
	return r
end

local function room_add(room, p)
	local m = room.members
	m[#m + 1] = p
	p.midx = #m
end

-- O(1) swap-remove, keeping each member's stored index correct.
local function room_remove(room, p)
	local m = room.members
	local i, last = p.midx, #m
	if i >= 1 and i <= last and m[i] == p then
		m[i] = m[last]
		m[i].midx = i
		m[last] = nil
	end
	p.room = nil
end

-- ---------------------------------------------------------------------------
-- per-tick simulation + broadcast (one snapshot built per room, shared by all)
-- ---------------------------------------------------------------------------

local function build_snapshot(room)
	local members = room.members
	local n = #members
	room.tick = (room.tick + 1) % 4294967296
	local parts = { char(MSG_SNAPSHOT), wr_u32(room.tick),
	                char(band(rshift(n, 8), 0xFF), band(n, 0xFF)) }
	local k = 3
	for i = 1, n do
		local p = members[i]
		p.x = p.x + p.vx
		p.y = p.y + p.vy
		k = k + 1
		parts[k] = wr_u32(p.pid) .. wr_u32(p.x) .. wr_u32(p.y) .. wr_u32(p.last_seq)
	end
	local payload = concat(parts)
	return wr_u32(#payload) .. payload
end

local function tick_loop(period)
	local monotime, sleep = cqueues.monotime, cqueues.sleep
	local nxt = monotime() + period
	while true do
		local now = monotime()
		if nxt > now then sleep(nxt - now) end
		nxt = nxt + period
		for _, room in pairs(rooms) do
			if #room.members > 0 then
				local frame = build_snapshot(room)
				local members = room.members
				for i = 1, #members do
					local p = members[i]
					-- shed load: drop the freshest tick for a backed-up client
					if #p.outbuf < WBUF_CAP then
						p.outbuf = p.outbuf .. frame
						p.cv:signal()
					end
				end
			end
		end
		-- if we fell more than a full period behind, snap forward (no spiral)
		if monotime() - nxt > period then nxt = monotime() + period end
	end
end

-- ---------------------------------------------------------------------------
-- per-connection reader + writer coroutines
-- ---------------------------------------------------------------------------

local function reader(p)
	local sock = p.sock
	while true do
		local hdr = sock:read(4)         -- exactly 4 bytes, or nil on EOF
		if not hdr then return end
		local len = rd_u32(hdr, 1)
		if len == 0 or len > MAX_FRAME then return end
		local payload = sock:read(len)
		if not payload then return end
		local t = byte(payload, 1)
		if t == MSG_JOIN then
			if len >= 5 then
				local room_id = rd_u32(payload, 2)
				local room = get_room(room_id)
				p.pid, p.room = new_pid(), room
				room_add(room, p)
				local jp = char(MSG_JOINED) .. wr_u32(p.pid) .. wr_u32(room_id)
				p.outbuf = p.outbuf .. wr_u32(#jp) .. jp
				p.cv:signal()
			end
		elseif t == MSG_MOVE then
			if len >= 9 and p.room then
				p.last_seq = rd_u32(payload, 2)
				p.vx = rd_i16(payload, 6)
				p.vy = rd_i16(payload, 8)
			end
		end
	end
end

local function writer(p)
	local sock = p.sock
	while true do
		if #p.outbuf == 0 then
			if p.closed then return end
			p.cv:wait()               -- sleep until the tick (or JOIN) enqueues
		else
			local data = p.outbuf
			p.outbuf = ""
			sock:write(data)          -- buffers; yields this coroutine only
			sock:flush()              -- push to the socket (yields on backpressure)
		end
	end
end

-- serve owns one accepted connection start-to-finish: set TCP_NODELAY, spawn the
-- writer, run the reader until disconnect, then clean up. Both coroutines run
-- inside pcall so one broken connection can never abort the whole event loop.
local loop -- assigned in main; upvalue captured here
local function serve(sock)
	ffi.C.setsockopt(sock:pollfd(), IPPROTO_TCP, TCP_NODELAY, NODELAY_ON, 4)
	sock:setmode("b", "bf") -- binary reads; full-buffered writes we flush ourselves
	local p = {
		sock = sock, pid = 0, room = nil, midx = 0,
		x = 0, y = 0, vx = 0, vy = 0, last_seq = 0,
		outbuf = "", cv = condition.new(), closed = false,
	}
	loop:wrap(function() pcall(writer, p) end)
	pcall(reader, p)
	p.closed = true
	p.cv:signal() -- wake the writer so it observes closed and exits
	if p.room then room_remove(p.room, p) end
	pcall(function() sock:close() end)
end

-- ---------------------------------------------------------------------------
-- main
-- ---------------------------------------------------------------------------

local function parse_port(addr)
	local s = addr:match("([^:]*)$")
	return tonumber(s) or 9000
end

local function main()
	local addr, tick_hz = ":9000", 30
	local a = arg
	local i = 1
	while i <= #a do
		if (a[i] == "-addr" or a[i] == "--addr") and a[i + 1] then
			addr = a[i + 1]; i = i + 2
		elseif (a[i] == "-tick" or a[i] == "--tick") and a[i + 1] then
			tick_hz = tonumber(a[i + 1]) or tick_hz; i = i + 2
		else
			i = i + 1
		end
	end
	if tick_hz <= 0 then tick_hz = 30 end
	local port = parse_port(addr)

	-- reuseport lets the runner launch N single-core processes on one port; the
	-- kernel load-balances connections across them (Lua's multi-core story).
	local srv = socket.listen{
		host = "0.0.0.0", port = port,
		reuseaddr = true, reuseport = true,
	}
	assert(srv:listen())

	loop = cqueues.new()
	loop:wrap(function() tick_loop(1 / tick_hz) end)
	loop:wrap(function()
		for conn in srv:clients() do
			loop:wrap(function() serve(conn) end)
		end
	end)

	io.write(("lua (luajit+cqueues) game server on :%d, tick=%dHz\n"):format(port, tick_hz))
	io.flush()
	assert(loop:loop())
end

main()
