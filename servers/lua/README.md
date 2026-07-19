# Lua server (LuaJIT + cqueues)

Lua has **no networking in its standard library** and a Lua state is
single-threaded with no in-state parallelism, so the idiomatic high-throughput
design is the **same as the Python server**, not the thread-per-core Odin/Go-epoll
reactor:

- one cqueues event loop **per process = one core**;
- to use N cores the runner launches **N processes**, each pinned to one core and
  sharing the port via `SO_REUSEPORT` (the kernel load-balances connections, so a
  room lives entirely on whichever process accepted its members — rooms shard
  across processes). This is the Python multi-core story that METHODOLOGY.md
  blesses as fair.

Within a process the model mirrors the Go **reference** server: two coroutines per
connection (a reader and a writer) plus one global tick coroutine, with a `Room`
owning its players' state (no locks — the loop is cooperative and single-threaded).
A slow client only stalls its own writer coroutine; the tick sheds load by dropping
the freshest snapshot when a client is `WBUF_CAP` (1 MiB) backed up, exactly like
the other servers. See the header comment in `server.lua` for details.

## Dependencies

- **LuaJIT** (Lua 5.1 ABI) — `luajit` on `PATH`.
- **cqueues** — the coroutine socket library, built against the LuaJIT headers into
  a **local rocks tree** (`servers/lua/rocks/`, gitignored). `server.lua`
  self-bootstraps that tree relative to its own path, so no env setup is needed.

Building cqueues needs a C compiler, `m4`, and OpenSSL headers.

### Install cqueues into the local tree

```sh
cd servers/lua
luarocks --lua-version=5.1 --tree=rocks install cqueues \
    LUA_INCDIR=/usr/include/luajit-2.1 \
    LUA_LIBDIR=/usr/lib \
    CFLAGS="-O2 -fPIC -DLUA_COMPAT_5_3"
```

Adjust `LUA_INCDIR` to wherever your distro ships the LuaJIT headers (it must
contain `lua.h`). Verify:

```sh
eval "$(luarocks --lua-version=5.1 --tree=rocks path)"
luajit -e "require('cqueues'); print('ok')"
```

## Run

```sh
luajit servers/lua/server.lua -addr :9000 -tick 30
```

Same flags and defaults as every other server. The runner launches one such
process per server core (`SO_REUSEPORT`), the way it launches N Python processes.

## Notes

- LuaJIT is Lua 5.1: no `string.pack` and no bitwise operators, so wire encoding
  uses the `bit` library and `string.char`/`string.byte`.
- cqueues does **not** copy the listener's `nodelay` option onto accepted sockets,
  so `TCP_NODELAY` (non-negotiable per METHODOLOGY.md) is set explicitly via an FFI
  `setsockopt` on each connection's fd.
- Snapshots are built with ordinary Lua string concatenation, not an FFI scratch
  buffer. The resulting per-tick allocation + GC is an honest characteristic of an
  idiomatic Lua server under fan-out — which is what this benchmark measures — so
  it is deliberately **not** optimized away.
