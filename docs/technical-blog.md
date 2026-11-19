# Building an L4 Load Balancer in Python with `asyncio`

This is a walkthrough of a small Layer 4 (TCP) load balancer written in Python. It was built as a learning project with a deliberately narrow scope: forward raw bytes, choose a backend fairly, notice when a backend dies, enforce a per-backend connection cap, survive a restart without dropping in-flight work, and do it all without a web framework.

The whole thing is a handful of modules, uses only `asyncio` and `pyyaml`, and runs three nginx containers as backends. This post explains how it works and why it is shaped the way it is.

## What it does

- Listens for TCP connections on a configurable host and port.
- Selects a backend using a hybrid of round robin and least connections.
- Rejects a client when every backend is at its connection cap.
- Closes connections that sit idle for longer than a configured timeout.
- Runs active TCP health checks and reacts to passive connect failures.
- Retries failed connection attempts against a different backend.
- Tracks active and cumulative connections per backend.
- Tags logs with short per-connection IDs.
- Shuts down gracefully on `SIGINT` or `SIGTERM`.

It is not an HTTP proxy. Once a client is paired with a backend, the balancer never parses the payload. That keeps the implementation small and makes the tricky parts about concurrency and lifecycle rather than protocol specifics.

## Architecture at a glance

```
client ──► start_server ──► handle_client
                              │
                              ├─ strategy.select(backends, max_cap)
                              ├─ backend.reserve()
                              ├─ open_connection(backend)
                              ├─ pump client ◄──► backend
                              └─ finally: close sockets, backend.release()

health checker ──► TCP connect to each backend every N seconds
stats task     ──► logs counters every 10 seconds
```

The code is split by responsibility:

| File | Responsibility |
| --- | --- |
| `config.py` | Load and validate YAML into typed dataclasses |
| `backend.py` | Mutable per-backend state and counters |
| `strategies.py` | Backend selection algorithms |
| `health.py` | Active health checks |
| `proxy.py` | Per-connection lifecycle and byte pumping |
| `main.py` | Wiring, background tasks, signals, shutdown |

Every module talks to the same `Backend` objects. That shared state is the core of the design: selection, health checks, and the proxy all read and mutate the same counters, which is safe because the event loop is single-threaded.

## Configuration, validated up front

The config is read once at startup. A broken config should fail loudly and immediately, not halfway through serving traffic.

```yaml
listen:
  host: "0.0.0.0"
  port: 8080

max_cap: 2

health_check:
  interval_seconds: 5
  timeout_seconds: 2
  failure_threshold: 2

connect_timeout_seconds: 3
max_retries: 2
idle_timeout_seconds: 60
shutdown_grace_seconds: 10

backends:
  - host: "127.0.0.1"
    port: 43001
  - host: "127.0.0.1"
    port: 43002
  - host: "127.0.0.1"
    port: 43003
```

`config.py` parses this into frozen dataclasses and validates ports, timeouts, `max_cap`, and duplicate backends. Errors are `ConfigError` with the offending field named, so a bad value points at itself.

Frozen config plus mutable backends is a useful split: configuration cannot drift at runtime, while connection state clearly can.

## The `Backend` object

Each backend holds its identity and its live counters:

```python
@dataclass
class Backend:
    host: str
    port: int
    healthy: bool = True
    active_connections: int = 0
    total_connections: int = 0
    consecutive_failures: int = 0

    def available(self, max_cap: int) -> bool:
        return self.healthy and self.active_connections < max_cap
```

Two counters do different jobs. `active_connections` enforces the cap and drives least-connections selection. `total_connections` is cumulative and only used for stats.

Health transitions are event-like. `record_failure()` returns `True` only when the backend crosses from healthy to unhealthy, so the caller can log the change once instead of every failed check. `record_success()` does the same for recovery.

## Selection: round robin with a least-connections fallback

Strategies expose one method:

```python
def select(self, backends, max_cap, exclude=None) -> Backend | None
```

`RoundRobin` keeps an index and skips unavailable backends. `HybridCapped`, the algorithm actually used, first asks round robin for a candidate. If that backend is healthy and under its cap, it wins. Otherwise the balancer falls back to the healthy, under-cap backend with the fewest active connections. If none qualify, `select` returns `None`.

```python
candidate = self._round_robin.next_candidate(backends, exclude)
if candidate is not None and candidate.available(max_cap):
    return candidate

available = [
    backend for backend in backends
    if backend.available(max_cap) and (exclude is None or backend.name not in exclude)
]
return min(available, key=lambda b: b.active_connections) if available else None
```

This gives predictable spread under normal load and graceful behavior when one backend fills up. The `exclude` set exists for retries: after a failed connect, the next attempt must land somewhere else.

### The concurrency detail that makes `max_cap` correct

`asyncio` is single-threaded, but that alone does not make selection safe. The invariant is that selecting and reserving must happen in one synchronous step:

```python
backend = select_backend(tried)
# no await here
backend.reserve()
```

Because there is no suspension point between "check `active_connections < max_cap`" and "increment it", no other task can slip in between. If an `await` were introduced between those lines, two clients could both observe the last slot as free and overfill the backend. The code is deliberately arranged to avoid that.

When every backend is at the cap, the client is closed and the rejection is logged. Queueing was left out on purpose: a proxy does not know how long the client is willing to wait, and a queue would add backpressure policy without adding much learning value.

## The connection lifecycle

`handle_client` is the heart of the proxy:

1. Select a backend and reserve a slot.
2. Open a TCP connection with a timeout.
3. Pump bytes in both directions.
4. Always release the slot and close both sockets.

The retry loop uses `1 + max_retries` attempts and adds each failed backend to `tried`, so a retry never targets the same backend twice:

```python
for _ in range(1 + config.max_retries):
    backend = select_backend(tried)
    if backend is None:
        return

    backend.reserve()
    reserved = True
    try:
        backend_reader, backend_writer = await asyncio.wait_for(
            asyncio.open_connection(backend.host, backend.port),
            timeout=config.connect_timeout_seconds,
        )
    except (OSError, asyncio.TimeoutError) as exc:
        backend.release()
        reserved = False
        tried.add(backend.name)
        backend.record_failure(config.health_check.failure_threshold)
        backend = None
        continue
    break
```

A failed connect is treated as a passive health signal and feeds the same `consecutive_failures` counter as the health checker. That means a dead backend can be marked down in milliseconds instead of waiting for the next active check.

Retries cover only the connection phase. Once bytes are flowing, replaying them is not safe for an L4 proxy, so a mid-stream failure simply closes the connection.

### Byte pumping and half-close

Two pumps run concurrently with `asyncio.gather`, one per direction:

```python
while True:
    data = await reader.read(CHUNK_SIZE)
    if not data:
        break
    writer.write(data)
    await writer.drain()
```

`drain()` is what applies backpressure: a slow reader stops the writer from buffering without bound. When a direction hits EOF, the pump calls `write_eof()` on the other socket, producing a half-close. The handler finishes after both pumps complete, so the connection is only torn down when both sides are done.

The `finally` block is non-negotiable:

```python
finally:
    _close(backend_writer)
    _close(client_writer)
    if reserved and backend is not None:
        backend.release()
```

If `release()` were skipped on any path, `active_connections` would drift upward and the backend would eventually look permanently full.

## Health checking

`HealthChecker` opens a TCP connection to each backend, closes it immediately, and records the result. Checks run concurrently for all backends.

A backend is marked down after `failure_threshold` consecutive failures, and marked up on the first success after being down. Only transitions are logged at `INFO`/`WARNING`; routine failed checks are `DEBUG`, which keeps the log readable during an outage.

One important detail: a check round runs before the server starts accepting traffic. This avoids the awkward window where the balancer is live but has not yet verified its backends:

```python
health_checker = HealthChecker(backends, config.health_check)
await health_checker.check_once()
server = await asyncio.start_server(...)
```

The background task then sleeps for the interval before its first round. Sleeping first is intentional: an immediate extra round would double-count the pre-flight check's failures and mark a backend down sooner than the threshold implies. That bug existed briefly during development and produced exactly that symptom.

## Stats and observability

A small task logs one line per backend every ten seconds:

```
stats 127.0.0.1:43001 state=up active=0 total=27 failures=0
```

Active counts are the useful signal for cap behavior; totals show how work was distributed. Because the handler releases in `finally`, these numbers return to zero when traffic stops, which is a quick way to detect a counter leak.

## Graceful shutdown

`main` installs handlers for `SIGINT` and `SIGTERM` and waits on an `asyncio.Event`:

```python
loop = asyncio.get_running_loop()
stop_event = asyncio.Event()
for sig in (signal.SIGINT, signal.SIGTERM):
    loop.add_signal_handler(sig, stop_event.set)
```

On shutdown it:

1. Closes the listening server so no new clients are accepted.
2. Cancels the health and stats tasks.
3. Waits up to `shutdown_grace_seconds` for in-flight connections to finish.
4. Cancels whatever remains.
5. Exits.

Connection handlers are tracked in a set, with a done callback removing finished tasks. There is also a guard in the accept callback that closes any connection that races with shutdown, after the stop event is set but before the listener is fully closed.

`loop.add_signal_handler` is not available on Windows, so the call is wrapped and degrades to a warning. The `KeyboardInterrupt` fallback still lets the process exit.

## Additional changes for robustness

After the core behavior was working, a handful of additions made the proxy harder to knock over. None of them changed the architecture.

### Idle connections time out

A stalled client with an open socket would otherwise hold a backend slot forever. With `max_cap` in place, a few dead connections are enough to lock a backend out of rotation entirely.

`idle_timeout_seconds` closes a connection after that many seconds with no bytes moving in either direction. Both pumps share an `IdleTimer`; each successful read calls `touch()`, and a third task closes both sockets when the deadline passes.

```yaml
idle_timeout_seconds: 60
```

The timer is a small class rather than a `wait_for` on a single read, because activity in either direction has to reset it:

```python
class IdleTimer:
    def touch(self) -> None:
        self._last = asyncio.get_running_loop().time()

    async def wait(self) -> None:
        interval = max(min(self.timeout / 4, 1.0), 0.01)
        while True:
            await asyncio.sleep(interval)
            if asyncio.get_running_loop().time() - self._last >= self.timeout:
                return
```

### All backends unhealthy rejects loudly

The policy is decided up front: if no backend is healthy, reject immediately. There is no fail-open mode that tries unhealthy backends anyway.

The rejection path distinguishes the two causes. Capacity exhaustion is a `WARNING`; a fully unhealthy pool is an `ERROR`, because that usually means something downstream is badly broken.

```python
if not any(candidate.healthy for candidate in backends):
    logger.error("%s all backends are unhealthy, rejecting client %s", prefix, peer)
else:
    logger.warning("%s no backend with capacity, rejecting client %s", prefix, peer)
```

### Existing connections survive a backend going down

Marking a backend unhealthy only changes selection. Active connections to it are not touched: they finish if the backend keeps serving, and fail on their own if it does not. This is deliberate. Killing healthy in-flight TCP streams because a health check failed would turn a partial outage into a larger one, and an L4 proxy cannot replay those bytes anyway.

### Pump errors are normal

`ConnectionResetError`, `BrokenPipeError`, and `asyncio.IncompleteReadError` happen during ordinary operation: clients disappear, backends restart, and mobile networks drop. The pumps catch them at `DEBUG`, close both sides, and let the handler finish cleanly.

When one pump hits an error, the other one would otherwise keep waiting forever. The error path calls a shared `close_all()` so the sibling unblocks, and an unexpected exception cancels both pump tasks before propagating.

```python
except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, ConnectionError, OSError) as exc:
    logger.debug("[conn %d] pump closed: %s", conn_id, exc)
    close_all()
```

A clean EOF is different: it triggers `write_eof()` and leaves the other direction running, preserving the TCP half-close behavior.

### Connection IDs make concurrent logs readable

Every accepted client gets a short sequential ID, and every proxy log line carries it:

```
[conn 17] proxying ('127.0.0.1', 54012) <-> 127.0.0.1:43002 (active=1)
[conn 17] idle for 60.0s, closing connection
```

Without IDs, interleaved connect failures and rejections from multiple clients are almost impossible to attribute. The IDs are also used in task names, which makes `asyncio` debugging output more useful.

### Socket options

Both proxied sockets get `TCP_NODELAY`, which disables Nagle's algorithm for lower latency. The listening socket uses `reuse_address=True` so a restart does not fail with "address already in use" while old sockets sit in `TIME_WAIT`.

```python
sock = writer.get_extra_info("socket")
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
```

The listener is created with `reuse_address=True`.

### File descriptor limits

Each proxied connection consumes two sockets, so the ceiling is roughly half the file descriptor limit. The process logs its soft and hard `RLIMIT_NOFILE` at startup and warns when the soft limit is below 4096. This turns a mysterious failure under load into a visible configuration problem.

```python
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
logger.info("file descriptor limit: soft=%s hard=%s", soft, hard)
```

### Test tools

Two small scripts make manual testing repeatable:

- `tools/slow_echo.py` starts one or more TCP backends that sleep for a configurable delay before echoing. That is enough to hold connections open and exercise `max_cap` and graceful shutdown.
- `tools/loadgen.py` sends a configurable number of HTTP requests with bounded concurrency and tallies the response bodies, which makes distribution visible without external tooling.

External tools work too. `hey` or `ab` can drive load against the listener:

```bash
hey -n 1000 -c 50 http://127.0.0.1:8080/
ab -n 1000 -c 50 http://127.0.0.1:8080/
```

Keep concurrency at or below `backends * max_cap` when measuring distribution; above that, rejected connections are expected and will show up as resets.

### Recovery fairness

A backend coming back from a health failure should rejoin rotation, not absorb a burst. The round-robin index advances on every selection attempt, including ones that skip an unhealthy backend. When the backend recovers, the index is wherever it would have been, so the recovered backend gets its normal turn.

That is an easy property to break by only advancing the index on successful selections. It was verified both in isolation and against the running proxy: with one backend down, requests alternated across the remaining two; after recovery, the sequence returned to an even rotation with no burst.

## Verifying it

The build was verified in layers rather than all at once:

- Three nginx containers on ports 43001–43003, each serving a distinct body.
- Six sequential requests produced `backend-1`, `backend-2`, `backend-3`, repeating.
- Stopping one container caused a `down` transition after two failed checks; traffic stopped selecting it. Restarting it produced an `up` transition and it rejoined rotation without a burst.
- With `max_cap` set to 1 and slow backends holding connections, the first three clients occupied all three slots and a fourth client was accepted and immediately closed.
- Pointing one backend at a closed port caused the first connect attempt to fail, the backend to be marked down passively, and the client to succeed through another backend.
- With `idle_timeout_seconds: 1.5`, a client that connected and sent nothing was closed after about 1.5 seconds, and the slot was released.
- With every backend down, a client was rejected immediately and the log recorded `all backends are unhealthy` at `ERROR`.
- A client that reset its connection mid-request produced no traceback and did not affect subsequent requests.
- Sending `SIGTERM` while a connection was open refused new connections, let the open connection finish, and exited within the grace period. Holding a connection past the grace period caused a clean cancellation instead.
- The startup log reported the file descriptor limit, and `tools/loadgen.py` showed an even spread across three backends at a concurrency below total capacity.

## Tradeoffs

- **Reject at capacity instead of queueing.** Simpler, and avoids inventing a queueing policy. Queueing is a natural next step.
- **TCP health checks.** They answer "is the port open", not "is the application healthy". That matches an L4 proxy; HTTP checks would be an upgrade.
- **Active plus passive signals.** More code than active-only, but much faster detection of a backend that dies between check intervals.
- **Streams API.** `start_server` and `open_connection` are easy to read. Protocols would be faster, but the verbosity would obscure the concurrency lessons.
- **Config read once.** No hot reload. Restarting is an acceptable operational cost at this scale.

## Running it

Start the backends, then the balancer:

```bash
docker run -d --name lb-nginx-1 -p 43001:80 nginx:alpine
docker run -d --name lb-nginx-2 -p 43002:80 nginx:alpine
docker run -d --name lb-nginx-3 -p 43003:80 nginx:alpine

python3 main.py
curl http://127.0.0.1:8080/
```

Send `SIGTERM` to the process and watch it drain. Stop a container and watch the health checker react. The project is intentionally small enough that every moving part can be held in your head at once, which is the point.

For cap and shutdown testing, run the slow echo backends and a load generator:

```bash
python3 tools/slow_echo.py --ports 43001 43002 43003 --delay 5
python3 tools/loadgen.py --requests 200 --concurrency 3
```

The slow echo tool holds connections open long enough to observe slot usage, idle timeouts, and draining on shutdown. The load generator prints how many responses came from each backend.

## What is out of scope

Weighted and dynamic algorithms beyond least connections, config hot reload, TLS, HTTP-aware routing, and metrics endpoints were all left out. The strategy interface is where the first two would land; the rest would require making the proxy speak more than raw TCP.
