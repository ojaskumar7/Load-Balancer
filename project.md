# Python Load Balancer: Level 0 + Level 1 Plan

## Assumptions
- **Algorithm switching:** Round robin is the default. If the backend round robin would pick is at `max_cap`, skip it and choose the healthy backend with the fewest active connections (least connections). If every backend is at cap, reject the client connection (close it and log it) rather than queue it.
- Python 3.11+, `asyncio`, `pyyaml`, basic `logging`, no tests or CLI.
- Three nginx containers on ports 43001, 43002 and 43003.
- Health checks are TCP connect, every 5 seconds, marked down after 2 consecutive failures.
- Config is read once at startup.

## Project layout
- `config.py`: loads and validates the YAML into typed config objects.
- `backend.py`: the `Backend` state object.
- `strategies.py`: the algorithm interface and implementations.
- `health.py`: the background health checker.
- `proxy.py`: the connection handler and byte-pumping logic.
- `main.py`: startup, signal handling and graceful shutdown.
- `config.yaml`: the sample config.

## Config (`config.yaml`)
Fields:
- `listen`: `host`, `port`.
- `max_cap`: the maximum in-flight connections per backend.
- `health_check`: `interval_seconds` (5), `timeout_seconds` (2), `failure_threshold` (2).
- `connect_timeout_seconds` and `max_retries`: used by Level 1 failure handling.
- `shutdown_grace_seconds`: how long to wait for in-flight connections on shutdown.
- `backends`: a list of `{host, port}`.

Validate at load time: ports in range, at least one backend, `max_cap >= 1`, no duplicate backends. Fail fast with a clear error.

## Module design

### 1. `Backend` (shared state)
Holds `host`, `port`, `healthy` (bool), `active_connections`, `total_connections`, `consecutive_failures`. Add a derived property such as `available(max_cap)`, meaning healthy and under cap. Every other module works through this object.

### 2. Strategy interface (`strategies.py`)
A small abstraction with one job: given the backends, pick one, or return none. Two implementations:
- **RoundRobin:** keeps an index and advances it past unavailable backends.
- **Hybrid/Capped (the one you'll actually run):** asks round robin first; if the pick is at cap or unhealthy, falls back to least connections among available backends.

Keeping this behind an interface lets you add weighted round robin, IP hash, random and least response time later without touching the proxy.

**Important concurrency detail:** `asyncio` is single-threaded, so there is no race between "pick a backend" and "increment its counter" as long as there is no `await` between them. Do the selection and the increment in one synchronous step. That is what makes `max_cap` enforcement correct.

### 3. Health checker (`health.py`)
- A background task loops forever: every interval, it attempts a TCP connect to each backend (concurrently, with a timeout) and closes the socket immediately.
- On success: reset `consecutive_failures` and mark the backend healthy.
- On failure: increment `consecutive_failures`; at the threshold, mark it unhealthy and log the transition.
- Log only **state changes** (up to down, down to up), not every check.
- Run one check round **before** the proxy starts accepting traffic, so you don't route to a dead backend in the first 5 seconds.

### 4. Proxy handler (`proxy.py`)
Per client connection:
1. Ask the strategy for a backend. If none is available, log it and close the client.
2. Reserve the slot (increment `active_connections` and `total_connections`) immediately.
3. Open a connection to the backend with a timeout.
4. Run two pump tasks, client to backend and backend to client, each reading chunks and writing them out, with `drain()` after writes so a slow reader applies backpressure.
5. When one direction hits EOF, half-close the other side, and finish when both are done.
6. In a `finally` block, close both sockets and decrement `active_connections`. This must always run, or the counters will drift and backends will wrongly look full.

### 5. Failure handling (Level 1)
- If connecting to the chosen backend fails or times out:
  - Count it as a failure on that backend (a "passive" health signal feeding the same `consecutive_failures` counter).
  - Release the reserved slot.
  - Retry with a **different** backend, tracking which ones were already tried, up to `max_retries`.
  - If all attempts fail, close the client connection and log it.
- Retry **only the connect phase.** Once bytes have started flowing, a TCP-level proxy can't safely replay them, so mid-stream failures just close the connection.
- Recovery to healthy status comes only from the active health checker, which avoids flapping.

### 6. Connection counting (Level 1)
- `active_connections` is used for cap enforcement and least-connections.
- `total_connections` is cumulative and used for stats.
- A small periodic task logs a stats line every ~10 seconds: per-backend active, total, and health.

### 7. Graceful shutdown (Level 1)
1. Handle SIGINT and SIGTERM via the event loop's signal handler support (not available on Windows).
2. Stop accepting new connections (close the listening server).
3. Stop the health checker and stats tasks.
4. Wait for in-flight connection tasks to finish, up to `shutdown_grace_seconds`. This means tracking handler tasks in a set.
5. After the grace period, cancel whatever is left, then exit cleanly.

## Build order (each step is runnable and verifiable)
1. **Backends:** start three nginx containers on 43001 to 43003.
2. **Config loading:** parse and validate the YAML; print the result.
3. **Dumb TCP proxy:** one hardcoded backend, pump bytes both ways. Verify with `curl`.
4. **Round robin across all backends:** hit it repeatedly and check the distribution in each container's logs.
5. **Health checker:** stop one container and confirm it's marked down and skipped, then restart it and confirm it recovers.
6. **Connection counters and the `finally` cleanup:** verify counts return to zero after traffic stops.
7. **`max_cap` and the hybrid fallback:** set `max_cap` low (1 or 2) and open several long-lived connections. nginx responds too fast to hold slots open, so use `nc` or a small slow Python server as a backend for this test.
8. **Retries and passive failure marking:** point one backend at a closed port and confirm the client still succeeds via another backend.
9. **Graceful shutdown:** hold a connection open, send SIGTERM, and confirm no new connections are accepted but the open one finishes.

## Tradeoffs and decisions
- **All backends at cap:** reject-and-close for simplicity. The alternative is queueing, which is more complex and shifts the problem to client wait times. A good extension later.
- **TCP vs HTTP health checks:** TCP only tells you the port is open, not that the app is healthy. It fits an L4 balancer and keeps this simple; HTTP checks are a natural upgrade.
- **Active + passive health signals:** slightly more code than active-only, but failure detection is much faster than waiting up to 5 seconds.
- **Streams API vs lower-level protocols:** use `asyncio` streams (`start_server` and `open_connection`). Protocols are faster but much more verbose, which defeats the learning goal.

## Out of scope for now
Weighted and dynamic algorithms beyond least connections, config hot-reload, TLS, HTTP-aware (L7) routing, metrics endpoints.
