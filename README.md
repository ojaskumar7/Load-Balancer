# Python TCP Load Balancer

A small Layer 4 load balancer built with `asyncio`. It forwards raw TCP bytes to a pool of backends, spreads connections with a hybrid round-robin / least-connections strategy, enforces a per-backend connection cap, performs active health checks, retries failed connects on a different backend, and shuts down gracefully.

This is a learning project: the implementation favors clarity over throughput. It is not an HTTP-aware proxy and does not parse traffic.

## Features

- TCP proxying with bidirectional byte pumping and backpressure
- Round-robin selection with least-connections fallback
- Per-backend `max_cap`; clients are rejected and logged when every backend is full
- Idle connection timeout so stalled clients cannot hold backend slots forever
- Active TCP health checks with configurable interval, timeout, and failure threshold
- Passive failure detection from failed client connects
- Connect retries against untried backends
- Active and cumulative connection counters with periodic stats logging
- Per-connection IDs in logs for concurrent traffic
- `TCP_NODELAY` on proxied sockets and `reuse_address` on the listener
- Startup logging of file descriptor limits
- Graceful shutdown on `SIGINT` / `SIGTERM`
- Strict config validation at startup

## Requirements

- Python 3.11+
- `pyyaml`
- Docker (optional, for the sample nginx backends)

```bash
pip install pyyaml
```

## Quick start

Start three backends with distinct responses so the distribution is visible:

```bash
mkdir -p /tmp/lb-backends/{1,2,3}
for i in 1 2 3; do
  printf 'backend-%s\n' "$i" > "/tmp/lb-backends/$i/index.html"
  docker run -d --name "lb-nginx-$i" \
    -p "4300$i:80" \
    -v "/tmp/lb-backends/$i:/usr/share/nginx/html:ro" \
    nginx:alpine
done
```

Run the balancer from the repository root, because it loads `config.yaml` from the working directory:

```bash
python3 main.py
```

Send requests:

```bash
for i in 1 2 3 4 5 6; do curl -s http://127.0.0.1:8080/; done
```

Expected output:

```
backend-1
backend-2
backend-3
backend-1
backend-2
backend-3
```

Stop gracefully with `Ctrl+C` or:

```bash
kill -TERM <pid>
```

## Configuration

The balancer reads `config.yaml` once at startup. Invalid values fail fast with a message naming the offending field.

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

| Field | Meaning | Default |
| --- | --- | --- |
| `listen.host` / `listen.port` | Address the balancer binds to | required |
| `max_cap` | Maximum in-flight connections per backend | required |
| `health_check.interval_seconds` | Seconds between health check rounds | `5` |
| `health_check.timeout_seconds` | TCP connect timeout for a health check | `2` |
| `health_check.failure_threshold` | Consecutive failures before marking a backend down | `2` |
| `connect_timeout_seconds` | Timeout for connecting to a backend | `3` |
| `max_retries` | Additional attempts against other backends | `2` |
| `idle_timeout_seconds` | Close a connection after this long with no traffic | `60` |
| `shutdown_grace_seconds` | Time to drain in-flight connections on shutdown | `10` |
| `backends` | Non-empty list of `{host, port}` entries | required |

Ports must be in `1..65535`, `max_cap` must be at least `1`, and duplicate backends are rejected.

## Test tools

`tools/slow_echo.py` starts slow TCP backends that sleep before echoing. Use it to hold connections open for `max_cap`, idle-timeout, and shutdown tests:

```bash
python3 tools/slow_echo.py --ports 43001 43002 43003 --delay 5
```

`tools/loadgen.py` sends HTTP requests with bounded concurrency and prints how many responses came from each backend:

```bash
python3 tools/loadgen.py --requests 200 --concurrency 3
```

External load tools work as well:

```bash
hey -n 1000 -c 50 http://127.0.0.1:8080/
ab -n 1000 -c 50 http://127.0.0.1:8080/
```

Keep concurrency at or below `backends * max_cap` when checking distribution; above that, rejections are expected.

## How it works

```
client ──► start_server ──► handle_client
                              │
                              ├─ strategy.select(backends, max_cap)
                              ├─ backend.reserve()
                              ├─ open_connection(backend)
                              ├─ pump client ◄──► backend
                              └─ finally: close sockets, backend.release()

health checker ──► TCP connect to each backend every interval
stats task     ──► logs per-backend counters every 10 seconds
```

A few details that matter:

- **Selection and reservation are atomic.** There is no `await` between choosing a backend and incrementing `active_connections`. Since `asyncio` runs on a single thread, this prevents two clients from claiming the last slot.
- **Capacity is enforced by rejection.** If every backend is healthy but at `max_cap`, the client connection is closed and the event is logged. There is no queue. If every backend is unhealthy, rejection is logged at `ERROR`.
- **Idle connections are closed.** Both pumps share a timer; any byte in either direction resets it. A stalled client is closed after `idle_timeout_seconds` and its slot is released.
- **Retries cover only the connect phase.** Once bytes have moved, an L4 proxy cannot safely replay them, so a mid-stream failure closes the connection.
- **Health checks run once before the listener starts.** This avoids routing traffic to a dead backend during the first interval. Background rounds then sleep first, so startup failures are not double-counted.
- **Active and passive signals share one failure counter.** A failed client connect can mark a backend down immediately; only the active health checker marks it back up.
- **Marking a backend down only affects new traffic.** Existing connections finish or fail on their own.
- **Pump errors are expected.** Connection resets, broken pipes, and incomplete reads are logged at `DEBUG`; one pump failing closes both sockets so its sibling cannot hang.
- **Cleanup always runs.** A `finally` block closes both sockets and releases the reserved slot, keeping counters accurate.

## Project layout

| Path | Purpose |
| --- | --- |
| `main.py` | Startup, background tasks, signal handling, graceful shutdown |
| `config.py` | YAML loading and validation into typed dataclasses |
| `backend.py` | `Backend` state object and counters |
| `strategies.py` | `RoundRobin` and `HybridCapped` selection |
| `health.py` | Active TCP health checker |
| `proxy.py` | Per-client handler, byte pumps, and idle timeout |
| `config.yaml` | Sample configuration |
| `tools/slow_echo.py` | Slow echo backend for cap and shutdown testing |
| `tools/loadgen.py` | Simple HTTP load generator for distribution checks |
| `project.md` | Original design and build plan |
| `docs/technical-blog.md` | Detailed technical walkthrough |

## Verification

The project has no automated test suite. Behavior was verified manually in layers:

- Round robin distributes sequential requests evenly across three backends.
- Stopping a backend causes a down transition after the failure threshold; traffic skips it, and it recovers when restarted without a traffic burst.
- With `max_cap: 1` and slow backends holding connections, the first three clients occupy the pool and a fourth is rejected.
- A backend pointed at a closed port fails its connect, is marked down passively, and the client succeeds through another backend.
- A client that connects and sends nothing is closed after `idle_timeout_seconds`.
- With every backend down, clients are rejected immediately with an `ERROR` log line.
- Connection resets from clients do not produce tracebacks or stall the pumps.
- `SIGTERM` closes the listener, lets open connections finish within the grace period, then cancels anything still running.

## Limitations and next steps

- No queueing when all backends are at capacity
- TCP-only health checks (port reachability, not application health)
- No TLS, HTTP-aware routing, metrics endpoint, or config hot reload
- Algorithms beyond round robin and least connections are not implemented; the `Strategy` interface is the extension point

See [docs/technical-blog.md](docs/technical-blog.md) for the full design discussion and tradeoffs.
