from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import Callable

from backend import Backend
from config import Config

logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024


class IdleTimer:
    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self._last = asyncio.get_running_loop().time()

    def touch(self) -> None:
        self._last = asyncio.get_running_loop().time()

    async def wait(self) -> None:
        interval = max(min(self.timeout / 4, 1.0), 0.01)
        while True:
            await asyncio.sleep(interval)
            if asyncio.get_running_loop().time() - self._last >= self.timeout:
                return


def _set_tcp_nodelay(writer: asyncio.StreamWriter) -> None:
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass


async def _pump(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    idle: IdleTimer,
    conn_id: int,
    close_all: Callable[[], None],
) -> None:
    try:
        while True:
            data = await reader.read(CHUNK_SIZE)
            if not data:
                break
            idle.touch()
            writer.write(data)
            await writer.drain()
    except (
        asyncio.IncompleteReadError,
        ConnectionResetError,
        BrokenPipeError,
        ConnectionError,
        OSError,
    ) as exc:
        logger.debug("[conn %d] pump closed: %s", conn_id, exc)
        close_all()
    finally:
        try:
            if writer.can_write_eof():
                writer.write_eof()
        except (ConnectionError, OSError):
            pass


async def _idle_guard(
    idle: IdleTimer,
    conn_id: int,
    close_all: Callable[[], None],
) -> None:
    await idle.wait()
    logger.warning("[conn %d] idle for %.1fs, closing connection", conn_id, idle.timeout)
    close_all()


def _close(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    try:
        writer.close()
    except (ConnectionError, OSError):
        pass


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    conn_id: int,
    select_backend: Callable[[set[str]], Backend | None],
    config: Config,
    backends: list[Backend],
) -> None:
    peer = client_writer.get_extra_info("peername")
    prefix = f"[conn {conn_id}]"
    tried: set[str] = set()
    backend: Backend | None = None
    backend_reader: asyncio.StreamReader | None = None
    backend_writer: asyncio.StreamWriter | None = None
    reserved = False

    _set_tcp_nodelay(client_writer)

    try:
        for _ in range(1 + config.max_retries):
            backend = select_backend(tried)
            if backend is None:
                if not any(candidate.healthy for candidate in backends):
                    logger.error(
                        "%s all backends are unhealthy, rejecting client %s", prefix, peer
                    )
                else:
                    logger.warning(
                        "%s no backend with capacity, rejecting client %s (tried=%s)",
                        prefix,
                        peer,
                        sorted(tried) or "none",
                    )
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
                backend_writer = None
                tried.add(backend.name)
                if backend.record_failure(config.health_check.failure_threshold):
                    logger.warning(
                        "%s backend %s marked down: %s", prefix, backend.name, exc
                    )
                else:
                    logger.warning("%s connect to %s failed: %s", prefix, backend.name, exc)
                backend = None
                continue
            break

        if backend is None:
            logger.warning("%s all connect attempts failed for client %s", prefix, peer)
            return

        _set_tcp_nodelay(backend_writer)

        idle = IdleTimer(config.idle_timeout_seconds)

        def close_all() -> None:
            _close(backend_writer)
            _close(client_writer)

        logger.info(
            "%s proxying %s <-> %s (active=%d)",
            prefix,
            peer,
            backend.name,
            backend.active_connections,
        )

        pumps = [
            asyncio.create_task(
                _pump(client_reader, backend_writer, idle, conn_id, close_all),
                name=f"conn-{conn_id}-c2b",
            ),
            asyncio.create_task(
                _pump(backend_reader, client_writer, idle, conn_id, close_all),
                name=f"conn-{conn_id}-b2c",
            ),
        ]
        idle_task = asyncio.create_task(
            _idle_guard(idle, conn_id, close_all), name=f"conn-{conn_id}-idle"
        )
        try:
            await asyncio.gather(*pumps)
        except BaseException:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            raise
        finally:
            idle_task.cancel()
            await asyncio.gather(idle_task, return_exceptions=True)
    finally:
        _close(backend_writer)
        _close(client_writer)
        if reserved and backend is not None:
            backend.release()
