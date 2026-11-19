from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from backend import Backend
from config import Config

logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024


async def _pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            data = await reader.read(CHUNK_SIZE)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError) as exc:
        logger.debug("pump stopped: %s", exc)
    finally:
        try:
            if writer.can_write_eof():
                writer.write_eof()
        except (ConnectionError, OSError):
            pass


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
    select_backend: Callable[[set[str]], Backend | None],
    config: Config,
) -> None:
    peer = client_writer.get_extra_info("peername")
    tried: set[str] = set()
    backend: Backend | None = None
    backend_reader: asyncio.StreamReader | None = None
    backend_writer: asyncio.StreamWriter | None = None
    reserved = False

    try:
        for _ in range(1 + config.max_retries):
            backend = select_backend(tried)
            if backend is None:
                logger.warning(
                    "no backend available for client %s (tried=%s)", peer, sorted(tried) or "none"
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
                    logger.warning("backend %s marked down: %s", backend.name, exc)
                else:
                    logger.warning("connect to %s failed: %s", backend.name, exc)
                backend = None
                continue
            break

        if backend is None:
            logger.warning("all connect attempts failed for client %s", peer)
            return

        logger.info(
            "proxying %s <-> %s (active=%d)", peer, backend.name, backend.active_connections
        )
        await asyncio.gather(
            _pump(client_reader, backend_writer),
            _pump(backend_reader, client_writer),
        )
    finally:
        _close(backend_writer)
        _close(client_writer)
        if reserved and backend is not None:
            backend.release()
