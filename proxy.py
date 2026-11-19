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
    select_backend: Callable[[], Backend | None],
    config: Config,
) -> None:
    peer = client_writer.get_extra_info("peername")
    backend = select_backend()
    if backend is None:
        logger.warning("no backend available, closing client %s", peer)
        _close(client_writer)
        return

    backend.reserve()
    backend_writer: asyncio.StreamWriter | None = None
    try:
        try:
            backend_reader, backend_writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=config.connect_timeout_seconds,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            logger.warning("cannot connect to %s: %s", backend.name, exc)
            _close(client_writer)
            return

        logger.info("proxying %s <-> %s", peer, backend.name)
        await asyncio.gather(
            _pump(client_reader, backend_writer),
            _pump(backend_reader, client_writer),
        )
    finally:
        _close(backend_writer)
        _close(client_writer)
        backend.release()
