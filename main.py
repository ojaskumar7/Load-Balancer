from __future__ import annotations

import asyncio
import logging

from backend import Backend
from config import load_config
from proxy import handle_client

logger = logging.getLogger(__name__)


async def main() -> None:
    config = load_config("config.yaml")
    backends = [Backend(b.host, b.port) for b in config.backends]

    def select_backend() -> Backend | None:
        return backends[0]

    server = await asyncio.start_server(
        lambda reader, writer: handle_client(reader, writer, select_backend, config),
        config.listen.host,
        config.listen.port,
    )
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    logger.info("listening on %s", addresses)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
