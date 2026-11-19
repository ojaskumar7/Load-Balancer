from __future__ import annotations

import asyncio
import logging

from backend import Backend
from config import load_config
from health import HealthChecker
from proxy import handle_client
from strategies import HybridCapped

logger = logging.getLogger(__name__)

STATS_INTERVAL_SECONDS = 10.0


async def log_stats(backends: list[Backend]) -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL_SECONDS)
        for backend in backends:
            logger.info("stats %s", backend.stats())


async def main() -> None:
    config = load_config("config.yaml")
    backends = [Backend(b.host, b.port) for b in config.backends]
    strategy = HybridCapped()

    def select_backend() -> Backend | None:
        return strategy.select(backends, config.max_cap)

    health_checker = HealthChecker(backends, config.health_check)
    await health_checker.check_once()

    server = await asyncio.start_server(
        lambda reader, writer: handle_client(reader, writer, select_backend, config),
        config.listen.host,
        config.listen.port,
    )
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    logger.info("listening on %s", addresses)

    health_task = asyncio.create_task(health_checker.run(), name="health-checker")
    stats_task = asyncio.create_task(log_stats(backends), name="stats")
    try:
        async with server:
            await server.serve_forever()
    finally:
        for task in (health_task, stats_task):
            task.cancel()
        for task in (health_task, stats_task):
            try:
                await task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
