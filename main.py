from __future__ import annotations

import asyncio
import itertools
import logging
import signal

from backend import Backend
from config import load_config
from health import HealthChecker
from proxy import handle_client
from strategies import HybridCapped

logger = logging.getLogger(__name__)

STATS_INTERVAL_SECONDS = 10.0


def _log_fd_limit() -> None:
    try:
        import resource
    except ImportError:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (OSError, ValueError):
        return
    logger.info("file descriptor limit: soft=%s hard=%s", soft, hard)
    if soft != resource.RLIM_INFINITY and soft < 4096:
        logger.warning(
            "low file descriptor limit (%s); each proxied connection needs 2 sockets",
            soft,
        )


async def log_stats(backends: list[Backend]) -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL_SECONDS)
        for backend in backends:
            logger.info("stats %s", backend.stats())


async def _cancel_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass


async def main() -> None:
    _log_fd_limit()
    config = load_config("config.yaml")
    backends = [Backend(b.host, b.port) for b in config.backends]
    strategy = HybridCapped()

    def select_backend(exclude: set[str] | None = None) -> Backend | None:
        return strategy.select(backends, config.max_cap, exclude)

    health_checker = HealthChecker(backends, config.health_check)
    await health_checker.check_once()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            logger.warning("signal handlers unavailable for %s", sig.name)

    connection_tasks: set[asyncio.Task] = set()
    client_ids = itertools.count(1)

    def on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if stop_event.is_set():
            writer.close()
            return
        conn_id = next(client_ids)
        task = asyncio.create_task(
            handle_client(reader, writer, conn_id, select_backend, config, backends),
            name=f"conn-{conn_id}",
        )
        connection_tasks.add(task)
        task.add_done_callback(connection_tasks.discard)

    server = await asyncio.start_server(
        on_client,
        config.listen.host,
        config.listen.port,
        reuse_address=True,
    )
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    logger.info("listening on %s", addresses)

    health_task = asyncio.create_task(health_checker.run(), name="health-checker")
    stats_task = asyncio.create_task(log_stats(backends), name="stats")

    try:
        await stop_event.wait()
        logger.info("shutdown requested, no longer accepting connections")
    finally:
        server.close()
        await _cancel_tasks([health_task, stats_task])

        if connection_tasks:
            logger.info(
                "waiting up to %ss for %d in-flight connection(s)",
                config.shutdown_grace_seconds,
                len(connection_tasks),
            )
            _, pending = await asyncio.wait(
                set(connection_tasks),
                timeout=config.shutdown_grace_seconds,
            )
            if pending:
                logger.warning("cancelling %d connection(s) after grace period", len(pending))
                await _cancel_tasks(list(pending))

        logger.info("shutdown complete")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
