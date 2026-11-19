from __future__ import annotations

import asyncio
import logging

from backend import Backend
from config import HealthCheckConfig

logger = logging.getLogger(__name__)


class HealthChecker:
    def __init__(self, backends: list[Backend], config: HealthCheckConfig) -> None:
        self._backends = backends
        self._config = config

    async def _check(self, backend: Backend) -> None:
        writer: asyncio.StreamWriter | None = None
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=self._config.timeout_seconds,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            if backend.record_failure(self._config.failure_threshold):
                logger.warning("backend %s is down: %s", backend.name, exc)
            else:
                logger.debug("health check failed for %s: %s", backend.name, exc)
        else:
            if backend.record_success():
                logger.info("backend %s is up", backend.name)
        finally:
            if writer is not None:
                try:
                    writer.close()
                except (ConnectionError, OSError):
                    pass

    async def check_once(self) -> None:
        await asyncio.gather(*(self._check(backend) for backend in self._backends))

    async def run(self) -> None:
        await self.check_once()
        while True:
            await asyncio.sleep(self._config.interval_seconds)
            await self.check_once()
