from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Backend:
    host: str
    port: int
    healthy: bool = True
    active_connections: int = 0
    total_connections: int = 0
    consecutive_failures: int = 0

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"

    def available(self, max_cap: int) -> bool:
        return self.healthy and self.active_connections < max_cap

    def reserve(self) -> None:
        self.active_connections += 1
        self.total_connections += 1

    def release(self) -> None:
        if self.active_connections > 0:
            self.active_connections -= 1

    def record_success(self) -> bool:
        was_healthy = self.healthy
        self.consecutive_failures = 0
        self.healthy = True
        return not was_healthy

    def record_failure(self, failure_threshold: int) -> bool:
        self.consecutive_failures += 1
        was_healthy = self.healthy
        if self.consecutive_failures >= failure_threshold:
            self.healthy = False
        return was_healthy and not self.healthy

    def stats(self) -> str:
        state = "up" if self.healthy else "down"
        return (
            f"{self.name} state={state} active={self.active_connections} "
            f"total={self.total_connections} failures={self.consecutive_failures}"
        )
