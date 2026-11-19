from __future__ import annotations

from abc import ABC, abstractmethod

from backend import Backend


class Strategy(ABC):
    @abstractmethod
    def select(self, backends: list[Backend], max_cap: int) -> Backend | None:
        raise NotImplementedError


class RoundRobin(Strategy):
    def __init__(self) -> None:
        self._index = 0

    def next_candidate(self, backends: list[Backend]) -> Backend | None:
        if not backends:
            return None
        backend = backends[self._index % len(backends)]
        self._index = (self._index + 1) % len(backends)
        return backend

    def select(self, backends: list[Backend], max_cap: int) -> Backend | None:
        for _ in range(len(backends)):
            candidate = self.next_candidate(backends)
            if candidate is not None and candidate.available(max_cap):
                return candidate
        return None


class HybridCapped(Strategy):
    def __init__(self) -> None:
        self._round_robin = RoundRobin()

    def select(self, backends: list[Backend], max_cap: int) -> Backend | None:
        candidate = self._round_robin.next_candidate(backends)
        if candidate is not None and candidate.available(max_cap):
            return candidate

        available = [backend for backend in backends if backend.available(max_cap)]
        if not available:
            return None
        return min(available, key=lambda backend: backend.active_connections)
