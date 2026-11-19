from __future__ import annotations

from abc import ABC, abstractmethod

from backend import Backend


class Strategy(ABC):
    @abstractmethod
    def select(
        self,
        backends: list[Backend],
        max_cap: int,
        exclude: set[str] | None = None,
    ) -> Backend | None:
        raise NotImplementedError


class RoundRobin(Strategy):
    def __init__(self) -> None:
        self._index = 0

    def next_candidate(
        self,
        backends: list[Backend],
        exclude: set[str] | None = None,
    ) -> Backend | None:
        if not backends:
            return None
        for _ in range(len(backends)):
            backend = backends[self._index % len(backends)]
            self._index = (self._index + 1) % len(backends)
            if exclude is None or backend.name not in exclude:
                return backend
        return None

    def select(
        self,
        backends: list[Backend],
        max_cap: int,
        exclude: set[str] | None = None,
    ) -> Backend | None:
        for _ in range(len(backends)):
            candidate = self.next_candidate(backends, exclude)
            if candidate is not None and candidate.available(max_cap):
                return candidate
        return None


class HybridCapped(Strategy):
    def __init__(self) -> None:
        self._round_robin = RoundRobin()

    def select(
        self,
        backends: list[Backend],
        max_cap: int,
        exclude: set[str] | None = None,
    ) -> Backend | None:
        candidate = self._round_robin.next_candidate(backends, exclude)
        if candidate is not None and candidate.available(max_cap):
            return candidate

        available = [
            backend
            for backend in backends
            if backend.available(max_cap) and (exclude is None or backend.name not in exclude)
        ]
        if not available:
            return None
        return min(available, key=lambda backend: backend.active_connections)
