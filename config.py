from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class ListenConfig:
    host: str
    port: int


@dataclass(frozen=True)
class BackendConfig:
    host: str
    port: int

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class HealthCheckConfig:
    interval_seconds: float = 5.0
    timeout_seconds: float = 2.0
    failure_threshold: int = 2


@dataclass(frozen=True)
class Config:
    listen: ListenConfig
    max_cap: int
    backends: tuple[BackendConfig, ...]
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    connect_timeout_seconds: float = 3.0
    max_retries: int = 2
    shutdown_grace_seconds: float = 10.0


def _require(mapping: dict, key: str, where: str):
    if key not in mapping:
        raise ConfigError(f"missing required field '{where}{key}'")
    return mapping[key]


def _validate_port(value, where: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ConfigError(f"'{where}' must be an integer, got {value!r}")
    if not 1 <= value <= 65535:
        raise ConfigError(f"'{where}' must be in 1..65535, got {value}")
    return value


def _positive(value, where: str, allow_zero: bool = False) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigError(f"'{where}' must be a number, got {value!r}")
    if value < 0 or (value == 0 and not allow_zero):
        raise ConfigError(f"'{where}' must be {'>= 0' if allow_zero else '> 0'}, got {value}")
    return float(value)


def _parse_backends(raw) -> tuple[BackendConfig, ...]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("'backends' must be a non-empty list")
    backends: list[BackendConfig] = []
    seen: set[tuple[str, int]] = set()
    for index, entry in enumerate(raw):
        where = f"backends[{index}]"
        if not isinstance(entry, dict):
            raise ConfigError(f"'{where}' must be a mapping with host and port")
        host = _require(entry, "host", f"{where}.")
        port = _validate_port(_require(entry, "port", f"{where}."), f"{where}.port")
        if not isinstance(host, str) or not host.strip():
            raise ConfigError(f"'{where}.host' must be a non-empty string")
        key = (host, port)
        if key in seen:
            raise ConfigError(f"duplicate backend {host}:{port}")
        seen.add(key)
        backends.append(BackendConfig(host=host, port=port))
    return tuple(backends)


def _parse_health_check(raw) -> HealthCheckConfig:
    if raw is None:
        return HealthCheckConfig()
    if not isinstance(raw, dict):
        raise ConfigError("'health_check' must be a mapping")
    defaults = HealthCheckConfig()
    interval = _positive(
        raw.get("interval_seconds", defaults.interval_seconds),
        "health_check.interval_seconds",
    )
    timeout = _positive(
        raw.get("timeout_seconds", defaults.timeout_seconds),
        "health_check.timeout_seconds",
    )
    threshold = raw.get("failure_threshold", defaults.failure_threshold)
    if not isinstance(threshold, int) or isinstance(threshold, bool) or threshold < 1:
        raise ConfigError(
            f"'health_check.failure_threshold' must be an integer >= 1, got {threshold!r}"
        )
    return HealthCheckConfig(
        interval_seconds=interval,
        timeout_seconds=timeout,
        failure_threshold=threshold,
    )


def parse_config(data: dict) -> Config:
    if not isinstance(data, dict):
        raise ConfigError("config root must be a mapping")

    raw_listen = _require(data, "listen", "")
    if not isinstance(raw_listen, dict):
        raise ConfigError("'listen' must be a mapping")
    host = _require(raw_listen, "host", "listen.")
    if not isinstance(host, str) or not host.strip():
        raise ConfigError("'listen.host' must be a non-empty string")
    port = _validate_port(_require(raw_listen, "port", "listen."), "listen.port")

    max_cap = _require(data, "max_cap", "")
    if not isinstance(max_cap, int) or isinstance(max_cap, bool) or max_cap < 1:
        raise ConfigError(f"'max_cap' must be an integer >= 1, got {max_cap!r}")

    connect_timeout = _positive(
        data.get("connect_timeout_seconds", 3.0), "connect_timeout_seconds"
    )

    max_retries = data.get("max_retries", 2)
    if not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0:
        raise ConfigError(f"'max_retries' must be an integer >= 0, got {max_retries!r}")

    shutdown_grace = _positive(
        data.get("shutdown_grace_seconds", 10.0),
        "shutdown_grace_seconds",
        allow_zero=True,
    )

    return Config(
        listen=ListenConfig(host=host, port=port),
        max_cap=max_cap,
        backends=_parse_backends(_require(data, "backends", "")),
        health_check=_parse_health_check(data.get("health_check")),
        connect_timeout_seconds=connect_timeout,
        max_retries=max_retries,
        shutdown_grace_seconds=shutdown_grace,
    )


def load_config(path: str | Path) -> Config:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config file '{path}': {exc}") from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in '{path}': {exc}") from exc
    if data is None:
        raise ConfigError(f"config file '{path}' is empty")
    return parse_config(data)
