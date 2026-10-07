"""Versioned response cache.

Every key embeds a version number that the pipeline bumps after each dbt
build. Old entries are never served again and simply expire, so there is
no need to find and delete stale keys.
"""
import json
from collections.abc import Callable
from typing import Any, Protocol

VERSION_KEY = "mf:cache_version"
DEFAULT_TTL_SECONDS = 3600


class CacheBackend(Protocol):
    def get(self, key: str) -> bytes | str | None: ...
    def set(self, key: str, value: str, ex: int | None = None) -> Any: ...


class ResponseCache:
    def __init__(self, backend: CacheBackend, ttl_seconds: int = DEFAULT_TTL_SECONDS):
        self.backend = backend
        self.ttl_seconds = ttl_seconds

    def _version(self) -> str:
        version = self.backend.get(VERSION_KEY)
        if isinstance(version, bytes):
            version = version.decode()
        return version or "0"

    def get_or_load(self, key: str, loader: Callable[[], Any]) -> tuple[Any, bool]:
        """Return (value, cache_hit)."""
        full_key = f"mf:v{self._version()}:{key}"
        cached = self.backend.get(full_key)
        if cached is not None:
            return json.loads(cached), True
        value = loader()
        self.backend.set(full_key, json.dumps(value, default=str), ex=self.ttl_seconds)
        return value, False
