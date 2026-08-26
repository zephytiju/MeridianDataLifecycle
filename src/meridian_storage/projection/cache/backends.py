# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cache backend port and adapters for the authoritative cache Catalog."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from threading import RLock
from typing import Final, Protocol, cast

from meridian_storage.semantics import CacheEntry, FrozenJson

from meridian_storage import Expression, Meridian, OperationResult
from meridian_storage.projection._canonical import (
    canonical_json_bytes,
    parse_rfc3339,
    to_json_value,
)

CACHE_ENVELOPE_FORMAT_VERSION: Final = "meridian.cache-envelope.v1"


@dataclass(frozen=True, slots=True)
class VersionedCacheEntry:
    entry: CacheEntry
    version: str | int


class CacheBackend(Protocol):
    def get(self, resource: str, key: object) -> VersionedCacheEntry | None: ...

    def put(self, resource: str, entry: CacheEntry, *, ttl_ms: int) -> str | int: ...

    def put_if_absent(self, resource: str, entry: CacheEntry, *, ttl_ms: int) -> bool: ...

    def compare_and_set(
        self,
        resource: str,
        entry: CacheEntry,
        *,
        expected_version: str | int,
        ttl_ms: int,
    ) -> bool: ...

    def delete(self, resource: str, key: object) -> bool: ...

    def invalidate(self, resource: str, selector: Mapping[str, object]) -> int: ...


class _CacheSurface(Protocol):
    def get(self, *, resource: str, key: object) -> Expression: ...

    def put(
        self,
        *,
        resource: str,
        key: object,
        value: object,
        ttl_ms: int | None = None,
        source_version: str | int | None = None,
    ) -> Expression: ...

    def put_if_absent(
        self,
        *,
        resource: str,
        key: object,
        value: object,
        ttl_ms: int | None = None,
        source_version: str | int | None = None,
    ) -> Expression: ...

    def compare_and_set(
        self,
        *,
        resource: str,
        key: object,
        expected_version: str | int,
        value: object,
        ttl_ms: int | None = None,
    ) -> Expression: ...

    def delete(self, *, resource: str, key: object) -> Expression: ...

    def invalidate(self, *, resource: str, selector: Mapping[str, object]) -> Expression: ...


def _mapping(result: OperationResult) -> Mapping[str, object] | None:
    if result.data is None:
        return None
    if not isinstance(result.data, Mapping):
        raise ValueError("cache Catalog returned non-mapping Data")
    return cast(Mapping[str, object], result.data)


def _wire_entry(entry: CacheEntry) -> dict[str, object]:
    return {
        "formatVersion": CACHE_ENVELOPE_FORMAT_VERSION,
        **entry.to_dict(),
    }


def _entry_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    candidate: object = value.get("entry", value)
    if not isinstance(candidate, Mapping):
        raise ValueError("cache entry must be a mapping")
    if candidate.get("formatVersion") == CACHE_ENVELOPE_FORMAT_VERSION:
        return cast(Mapping[str, object], candidate)
    nested = candidate.get("value")
    if isinstance(nested, Mapping) and nested.get("formatVersion") == CACHE_ENVELOPE_FORMAT_VERSION:
        return cast(Mapping[str, object], nested)
    if "serializerId" in candidate and "schemaFingerprint" in candidate:
        return cast(Mapping[str, object], candidate)
    raise ValueError("cache Catalog result does not contain a Meridian cache envelope")


def _entry(value: Mapping[str, object]) -> CacheEntry:
    created = value.get("createdAt")
    expires = value.get("expiresAt")
    if not isinstance(created, str):
        raise ValueError("cache entry createdAt must be an RFC 3339 string")
    if expires is not None and not isinstance(expires, str):
        raise ValueError("cache entry expiresAt must be an RFC 3339 string or null")
    serializer = value.get("serializerId")
    fingerprint = value.get("schemaFingerprint")
    if not isinstance(serializer, str) or not isinstance(fingerprint, str):
        raise ValueError("cache entry serializer and Schema fingerprint are required")
    source_version = value.get("sourceVersion")
    if source_version is not None and (
        not isinstance(source_version, (str, int)) or isinstance(source_version, bool)
    ):
        raise ValueError("cache sourceVersion must be a string, integer, or null")
    return CacheEntry(
        key=cast(FrozenJson, to_json_value(value.get("key"))),
        value=cast(FrozenJson, to_json_value(value.get("value"))),
        serializer_id=serializer,
        schema_fingerprint=fingerprint,
        created_at=parse_rfc3339(created, field="createdAt"),
        expires_at=parse_rfc3339(expires, field="expiresAt") if expires else None,
        source_version=source_version,
    )


class MeridianCacheBackend:
    """Adapt the released mapping-first cache Catalog to the coordinator port."""

    def __init__(self, meridian: Meridian) -> None:
        self._meridian = meridian

    @property
    def _surface(self) -> _CacheSurface:
        return cast(_CacheSurface, self._meridian.catalog("cache"))

    def get(self, resource: str, key: object) -> VersionedCacheEntry | None:
        result = self._meridian.execute(self._surface.get(resource=resource, key=key))
        mapping = _mapping(result)
        if mapping is None or mapping.get("found") is False:
            return None
        candidate = mapping.get("entry")
        candidate_mapping = candidate if isinstance(candidate, Mapping) else {}
        version = mapping.get(
            "entryVersion",
            mapping.get(
                "version",
                candidate_mapping.get(
                    "entryVersion", candidate_mapping.get("version", result.operation_fingerprint)
                ),
            ),
        )
        if not isinstance(version, (str, int)) or isinstance(version, bool):
            raise ValueError("cache entry version must be a string or integer")
        entry = _entry(_entry_mapping(mapping))
        if canonical_json_bytes(entry.key) != canonical_json_bytes(key):
            raise ValueError("cache Catalog returned an entry for a different key")
        return VersionedCacheEntry(entry=entry, version=version)

    @staticmethod
    def _result_version(result: OperationResult) -> str | int:
        if isinstance(result.data, Mapping):
            value = result.data.get("entryVersion", result.data.get("version"))
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                return value
        return cast(str, result.operation_fingerprint)

    def put(self, resource: str, entry: CacheEntry, *, ttl_ms: int) -> str | int:
        result = self._meridian.execute(
            self._surface.put(
                resource=resource,
                key=entry.key,
                value=_wire_entry(entry),
                ttl_ms=ttl_ms,
                source_version=entry.source_version,
            )
        )
        return self._result_version(result)

    def put_if_absent(self, resource: str, entry: CacheEntry, *, ttl_ms: int) -> bool:
        result = self._meridian.execute(
            self._surface.put_if_absent(
                resource=resource,
                key=entry.key,
                value=_wire_entry(entry),
                ttl_ms=ttl_ms,
                source_version=entry.source_version,
            )
        )
        return not isinstance(result.data, Mapping) or result.data.get("written", True) is True

    def compare_and_set(
        self,
        resource: str,
        entry: CacheEntry,
        *,
        expected_version: str | int,
        ttl_ms: int,
    ) -> bool:
        result = self._meridian.execute(
            self._surface.compare_and_set(
                resource=resource,
                key=entry.key,
                expected_version=expected_version,
                value=_wire_entry(entry),
                ttl_ms=ttl_ms,
            )
        )
        return not isinstance(result.data, Mapping) or result.data.get("swapped", True) is True

    def delete(self, resource: str, key: object) -> bool:
        result = self._meridian.execute(self._surface.delete(resource=resource, key=key))
        return not isinstance(result.data, Mapping) or result.data.get("deleted", True) is True

    def invalidate(self, resource: str, selector: Mapping[str, object]) -> int:
        result = self._meridian.execute(
            self._surface.invalidate(resource=resource, selector=selector)
        )
        if isinstance(result.data, Mapping):
            count = result.data.get("invalidated", 0)
            if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                return count
            raise ValueError("cache invalidation count must be a non-negative integer")
        return 0


class InMemoryCacheBackend:
    """Thread-safe conformance backend with explicit outage injection."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], VersionedCacheEntry] = {}
        self._revision = 0
        self._available = True
        self._lock = RLock()

    def set_available(self, available: bool) -> None:
        with self._lock:
            self._available = available

    def _require_available(self) -> None:
        if not self._available:
            raise RuntimeError("cache backend unavailable")

    @staticmethod
    def _key(resource: str, key: object) -> tuple[str, str]:
        from meridian_storage.projection._canonical import sha256_digest

        return resource, sha256_digest(key)

    def _next_version(self) -> int:
        self._revision += 1
        return self._revision

    def get(self, resource: str, key: object) -> VersionedCacheEntry | None:
        with self._lock:
            self._require_available()
            return self._entries.get(self._key(resource, key))

    def put(self, resource: str, entry: CacheEntry, *, ttl_ms: int) -> str | int:
        del ttl_ms
        with self._lock:
            self._require_available()
            version = self._next_version()
            self._entries[self._key(resource, entry.key)] = VersionedCacheEntry(entry, version)
            return version

    def put_if_absent(self, resource: str, entry: CacheEntry, *, ttl_ms: int) -> bool:
        del ttl_ms
        with self._lock:
            self._require_available()
            key = self._key(resource, entry.key)
            if key in self._entries:
                return False
            self._entries[key] = VersionedCacheEntry(entry, self._next_version())
            return True

    def compare_and_set(
        self,
        resource: str,
        entry: CacheEntry,
        *,
        expected_version: str | int,
        ttl_ms: int,
    ) -> bool:
        del ttl_ms
        with self._lock:
            self._require_available()
            key = self._key(resource, entry.key)
            current = self._entries.get(key)
            if current is None or current.version != expected_version:
                return False
            self._entries[key] = VersionedCacheEntry(entry, self._next_version())
            return True

    def delete(self, resource: str, key: object) -> bool:
        with self._lock:
            self._require_available()
            return self._entries.pop(self._key(resource, key), None) is not None

    def invalidate(self, resource: str, selector: Mapping[str, object]) -> int:
        with self._lock:
            self._require_available()
            if selector.get("all") is not True:
                raise ValueError("in-memory invalidation supports only {'all': true}")
            keys = [key for key in self._entries if key[0] == resource]
            for key in keys:
                del self._entries[key]
            return len(keys)
