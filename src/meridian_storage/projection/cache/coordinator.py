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

"""Read-through, single-flight, and post-commit invalidation coordination."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from threading import Event, Lock
from typing import Final, Protocol, cast

from meridian_storage.semantics import CacheEntry, FrozenJson

from meridian_storage import MeridianError
from meridian_storage.projection._canonical import (
    canonical_json_bytes,
    ensure_utc,
    parse_rfc3339,
    sha256_digest,
    to_json_value,
    utc_now,
)
from meridian_storage.projection.cache.backends import CacheBackend, VersionedCacheEntry
from meridian_storage.projection.cache.model import (
    CacheOutcome,
    CachePolicy,
    CacheRead,
    InvalidationResult,
    LoadedValue,
    SourceVersionStrategy,
    VersionComparator,
)
from meridian_storage.projection.errors import CacheUnavailable
from meridian_storage.projection.evidence import EvidenceSink, LifecycleEvidence, NullEvidenceSink

_NEGATIVE: Final = {"$meridian.cache.negative": True}
_VALUE_FIELD: Final = "$meridian.cache.value"


class CacheSerializer(Protocol):
    id: str
    deterministic: bool

    def encode(self, value: object) -> object: ...

    def decode(self, value: object) -> object: ...


class CanonicalJsonSerializer:
    id = "canonical-json-v1"
    deterministic = True

    def encode(self, value: object) -> object:
        return to_json_value(value)

    def decode(self, value: object) -> object:
        return json.loads(canonical_json_bytes(value))


class CacheCoordinator:
    """Coordinate disposable cache reuse without changing source correctness."""

    def __init__(
        self,
        *,
        policy: CachePolicy,
        backend: CacheBackend,
        serializer: CacheSerializer | None = None,
        evidence: EvidenceSink | None = None,
        version_comparator: VersionComparator | None = None,
        singleflight_wait_seconds: float = 5.0,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.policy = policy
        self._backend = backend
        self._serializer = serializer or CanonicalJsonSerializer()
        if not self._serializer.deterministic:
            raise ValueError("cache serializer must be deterministic")
        if self._serializer.id != policy.serializer_id:
            raise ValueError("cache serializer id does not match policy")
        if singleflight_wait_seconds < 0:
            raise ValueError("singleflight_wait_seconds cannot be negative")
        self._evidence = evidence or NullEvidenceSink()
        self._version_comparator = version_comparator
        self._singleflight_wait_seconds = singleflight_wait_seconds
        self._clock = clock
        self._flights: dict[str, Event] = {}
        self._flights_lock = Lock()

    @staticmethod
    def _error_code(error: BaseException) -> str:
        return str(error.code) if isinstance(error, MeridianError) else type(error).__name__

    def _record_failure(self, state: str, error: BaseException, *, now: datetime) -> str:
        code = self._error_code(error)
        self._evidence.record(
            LifecycleEvidence(
                kind="cache",
                subject=self.policy.source_resource,
                state=state,
                details={"errorCode": code},
                occurred_at=now,
            )
        )
        return code

    def _version_valid(
        self,
        cached: str | int | None,
        required: str | int | None,
    ) -> bool:
        if required is None or self.policy.source_version_strategy is SourceVersionStrategy.NONE:
            return True
        if cached is None:
            return False
        if self.policy.source_version_strategy is SourceVersionStrategy.EXACT:
            return cached == required
        if self._version_comparator is not None:
            return self._version_comparator(cached, required)
        if isinstance(cached, int) and isinstance(required, int):
            return cached >= required
        return cached == required

    def _valid_entry(
        self,
        entry: CacheEntry,
        *,
        required_source_version: str | int | None,
        now: datetime,
    ) -> bool:
        if entry.serializer_id != self.policy.serializer_id:
            return False
        if entry.schema_fingerprint != self.policy.schema_fingerprint:
            return False
        created_at = (
            parse_rfc3339(entry.created_at, field="createdAt")
            if isinstance(entry.created_at, str)
            else ensure_utc(entry.created_at, field="createdAt")
        )
        expires_at = (
            parse_rfc3339(entry.expires_at, field="expiresAt")
            if isinstance(entry.expires_at, str)
            else ensure_utc(entry.expires_at, field="expiresAt")
            if entry.expires_at is not None
            else None
        )
        if expires_at is not None and expires_at <= now:
            return False
        if created_at + self.policy.maximum_staleness < now:
            return False
        return self._version_valid(entry.source_version, required_source_version)

    def _cached(
        self,
        key: object,
        *,
        required_source_version: str | int | None,
        now: datetime,
    ) -> tuple[CacheRead | None, str | None]:
        try:
            found = self._backend.get(self.policy.cache_resource, key)
        except Exception as error:
            code = self._record_failure("READ_FAILED", error, now=now)
            if not self.policy.fallback_to_authoritative:
                raise CacheUnavailable("cache read failed and fallback is disabled") from error
            return None, code
        if found is None:
            return None, None
        if not self._valid_entry(
            found.entry,
            required_source_version=required_source_version,
            now=now,
        ):
            try:
                self._backend.delete(self.policy.cache_resource, key)
            except Exception as error:
                self._record_failure("STALE_DELETE_FAILED", error, now=now)
            return None, None
        if found.entry.value == _NEGATIVE:
            return (
                CacheRead(
                    value=None,
                    source_version=found.entry.source_version,
                    outcome=CacheOutcome.NEGATIVE_HIT,
                ),
                None,
            )
        try:
            encoded = found.entry.value
            if not isinstance(encoded, Mapping) or set(encoded) != {_VALUE_FIELD}:
                raise ValueError("cache value does not contain a tagged payload")
            value = self._serializer.decode(encoded[_VALUE_FIELD])
        except Exception as error:
            try:
                self._backend.delete(self.policy.cache_resource, key)
            except Exception as delete_error:
                self._record_failure("CORRUPT_DELETE_FAILED", delete_error, now=now)
            self._record_failure("CORRUPT", error, now=now)
            return None, None
        return (
            CacheRead(
                value=value,
                source_version=found.entry.source_version,
                outcome=CacheOutcome.HIT,
            ),
            None,
        )

    def _entry(
        self,
        key: object,
        loaded: LoadedValue | None,
        *,
        now: datetime,
    ) -> tuple[CacheEntry | None, timedelta | None]:
        if loaded is None:
            if self.policy.negative_ttl is None:
                return None, None
            ttl = self.policy.negative_ttl
            value: object = _NEGATIVE
            source_version: str | int | None = None
        else:
            ttl = self.policy.default_ttl
            value = {_VALUE_FIELD: self._serializer.encode(loaded.value)}
            source_version = loaded.source_version
        return (
            CacheEntry(
                key=cast(FrozenJson, to_json_value(key)),
                value=cast(FrozenJson, to_json_value(value)),
                serializer_id=self.policy.serializer_id,
                schema_fingerprint=self.policy.schema_fingerprint,
                created_at=now,
                expires_at=now + ttl,
                source_version=source_version,
            ),
            ttl,
        )

    def _load(
        self,
        key: object,
        loader: Callable[[], LoadedValue | None],
        *,
        cache_error: str | None,
        now: datetime,
    ) -> CacheRead:
        loaded = loader()
        entry, ttl = self._entry(key, loaded, now=now)
        if entry is not None and ttl is not None:
            try:
                self._backend.put(
                    self.policy.cache_resource,
                    entry,
                    ttl_ms=max(1, int(ttl.total_seconds() * 1000)),
                )
            except Exception as error:
                cache_error = self._record_failure("POPULATE_FAILED", error, now=now)
        return CacheRead(
            value=None if loaded is None else loaded.value,
            source_version=None if loaded is None else loaded.source_version,
            outcome=CacheOutcome.BYPASS if cache_error else CacheOutcome.MISS,
            cache_error=cache_error,
        )

    def read(
        self,
        identity: Mapping[str, object],
        loader: Callable[[], LoadedValue | None],
        *,
        required_source_version: str | int | None = None,
        now: datetime | None = None,
    ) -> CacheRead:
        current = ensure_utc(now or self._clock(), field="now")
        if not self.policy.enabled:
            loaded = loader()
            return CacheRead(
                value=None if loaded is None else loaded.value,
                source_version=None if loaded is None else loaded.source_version,
                outcome=CacheOutcome.BYPASS,
            )
        key = self.policy.key(identity)
        cached, error_code = self._cached(
            key,
            required_source_version=required_source_version,
            now=current,
        )
        if cached is not None:
            return cached
        flight_key = sha256_digest(key)
        with self._flights_lock:
            event = self._flights.get(flight_key)
            leader = event is None
            if event is None:
                event = Event()
                self._flights[flight_key] = event
        if not leader:
            event.wait(self._singleflight_wait_seconds)
            cached, follower_error = self._cached(
                key,
                required_source_version=required_source_version,
                now=ensure_utc(self._clock(), field="now"),
            )
            if cached is not None:
                return cached
            return self._load(
                key,
                loader,
                cache_error=follower_error or error_code,
                now=ensure_utc(self._clock(), field="now"),
            )
        try:
            return self._load(key, loader, cache_error=error_code, now=current)
        finally:
            with self._flights_lock:
                finished = self._flights.pop(flight_key)
                finished.set()

    def invalidate_after_commit(
        self,
        selector: Mapping[str, object],
        *,
        now: datetime | None = None,
    ) -> InvalidationResult:
        current = ensure_utc(now or self._clock(), field="now")
        try:
            count = self._backend.invalidate(self.policy.cache_resource, selector)
        except Exception as error:
            code = self._record_failure("INVALIDATION_FAILED", error, now=current)
            return InvalidationResult(successful=False, error_code=code)
        return InvalidationResult(successful=True, invalidated=count)

    def put_if_absent(
        self,
        identity: Mapping[str, object],
        value: LoadedValue,
        *,
        ttl: timedelta | None = None,
        now: datetime | None = None,
    ) -> bool:
        current = ensure_utc(now or self._clock(), field="now")
        selected_ttl = ttl if ttl is not None else self.policy.default_ttl
        if selected_ttl <= timedelta(0) or selected_ttl > self.policy.maximum_ttl:
            raise ValueError("explicit cache TTL is outside policy bounds")
        entry = CacheEntry(
            key=cast(FrozenJson, to_json_value(self.policy.key(identity))),
            value=cast(
                FrozenJson,
                to_json_value({_VALUE_FIELD: self._serializer.encode(value.value)}),
            ),
            serializer_id=self.policy.serializer_id,
            schema_fingerprint=self.policy.schema_fingerprint,
            created_at=current,
            expires_at=current + selected_ttl,
            source_version=value.source_version,
        )
        try:
            return self._backend.put_if_absent(
                self.policy.cache_resource,
                entry,
                ttl_ms=max(1, int(selected_ttl.total_seconds() * 1000)),
            )
        except Exception as error:
            raise CacheUnavailable("put_if_absent failed") from error

    def compare_and_set(
        self,
        identity: Mapping[str, object],
        value: LoadedValue,
        *,
        expected_version: str | int,
        ttl: timedelta | None = None,
        now: datetime | None = None,
    ) -> bool:
        current = ensure_utc(now or self._clock(), field="now")
        selected_ttl = ttl if ttl is not None else self.policy.default_ttl
        if selected_ttl <= timedelta(0) or selected_ttl > self.policy.maximum_ttl:
            raise ValueError("explicit cache TTL is outside policy bounds")
        entry = CacheEntry(
            key=cast(FrozenJson, to_json_value(self.policy.key(identity))),
            value=cast(
                FrozenJson,
                to_json_value({_VALUE_FIELD: self._serializer.encode(value.value)}),
            ),
            serializer_id=self.policy.serializer_id,
            schema_fingerprint=self.policy.schema_fingerprint,
            created_at=current,
            expires_at=current + selected_ttl,
            source_version=value.source_version,
        )
        try:
            return self._backend.compare_and_set(
                self.policy.cache_resource,
                entry,
                expected_version=expected_version,
                ttl_ms=max(1, int(selected_ttl.total_seconds() * 1000)),
            )
        except Exception as error:
            raise CacheUnavailable("compare_and_set failed") from error

    def delete(self, identity: Mapping[str, object]) -> bool:
        try:
            return self._backend.delete(self.policy.cache_resource, self.policy.key(identity))
        except Exception as error:
            raise CacheUnavailable("cache delete failed") from error

    def inspect(self, identity: Mapping[str, object]) -> VersionedCacheEntry | None:
        """Expose the explicit cache entry/version contract for coordination callers."""

        try:
            return self._backend.get(self.policy.cache_resource, self.policy.key(identity))
        except Exception as error:
            raise CacheUnavailable("cache get failed") from error
