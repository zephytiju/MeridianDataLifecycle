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

"""Cache policy and read-through result contracts."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

from meridian_storage.projection._canonical import canonical_json_bytes


class SourceVersionStrategy(StrEnum):
    NONE = "none"
    EXACT = "exact"
    AT_LEAST = "at-least"


class CacheOutcome(StrEnum):
    HIT = "hit"
    MISS = "miss"
    NEGATIVE_HIT = "negative-hit"
    STALE = "stale"
    BYPASS = "bypass"


@dataclass(frozen=True, slots=True)
class CachePolicy:
    """Validated Collection policy used by explicit and transparent reads."""

    source_resource: str
    cache_resource: str
    key_fields: tuple[str, ...]
    serializer_id: str
    schema_fingerprint: str
    default_ttl: timedelta
    maximum_ttl: timedelta
    maximum_staleness: timedelta
    negative_ttl: timedelta | None = None
    source_version_strategy: SourceVersionStrategy = SourceVersionStrategy.EXACT
    enabled: bool = True
    fallback_to_authoritative: bool = True
    maximum_key_bytes: int = 4096

    def __post_init__(self) -> None:
        for field_name in (
            "source_resource",
            "cache_resource",
            "serializer_id",
            "schema_fingerprint",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must be non-empty")
        if not self.key_fields or len(set(self.key_fields)) != len(self.key_fields):
            raise ValueError("key_fields must be non-empty and unique")
        if not all(isinstance(field, str) and field for field in self.key_fields):
            raise ValueError("key_fields must contain non-empty strings")
        if not isinstance(self.source_version_strategy, SourceVersionStrategy):
            raise ValueError("source_version_strategy must be a SourceVersionStrategy")
        if not isinstance(self.enabled, bool) or not isinstance(
            self.fallback_to_authoritative, bool
        ):
            raise ValueError("cache enabled and fallback flags must be booleans")
        if re.fullmatch(r"sha256:[0-9a-f]{64}", self.schema_fingerprint) is None:
            raise ValueError("schema_fingerprint must use the sha256:<hex> form")
        if self.default_ttl <= timedelta(0) or self.maximum_ttl <= timedelta(0):
            raise ValueError("cache TTLs must be positive")
        if self.default_ttl > self.maximum_ttl:
            raise ValueError("default_ttl cannot exceed maximum_ttl")
        if self.maximum_staleness < timedelta(0):
            raise ValueError("maximum_staleness cannot be negative")
        if self.negative_ttl is not None:
            if self.negative_ttl <= timedelta(0):
                raise ValueError("negative_ttl must be positive")
            if self.negative_ttl > self.maximum_ttl:
                raise ValueError("negative_ttl cannot exceed maximum_ttl")
        if isinstance(self.maximum_key_bytes, bool) or self.maximum_key_bytes < 64:
            raise ValueError("maximum_key_bytes must be at least 64")

    def key(self, identity: Mapping[str, object]) -> list[object]:
        missing = [name for name in self.key_fields if name not in identity]
        if missing:
            raise ValueError(f"cache identity is missing key fields: {', '.join(missing)}")
        invalid = [
            name
            for name in self.key_fields
            if identity[name] is None
            or isinstance(identity[name], (Mapping, list, tuple, set, bytes, bytearray))
        ]
        if invalid:
            raise ValueError(f"cache key fields must be non-null scalars: {', '.join(invalid)}")
        key: list[object] = [self.source_resource]
        key.extend(identity[name] for name in self.key_fields)
        if len(canonical_json_bytes(key)) > self.maximum_key_bytes:
            raise ValueError("cache key exceeds maximum_key_bytes")
        return key

    def to_mapping(self) -> dict[str, object]:
        return {
            "sourceResource": self.source_resource,
            "cacheResource": self.cache_resource,
            "keyFields": list(self.key_fields),
            "serializerId": self.serializer_id,
            "schemaFingerprint": self.schema_fingerprint,
            "defaultTtlMs": max(1, int(self.default_ttl.total_seconds() * 1000)),
            "maximumTtlMs": max(1, int(self.maximum_ttl.total_seconds() * 1000)),
            "maximumStalenessMs": int(self.maximum_staleness.total_seconds() * 1000),
            "negativeTtlMs": (
                max(1, int(self.negative_ttl.total_seconds() * 1000))
                if self.negative_ttl is not None
                else None
            ),
            "sourceVersionStrategy": self.source_version_strategy.value,
            "enabled": self.enabled,
            "fallbackToAuthoritative": self.fallback_to_authoritative,
            "maximumKeyBytes": self.maximum_key_bytes,
        }


@dataclass(frozen=True, slots=True)
class LoadedValue:
    value: object
    source_version: str | int | None = None


@dataclass(frozen=True, slots=True)
class CacheRead:
    value: object | None
    source_version: str | int | None
    outcome: CacheOutcome
    cache_error: str | None = None


@dataclass(frozen=True, slots=True)
class InvalidationResult:
    successful: bool
    invalidated: int = 0
    error_code: str | None = None


VersionComparator = Callable[[str | int, str | int], bool]
