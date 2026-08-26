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

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import cast

import pytest
from meridian_storage.semantics import CacheEntry, FrozenJson, JsonValue

from meridian_storage import Expression, Meridian, OperationResult
from meridian_storage.projection._canonical import canonical_json_bytes
from meridian_storage.projection.cache import (
    CACHE_ENVELOPE_FORMAT_VERSION,
    MeridianCacheBackend,
)
from tests.conftest import result

FINGERPRINT = "sha256:" + "a" * 64


class CacheSurface:
    @staticmethod
    def _expression(method: str, **arguments: object) -> Expression:
        return Expression("cache", method, arguments)

    def get(self, *, resource: str, key: object) -> Expression:
        return self._expression("get", resource=resource, key=key)

    def put(
        self,
        *,
        resource: str,
        key: object,
        value: object,
        ttl_ms: int | None = None,
        source_version: str | int | None = None,
    ) -> Expression:
        return self._expression(
            "put",
            resource=resource,
            key=key,
            value=value,
            ttlMs=ttl_ms,
            sourceVersion=source_version,
        )

    def put_if_absent(
        self,
        *,
        resource: str,
        key: object,
        value: object,
        ttl_ms: int | None = None,
        source_version: str | int | None = None,
    ) -> Expression:
        return self._expression(
            "put_if_absent",
            resource=resource,
            key=key,
            value=value,
            ttlMs=ttl_ms,
            sourceVersion=source_version,
        )

    def compare_and_set(
        self,
        *,
        resource: str,
        key: object,
        expected_version: str | int,
        value: object,
        ttl_ms: int | None = None,
    ) -> Expression:
        return self._expression(
            "compare_and_set",
            resource=resource,
            key=key,
            expectedVersion=expected_version,
            value=value,
            ttlMs=ttl_ms,
        )

    def delete(self, *, resource: str, key: object) -> Expression:
        return self._expression("delete", resource=resource, key=key)

    def invalidate(self, *, resource: str, selector: Mapping[str, object]) -> Expression:
        return self._expression("invalidate", resource=resource, selector=selector)


class Runtime:
    def __init__(self) -> None:
        self.surface = CacheSurface()
        self.entries: dict[bytes, tuple[object, object, int]] = {}
        self.revision = 0
        self.expressions: list[Expression] = []

    def catalog(self, name: str) -> object:
        assert name == "cache"
        return self.surface

    def _result(self, data: object) -> OperationResult:
        return result(
            cast(JsonValue, data),
            catalog="cache",
            resource="cache:investigation.case_cache",
        )

    def execute(self, expression: Expression) -> OperationResult:
        self.expressions.append(expression)
        arguments = expression.arguments
        key = canonical_json_bytes(arguments.get("key"))
        if expression.method == "get":
            found = self.entries.get(key)
            if found is None:
                return self._result({"found": False})
            stored_key, value, version = found
            return self._result(
                {
                    "found": True,
                    "key": stored_key,
                    "value": value,
                    "entryVersion": version,
                }
            )
        if expression.method in {"put", "put_if_absent", "compare_and_set"}:
            if expression.method == "put_if_absent" and key in self.entries:
                return self._result({"written": False})
            if expression.method == "compare_and_set":
                current = self.entries.get(key)
                if current is None or current[2] != arguments["expectedVersion"]:
                    return self._result({"swapped": False})
            self.revision += 1
            self.entries[key] = (arguments["key"], arguments["value"], self.revision)
            field = "swapped" if expression.method == "compare_and_set" else "written"
            return self._result({field: True, "entryVersion": self.revision})
        if expression.method == "delete":
            return self._result({"deleted": self.entries.pop(key, None) is not None})
        if expression.method == "invalidate":
            count = len(self.entries)
            self.entries.clear()
            return self._result({"invalidated": count})
        raise AssertionError(expression.method)


def _entry(fixed_time: datetime, *, value: object = None) -> CacheEntry:
    return CacheEntry(
        key=("investigation.cases", "case-1"),
        value=cast(FrozenJson, {"case": 1} if value is None else value),
        serializer_id="canonical-json-v1",
        schema_fingerprint=FINGERPRINT,
        created_at=fixed_time,
        expires_at=fixed_time + timedelta(seconds=30),
        source_version=7,
    )


def test_meridian_cache_backend_round_trip_preserves_envelope(fixed_time: datetime) -> None:
    runtime = Runtime()
    backend = MeridianCacheBackend(cast(Meridian, runtime))
    entry = _entry(fixed_time)
    assert backend.get("investigation.case_cache", entry.key) is None
    assert backend.put("investigation.case_cache", entry, ttl_ms=30_000) == 1
    stored = runtime.expressions[-1].arguments["value"]
    assert isinstance(stored, Mapping)
    assert stored["formatVersion"] == CACHE_ENVELOPE_FORMAT_VERSION

    found = backend.get("investigation.case_cache", entry.key)
    assert found is not None
    assert found.entry == entry
    assert found.version == 1

    assert not backend.put_if_absent("investigation.case_cache", entry, ttl_ms=30_000)
    replacement = _entry(fixed_time, value={"case": 2})
    assert not backend.compare_and_set(
        "investigation.case_cache", replacement, expected_version=99, ttl_ms=30_000
    )
    assert backend.compare_and_set(
        "investigation.case_cache", replacement, expected_version=1, ttl_ms=30_000
    )
    assert backend.delete("investigation.case_cache", entry.key)
    assert not backend.delete("investigation.case_cache", entry.key)
    backend.put("investigation.case_cache", entry, ttl_ms=30_000)
    assert backend.invalidate("investigation.case_cache", {"all": True}) == 1


def test_meridian_cache_backend_rejects_corrupt_results(fixed_time: datetime) -> None:
    class CorruptRuntime(Runtime):
        def execute(self, expression: Expression) -> OperationResult:
            if expression.method == "get":
                return self._result({"found": True, "value": {"not": "an-envelope"}})
            if expression.method == "invalidate":
                return self._result({"invalidated": -1})
            return super().execute(expression)

    backend = MeridianCacheBackend(cast(Meridian, CorruptRuntime()))
    with pytest.raises(ValueError, match="envelope"):
        backend.get("investigation.case_cache", _entry(fixed_time).key)
    with pytest.raises(ValueError, match="non-negative"):
        backend.invalidate("investigation.case_cache", {"all": True})
