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

from datetime import datetime, timedelta
from threading import Event, Lock, Thread
from typing import cast

import pytest
from meridian_storage.semantics import CacheEntry

from meridian_storage.projection.cache import (
    CacheCoordinator,
    CacheOutcome,
    CachePolicy,
    CacheSerializer,
    InMemoryCacheBackend,
    LoadedValue,
    SourceVersionStrategy,
)
from meridian_storage.projection.errors import CacheUnavailable
from tests.conftest import MutableClock

FINGERPRINT = "sha256:" + "a" * 64


def _policy(**overrides: object) -> CachePolicy:
    values: dict[str, object] = {
        "source_resource": "investigation.cases",
        "cache_resource": "investigation.case_cache",
        "key_fields": ("case_id",),
        "serializer_id": "canonical-json-v1",
        "schema_fingerprint": FINGERPRINT,
        "default_ttl": timedelta(seconds=30),
        "maximum_ttl": timedelta(minutes=5),
        "maximum_staleness": timedelta(seconds=30),
    }
    values.update(overrides)
    return CachePolicy(**values)  # type: ignore[arg-type]


def test_cache_policy_validation_and_key() -> None:
    policy = _policy(negative_ttl=timedelta(seconds=5))
    assert policy.key({"case_id": "case-1"}) == ["investigation.cases", "case-1"]
    assert policy.to_mapping()["defaultTtlMs"] == 30_000
    with pytest.raises(ValueError, match="missing"):
        policy.key({})
    with pytest.raises(ValueError, match="scalars"):
        policy.key({"case_id": {"unbounded": True}})
    with pytest.raises(ValueError, match="unique"):
        _policy(key_fields=("id", "id"))
    with pytest.raises(ValueError, match="sha256"):
        _policy(schema_fingerprint="bad")
    with pytest.raises(ValueError, match="cannot exceed"):
        _policy(default_ttl=timedelta(minutes=10))
    with pytest.raises(ValueError, match="negative_ttl"):
        _policy(negative_ttl=timedelta(0))


def test_read_through_hit_stale_negative_and_disabled(fixed_time: datetime) -> None:
    clock = MutableClock(fixed_time)
    backend = InMemoryCacheBackend()
    coordinator = CacheCoordinator(policy=_policy(), backend=backend, clock=clock)
    calls = 0

    def load() -> LoadedValue:
        nonlocal calls
        calls += 1
        return LoadedValue({"case_id": "case-1"}, source_version=1)

    first = coordinator.read({"case_id": "case-1"}, load, required_source_version=1)
    second = coordinator.read({"case_id": "case-1"}, load, required_source_version=1)
    assert first.outcome is CacheOutcome.MISS
    assert second.outcome is CacheOutcome.HIT
    assert calls == 1
    assert second.value == {"case_id": "case-1"}

    clock.advance(seconds=31)
    stale = coordinator.read({"case_id": "case-1"}, load, required_source_version=2)
    assert stale.outcome is CacheOutcome.MISS
    assert calls == 2

    negative = CacheCoordinator(
        policy=_policy(negative_ttl=timedelta(seconds=5)),
        backend=InMemoryCacheBackend(),
        clock=clock,
    )
    assert negative.read({"case_id": "missing"}, lambda: None).outcome is CacheOutcome.MISS
    assert negative.read({"case_id": "missing"}, lambda: None).outcome is CacheOutcome.NEGATIVE_HIT

    disabled = CacheCoordinator(policy=_policy(enabled=False), backend=backend, clock=clock)
    assert disabled.read({"case_id": "case-1"}, load).outcome is CacheOutcome.BYPASS


def test_cache_outage_and_explicit_coordination(fixed_time: datetime) -> None:
    clock = MutableClock(fixed_time)
    backend = InMemoryCacheBackend()
    coordinator = CacheCoordinator(policy=_policy(), backend=backend, clock=clock)
    value = LoadedValue({"id": "case-1"}, 1)
    assert coordinator.put_if_absent({"case_id": "case-1"}, value)
    assert not coordinator.put_if_absent({"case_id": "case-1"}, value)
    current = coordinator.inspect({"case_id": "case-1"})
    assert current is not None
    assert not coordinator.compare_and_set({"case_id": "case-1"}, value, expected_version="wrong")
    assert coordinator.compare_and_set(
        {"case_id": "case-1"},
        LoadedValue({"id": "case-1", "v": 2}, 2),
        expected_version=current.version,
    )
    assert coordinator.delete({"case_id": "case-1"})
    assert not coordinator.delete({"case_id": "case-1"})

    backend.set_available(False)
    fallback = coordinator.read(
        {"case_id": "case-1"}, lambda: LoadedValue({"authoritative": True}, 3)
    )
    assert fallback.outcome is CacheOutcome.BYPASS
    assert fallback.cache_error == "RuntimeError"
    invalidation = coordinator.invalidate_after_commit({"all": True})
    assert not invalidation.successful
    with pytest.raises(CacheUnavailable):
        coordinator.inspect({"case_id": "case-1"})
    with pytest.raises(CacheUnavailable):
        coordinator.put_if_absent({"case_id": "case-1"}, value)

    no_fallback = CacheCoordinator(
        policy=_policy(fallback_to_authoritative=False), backend=backend, clock=clock
    )
    with pytest.raises(CacheUnavailable):
        no_fallback.read({"case_id": "case-1"}, lambda: value)


class BrokenSerializer:
    id = "broken"
    deterministic = True

    def encode(self, value: object) -> object:
        return value

    def decode(self, value: object) -> object:
        del value
        raise ValueError("corrupt")


class NondeterministicSerializer:
    id = "canonical-json-v1"
    deterministic = False

    def encode(self, value: object) -> object:
        return value

    def decode(self, value: object) -> object:
        return value


def test_corrupt_entry_and_serializer_validation(fixed_time: datetime) -> None:
    backend = InMemoryCacheBackend()
    broken_policy = _policy(serializer_id="broken")
    entry = CacheEntry(
        key=("investigation.cases", "case-1"),
        value={"bad": True},
        serializer_id="broken",
        schema_fingerprint=FINGERPRINT,
        created_at=fixed_time,
        expires_at=fixed_time + timedelta(seconds=30),
        source_version=1,
    )
    backend.put("investigation.case_cache", entry, ttl_ms=30_000)
    coordinator = CacheCoordinator(
        policy=broken_policy,
        backend=backend,
        serializer=BrokenSerializer(),
        clock=lambda: fixed_time,
    )
    read = coordinator.read(
        {"case_id": "case-1"}, lambda: LoadedValue({"good": True}, source_version=2)
    )
    assert read.outcome is CacheOutcome.MISS
    with pytest.raises(ValueError, match="deterministic"):
        CacheCoordinator(policy=_policy(), backend=backend, serializer=NondeterministicSerializer())
    with pytest.raises(ValueError, match="id"):
        CacheCoordinator(
            policy=_policy(),
            backend=backend,
            serializer=cast(CacheSerializer, BrokenSerializer()),
        )


def test_at_least_version_and_ttl_bounds(fixed_time: datetime) -> None:
    coordinator = CacheCoordinator(
        policy=_policy(source_version_strategy=SourceVersionStrategy.AT_LEAST),
        backend=InMemoryCacheBackend(),
        clock=lambda: fixed_time,
    )
    value = LoadedValue({"id": 1}, 4)
    coordinator.put_if_absent({"case_id": "case-1"}, value)
    assert (
        coordinator.read({"case_id": "case-1"}, lambda: value, required_source_version=3).outcome
        is CacheOutcome.HIT
    )
    with pytest.raises(ValueError, match="bounds"):
        coordinator.put_if_absent({"case_id": "other"}, value, ttl=timedelta(minutes=10))
    with pytest.raises(ValueError, match="bounds"):
        coordinator.compare_and_set(
            {"case_id": "case-1"}, value, expected_version=1, ttl=timedelta(0)
        )


def test_user_value_cannot_collide_with_negative_cache_marker(fixed_time: datetime) -> None:
    coordinator = CacheCoordinator(
        policy=_policy(negative_ttl=timedelta(seconds=5)),
        backend=InMemoryCacheBackend(),
        clock=lambda: fixed_time,
    )
    user_value = {"$meridian.cache.negative": True}
    first = coordinator.read(
        {"case_id": "case-1"}, lambda: LoadedValue(user_value, source_version=1)
    )
    second = coordinator.read(
        {"case_id": "case-1"}, lambda: pytest.fail("authoritative loader must not run")
    )
    assert first.value == user_value
    assert second.outcome is CacheOutcome.HIT
    assert second.value == user_value


def test_singleflight_prevents_stampede(fixed_time: datetime) -> None:
    backend = InMemoryCacheBackend()
    coordinator = CacheCoordinator(
        policy=_policy(), backend=backend, clock=lambda: fixed_time, singleflight_wait_seconds=2
    )
    leader_entered = Event()
    release = Event()
    count_lock = Lock()
    loads = 0
    reads: list[object] = []

    def loader() -> LoadedValue:
        nonlocal loads
        with count_lock:
            loads += 1
        leader_entered.set()
        assert release.wait(2)
        return LoadedValue({"id": "case-1"}, 1)

    def read() -> None:
        reads.append(coordinator.read({"case_id": "case-1"}, loader).value)

    first = Thread(target=read)
    second = Thread(target=read)
    first.start()
    assert leader_entered.wait(2)
    second.start()
    release.set()
    first.join(2)
    second.join(2)
    assert loads == 1
    assert reads == [{"id": "case-1"}, {"id": "case-1"}]
