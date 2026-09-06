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

"""Adapter-consumable lifecycle assertions; no pytest or Engine dependencies.

The harness callbacks are test setup/inspection, not additions to OutboxPort.
An adapter supplies an empty isolated scope, poison_threshold=2 and a reopen
callback that recreates its port over the same storage. The host cleans up.
The reference store can exercise these assertions but cannot prove durability.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial

from meridian_storage import MeridianError
from meridian_storage.projection.outbox import (
    Checkpoint,
    OutboxDataV1,
    OutboxPort,
    OutboxRecord,
    OutboxState,
)


@dataclass(frozen=True, slots=True)
class OutboxConformanceTarget:
    outbox: OutboxPort
    seed: Callable[[OutboxDataV1], None]
    inspect_record: Callable[[str], OutboxRecord]
    checkpoint: Callable[[str], Checkpoint]
    reopen: Callable[[], OutboxPort]
    source_resource: str = "conformance.source"
    source_schema: str = "conformance.source@1"
    target_labels: tuple[str, ...] = ()
    source_catalog: str = "structured"


@dataclass(frozen=True, slots=True)
class OutboxConformanceReport:
    checks: tuple[str, ...]
    same_owner_completion: str
    same_owner_release: str

    def to_mapping(self) -> dict[str, object]:
        return {
            "checks": list(self.checks),
            "sameOwnerCompletion": self.same_owner_completion,
            "sameOwnerRelease": self.same_owner_release,
            "generationFencing": "unsupported-by-owner-only-contract",
            "durability": "requires-adapter-owned-real-storage-and-restart-evidence",
        }


def _reject(call: Callable[[], object], code: str) -> None:
    try:
        call()
    except MeridianError as error:
        assert str(error.code) == code, error
    else:
        raise AssertionError(f"expected {code}")


def run_outbox_conformance(target: OutboxConformanceTarget) -> OutboxConformanceReport:
    """Run deterministic transitions in one fresh scope; raise on violation.

    Call without Python -O: the executable acceptance checks use assertions.
    Same-owner reclaim is characterized separately, never counted as fencing.
    """
    if not __debug__:
        raise RuntimeError("conformance assertions require Python without -O")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    fingerprint = "sha256:" + "a" * 64
    port = target.outbox
    checks: list[str] = []

    def event(name: str, *, identity: str | None = None, version: int = 1) -> OutboxDataV1:
        return OutboxDataV1(
            event_id=name,
            source_catalog=target.source_catalog,
            source_resource=target.source_resource,
            source_schema=target.source_schema,
            source_identity=identity or name,
            source_version=version,
            mutation_kind="put",
            payload={"id": identity or name},
            target_labels=target.target_labels,
            occurred_at=now,
        )

    def claim(owner: str, second: int, duration: int = 10) -> tuple[OutboxRecord, ...]:
        return port.atomic_claim(
            owner=owner,
            limit=1,
            lease_duration=timedelta(seconds=duration),
            now=now + timedelta(seconds=second),
        )

    def complete(
        data: OutboxDataV1, owner: str, second: int, version: str | int | None = 1
    ) -> Checkpoint:
        return port.complete(
            data.event_id,
            owner=owner,
            acknowledged_source_version=version,
            target_fingerprint=fingerprint,
            now=now + timedelta(seconds=second),
        )

    def release(data: OutboxDataV1, owner: str, second: int) -> OutboxRecord:
        return port.release(
            data.event_id,
            owner=owner,
            error=RuntimeError("token=synthetic-secret"),
            retryable=True,
            now=now + timedelta(seconds=second),
        )

    def unchanged(data: OutboxDataV1, before: OutboxRecord, checkpoint: Checkpoint) -> None:
        assert target.inspect_record(data.event_id) == before
        assert target.checkpoint(data.partition_key) == checkpoint

    first = event("01-first", identity="ordered")
    second = event("02-second", identity="ordered", version=2)
    independent = event("03-independent")
    target.seed(first)
    target.seed(second)
    target.seed(independent)
    assert port.lag(now=now).incomplete_count == 3
    (leased,) = claim("owner-a", 0, 1)
    assert leased.data == first and leased.state is OutboxState.LEASED
    assert leased.lease is not None and leased.lease.attempt == leased.attempt_count == 1
    assert leased.lease.owner == "owner-a"
    assert leased.lease.acquired_at == now
    assert leased.lease.expires_at == now + timedelta(seconds=1)
    (parallel,) = claim("owner-b", 0)
    assert parallel.data == independent
    assert claim("owner-c", 0) == ()
    assert complete(independent, "owner-b", 0).revision == 1
    checks.append("bounded-exclusive-claim-and-source-ordering")

    checkpoint = target.checkpoint(first.partition_key)
    assert checkpoint.revision == 0
    _reject(lambda: complete(first, "other", 0), "MERIDIAN_OUTBOX_LEASE_LOST")
    _reject(lambda: release(first, "other", 0), "MERIDIAN_OUTBOX_LEASE_LOST")
    unchanged(first, leased, checkpoint)
    _reject(lambda: complete(first, "owner-a", 1), "MERIDIAN_OUTBOX_LEASE_LOST")
    _reject(lambda: release(first, "owner-a", 1), "MERIDIAN_OUTBOX_LEASE_LOST")
    unchanged(first, leased, checkpoint)
    checks.append("wrong-owner-and-exact-expiry-rejection")

    port = target.reopen()
    (reclaimed,) = claim("owner-b", 2)
    assert reclaimed.data == first and reclaimed.attempt_count == 2
    assert reclaimed.lease is not None and reclaimed.lease.owner == "owner-b"
    _reject(lambda: complete(first, "owner-a", 3), "MERIDIAN_OUTBOX_LEASE_LOST")
    _reject(lambda: release(first, "owner-a", 3), "MERIDIAN_OUTBOX_LEASE_LOST")
    for wrong_version in (2, "1", True, None):
        _reject(
            partial(complete, first, "owner-b", 3, wrong_version), "MERIDIAN_CHECKPOINT_CONFLICT"
        )
        unchanged(first, reclaimed, checkpoint)
    checks.append("reopen-expiry-reclaim-and-exact-version-acknowledgement")
    committed = complete(first, "owner-b", 3)
    assert committed.revision == 1 and committed.event_id == first.event_id
    assert committed.source_version == 1 and committed.target_fingerprint == fingerprint
    assert committed.advanced_at == now + timedelta(seconds=3)
    port = target.reopen()
    assert target.checkpoint(first.partition_key) == committed
    finished = target.inspect_record(first.event_id)
    assert finished.state is OutboxState.COMPLETED and finished.lease is None
    assert (
        finished.target_fingerprint == fingerprint
        and finished.completed_at == committed.advanced_at
    )
    _reject(lambda: complete(first, "owner-b", 3), "MERIDIAN_OUTBOX_LEASE_LOST")
    assert target.checkpoint(first.partition_key) == committed
    (next_record,) = claim("owner-c", 4)
    assert next_record.data == second
    assert complete(second, "owner-c", 5, 2).revision == 2
    checks.append("completion-checkpoint-reopen-and-monotonic-revision")

    poison = event("03-poison")
    target.seed(poison)
    claim("owner-c", 6)
    released = release(poison, "owner-c", 7)
    assert released.state is OutboxState.RETRYABLE and released.lease is None
    assert target.checkpoint(poison.partition_key).revision == 0
    assert (
        released.failure is not None and "synthetic-secret" not in released.failure.redacted_cause
    )
    port = target.reopen()
    assert target.inspect_record(poison.event_id) == released
    (retried,) = claim("owner-d", 8)
    assert retried.attempt_count == 2
    quarantined = release(poison, "owner-d", 9)
    assert quarantined.state is OutboxState.QUARANTINED and quarantined.lease is None
    port = target.reopen()
    assert target.inspect_record(poison.event_id) == quarantined
    target.seed(event("04-after-poison", identity="03-poison"))
    assert claim("owner-e", 10) == ()
    assert target.checkpoint(poison.partition_key).revision == 0
    checks.append("retry-quarantine-redaction-reopen-and-no-silent-skip")

    # Old and current attempts carry identical owner-only call arguments.
    # Acceptance requires no stronger guarantee. Record the observed behavior.
    outcomes: dict[str, str] = {}
    for index, action in enumerate(("complete", "release"), start=5):
        data = event(f"0{index}-same-owner")
        target.seed(data)
        (old,) = claim("reused-owner", 11, 1)
        (current,) = claim("reused-owner", 13)
        assert old.data == current.data == data
        assert old.lease is not None and current.lease is not None
        assert old.lease.owner == current.lease.owner and current.attempt_count == 2
        try:
            if action == "complete":
                complete(data, old.lease.owner, 14)
                assert target.checkpoint(data.partition_key).revision == 1
            else:
                release(data, old.lease.owner, 14)
                assert target.checkpoint(data.partition_key).revision == 0
            outcomes[action] = "accepted-indistinguishable-owner"
        except MeridianError as error:
            assert str(error.code) == "MERIDIAN_OUTBOX_LEASE_LOST"
            outcomes[action] = "rejected-by-adapter-policy-not-port-guarantee"
    assert port.lag(now=now + timedelta(seconds=15)).incomplete_count >= 2
    return OutboxConformanceReport(tuple(checks), outcomes["complete"], outcomes["release"])
