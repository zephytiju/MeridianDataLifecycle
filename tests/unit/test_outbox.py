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
from typing import cast

import pytest

from meridian_storage import ConflictError, NotFoundError, TransientError
from meridian_storage.projection.errors import CheckpointConflict, LeaseLostError
from meridian_storage.projection.outbox import (
    Checkpoint,
    InMemoryOutboxStore,
    OutboxDataV1,
    OutboxState,
)

TARGET_FINGERPRINT = "sha256:" + "d" * 64


def _event(
    fixed_time: datetime,
    *,
    event_id: str,
    identity: str = "case-1",
    version: int = 1,
) -> OutboxDataV1:
    return OutboxDataV1(
        source_resource="investigation.cases",
        source_schema="investigation.case@1",
        source_identity=identity,
        source_version=version,
        mutation_kind="put",
        payload={"case_id": identity, "version": version},
        target_labels=("search",),
        occurred_at=fixed_time,
        operation_context={"tenant": "tenant-1"},
        event_id=event_id,
    )


def test_outbox_data_contract(fixed_time: datetime) -> None:
    event = _event(fixed_time, event_id="event-1")
    assert event.digest.startswith("sha256:")
    assert event.partition_key.startswith("sha256:")
    assert event.to_mapping()["formatVersion"] == "meridian.outbox.v1"
    replay = OutboxDataV1(
        source_resource=event.source_resource,
        source_schema=event.source_schema,
        source_identity=event.source_identity,
        source_version=event.source_version,
        mutation_kind=event.mutation_kind,
        payload=event.payload,
        occurred_at=event.occurred_at,
        operation_context=event.operation_context,
        target_labels=event.target_labels,
        event_id=event.event_id,
        digest=event.digest,
    )
    assert replay == event
    with pytest.raises(ValueError, match="payload or immutable"):
        OutboxDataV1("r", "s", "id", "put")
    with pytest.raises(ValueError, match="mutually exclusive"):
        OutboxDataV1("r", "s", "id", "put", payload={}, immutable_reference={})
    with pytest.raises(ValueError, match="duplicates"):
        OutboxDataV1("r", "s", "id", "put", payload={}, target_labels=("x", "x"))
    with pytest.raises(ValueError, match="digest"):
        OutboxDataV1("r", "s", "id", "put", payload={}, digest="sha256:bad")
    with pytest.raises(ValueError, match="format_version"):
        OutboxDataV1("r", "s", "id", "put", payload={}, format_version="future")
    with pytest.raises(ValueError, match="unknown source Catalog"):
        OutboxDataV1("r", "s", "id", "put", payload={}, source_catalog="projection")
    with pytest.raises(ValueError, match="source_version"):
        OutboxDataV1("r", "s", "id", "put", payload={}, source_version=True)
    with pytest.raises(ValueError, match="operation_context"):
        OutboxDataV1(
            "r",
            "s",
            "id",
            "put",
            payload={},
            operation_context={"attempt": cast(str, 1)},
        )

    mutable_payload = {"nested": {"items": [1]}}
    immutable = OutboxDataV1("r", "s", "id", "put", payload=mutable_payload)
    cast(dict[str, object], mutable_payload["nested"])["items"] = [2]
    assert immutable.to_mapping()["payload"] == {"nested": {"items": [1]}}


def test_claim_order_completion_and_lag(fixed_time: datetime) -> None:
    store = InMemoryOutboxStore(poison_threshold=2, visibility_target_seconds=30)
    first = _event(fixed_time, event_id="a", version=1)
    second = _event(fixed_time + timedelta(seconds=1), event_id="b", version=2)
    independent = _event(
        fixed_time + timedelta(seconds=2), event_id="c", identity="case-2", version=1
    )
    store.append_many((first, second, independent))
    store.append(first)
    with pytest.raises(ConflictError):
        store.append(
            OutboxDataV1(
                "other",
                "schema@1",
                "id",
                "put",
                payload={},
                event_id="a",
                occurred_at=fixed_time,
            )
        )
    claimed = store.atomic_claim(
        owner="worker", limit=10, lease_duration=timedelta(seconds=10), now=fixed_time
    )
    assert [item.data.event_id for item in claimed] == ["a", "c"]
    with pytest.raises(CheckpointConflict):
        store.complete(
            "a",
            owner="worker",
            acknowledged_source_version=999,
            target_fingerprint=TARGET_FINGERPRINT,
            now=fixed_time,
        )
    checkpoint = store.complete(
        "a",
        owner="worker",
        acknowledged_source_version=1,
        target_fingerprint=TARGET_FINGERPRINT,
        now=fixed_time,
    )
    assert checkpoint.revision == 1
    assert store.get("a").state is OutboxState.COMPLETED
    assert store.checkpoint(first.partition_key) == checkpoint
    next_claim = store.atomic_claim(
        owner="worker", limit=1, lease_duration=timedelta(seconds=10), now=fixed_time
    )
    assert next_claim[0].data.event_id == "b"
    lag = store.lag(now=fixed_time + timedelta(seconds=40))
    assert lag.incomplete_count == 2
    assert not lag.within_target


def test_lease_expiry_retry_and_quarantine(fixed_time: datetime) -> None:
    store = InMemoryOutboxStore(poison_threshold=2)
    event = _event(fixed_time, event_id="event")
    store.append(event)
    store.atomic_claim(owner="one", limit=1, lease_duration=timedelta(seconds=1), now=fixed_time)
    with pytest.raises(LeaseLostError):
        store.complete(
            "event",
            owner="two",
            acknowledged_source_version=1,
            target_fingerprint=TARGET_FINGERPRINT,
            now=fixed_time,
        )
    reclaimed = store.atomic_claim(
        owner="two",
        limit=1,
        lease_duration=timedelta(seconds=5),
        now=fixed_time + timedelta(seconds=2),
    )
    assert reclaimed[0].attempt_count == 2
    released = store.release(
        "event",
        owner="two",
        error=TransientError("TEMP", "token=secret endpoint=http://private"),
        retryable=True,
        now=fixed_time + timedelta(seconds=2),
    )
    assert released.state is OutboxState.QUARANTINED
    assert released.failure is not None
    assert "secret" not in released.failure.redacted_cause
    retried = store.operator_retry("event", reason="operator corrected target")
    assert retried.state is OutboxState.RETRYABLE
    with pytest.raises(ConflictError):
        store.operator_retry("event", reason="again")
    with pytest.raises(ValueError):
        store.operator_retry("event", reason=" ")
    with pytest.raises(NotFoundError):
        store.get("missing")


def test_checkpoint_cas_and_argument_validation(fixed_time: datetime) -> None:
    store = InMemoryOutboxStore()
    checkpoint = Checkpoint("partition", 1, event_id="event", advanced_at=fixed_time)
    assert store.compare_and_set_checkpoint(checkpoint, expected_revision=0) == checkpoint
    with pytest.raises(CheckpointConflict):
        store.compare_and_set_checkpoint(Checkpoint("partition", 2), expected_revision=0)
    with pytest.raises(CheckpointConflict):
        store.compare_and_set_checkpoint(Checkpoint("other", 2), expected_revision=0)
    with pytest.raises(ValueError):
        store.atomic_claim(owner="", limit=1, lease_duration=timedelta(seconds=1))
    with pytest.raises(ValueError):
        store.atomic_claim(owner="x", limit=0, lease_duration=timedelta(seconds=1))
    with pytest.raises(ValueError):
        store.atomic_claim(owner="x", limit=1, lease_duration=timedelta(0))
    event = _event(fixed_time, event_id="fingerprint")
    store.append(event)
    store.atomic_claim(owner="x", limit=1, lease_duration=timedelta(seconds=1), now=fixed_time)
    with pytest.raises(ValueError, match="target_fingerprint"):
        store.complete(
            "fingerprint",
            owner="x",
            acknowledged_source_version=event.source_version,
            target_fingerprint="not-a-digest",
            now=fixed_time,
        )
