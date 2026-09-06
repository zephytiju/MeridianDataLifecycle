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

"""Outbox persistence port and deterministic in-memory reference store."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Protocol

from meridian_storage import ConflictError, MeridianError, NotFoundError
from meridian_storage.projection._canonical import ensure_utc, require_sha256, utc_now
from meridian_storage.projection.errors import CheckpointConflict, LeaseLostError
from meridian_storage.projection.outbox.model import (
    Checkpoint,
    OutboxDataV1,
    OutboxFailure,
    OutboxLease,
    OutboxRecord,
    OutboxState,
    ProjectionLag,
)

_SENSITIVE_VALUE = re.compile(r"(?i)(password|secret|token|credential|endpoint)\s*[:=]\s*[^\s,;]+")


class OutboxPort(Protocol):
    """Durable adapter port required by :class:`ProjectionRunner`.

    Completion and release check the current non-expired owner. This protocol
    carries no claim generation: reusing an owner while an older attempt can
    still call the port does not fence that attempt. Hosts must avoid overlapping
    owner reuse. ``OutboxLease.attempt`` is observation metadata, not a token.
    """

    def atomic_claim(
        self,
        *,
        owner: str,
        limit: int,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> tuple[OutboxRecord, ...]: ...

    def complete(
        self,
        event_id: str,
        *,
        owner: str,
        acknowledged_source_version: str | int | None,
        target_fingerprint: str,
        now: datetime | None = None,
    ) -> Checkpoint: ...

    def release(
        self,
        event_id: str,
        *,
        owner: str,
        error: BaseException,
        retryable: bool,
        now: datetime | None = None,
    ) -> OutboxRecord: ...

    def lag(self, *, now: datetime | None = None) -> ProjectionLag: ...


@dataclass(slots=True)
class _MutableRecord:
    data: OutboxDataV1
    state: OutboxState = OutboxState.PENDING
    attempt_count: int = 0
    lease: OutboxLease | None = None
    failure: OutboxFailure | None = None
    target_fingerprint: str | None = None
    completed_at: datetime | None = None

    def snapshot(self) -> OutboxRecord:
        return OutboxRecord(
            data=self.data,
            state=self.state,
            attempt_count=self.attempt_count,
            lease=self.lease,
            failure=self.failure,
            target_fingerprint=self.target_fingerprint,
            completed_at=self.completed_at,
        )


class InMemoryOutboxStore:
    """Thread-safe conformance implementation; adapters own production durability."""

    def __init__(self, *, poison_threshold: int = 5, visibility_target_seconds: float = 30) -> None:
        if poison_threshold < 1:
            raise ValueError("poison_threshold must be positive")
        if visibility_target_seconds <= 0:
            raise ValueError("visibility_target_seconds must be positive")
        self._poison_threshold = poison_threshold
        self._visibility_target_seconds = visibility_target_seconds
        self._records: dict[str, _MutableRecord] = {}
        self._checkpoints: dict[str, Checkpoint] = {}
        self._lock = RLock()

    def append(self, data: OutboxDataV1) -> None:
        """Append idempotently by event id and canonical digest."""

        with self._lock:
            existing = self._records.get(data.event_id)
            if existing is None:
                self._records[data.event_id] = _MutableRecord(data=data)
                return
            if existing.data.digest != data.digest:
                raise ConflictError(
                    "MERIDIAN_OUTBOX_IDEMPOTENCY_CONFLICT",
                    f"event {data.event_id!r} already exists with different content",
                )

    def atomic_claim(
        self,
        *,
        owner: str,
        limit: int,
        lease_duration: timedelta,
        now: datetime | None = None,
    ) -> tuple[OutboxRecord, ...]:
        if not owner:
            raise ValueError("owner must be non-empty")
        if limit < 1:
            raise ValueError("limit must be positive")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        current = ensure_utc(now or utc_now(), field="now")
        claimed: list[OutboxRecord] = []
        with self._lock:
            ordered = sorted(
                self._records.values(),
                key=lambda record: (record.data.occurred_at, record.data.event_id),
            )
            seen_partitions: set[str] = set()
            for record in ordered:
                if record.state is OutboxState.COMPLETED:
                    continue
                partition = record.data.partition_key
                if partition in seen_partitions:
                    continue
                seen_partitions.add(partition)
                if record.state is OutboxState.LEASED:
                    assert record.lease is not None
                    if record.lease.expires_at > current:
                        continue
                    record.state = OutboxState.RETRYABLE
                    record.lease = None
                if record.state not in (OutboxState.PENDING, OutboxState.RETRYABLE):
                    continue
                record.attempt_count += 1
                record.failure = None
                record.state = OutboxState.LEASED
                record.lease = OutboxLease(
                    owner=owner,
                    acquired_at=current,
                    expires_at=current + lease_duration,
                    attempt=record.attempt_count,
                )
                claimed.append(record.snapshot())
                if len(claimed) == limit:
                    break
        return tuple(claimed)

    def _leased_record(self, event_id: str, *, owner: str, now: datetime) -> _MutableRecord:
        record = self._records.get(event_id)
        if record is None:
            raise NotFoundError(
                "MERIDIAN_OUTBOX_NOT_FOUND", f"outbox event {event_id!r} was not found"
            )
        lease = record.lease
        if record.state is not OutboxState.LEASED or lease is None:
            raise LeaseLostError(f"outbox event {event_id!r} is not leased")
        if lease.owner != owner:
            raise LeaseLostError(f"outbox event {event_id!r} is leased by another owner")
        if lease.expires_at <= now:
            raise LeaseLostError(f"lease for outbox event {event_id!r} has expired")
        return record

    def complete(
        self,
        event_id: str,
        *,
        owner: str,
        acknowledged_source_version: str | int | None,
        target_fingerprint: str,
        now: datetime | None = None,
    ) -> Checkpoint:
        require_sha256(target_fingerprint, field="target_fingerprint")
        current = ensure_utc(now or utc_now(), field="now")
        with self._lock:
            record = self._leased_record(event_id, owner=owner, now=current)
            if (
                type(acknowledged_source_version) is not type(record.data.source_version)
                or acknowledged_source_version != record.data.source_version
            ):
                raise CheckpointConflict(
                    "target acknowledgement does not match the exact source version"
                )
            record.state = OutboxState.COMPLETED
            record.lease = None
            record.failure = None
            record.target_fingerprint = target_fingerprint
            record.completed_at = current
            partition = record.data.partition_key
            previous = self._checkpoints.get(partition)
            checkpoint = Checkpoint(
                partition_key=partition,
                revision=1 if previous is None else previous.revision + 1,
                event_id=event_id,
                source_version=record.data.source_version,
                target_fingerprint=target_fingerprint,
                advanced_at=current,
            )
            self._checkpoints[partition] = checkpoint
            return checkpoint

    @staticmethod
    def _redacted_failure(error: BaseException, *, failed_at: datetime) -> OutboxFailure:
        if isinstance(error, MeridianError):
            code = str(error.code)
        else:
            code = f"{type(error).__module__}.{type(error).__qualname__}"
        cause = _SENSITIVE_VALUE.sub(r"\1=<redacted>", str(error)).strip()
        cause = (cause or type(error).__name__)[:512]
        return OutboxFailure(code=code, redacted_cause=cause, failed_at=failed_at)

    def release(
        self,
        event_id: str,
        *,
        owner: str,
        error: BaseException,
        retryable: bool,
        now: datetime | None = None,
    ) -> OutboxRecord:
        current = ensure_utc(now or utc_now(), field="now")
        with self._lock:
            record = self._leased_record(event_id, owner=owner, now=current)
            record.failure = self._redacted_failure(error, failed_at=current)
            record.lease = None
            if not retryable or record.attempt_count >= self._poison_threshold:
                record.state = OutboxState.QUARANTINED
            else:
                record.state = OutboxState.RETRYABLE
            return record.snapshot()

    def operator_retry(self, event_id: str, *, reason: str) -> OutboxRecord:
        """Explicitly return one quarantined event to retryable state."""

        if not reason.strip():
            raise ValueError("operator retry reason must be non-empty")
        with self._lock:
            record = self._records.get(event_id)
            if record is None:
                raise NotFoundError(
                    "MERIDIAN_OUTBOX_NOT_FOUND", f"outbox event {event_id!r} was not found"
                )
            if record.state is not OutboxState.QUARANTINED:
                raise ConflictError(
                    "MERIDIAN_OUTBOX_STATE_CONFLICT",
                    f"outbox event {event_id!r} is not quarantined",
                )
            record.state = OutboxState.RETRYABLE
            record.failure = None
            return record.snapshot()

    def compare_and_set_checkpoint(
        self,
        checkpoint: Checkpoint,
        *,
        expected_revision: int,
    ) -> Checkpoint:
        """Reference CAS hook for adapter conformance fixtures."""

        with self._lock:
            current = self._checkpoints.get(checkpoint.partition_key)
            revision = 0 if current is None else current.revision
            if revision != expected_revision:
                raise CheckpointConflict(
                    f"checkpoint revision {revision} does not match {expected_revision}"
                )
            if checkpoint.revision != expected_revision + 1:
                raise CheckpointConflict("new checkpoint revision must increment by one")
            self._checkpoints[checkpoint.partition_key] = checkpoint
            return checkpoint

    def checkpoint(self, partition_key: str) -> Checkpoint:
        with self._lock:
            return self._checkpoints.get(partition_key, Checkpoint(partition_key, 0))

    def get(self, event_id: str) -> OutboxRecord:
        with self._lock:
            record = self._records.get(event_id)
            if record is None:
                raise NotFoundError(
                    "MERIDIAN_OUTBOX_NOT_FOUND", f"outbox event {event_id!r} was not found"
                )
            return record.snapshot()

    def records(self) -> tuple[OutboxRecord, ...]:
        with self._lock:
            return tuple(
                record.snapshot()
                for record in sorted(
                    self._records.values(),
                    key=lambda item: (item.data.occurred_at, item.data.event_id),
                )
            )

    def lag(self, *, now: datetime | None = None) -> ProjectionLag:
        current = ensure_utc(now or utc_now(), field="now")
        with self._lock:
            incomplete = [
                record.data.occurred_at
                for record in self._records.values()
                if record.state is not OutboxState.COMPLETED
            ]
        oldest = min(incomplete) if incomplete else None
        lag_seconds = max(0.0, (current - oldest).total_seconds()) if oldest else 0.0
        return ProjectionLag(
            incomplete_count=len(incomplete),
            oldest_incomplete_at=oldest,
            lag_seconds=lag_seconds,
            target_seconds=self._visibility_target_seconds,
        )

    def append_many(self, records: Sequence[OutboxDataV1]) -> None:
        for record in records:
            self.append(record)
