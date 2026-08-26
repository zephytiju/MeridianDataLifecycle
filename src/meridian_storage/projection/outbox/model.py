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

"""Transactional outbox wire and state models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Final
from uuid import uuid4

from meridian_storage.semantics import CatalogName

from meridian_storage.projection._canonical import (
    ensure_utc,
    immutable_mapping,
    immutable_value,
    rfc3339,
    sha256_digest,
    to_json_value,
    utc_now,
)

OUTBOX_FORMAT_VERSION: Final = "meridian.outbox.v1"
_CATALOG_NAMES: Final = frozenset(catalog.value for catalog in CatalogName)


class OutboxState(StrEnum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    COMPLETED = "COMPLETED"
    RETRYABLE = "RETRYABLE"
    QUARANTINED = "QUARANTINED"


@dataclass(frozen=True, slots=True)
class OutboxDataV1:
    """Immutable projection intent committed with an authoritative mutation."""

    source_resource: str
    source_schema: str
    source_identity: object
    mutation_kind: str
    source_version: str | int | None = None
    payload: Mapping[str, object] | None = None
    immutable_reference: Mapping[str, object] | None = None
    target_labels: tuple[str, ...] = ()
    occurred_at: datetime = field(default_factory=utc_now)
    operation_context: Mapping[str, str] = field(default_factory=dict)
    source_catalog: str = "structured"
    event_id: str = field(default_factory=lambda: str(uuid4()))
    digest: str = ""
    format_version: str = OUTBOX_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != OUTBOX_FORMAT_VERSION:
            raise ValueError(f"format_version must be {OUTBOX_FORMAT_VERSION!r}")
        for field_name in (
            "source_resource",
            "source_schema",
            "mutation_kind",
            "source_catalog",
            "event_id",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must be non-empty")
        if self.payload is None and self.immutable_reference is None:
            raise ValueError("outbox data requires a payload or immutable reference")
        if self.payload is not None and self.immutable_reference is not None:
            raise ValueError("payload and immutable_reference are mutually exclusive")
        if len(set(self.target_labels)) != len(self.target_labels):
            raise ValueError("target_labels must not contain duplicates")
        if self.source_catalog not in _CATALOG_NAMES:
            raise ValueError(f"unknown source Catalog {self.source_catalog!r}")
        if isinstance(self.source_version, bool):
            raise ValueError("source_version cannot be boolean")
        if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in self.operation_context.items()
        ):
            raise ValueError("operation_context must contain only string keys and values")
        object.__setattr__(self, "source_identity", immutable_value(self.source_identity))
        object.__setattr__(self, "target_labels", tuple(self.target_labels))
        object.__setattr__(self, "occurred_at", ensure_utc(self.occurred_at, field="occurred_at"))
        object.__setattr__(
            self,
            "operation_context",
            immutable_mapping(dict(self.operation_context)),
        )
        if self.payload is not None:
            object.__setattr__(self, "payload", immutable_mapping(self.payload))
        if self.immutable_reference is not None:
            object.__setattr__(
                self,
                "immutable_reference",
                immutable_mapping(self.immutable_reference),
            )
        expected = sha256_digest(self._digest_material())
        if self.digest and self.digest != expected:
            raise ValueError("outbox digest does not match canonical content")
        object.__setattr__(self, "digest", expected)

    @property
    def partition_key(self) -> str:
        """Stable ordering key for one source Data identity."""

        return sha256_digest(
            {
                "sourceCatalog": self.source_catalog,
                "sourceResource": self.source_resource,
                "sourceIdentity": self.source_identity,
            }
        )

    def _digest_material(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "eventId": self.event_id,
            "sourceCatalog": self.source_catalog,
            "sourceResource": self.source_resource,
            "sourceSchema": self.source_schema,
            "sourceIdentity": self.source_identity,
            "sourceVersion": self.source_version,
            "mutationKind": self.mutation_kind,
            "payload": self.payload,
            "immutableReference": self.immutable_reference,
            "targetLabels": self.target_labels,
            "occurredAt": rfc3339(self.occurred_at),
            "operationContext": self.operation_context,
        }

    def to_mapping(self) -> dict[str, object]:
        material = to_json_value(self._digest_material())
        assert isinstance(material, dict)
        return {
            **material,
            "digest": self.digest,
        }


@dataclass(frozen=True, slots=True)
class OutboxLease:
    owner: str
    acquired_at: datetime
    expires_at: datetime
    attempt: int

    def __post_init__(self) -> None:
        if not self.owner:
            raise ValueError("lease owner must be non-empty")
        if self.attempt < 1:
            raise ValueError("lease attempt must be positive")
        acquired = ensure_utc(self.acquired_at, field="acquired_at")
        expires = ensure_utc(self.expires_at, field="expires_at")
        if expires <= acquired:
            raise ValueError("lease expiry must follow acquisition")
        object.__setattr__(self, "acquired_at", acquired)
        object.__setattr__(self, "expires_at", expires)


@dataclass(frozen=True, slots=True)
class OutboxFailure:
    code: str
    redacted_cause: str
    failed_at: datetime

    def __post_init__(self) -> None:
        if not self.code or not self.redacted_cause:
            raise ValueError("failure code and redacted cause must be non-empty")
        object.__setattr__(self, "failed_at", ensure_utc(self.failed_at, field="failed_at"))


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    data: OutboxDataV1
    state: OutboxState
    attempt_count: int = 0
    lease: OutboxLease | None = None
    failure: OutboxFailure | None = None
    target_fingerprint: str | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.attempt_count < 0:
            raise ValueError("outbox attempt_count cannot be negative")
        if (self.state is OutboxState.LEASED) != (self.lease is not None):
            raise ValueError("only a LEASED outbox record may contain a lease")
        if self.completed_at is not None:
            object.__setattr__(
                self, "completed_at", ensure_utc(self.completed_at, field="completed_at")
            )


@dataclass(frozen=True, slots=True)
class Checkpoint:
    partition_key: str
    revision: int
    event_id: str | None = None
    source_version: str | int | None = None
    target_fingerprint: str | None = None
    advanced_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.partition_key or self.revision < 0:
            raise ValueError("checkpoint partition_key and revision are invalid")
        if self.advanced_at is not None:
            object.__setattr__(
                self, "advanced_at", ensure_utc(self.advanced_at, field="advanced_at")
            )


@dataclass(frozen=True, slots=True)
class ProjectionLag:
    incomplete_count: int
    oldest_incomplete_at: datetime | None
    lag_seconds: float
    target_seconds: float

    def __post_init__(self) -> None:
        if self.incomplete_count < 0 or self.lag_seconds < 0 or self.target_seconds <= 0:
            raise ValueError("projection lag counts and durations are invalid")

    @property
    def within_target(self) -> bool:
        return self.lag_seconds <= self.target_seconds
