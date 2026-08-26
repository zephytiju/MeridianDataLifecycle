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

"""Projection rebuild and generation cutover coordination."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from meridian_storage.projection._canonical import (
    ensure_utc,
    require_sha256,
    rfc3339,
    sha256_digest,
    utc_now,
)
from meridian_storage.projection.control import CancellationToken
from meridian_storage.projection.errors import MigrationConflict, MigrationFailed
from meridian_storage.projection.evidence import EvidenceSink, LifecycleEvidence, NullEvidenceSink


@dataclass(frozen=True, slots=True)
class RebuildPlan:
    projection: str
    source_resource: str
    target_resource: str
    batch_size: int = 500
    rollback_interval: timedelta = timedelta(hours=24)

    def __post_init__(self) -> None:
        if not self.projection or not self.source_resource or not self.target_resource:
            raise ValueError("rebuild projection and Resources must be non-empty")
        if self.batch_size < 1 or self.batch_size > 10_000:
            raise ValueError("rebuild batch_size must be between 1 and 10000")
        if self.rollback_interval <= timedelta(0):
            raise ValueError("rollback_interval must be positive")


@dataclass(frozen=True, slots=True)
class RebuildBoundary:
    boundary_id: str
    source_fingerprint: str

    def __post_init__(self) -> None:
        if not self.boundary_id:
            raise ValueError("rebuild boundary_id must be non-empty")
        require_sha256(self.source_fingerprint, field="source_fingerprint")


@dataclass(frozen=True, slots=True)
class RebuildGeneration:
    generation_ref: str
    previous_generation_ref: str

    def __post_init__(self) -> None:
        if not self.generation_ref or not self.previous_generation_ref:
            raise ValueError("rebuild generation references must be non-empty")
        if self.generation_ref == self.previous_generation_ref:
            raise ValueError("rebuild generation must differ from the previous generation")


@dataclass(frozen=True, slots=True)
class RebuildTailResult:
    applied_events: int
    final_boundary: str

    def __post_init__(self) -> None:
        if self.applied_events < 0 or not self.final_boundary:
            raise ValueError("rebuild tail result is invalid")


@dataclass(frozen=True, slots=True)
class RebuildValidation:
    passed: bool
    source_count: int
    target_count: int
    sampled_digest: str
    report_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.passed, bool) or self.source_count < 0 or self.target_count < 0:
            raise ValueError("rebuild validation counts or state are invalid")
        require_sha256(self.sampled_digest, field="sampled_digest")
        require_sha256(self.report_digest, field="report_digest")


@dataclass(frozen=True, slots=True)
class RebuildActivation:
    previous_generation_ref: str
    active_generation_ref: str
    target_fingerprint: str

    def __post_init__(self) -> None:
        if not self.previous_generation_ref or not self.active_generation_ref:
            raise ValueError("rebuild activation generation references must be non-empty")
        require_sha256(self.target_fingerprint, field="target_fingerprint")


@dataclass(frozen=True, slots=True)
class RebuildEvidence:
    projection: str
    source_boundary: str
    source_fingerprint: str
    target_fingerprint: str
    previous_generation: str
    active_generation: str
    validation_report_digest: str
    applied_tail_events: int
    rollback_until: datetime
    activated_at: datetime
    digest: str = ""

    def __post_init__(self) -> None:
        for field_name in (
            "projection",
            "source_boundary",
            "previous_generation",
            "active_generation",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must be non-empty")
        for field_name in (
            "source_fingerprint",
            "target_fingerprint",
            "validation_report_digest",
        ):
            require_sha256(getattr(self, field_name), field=field_name)
        if self.applied_tail_events < 0:
            raise ValueError("applied_tail_events cannot be negative")
        object.__setattr__(
            self, "rollback_until", ensure_utc(self.rollback_until, field="rollback_until")
        )
        object.__setattr__(
            self, "activated_at", ensure_utc(self.activated_at, field="activated_at")
        )
        expected = sha256_digest(self._digest_material())
        if self.digest and self.digest != expected:
            raise ValueError("rebuild evidence digest does not match canonical content")
        object.__setattr__(self, "digest", expected)

    def _digest_material(self) -> dict[str, object]:
        return {
            "projection": self.projection,
            "sourceBoundary": self.source_boundary,
            "sourceFingerprint": self.source_fingerprint,
            "targetFingerprint": self.target_fingerprint,
            "previousGeneration": self.previous_generation,
            "activeGeneration": self.active_generation,
            "validationReportDigest": self.validation_report_digest,
            "appliedTailEvents": self.applied_tail_events,
            "rollbackUntil": rfc3339(self.rollback_until),
            "activatedAt": rfc3339(self.activated_at),
        }

    def to_mapping(self) -> dict[str, object]:
        return {**self._digest_material(), "digest": self.digest}


class RebuildHooks(Protocol):
    """Adapter-owned generation and activation operations."""

    def capture_boundary(self, plan: RebuildPlan) -> RebuildBoundary: ...

    def create_generation(
        self, plan: RebuildPlan, boundary: RebuildBoundary
    ) -> RebuildGeneration: ...

    def scan(
        self, plan: RebuildPlan, boundary: RebuildBoundary
    ) -> Iterable[Sequence[Mapping[str, object]]]: ...

    def write(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        records: Sequence[Mapping[str, object]],
    ) -> None: ...

    def tail(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        boundary: RebuildBoundary,
    ) -> RebuildTailResult: ...

    def validate(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        boundary: RebuildBoundary,
    ) -> RebuildValidation: ...

    def activate(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        *,
        expected_previous: str,
    ) -> RebuildActivation: ...

    def retain(self, generation_ref: str, *, until: datetime) -> None: ...

    def discard(self, generation_ref: str) -> None: ...


class RebuildCoordinator:
    """Coordinate scan, tail, validation, atomic activation, and rollback retention."""

    def __init__(
        self,
        *,
        hooks: RebuildHooks,
        evidence: EvidenceSink | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._hooks = hooks
        self._evidence = evidence or NullEvidenceSink()
        self._clock = clock

    def rebuild(
        self,
        plan: RebuildPlan,
        *,
        cancellation: CancellationToken | None = None,
    ) -> RebuildEvidence:
        token = cancellation or CancellationToken()
        token.raise_if_cancelled()
        boundary = self._hooks.capture_boundary(plan)
        generation = self._hooks.create_generation(plan, boundary)
        activation_attempted = False
        try:
            for batch in self._hooks.scan(plan, boundary):
                token.raise_if_cancelled()
                if len(batch) > plan.batch_size:
                    raise MigrationFailed("rebuild scan exceeded the configured batch size")
                self._hooks.write(plan, generation, batch)
            token.raise_if_cancelled()
            tail = self._hooks.tail(plan, generation, boundary)
            token.raise_if_cancelled()
            validation = self._hooks.validate(plan, generation, boundary)
            if not validation.passed or validation.source_count != validation.target_count:
                raise MigrationFailed("rebuild validation failed")
            activation_attempted = True
            activation = self._hooks.activate(
                plan,
                generation,
                expected_previous=generation.previous_generation_ref,
            )
            if activation.previous_generation_ref != generation.previous_generation_ref:
                raise MigrationConflict("rebuild activation replaced an unexpected generation")
            if activation.active_generation_ref != generation.generation_ref:
                raise MigrationConflict("rebuild activation acknowledged the wrong generation")
            activated_at = ensure_utc(self._clock(), field="activated_at")
            rollback_until = activated_at + plan.rollback_interval
            self._hooks.retain(activation.previous_generation_ref, until=rollback_until)
            cutover = RebuildEvidence(
                projection=plan.projection,
                source_boundary=tail.final_boundary,
                source_fingerprint=boundary.source_fingerprint,
                target_fingerprint=activation.target_fingerprint,
                previous_generation=activation.previous_generation_ref,
                active_generation=activation.active_generation_ref,
                validation_report_digest=validation.report_digest,
                applied_tail_events=tail.applied_events,
                rollback_until=rollback_until,
                activated_at=activated_at,
            )
            self._evidence.record(
                LifecycleEvidence(
                    kind="projection-rebuild",
                    subject=plan.projection,
                    state="COMPLETED",
                    details=cutover.to_mapping(),
                    occurred_at=activated_at,
                )
            )
            return cutover
        finally:
            if not activation_attempted:
                self._hooks.discard(generation.generation_ref)
