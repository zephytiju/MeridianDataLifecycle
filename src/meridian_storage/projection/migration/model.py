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

"""Schema migration bundle, checkpoint, and cutover evidence contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Final
from uuid import uuid4

from meridian_storage import Operation
from meridian_storage.projection._canonical import (
    ensure_utc,
    immutable_mapping,
    require_sha256,
    rfc3339,
    sha256_digest,
    to_json_value,
    utc_now,
)

MIGRATION_FORMAT_VERSION: Final = "meridian.migration-bundle.v1"


class MigrationStepKind(StrEnum):
    ADD_FIELD = "add-field"
    BACKFILL = "backfill"
    ADD_OR_REPLACE_INDEX = "add-or-replace-index"
    TIGHTEN_CONSTRAINT = "tighten-constraint"
    TRANSFORM_RECORD = "transform-record"
    ACTIVATE_SCHEMA_VERSION = "activate-schema-version"
    RETIRE_PHYSICAL_GENERATION = "retire-physical-generation"


class LockClass(StrEnum):
    NONE = "none"
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class Reversibility(StrEnum):
    REVERSIBLE = "reversible"
    REACTIVATE_RETAINED_GENERATION = "reactivate-retained-generation"
    RESTORE_REQUIRED = "restore-required"


class MigrationState(StrEnum):
    PLANNED = "PLANNED"
    APPLYING = "APPLYING"
    VALIDATING = "VALIDATING"
    ACTIVATING = "ACTIVATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class MigrationCondition:
    name: str
    operation: Mapping[str, object]
    expectation: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("migration condition name must be non-empty")
        try:
            operation = Operation.from_mapping(self.operation)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "migration condition operation must be a serialized Meridian Operation V1"
            ) from error
        object.__setattr__(self, "operation", immutable_mapping(operation.to_dict()))
        object.__setattr__(self, "expectation", immutable_mapping(self.expectation))

    def to_mapping(self) -> dict[str, object]:
        return {
            "name": self.name,
            "operation": to_json_value(self.operation),
            "expectation": to_json_value(self.expectation),
        }


@dataclass(frozen=True, slots=True)
class MigrationStep:
    step_id: str
    kind: MigrationStepKind
    parameters: Mapping[str, object] = field(default_factory=dict)
    compiler_requirements: tuple[str, ...] = ()
    reversibility: Reversibility = Reversibility.REVERSIBLE

    def __post_init__(self) -> None:
        if not self.step_id:
            raise ValueError("migration step_id must be non-empty")
        if not all(self.compiler_requirements) or len(set(self.compiler_requirements)) != len(
            self.compiler_requirements
        ):
            raise ValueError("step compiler requirements must be unique")
        object.__setattr__(self, "parameters", immutable_mapping(self.parameters))

    @property
    def digest(self) -> str:
        return sha256_digest(self.to_mapping())

    def to_mapping(self) -> dict[str, object]:
        return {
            "stepId": self.step_id,
            "kind": self.kind.value,
            "parameters": to_json_value(self.parameters),
            "compilerRequirements": list(self.compiler_requirements),
            "reversibility": self.reversibility.value,
        }


@dataclass(frozen=True, slots=True)
class MigrationBundleV1:
    source_schema_fingerprint: str
    target_schema_fingerprint: str
    steps: tuple[MigrationStep, ...]
    preconditions: tuple[MigrationCondition, ...] = ()
    validations: tuple[MigrationCondition, ...] = ()
    postconditions: tuple[MigrationCondition, ...] = ()
    compiler_requirements: tuple[str, ...] = ()
    estimated_lock_class: LockClass = LockClass.EXCLUSIVE
    reversibility: Reversibility = Reversibility.RESTORE_REQUIRED
    bundle_id: str = field(default_factory=lambda: str(uuid4()))
    digest: str = ""
    format_version: str = MIGRATION_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != MIGRATION_FORMAT_VERSION:
            raise ValueError(f"format_version must be {MIGRATION_FORMAT_VERSION!r}")
        for name in ("source_schema_fingerprint", "target_schema_fingerprint"):
            require_sha256(getattr(self, name), field=name)
        if not self.bundle_id or not self.steps:
            raise ValueError("migration bundle_id and steps must be non-empty")
        step_ids = [step.step_id for step in self.steps]
        if len(set(step_ids)) != len(step_ids):
            raise ValueError("migration step ids must be unique")
        condition_names = [
            item.name for item in (*self.preconditions, *self.validations, *self.postconditions)
        ]
        if len(set(condition_names)) != len(condition_names):
            raise ValueError("migration condition names must be unique")
        if not all(self.compiler_requirements) or len(set(self.compiler_requirements)) != len(
            self.compiler_requirements
        ):
            raise ValueError("bundle compiler requirements must be unique")
        expected = sha256_digest(self._digest_material())
        if self.digest and self.digest != expected:
            raise ValueError("migration bundle digest does not match canonical content")
        object.__setattr__(self, "digest", expected)

    def _digest_material(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "bundleId": self.bundle_id,
            "sourceSchemaFingerprint": self.source_schema_fingerprint,
            "targetSchemaFingerprint": self.target_schema_fingerprint,
            "steps": [step.to_mapping() for step in self.steps],
            "preconditions": [item.to_mapping() for item in self.preconditions],
            "validations": [item.to_mapping() for item in self.validations],
            "postconditions": [item.to_mapping() for item in self.postconditions],
            "compilerRequirements": list(self.compiler_requirements),
            "estimatedLockClass": self.estimated_lock_class.value,
            "reversibility": self.reversibility.value,
        }

    def to_mapping(self) -> dict[str, object]:
        return {**self._digest_material(), "digest": self.digest}


@dataclass(frozen=True, slots=True)
class BindingStateEvidence:
    logical_fingerprint: str
    physical_fingerprint: str
    active_generation: str
    revision: str | int

    def __post_init__(self) -> None:
        require_sha256(self.logical_fingerprint, field="logical_fingerprint")
        require_sha256(self.physical_fingerprint, field="physical_fingerprint")
        if not self.active_generation or isinstance(self.revision, bool):
            raise ValueError("binding state generation and revision are invalid")


@dataclass(frozen=True, slots=True)
class MigrationLock:
    token: str
    owner: str
    bundle_digest: str
    expires_at: datetime

    def __post_init__(self) -> None:
        if not self.token or not self.owner or not self.bundle_digest:
            raise ValueError("migration lock fields must be non-empty")
        require_sha256(self.bundle_digest, field="bundle_digest")
        object.__setattr__(self, "expires_at", ensure_utc(self.expires_at, field="expires_at"))


@dataclass(frozen=True, slots=True)
class CompiledMigrationStep:
    step_id: str
    compiler_id: str
    artifact_digest: str
    opaque_artifact: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.step_id or not self.compiler_id or not self.artifact_digest:
            raise ValueError("compiled migration step metadata must be non-empty")
        require_sha256(self.artifact_digest, field="artifact_digest")

    def evidence_mapping(self) -> dict[str, str]:
        return {
            "stepId": self.step_id,
            "compilerId": self.compiler_id,
            "artifactDigest": self.artifact_digest,
        }


@dataclass(frozen=True, slots=True)
class CompiledMigrationPlan:
    bundle_digest: str
    steps: tuple[CompiledMigrationStep, ...]
    plan_digest: str = ""

    def __post_init__(self) -> None:
        if not self.bundle_digest:
            raise ValueError("compiled plan bundle digest must be non-empty")
        require_sha256(self.bundle_digest, field="bundle_digest")
        expected = sha256_digest(
            {
                "bundleDigest": self.bundle_digest,
                "steps": [step.evidence_mapping() for step in self.steps],
            }
        )
        if self.plan_digest and self.plan_digest != expected:
            raise ValueError("compiled migration plan digest is not deterministic")
        object.__setattr__(self, "plan_digest", expected)


@dataclass(frozen=True, slots=True)
class AppliedMigrationStep:
    step_id: str
    result_fingerprint: str
    checkpoint_token: str

    def __post_init__(self) -> None:
        if not self.step_id or not self.checkpoint_token:
            raise ValueError("applied migration step metadata must be non-empty")
        require_sha256(self.result_fingerprint, field="result_fingerprint")


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    name: str
    passed: bool
    result_digest: str
    summary: str = ""

    def __post_init__(self) -> None:
        if not self.name or not isinstance(self.passed, bool):
            raise ValueError("validation outcome name and passed state are invalid")
        require_sha256(self.result_digest, field="result_digest")


@dataclass(frozen=True, slots=True)
class MigrationCheckpoint:
    execution_id: str
    bundle_digest: str
    state: MigrationState
    sequence: int
    step_id: str | None = None
    result_fingerprint: str | None = None
    checkpoint_token: str | None = None
    failure_code: str | None = None
    recorded_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        if not self.execution_id or not self.bundle_digest or self.sequence < 0:
            raise ValueError("invalid migration checkpoint identity or sequence")
        require_sha256(self.bundle_digest, field="bundle_digest")
        if self.result_fingerprint is not None:
            require_sha256(self.result_fingerprint, field="result_fingerprint")
        object.__setattr__(self, "recorded_at", ensure_utc(self.recorded_at, field="recorded_at"))


@dataclass(frozen=True, slots=True)
class ActivationResult:
    active_generation: str
    previous_generation: str
    target_fingerprint: str
    source_boundary: str

    def __post_init__(self) -> None:
        if not self.active_generation or not self.previous_generation or not self.source_boundary:
            raise ValueError("activation result fields must be non-empty")
        require_sha256(self.target_fingerprint, field="target_fingerprint")


@dataclass(frozen=True, slots=True)
class CutoverEvidence:
    execution_id: str
    bundle_digest: str
    source_fingerprint: str
    target_fingerprint: str
    source_boundary: str
    previous_generation: str
    active_generation: str
    validation_report_digest: str
    activated_at: datetime
    digest: str = ""

    def __post_init__(self) -> None:
        for field_name in (
            "execution_id",
            "source_boundary",
            "previous_generation",
            "active_generation",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must be non-empty")
        for field_name in (
            "bundle_digest",
            "source_fingerprint",
            "target_fingerprint",
            "validation_report_digest",
        ):
            require_sha256(getattr(self, field_name), field=field_name)
        object.__setattr__(
            self, "activated_at", ensure_utc(self.activated_at, field="activated_at")
        )
        expected = sha256_digest(self._digest_material())
        if self.digest and self.digest != expected:
            raise ValueError("cutover evidence digest does not match canonical content")
        object.__setattr__(self, "digest", expected)

    def _digest_material(self) -> dict[str, object]:
        return {
            "executionId": self.execution_id,
            "bundleDigest": self.bundle_digest,
            "sourceFingerprint": self.source_fingerprint,
            "targetFingerprint": self.target_fingerprint,
            "sourceBoundary": self.source_boundary,
            "previousGeneration": self.previous_generation,
            "activeGeneration": self.active_generation,
            "validationReportDigest": self.validation_report_digest,
            "activatedAt": rfc3339(self.activated_at),
        }

    def to_mapping(self) -> dict[str, object]:
        return {**self._digest_material(), "digest": self.digest}


@dataclass(frozen=True, slots=True)
class MigrationReport:
    execution_id: str
    state: MigrationState
    bundle_digest: str
    checkpoints: tuple[MigrationCheckpoint, ...]
    validations: tuple[ValidationOutcome, ...]
    cutover: CutoverEvidence | None = None
    failure_code: str | None = None
