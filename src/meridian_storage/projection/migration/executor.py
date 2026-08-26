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

"""Explicit deployment-controlled migration execution hooks."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from threading import RLock
from typing import Protocol
from uuid import uuid4

from meridian_storage import MeridianError
from meridian_storage.projection._canonical import ensure_utc, sha256_digest, utc_now
from meridian_storage.projection.control import CancellationToken
from meridian_storage.projection.errors import MigrationConflict, MigrationFailed
from meridian_storage.projection.evidence import EvidenceSink, LifecycleEvidence, NullEvidenceSink
from meridian_storage.projection.migration.model import (
    ActivationResult,
    AppliedMigrationStep,
    BindingStateEvidence,
    CompiledMigrationPlan,
    CompiledMigrationStep,
    CutoverEvidence,
    MigrationBundleV1,
    MigrationCheckpoint,
    MigrationCondition,
    MigrationLock,
    MigrationReport,
    MigrationState,
    ValidationOutcome,
)


class MigrationAdapterHooks(Protocol):
    """Adapter-owned compilation, lock, application, and activation hooks."""

    def inspect_state(self, binding_ref: str) -> BindingStateEvidence: ...

    def acquire_lock(
        self,
        binding_ref: str,
        *,
        owner: str,
        bundle_digest: str,
        lease_duration: timedelta,
    ) -> MigrationLock: ...

    def compile(self, bundle: MigrationBundleV1) -> CompiledMigrationPlan: ...

    def apply_step(
        self,
        lock: MigrationLock,
        step: CompiledMigrationStep,
        *,
        resume_token: str | None,
    ) -> AppliedMigrationStep: ...

    def activate(
        self,
        lock: MigrationLock,
        bundle: MigrationBundleV1,
        *,
        expected_generation: str,
    ) -> ActivationResult: ...

    def release_lock(self, lock: MigrationLock) -> None: ...


class MigrationValidationRunner(Protocol):
    def run(self, condition: MigrationCondition) -> ValidationOutcome: ...


class MigrationRecorder(Protocol):
    def append(self, checkpoint: MigrationCheckpoint) -> None: ...

    def checkpoints(self, execution_id: str) -> tuple[MigrationCheckpoint, ...]: ...


class InMemoryMigrationRecorder:
    """Reference durable-checkpoint contract for tests and local execution."""

    def __init__(self) -> None:
        self._records: dict[str, list[MigrationCheckpoint]] = {}
        self._lock = RLock()

    def append(self, checkpoint: MigrationCheckpoint) -> None:
        with self._lock:
            records = self._records.setdefault(checkpoint.execution_id, [])
            if records and checkpoint.sequence <= records[-1].sequence:
                raise MigrationConflict("migration checkpoint sequence must increase")
            records.append(checkpoint)

    def checkpoints(self, execution_id: str) -> tuple[MigrationCheckpoint, ...]:
        with self._lock:
            return tuple(self._records.get(execution_id, ()))


class MigrationExecutor:
    """Run a migration only when explicitly called by a deployment job."""

    def __init__(
        self,
        *,
        adapter: MigrationAdapterHooks,
        validations: MigrationValidationRunner,
        recorder: MigrationRecorder,
        evidence: EvidenceSink | None = None,
        lock_seconds: float = 900,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if lock_seconds <= 0:
            raise ValueError("lock_seconds must be positive")
        self._adapter = adapter
        self._validations = validations
        self._recorder = recorder
        self._evidence = evidence or NullEvidenceSink()
        self._lock_duration = timedelta(seconds=lock_seconds)
        self._clock = clock

    def _checkpoint(
        self,
        *,
        execution_id: str,
        bundle: MigrationBundleV1,
        state: MigrationState,
        sequence: int,
        step: AppliedMigrationStep | None = None,
        failure_code: str | None = None,
    ) -> MigrationCheckpoint:
        checkpoint = MigrationCheckpoint(
            execution_id=execution_id,
            bundle_digest=bundle.digest,
            state=state,
            sequence=sequence,
            step_id=step.step_id if step else None,
            result_fingerprint=step.result_fingerprint if step else None,
            checkpoint_token=step.checkpoint_token if step else None,
            failure_code=failure_code,
            recorded_at=self._clock(),
        )
        self._recorder.append(checkpoint)
        return checkpoint

    @staticmethod
    def _verify_compiled(bundle: MigrationBundleV1, plan: CompiledMigrationPlan) -> None:
        if plan.bundle_digest != bundle.digest:
            raise MigrationConflict("compiled plan references a different migration bundle")
        expected_ids = [step.step_id for step in bundle.steps]
        actual_ids = [step.step_id for step in plan.steps]
        if actual_ids != expected_ids:
            raise MigrationConflict("compiled plan does not preserve logical step order")
        expected_digest = sha256_digest(
            {
                "bundleDigest": plan.bundle_digest,
                "steps": [step.evidence_mapping() for step in plan.steps],
            }
        )
        if plan.plan_digest != expected_digest:
            raise MigrationConflict("compiled migration plan is not deterministic")

    def _run_conditions(
        self,
        conditions: tuple[MigrationCondition, ...],
        cancellation: CancellationToken,
    ) -> tuple[ValidationOutcome, ...]:
        outcomes: list[ValidationOutcome] = []
        for condition in conditions:
            cancellation.raise_if_cancelled()
            outcome = self._validations.run(condition)
            outcomes.append(outcome)
            if not outcome.passed:
                raise MigrationFailed(f"migration condition {condition.name!r} failed")
        return tuple(outcomes)

    def execute(
        self,
        *,
        binding_ref: str,
        bundle: MigrationBundleV1,
        owner: str,
        cancellation: CancellationToken | None = None,
        execution_id: str | None = None,
    ) -> MigrationReport:
        if not binding_ref or not owner:
            raise ValueError("binding_ref and owner must be non-empty")
        token = cancellation or CancellationToken()
        if execution_id is not None and not execution_id:
            raise ValueError("execution_id must be non-empty when supplied")
        run_id = execution_id or str(uuid4())
        existing = self._recorder.checkpoints(run_id)
        if existing and any(item.bundle_digest != bundle.digest for item in existing):
            raise MigrationConflict("execution id is already bound to another bundle")
        if any(item.state is MigrationState.COMPLETED for item in existing):
            raise MigrationConflict("migration execution is already completed")
        sequence = existing[-1].sequence + 1 if existing else 0
        completed_steps = {
            item.step_id: item
            for item in existing
            if item.state is MigrationState.APPLYING and item.step_id is not None
        }
        lock: MigrationLock | None = None
        all_validations: list[ValidationOutcome] = []
        cutover: CutoverEvidence | None = None
        try:
            token.raise_if_cancelled()
            state = self._adapter.inspect_state(binding_ref)
            if state.logical_fingerprint != bundle.source_schema_fingerprint:
                raise MigrationConflict("current logical fingerprint does not match bundle source")
            all_validations.extend(self._run_conditions(bundle.preconditions, token))
            lock = self._adapter.acquire_lock(
                binding_ref,
                owner=owner,
                bundle_digest=bundle.digest,
                lease_duration=self._lock_duration,
            )
            if lock.owner != owner or lock.bundle_digest != bundle.digest:
                raise MigrationConflict("adapter returned a lock for a different owner or bundle")
            if lock.expires_at <= ensure_utc(self._clock(), field="now"):
                raise MigrationConflict("adapter returned an expired migration lock")
            plan = self._adapter.compile(bundle)
            self._verify_compiled(bundle, plan)
            self._checkpoint(
                execution_id=run_id,
                bundle=bundle,
                state=MigrationState.PLANNED,
                sequence=sequence,
            )
            sequence += 1
            self._evidence.record(
                LifecycleEvidence(
                    kind="migration",
                    subject=binding_ref,
                    state=MigrationState.PLANNED.value,
                    details={"bundleDigest": bundle.digest, "planDigest": plan.plan_digest},
                    occurred_at=self._clock(),
                )
            )
            for compiled in plan.steps:
                token.raise_if_cancelled()
                if lock.expires_at <= ensure_utc(self._clock(), field="now"):
                    raise MigrationConflict("migration lock expired while applying the plan")
                previous = completed_steps.get(compiled.step_id)
                if previous is not None:
                    continue
                applied = self._adapter.apply_step(lock, compiled, resume_token=None)
                if applied.step_id != compiled.step_id:
                    raise MigrationConflict("adapter acknowledged the wrong migration step")
                self._checkpoint(
                    execution_id=run_id,
                    bundle=bundle,
                    state=MigrationState.APPLYING,
                    sequence=sequence,
                    step=applied,
                )
                sequence += 1
            self._checkpoint(
                execution_id=run_id,
                bundle=bundle,
                state=MigrationState.VALIDATING,
                sequence=sequence,
            )
            sequence += 1
            all_validations.extend(self._run_conditions(bundle.validations, token))
            self._checkpoint(
                execution_id=run_id,
                bundle=bundle,
                state=MigrationState.ACTIVATING,
                sequence=sequence,
            )
            sequence += 1
            token.raise_if_cancelled()
            if lock.expires_at <= ensure_utc(self._clock(), field="now"):
                raise MigrationConflict("migration lock expired before activation")
            activation = self._adapter.activate(
                lock,
                bundle,
                expected_generation=state.active_generation,
            )
            if activation.target_fingerprint != bundle.target_schema_fingerprint:
                raise MigrationConflict("activation target fingerprint does not match bundle")
            # Once activation has been attempted, finish post-cutover validation and
            # durable evidence even if the caller concurrently requests cancellation.
            all_validations.extend(self._run_conditions(bundle.postconditions, CancellationToken()))
            validation_digest = sha256_digest(
                [
                    {
                        "name": item.name,
                        "passed": item.passed,
                        "resultDigest": item.result_digest,
                    }
                    for item in all_validations
                ]
            )
            cutover = CutoverEvidence(
                execution_id=run_id,
                bundle_digest=bundle.digest,
                source_fingerprint=bundle.source_schema_fingerprint,
                target_fingerprint=bundle.target_schema_fingerprint,
                source_boundary=activation.source_boundary,
                previous_generation=activation.previous_generation,
                active_generation=activation.active_generation,
                validation_report_digest=validation_digest,
                activated_at=self._clock(),
            )
            self._checkpoint(
                execution_id=run_id,
                bundle=bundle,
                state=MigrationState.COMPLETED,
                sequence=sequence,
            )
            self._evidence.record(
                LifecycleEvidence(
                    kind="migration",
                    subject=binding_ref,
                    state=MigrationState.COMPLETED.value,
                    details=cutover.to_mapping(),
                    occurred_at=self._clock(),
                )
            )
            return MigrationReport(
                execution_id=run_id,
                state=MigrationState.COMPLETED,
                bundle_digest=bundle.digest,
                checkpoints=self._recorder.checkpoints(run_id),
                validations=tuple(all_validations),
                cutover=cutover,
            )
        except Exception as error:
            failure_code = (
                str(error.code) if isinstance(error, MeridianError) else type(error).__name__
            )
            self._checkpoint(
                execution_id=run_id,
                bundle=bundle,
                state=MigrationState.FAILED,
                sequence=sequence,
                failure_code=failure_code,
            )
            self._evidence.record(
                LifecycleEvidence(
                    kind="migration",
                    subject=binding_ref,
                    state=MigrationState.FAILED.value,
                    details={"bundleDigest": bundle.digest, "failureCode": failure_code},
                    occurred_at=self._clock(),
                )
            )
            raise
        finally:
            if lock is not None:
                self._adapter.release_lock(lock)
