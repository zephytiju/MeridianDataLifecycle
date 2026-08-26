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

import pytest

from meridian_storage.projection.control import CancellationToken
from meridian_storage.projection.errors import (
    MigrationConflict,
    MigrationFailed,
    OperationCancelled,
)
from meridian_storage.projection.evidence import InMemoryEvidenceSink
from meridian_storage.projection.migration import (
    ActivationResult,
    AppliedMigrationStep,
    BindingStateEvidence,
    CompiledMigrationPlan,
    CompiledMigrationStep,
    InMemoryMigrationRecorder,
    MigrationBundleV1,
    MigrationCondition,
    MigrationExecutor,
    MigrationLock,
    MigrationState,
    MigrationStep,
    MigrationStepKind,
    ValidationOutcome,
)
from tests.conftest import serialized_operation

SOURCE = "sha256:" + "a" * 64
TARGET = "sha256:" + "b" * 64


def _condition(name: str) -> MigrationCondition:
    return MigrationCondition(name, serialized_operation(name), {"kind": "truthy"})


def _bundle() -> MigrationBundleV1:
    return MigrationBundleV1(
        SOURCE,
        TARGET,
        (
            MigrationStep("backfill", MigrationStepKind.BACKFILL),
            MigrationStep("activate", MigrationStepKind.ACTIVATE_SCHEMA_VERSION),
        ),
        preconditions=(_condition("precondition"),),
        validations=(_condition("validation"),),
        postconditions=(_condition("postcondition"),),
        bundle_id="bundle",
    )


class Adapter:
    def __init__(self, now: datetime) -> None:
        self.now = now
        self.source_fingerprint = SOURCE
        self.wrong_order = False
        self.released = False
        self.applied: list[str] = []
        self.activated = False
        self.fail_once: str | None = None

    def inspect_state(self, binding_ref: str) -> BindingStateEvidence:
        assert binding_ref == "binding"
        return BindingStateEvidence(
            self.source_fingerprint, "sha256:" + "f" * 64, "generation-1", 1
        )

    def acquire_lock(
        self,
        binding_ref: str,
        *,
        owner: str,
        bundle_digest: str,
        lease_duration: timedelta,
    ) -> MigrationLock:
        del binding_ref, lease_duration
        return MigrationLock("lock", owner, bundle_digest, self.now + timedelta(minutes=5))

    def compile(self, bundle: MigrationBundleV1) -> CompiledMigrationPlan:
        steps = tuple(
            CompiledMigrationStep(step.step_id, "test-compiler", step.digest, step)
            for step in bundle.steps
        )
        if self.wrong_order:
            steps = tuple(reversed(steps))
        return CompiledMigrationPlan(bundle.digest, steps)

    def apply_step(
        self,
        lock: MigrationLock,
        step: CompiledMigrationStep,
        *,
        resume_token: str | None,
    ) -> AppliedMigrationStep:
        del lock, resume_token
        if self.fail_once == step.step_id:
            self.fail_once = None
            raise RuntimeError("injected step failure")
        self.applied.append(step.step_id)
        return AppliedMigrationStep(step.step_id, step.artifact_digest, f"cp:{step.step_id}")

    def activate(
        self,
        lock: MigrationLock,
        bundle: MigrationBundleV1,
        *,
        expected_generation: str,
    ) -> ActivationResult:
        del lock, bundle
        self.activated = True
        return ActivationResult("generation-2", expected_generation, TARGET, "boundary-1")

    def release_lock(self, lock: MigrationLock) -> None:
        assert lock.token == "lock"
        self.released = True


class Validations:
    def __init__(self, failed: str | None = None, *, adapter: Adapter | None = None) -> None:
        self.failed = failed
        self.adapter = adapter
        self.calls: list[str] = []

    def run(self, condition: MigrationCondition) -> ValidationOutcome:
        self.calls.append(condition.name)
        if condition.name == "postcondition" and self.adapter is not None:
            assert self.adapter.activated
        return ValidationOutcome(
            condition.name,
            condition.name != self.failed,
            "sha256:" + "e" * 64,
        )


def test_migration_execution_and_cutover_evidence(fixed_time: datetime) -> None:
    adapter = Adapter(fixed_time)
    validations = Validations(adapter=adapter)
    evidence = InMemoryEvidenceSink()
    recorder = InMemoryMigrationRecorder()
    executor = MigrationExecutor(
        adapter=adapter,
        validations=validations,
        recorder=recorder,
        evidence=evidence,
        clock=lambda: fixed_time,
    )
    bundle = _bundle()
    report = executor.execute(
        binding_ref="binding", bundle=bundle, owner="deployment", execution_id="run"
    )
    assert report.state is MigrationState.COMPLETED
    assert adapter.applied == ["backfill", "activate"]
    assert adapter.released
    assert report.cutover is not None
    assert report.cutover.target_fingerprint == TARGET
    assert report.cutover.digest.startswith("sha256:")
    assert [item.state for item in report.checkpoints] == [
        MigrationState.PLANNED,
        MigrationState.APPLYING,
        MigrationState.APPLYING,
        MigrationState.VALIDATING,
        MigrationState.ACTIVATING,
        MigrationState.COMPLETED,
    ]
    assert evidence.events[-1].state == "COMPLETED"
    assert validations.calls == ["precondition", "validation", "postcondition"]
    with pytest.raises(MigrationConflict, match="already completed"):
        executor.execute(
            binding_ref="binding", bundle=bundle, owner="deployment", execution_id="run"
        )


def test_migration_failure_conflict_and_cancellation(fixed_time: datetime) -> None:
    recorder = InMemoryMigrationRecorder()
    adapter = Adapter(fixed_time)
    executor = MigrationExecutor(
        adapter=adapter,
        validations=Validations(failed="validation"),
        recorder=recorder,
        clock=lambda: fixed_time,
    )
    with pytest.raises(MigrationFailed):
        executor.execute(
            binding_ref="binding", bundle=_bundle(), owner="deployment", execution_id="failed"
        )
    assert recorder.checkpoints("failed")[-1].state is MigrationState.FAILED
    assert adapter.released

    mismatch = Adapter(fixed_time)
    mismatch.source_fingerprint = "sha256:" + "c" * 64
    with pytest.raises(MigrationConflict, match="logical fingerprint"):
        MigrationExecutor(
            adapter=mismatch,
            validations=Validations(),
            recorder=InMemoryMigrationRecorder(),
            clock=lambda: fixed_time,
        ).execute(binding_ref="binding", bundle=_bundle(), owner="deployment")

    wrong = Adapter(fixed_time)
    wrong.wrong_order = True
    with pytest.raises(MigrationConflict, match="step order"):
        MigrationExecutor(
            adapter=wrong,
            validations=Validations(),
            recorder=InMemoryMigrationRecorder(),
            clock=lambda: fixed_time,
        ).execute(binding_ref="binding", bundle=_bundle(), owner="deployment")
    assert wrong.released

    cancelled = CancellationToken()
    cancelled.cancel()
    with pytest.raises(OperationCancelled):
        MigrationExecutor(
            adapter=Adapter(fixed_time),
            validations=Validations(),
            recorder=InMemoryMigrationRecorder(),
            clock=lambda: fixed_time,
        ).execute(
            binding_ref="binding",
            bundle=_bundle(),
            owner="deployment",
            cancellation=cancelled,
        )


def test_recorder_and_executor_argument_validation(fixed_time: datetime) -> None:
    recorder = InMemoryMigrationRecorder()
    bundle = _bundle()
    with pytest.raises(ValueError, match="lock_seconds"):
        MigrationExecutor(
            adapter=Adapter(fixed_time),
            validations=Validations(),
            recorder=recorder,
            lock_seconds=0,
        )
    executor = MigrationExecutor(
        adapter=Adapter(fixed_time),
        validations=Validations(),
        recorder=recorder,
        clock=lambda: fixed_time,
    )
    with pytest.raises(ValueError, match="binding_ref"):
        executor.execute(binding_ref="", bundle=bundle, owner="x")


def test_migration_resume_skips_durable_steps(fixed_time: datetime) -> None:
    recorder = InMemoryMigrationRecorder()
    adapter = Adapter(fixed_time)
    adapter.fail_once = "activate"
    executor = MigrationExecutor(
        adapter=adapter,
        validations=Validations(adapter=adapter),
        recorder=recorder,
        clock=lambda: fixed_time,
    )
    bundle = _bundle()
    with pytest.raises(RuntimeError, match="injected"):
        executor.execute(
            binding_ref="binding", bundle=bundle, owner="deployment", execution_id="resume"
        )
    assert adapter.applied == ["backfill"]
    report = executor.execute(
        binding_ref="binding", bundle=bundle, owner="deployment", execution_id="resume"
    )
    assert report.state is MigrationState.COMPLETED
    assert adapter.applied == ["backfill", "activate"]
