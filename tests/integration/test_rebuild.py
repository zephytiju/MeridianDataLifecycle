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

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta

import pytest

from meridian_storage.projection.errors import MigrationConflict, MigrationFailed
from meridian_storage.projection.evidence import InMemoryEvidenceSink
from meridian_storage.projection.rebuild import (
    RebuildActivation,
    RebuildBoundary,
    RebuildCoordinator,
    RebuildGeneration,
    RebuildPlan,
    RebuildTailResult,
    RebuildValidation,
)

SOURCE_FINGERPRINT = "sha256:" + "a" * 64
TARGET_FINGERPRINT = "sha256:" + "b" * 64
SAMPLED_DIGEST = "sha256:" + "c" * 64
REPORT_DIGEST = "sha256:" + "d" * 64


class Hooks:
    def __init__(self) -> None:
        self.valid = True
        self.wrong_activation = False
        self.oversized = False
        self.writes: list[Sequence[Mapping[str, object]]] = []
        self.retained: list[tuple[str, datetime]] = []
        self.discarded: list[str] = []

    def capture_boundary(self, plan: RebuildPlan) -> RebuildBoundary:
        del plan
        return RebuildBoundary("boundary-1", SOURCE_FINGERPRINT)

    def create_generation(self, plan: RebuildPlan, boundary: RebuildBoundary) -> RebuildGeneration:
        del plan, boundary
        return RebuildGeneration("generation-2", "generation-1")

    def scan(
        self, plan: RebuildPlan, boundary: RebuildBoundary
    ) -> Iterable[Sequence[Mapping[str, object]]]:
        del plan, boundary
        if self.oversized:
            yield tuple({"id": str(index)} for index in range(3))
        else:
            yield ({"id": "1", "sourceVersion": 1},)
            yield ({"id": "2", "sourceVersion": 1},)

    def write(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        records: Sequence[Mapping[str, object]],
    ) -> None:
        del plan, generation
        self.writes.append(records)

    def tail(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        boundary: RebuildBoundary,
    ) -> RebuildTailResult:
        del plan, generation, boundary
        return RebuildTailResult(1, "boundary-2")

    def validate(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        boundary: RebuildBoundary,
    ) -> RebuildValidation:
        del plan, generation, boundary
        return RebuildValidation(
            self.valid,
            2,
            2 if self.valid else 1,
            SAMPLED_DIGEST,
            REPORT_DIGEST,
        )

    def activate(
        self,
        plan: RebuildPlan,
        generation: RebuildGeneration,
        *,
        expected_previous: str,
    ) -> RebuildActivation:
        del plan
        return RebuildActivation(
            "wrong" if self.wrong_activation else expected_previous,
            generation.generation_ref,
            TARGET_FINGERPRINT,
        )

    def retain(self, generation_ref: str, *, until: datetime) -> None:
        self.retained.append((generation_ref, until))

    def discard(self, generation_ref: str) -> None:
        self.discarded.append(generation_ref)


def _plan(*, batch_size: int = 2) -> RebuildPlan:
    return RebuildPlan(
        "case-search",
        "investigation.cases",
        "investigation.case_search",
        batch_size=batch_size,
        rollback_interval=timedelta(hours=1),
    )


def test_rebuild_scan_tail_validate_activate_and_retain(fixed_time: datetime) -> None:
    hooks = Hooks()
    evidence = InMemoryEvidenceSink()
    result = RebuildCoordinator(hooks=hooks, evidence=evidence, clock=lambda: fixed_time).rebuild(
        _plan()
    )
    assert len(hooks.writes) == 2
    assert hooks.retained == [("generation-1", fixed_time + timedelta(hours=1))]
    assert not hooks.discarded
    assert result.source_boundary == "boundary-2"
    assert result.applied_tail_events == 1
    assert result.digest.startswith("sha256:")
    assert evidence.events[0].state == "COMPLETED"


def test_rebuild_failure_discards_unactivated_generation() -> None:
    invalid = Hooks()
    invalid.valid = False
    with pytest.raises(MigrationFailed, match="validation"):
        RebuildCoordinator(hooks=invalid).rebuild(_plan())
    assert invalid.discarded == ["generation-2"]

    oversized = Hooks()
    oversized.oversized = True
    with pytest.raises(MigrationFailed, match="batch size"):
        RebuildCoordinator(hooks=oversized).rebuild(_plan(batch_size=2))
    assert oversized.discarded == ["generation-2"]

    wrong = Hooks()
    wrong.wrong_activation = True
    with pytest.raises(MigrationConflict, match="unexpected"):
        RebuildCoordinator(hooks=wrong).rebuild(_plan())
    # The activation call may have committed despite a bad acknowledgement.
    # Cleanup must not discard a generation that could now be active.
    assert not wrong.discarded


def test_rebuild_plan_validation() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        RebuildPlan("", "source", "target")
    with pytest.raises(ValueError, match="batch_size"):
        RebuildPlan("p", "source", "target", batch_size=0)
    with pytest.raises(ValueError, match="rollback_interval"):
        RebuildPlan("p", "source", "target", rollback_interval=timedelta(0))
