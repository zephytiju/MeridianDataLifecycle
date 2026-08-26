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

from datetime import datetime
from typing import cast

import pytest

from meridian_storage import Expression, Meridian, OperationResult
from meridian_storage.projection.outbox import InMemoryOutboxStore, OutboxDataV1, OutboxState
from meridian_storage.projection.runner import ProjectionRunner, ProjectionSpec
from tests.conftest import MutableClock, result


class Runtime:
    def __init__(self) -> None:
        self.writes = 0

    def catalog(self, name: str) -> object:
        del name
        return object()

    def execute(self, expression: Expression) -> OperationResult:
        del expression
        self.writes += 1
        return result({"acknowledgedSourceVersion": 7})


def test_crash_after_target_ack_replays_without_checkpoint_advance(fixed_time: datetime) -> None:
    clock = MutableClock(fixed_time)
    store = InMemoryOutboxStore()
    store.append(
        OutboxDataV1(
            "cases",
            "case@1",
            "case-1",
            "put",
            source_version=7,
            payload={"id": "case-1"},
            event_id="event",
            occurred_at=fixed_time,
        )
    )
    runtime = Runtime()
    crashed = False

    def acknowledge(result: OperationResult, source: OutboxDataV1) -> tuple[str | int | None, str]:
        nonlocal crashed
        del result
        if not crashed:
            crashed = True
            raise SystemExit("simulated process crash")
        return source.source_version, "sha256:" + "d" * 64

    runner = ProjectionRunner(
        meridian=cast(Meridian, runtime),
        spec=ProjectionSpec("p", "structured", "cases", "structured", "derived", "case@1", "d@1"),
        project=lambda source, context: Expression(
            "structured", "put", {"resource": "derived", "data": dict(source)}
        ),
        outbox=store,
        acknowledgement=acknowledge,
        lease_seconds=1,
        worker_id="worker",
        clock=clock,
    )
    with pytest.raises(SystemExit):
        runner.run_once()
    assert runtime.writes == 1
    assert store.get("event").state is OutboxState.LEASED
    assert store.checkpoint(store.get("event").data.partition_key).revision == 0

    clock.advance(seconds=2)
    assert runner.run_once().completed == 1
    assert runtime.writes == 2
    assert store.get("event").state is OutboxState.COMPLETED
