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

from collections.abc import Callable, Mapping
from datetime import datetime
from threading import Event
from typing import cast

import pytest

from meridian_storage import Expression, Meridian, OperationResult, UnavailableError
from meridian_storage.projection.evidence import InMemoryEvidenceSink
from meridian_storage.projection.outbox import InMemoryOutboxStore, OutboxDataV1, OutboxState
from meridian_storage.projection.runner import ProjectionRunner, ProjectionSpec
from tests.conftest import MutableClock, result


class FakeRuntime:
    def __init__(self, execute: Callable[[Expression], OperationResult]) -> None:
        self.execute_call = execute
        self.catalogs: list[str] = []
        self.expressions: list[Expression] = []

    def catalog(self, name: str) -> object:
        self.catalogs.append(name)
        return object()

    def execute(self, expression: Expression) -> OperationResult:
        self.expressions.append(expression)
        return self.execute_call(expression)


def _spec() -> ProjectionSpec:
    return ProjectionSpec(
        name="case-search-v1",
        source_catalog="structured",
        source="investigation.cases",
        target_catalog="structured",
        target="investigation.case_search",
        source_schema="investigation.case@1",
        target_schema="investigation.case_search@1",
        target_labels=("search",),
    )


def _event(fixed_time: datetime, *, reference: bool = False) -> OutboxDataV1:
    return OutboxDataV1(
        source_resource="investigation.cases",
        source_schema="investigation.case@1",
        source_identity="case-1",
        source_version=3,
        mutation_kind="put",
        payload=None if reference else {"case_id": "case-1"},
        immutable_reference={"case_id": "case-1"} if reference else None,
        target_labels=("search",),
        occurred_at=fixed_time,
        event_id="event-1",
    )


def _project(source: Mapping[str, object], context: object) -> Expression:
    del context
    return Expression(
        "structured",
        "put",
        {"resource": "investigation.case_search", "data": dict(source)},
    )


def test_projection_runner_success_and_reference_load(fixed_time: datetime) -> None:
    clock = MutableClock(fixed_time)
    store = InMemoryOutboxStore(visibility_target_seconds=999)
    store.append(_event(fixed_time, reference=True))
    runtime = FakeRuntime(lambda expression: result({"acknowledgedSourceVersion": 3}))
    evidence = InMemoryEvidenceSink()
    loads: list[tuple[Mapping[str, object], str | int | None]] = []

    def load(
        reference: Mapping[str, object], source_version: str | int | None
    ) -> Mapping[str, object]:
        loads.append((reference, source_version))
        return {"case_id": reference["case_id"], "loaded_version": source_version}

    runner = ProjectionRunner(
        meridian=cast(Meridian, runtime),
        spec=_spec(),
        project=_project,
        outbox=store,
        source_loader=load,
        evidence=evidence,
        worker_id="worker",
        clock=clock,
    )
    run = runner.run_once()
    assert run.completed == run.claimed == 1
    assert loads[0][1] == 3
    assert store.get("event-1").state is OutboxState.COMPLETED
    assert evidence.events[0].state == "COMPLETED"
    assert runtime.catalogs == ["structured", "structured"]
    assert runner.run_until_idle().claimed == 0
    lag = runner.lag(now=fixed_time)
    assert lag.within_target
    assert lag.target_seconds == _spec().eventual_visibility_seconds


def test_projection_retryable_and_rejected(fixed_time: datetime) -> None:
    store = InMemoryOutboxStore()
    store.append(_event(fixed_time))

    def unavailable(expression: Expression) -> OperationResult:
        del expression
        raise UnavailableError("TEMP", "target unavailable", retryable=True)

    runner = ProjectionRunner(
        meridian=cast(Meridian, FakeRuntime(unavailable)),
        spec=_spec(),
        project=_project,
        outbox=store,
        worker_id="worker",
        clock=lambda: fixed_time,
    )
    assert runner.run_once().retryable == 1
    assert store.get("event-1").state is OutboxState.RETRYABLE

    rejected_store = InMemoryOutboxStore()
    rejected_store.append(_event(fixed_time))

    def wrong(source: Mapping[str, object], context: object) -> Expression:
        del source, context
        return Expression("structured", "put", {"resource": "wrong.target", "data": {}})

    rejected = ProjectionRunner(
        meridian=cast(Meridian, FakeRuntime(lambda expression: result(None))),
        spec=_spec(),
        project=wrong,
        outbox=rejected_store,
        worker_id="worker",
        clock=lambda: fixed_time,
    )
    assert rejected.run_once().quarantined == 1
    assert rejected_store.get("event-1").state is OutboxState.QUARANTINED


def test_projection_spec_and_runner_argument_validation(fixed_time: datetime) -> None:
    with pytest.raises(ValueError, match="unknown source"):
        ProjectionSpec("x", "projection", "a", "structured", "b", "a@1", "b@1")
    with pytest.raises(ValueError, match="positive"):
        ProjectionSpec("x", "structured", "a", "structured", "b", "a@1", "b@1", (), 0)
    with pytest.raises(ValueError, match="batch_size"):
        ProjectionRunner(
            meridian=cast(Meridian, FakeRuntime(lambda expression: result(None))),
            spec=_spec(),
            project=_project,
            outbox=InMemoryOutboxStore(),
            batch_size=0,
        )

    runner = ProjectionRunner(
        meridian=cast(Meridian, FakeRuntime(lambda expression: result(None))),
        spec=_spec(),
        project=_project,
        outbox=InMemoryOutboxStore(),
    )
    with pytest.raises(ValueError, match="max_cycles"):
        runner.run_until_idle(max_cycles=0)
    with pytest.raises(ValueError, match="poll_interval"):
        runner.run_until_stopped(Event(), poll_interval_seconds=-1)
    stopped = Event()
    stopped.set()
    assert runner.run_until_stopped(stopped).claimed == 0


def test_projection_accepts_exact_mapping_resource(fixed_time: datetime) -> None:
    store = InMemoryOutboxStore()
    store.append(_event(fixed_time))
    runner = ProjectionRunner(
        meridian=cast(
            Meridian,
            FakeRuntime(lambda expression: result({"acknowledgedSourceVersion": 3})),
        ),
        spec=_spec(),
        project=lambda source, context: Expression(
            "structured",
            "put",
            {
                "resource": {
                    "catalog": "structured",
                    "namespace": "investigation",
                    "name": "case_search",
                },
                "data": dict(source),
            },
        ),
        outbox=store,
        worker_id="worker",
        clock=lambda: fixed_time,
    )
    assert runner.run_once().completed == 1
