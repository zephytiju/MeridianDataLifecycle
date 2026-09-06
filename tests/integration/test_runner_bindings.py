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

from dataclasses import replace
from datetime import datetime
from typing import NoReturn, cast

import pytest
from meridian_storage.registry import CapabilityRequirement

from meridian_storage import Meridian, MeridianError, OperationContext, ResourceRef
from meridian_storage.projection import InMemoryOutboxStore, ProjectionRunner, ProjectionSpec
from tests.conftest import result
from tests.integration.test_runner import FakeRuntime, _event, _project, _spec
from tests.projection_support import SOURCE, TARGET, real_facade


def with_resource(spec: ProjectionSpec, side: str, name: str) -> ProjectionSpec:
    return replace(spec, source=name) if side == "source" else replace(spec, target=name)


@pytest.mark.parametrize("side", ["source", "target"])
@pytest.mark.parametrize(
    "defect",
    [
        "missing-resource",
        "wrong-catalog",
        "invalid-resource",
        "wrong-schema",
        "missing-schema",
        "unregistered-schema",
        "missing-binding",
        "missing-manifest",
        "wrong-fingerprint",
        "wrong-adapter",
        "missing-operation",
        "wrong-operation-version",
        "missing-guarantee",
        "insufficient-limit",
    ],
)
def test_constructor_rejects_invalid_composition_before_any_work(side: str, defect: str) -> None:
    runtime = FakeRuntime(lambda expression: result(None))
    spec = _spec()
    ref = SOURCE if side == "source" else TARGET
    snapshot = runtime.snapshot
    resources = dict(snapshot.resources)
    bindings = dict(snapshot._bindings)
    schemas = dict(snapshot.schemas)
    if defect == "missing-resource":
        del resources[ref]
    elif defect == "wrong-catalog":
        spec = with_resource(spec, side, f"object:{ref.logical_name}")
    elif defect == "invalid-resource":
        spec = with_resource(spec, side, "unqualified")
    elif defect == "wrong-schema":
        spec = (
            replace(spec, source_schema="investigation.other@99")
            if side == "source"
            else replace(spec, target_schema="investigation.other@99")
        )
    elif defect == "missing-schema":
        resources[ref] = replace(resources[ref], schema=None)
    elif defect == "unregistered-schema":
        schema = resources[ref].schema
        assert schema is not None
        del schemas[schema]
    elif defect == "missing-binding":
        del bindings[ref]
    elif defect == "missing-manifest":
        runtime._capability_manifests.clear()
    elif defect == "wrong-fingerprint":
        bindings[ref] = replace(bindings[ref], capability_fingerprint="sha256:" + "f" * 64)
    elif defect == "wrong-adapter":
        bindings[ref] = replace(bindings[ref], adapter_id="other")
    else:
        requirement = resources[ref].requirements[0]
        if defect == "missing-operation":
            requirement = CapabilityRequirement("unavailable.operation.v1", "1.0.0")
        elif defect == "wrong-operation-version":
            requirement = replace(requirement, operation_version="99.0.0")
        elif defect == "missing-guarantee":
            requirement = replace(requirement, guarantees=("unavailable-guarantee",))
        else:
            requirement = replace(requirement, minimum_limits={"unavailable-limit": 100})
        resources[ref] = replace(resources[ref], requirements=(requirement,))
    runtime.snapshot = replace(snapshot, resources=resources, schemas=schemas, _bindings=bindings)
    store = InMemoryOutboxStore()

    def projector(source: object, context: object) -> NoReturn:
        raise AssertionError("constructor invoked the projector")

    with pytest.raises(MeridianError):
        ProjectionRunner(
            meridian=cast(Meridian, runtime),
            spec=spec,
            project=projector,
            outbox=store,
        )
    assert runtime.expressions == []
    assert store.records() == ()


def test_constructor_allows_distinct_resolved_bindings() -> None:
    runtime = FakeRuntime(lambda expression: result(None))
    binding = runtime.snapshot.binding_for(TARGET)
    bindings = dict(runtime.snapshot._bindings)
    bindings[TARGET] = replace(binding, binding_id="another-binding")
    runtime._capability_manifests["another-binding"] = runtime._capability_manifests["test"]
    runtime.snapshot = replace(runtime.snapshot, _bindings=bindings)
    ProjectionRunner(
        meridian=cast(Meridian, runtime),
        spec=_spec(),
        project=_project,
        outbox=InMemoryOutboxStore(),
    )
    assert runtime.expressions == []


def test_released_core_public_composition_and_missing_resource(fixed_time: datetime) -> None:
    runtime, adapter = real_facade()
    store = InMemoryOutboxStore()
    store.append(_event(fixed_time))
    # Core's readiness boundary is preserved; runner never starts the runtime.
    with pytest.raises(MeridianError):
        ProjectionRunner(meridian=runtime, spec=_spec(), project=_project, outbox=store)
    assert adapter.opens == adapter.sessions == 0
    runtime.start()
    try:
        for side in ("source", "target"):
            bad = with_resource(_spec(), side, "missing.resource")
            with pytest.raises(MeridianError):
                ProjectionRunner(meridian=runtime, spec=bad, project=_project, outbox=store)
        assert adapter.requests == [] and adapter.sessions == 0
        structured = runtime.catalog("structured")
        runner = ProjectionRunner(
            meridian=runtime,
            spec=_spec(),
            outbox=store,
            clock=lambda: fixed_time,
            project=lambda source, context: structured.put(
                resource="investigation.case_search", data={"id": "case-1"}
            ),
        )
        assert adapter.sessions == 0
        with runtime.context(OperationContext("conformance-test")):
            assert runner.run_once().completed == 1
        assert len(adapter.requests) == 1
        assert adapter.requests[0].operation.resources == (ResourceRef.parse(str(TARGET)),)
        assert store.checkpoint(_event(fixed_time).partition_key).revision == 1
    finally:
        runtime.close()
