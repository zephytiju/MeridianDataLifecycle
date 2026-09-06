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

from collections.abc import Iterator
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from meridian_storage.spi import ExecutionRequest, ExecutionResult, OperationCapability

from meridian_storage import ConflictError, Expression, Meridian, OperationContext
from meridian_storage.projection import OutboxDataV1, TransactionalOutboxWriter
from meridian_storage.projection.errors import DataLifecycleValidationError
from tests import projection_support as support


class WriterSession(support.RecordingSession):
    def __init__(self, runtime: WriterAdapter) -> None:
        super().__init__(runtime)
        self.adapter = runtime
        self.rows: dict[str, Any] = {}
        self.replays: dict[tuple[str, str], tuple[str, ExecutionResult]] = {}

    def begin(self) -> None:
        self.rows = deepcopy(self.adapter.rows)
        self.replays = dict(self.adapter.replays)

    def commit(self) -> None:
        self.adapter.rows = self.rows
        self.adapter.replays = self.replays
        self.adapter.commits += 1

    def rollback(self) -> None:
        self.adapter.rollbacks += 1

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        self.runtime.requests.append(request)
        operation = request.operation
        resource = operation.resources[0].name
        key = (request.context.idempotency_key or "", resource)
        fingerprint = operation.request_fingerprint
        if key[0] and key in self.replays:
            previous, response = self.replays[key]
            if previous != fingerprint:
                raise ConflictError("MERIDIAN_CONFLICT", "different request")
            return response
        data = operation.to_dict()["input"].get("data", {})
        if resource == "case_search":
            assert operation.input["mode"] == "if_absent"
            assert "expectedVersion" not in operation.input
            if self.adapter.fail_append:
                raise ConflictError("MERIDIAN_CONFLICT", "append failed")
            if "outbox" in self.rows:
                raise ConflictError("MERIDIAN_CONFLICT", "duplicate intent")
            self.rows["outbox"] = deepcopy(data)
            response = ExecutionResult(data, 0)
        else:
            self.rows["source"] = data
            response = ExecutionResult(self.adapter.returned, 0)
        if key[0]:
            self.replays[key] = (fingerprint, response)
        return response


class WriterAdapter(support.RecordingAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.rows: dict[str, Any] = {}
        self.replays: dict[tuple[str, str], tuple[str, ExecutionResult]] = {}
        self.returned: Any = {"id": "case-1", "title": "Case", "recordVersion": 1}
        self.fail_append = False
        self.commits = 0
        self.rollbacks = 0

    def open_session(self, *, transactional: bool) -> WriterSession:
        assert transactional
        return WriterSession(self)


@pytest.fixture
def facade(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Meridian, WriterAdapter]]:
    original = support.manifest

    def manifest() -> Any:
        value = original()
        return replace(
            value,
            available_operation_contracts=(
                *value.available_operation_contracts,
                "meridian.transaction",
            ),
            descriptor=replace(
                value.descriptor,
                capabilities=(
                    *value.descriptor.capabilities,
                    OperationCapability(
                        "meridian.transaction",
                        ("1.0.0",),
                        guarantees=("atomic", "no-dirty-reads"),
                    ),
                ),
            ),
        )

    adapter = WriterAdapter()
    monkeypatch.setattr(support, "manifest", manifest)
    monkeypatch.setattr(support, "RecordingAdapter", lambda: adapter)
    runtime, _ = support.real_facade()
    runtime.start()
    try:
        with runtime.context(OperationContext(principal_ref="test", request_id="request")):
            yield runtime, adapter
    finally:
        runtime.close()


def intent(**changes: Any) -> OutboxDataV1:
    return replace(
        OutboxDataV1(
            "investigation.cases",
            "investigation.case@1",
            "case-1",
            "put",
            source_version=1,
            payload={"id": "case-1", "title": "Case"},
            occurred_at=datetime(2026, 9, 6, tzinfo=UTC),
            event_id="event-1",
        ),
        **changes,
        digest="",
    )


def mutation(runtime: Meridian, **changes: Any) -> Expression:
    arguments = {"resource": "investigation.cases", "data": {"id": "case-1", "title": "Case"}}
    arguments["mode"] = "if_absent"
    arguments.update(changes)
    return Expression("structured", "put", arguments)


def writer(runtime: Meridian) -> TransactionalOutboxWriter:
    return TransactionalOutboxWriter(runtime, outbox_resource="investigation.case_search")


def test_public_facade_commits_source_and_explicit_insert(
    facade: tuple[Meridian, WriterAdapter],
) -> None:
    runtime, adapter = facade
    response = writer(runtime).commit(mutation(runtime), intent())
    assert response.data == adapter.returned
    assert adapter.rows["outbox"] == intent().to_mapping()
    assert adapter.commits == 1
    assert len(adapter.requests) == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"source_schema": "investigation.case@2"},
        {"source_identity": "other"},
        {"source_version": 0},
        {"source_version": "1"},
        {"source_version": None},
        {"mutation_kind": "patch"},
        {"payload": {"title": "incorrect"}},
        {"source_resource": "investigation.case_search"},
    ],
)
def test_intent_mismatch_rolls_back_source(
    facade: tuple[Meridian, WriterAdapter],
    changes: dict[str, Any],
) -> None:
    runtime, adapter = facade
    with pytest.raises(DataLifecycleValidationError):
        writer(runtime).commit(mutation(runtime), intent(**changes))
    assert adapter.rows == {}
    assert adapter.rollbacks == 1
    assert len(adapter.requests) == 1


@pytest.mark.parametrize(
    "returned",
    [
        None,
        [],
        [{"id": "case-1"}, {"id": "case-2"}],
        {"id": "other", "recordVersion": 1},
        {"recordVersion": 1},
        {"id": "case-1", "recordVersion": True},
        {"id": "case-1", "recordVersion": {}},
        {"id": "case-1", "recordVersion": 2},
    ],
)
def test_bad_result_rolls_back(facade: tuple[Meridian, WriterAdapter], returned: Any) -> None:
    runtime, adapter = facade
    adapter.returned = returned
    with pytest.raises(DataLifecycleValidationError):
        writer(runtime).commit(mutation(runtime), intent())
    assert adapter.rows == {}
    assert adapter.rollbacks == 1


def test_failed_append_rolls_back_and_error_propagates(
    facade: tuple[Meridian, WriterAdapter],
) -> None:
    runtime, adapter = facade
    adapter.fail_append = True
    with pytest.raises(ConflictError):
        writer(runtime).commit(mutation(runtime), intent())
    assert adapter.rows == {}
    assert adapter.rollbacks == 1


def test_recognized_replay_preserves_processing_state(
    facade: tuple[Meridian, WriterAdapter],
) -> None:
    runtime, adapter = facade
    with runtime.context(
        OperationContext(principal_ref="test", request_id="request", idempotency_key="same")
    ):
        first = writer(runtime).commit(mutation(runtime), intent())
        adapter.rows["progress"] = {"state": "COMPLETED", "attempt": 3, "checkpoint": 1}
        before = deepcopy(adapter.rows)
        second = writer(runtime).commit(mutation(runtime), intent())
        assert second.data == first.data
        assert adapter.rows == before
        with pytest.raises(ConflictError):
            writer(runtime).commit(mutation(runtime), intent(target_labels=("different",)))
        assert adapter.rows == before


@pytest.mark.parametrize("changed", [False, True])
def test_distinct_duplicate_cannot_overwrite_intent_or_progress(
    facade: tuple[Meridian, WriterAdapter],
    changed: bool,
) -> None:
    runtime, adapter = facade
    writer(runtime).commit(mutation(runtime), intent())
    adapter.rows["progress"] = {"state": "LEASED", "owner": "worker", "attempt": 2}
    before = deepcopy(adapter.rows)
    duplicate = intent(target_labels=("different",)) if changed else intent()
    with pytest.raises(ConflictError):
        writer(runtime).commit(mutation(runtime), duplicate)
    assert adapter.rows == before


def test_nested_failure_keeps_existing_transaction_rollback_behavior(
    facade: tuple[Meridian, WriterAdapter],
) -> None:
    runtime, adapter = facade
    with runtime.transaction("investigation.cases"), pytest.raises(DataLifecycleValidationError):
        writer(runtime).commit(mutation(runtime), intent(source_version=99))
    assert adapter.rows == {}
    assert adapter.rollbacks == 1


def test_mapping_resource_singleton_result_and_immutable_reference(
    facade: tuple[Meridian, WriterAdapter],
) -> None:
    runtime, adapter = facade
    adapter.returned = [adapter.returned]
    writer(runtime).commit(
        mutation(runtime, resource=support.SOURCE.to_dict()),
        intent(
            source_schema="structured:investigation.case@1",
            payload=None,
            immutable_reference={"resource": "investigation.cases", "id": "case-1", "version": 1},
        ),
    )
    assert adapter.commits == 1


def test_writer_rejects_empty_outbox_resource(facade: tuple[Meridian, WriterAdapter]) -> None:
    with pytest.raises(ValueError, match="outbox_resource"):
        TransactionalOutboxWriter(facade[0], outbox_resource="")


@pytest.mark.parametrize(
    "change", ["fingerprint", "registry", "contract", "version", "resource", "catalog"]
)
def test_inconsistent_result_provenance_aborts(
    facade: tuple[Meridian, WriterAdapter],
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    runtime, adapter = facade
    execute = runtime.execute

    def corrupted(expression: Expression) -> Any:
        response = execute(expression)
        updates: dict[str, Any] = {
            "fingerprint": {"operation_fingerprint": "sha256:" + "f" * 64},
            "registry": {"registry_fingerprint": "sha256:" + "f" * 64},
            "contract": {"operation_contract": "other.put"},
            "version": {"operation_version": "99.0.0"},
            "resource": {"resources": (support.TARGET,)},
            "catalog": {"catalog": "object", "resources": ("object:investigation.cases",)},
        }
        return replace(response, **updates[change])

    monkeypatch.setattr(runtime, "execute", corrupted)
    with pytest.raises(DataLifecycleValidationError):
        writer(runtime).commit(mutation(runtime), intent())
    assert adapter.rows == {}
    assert adapter.rollbacks == 1


def test_actual_identity_must_also_match_normalized_mutation(
    facade: tuple[Meridian, WriterAdapter],
) -> None:
    runtime, adapter = facade
    adapter.returned = {"id": "other", "recordVersion": 1}
    with pytest.raises(DataLifecycleValidationError, match="mutation Data"):
        writer(runtime).commit(mutation(runtime), intent(source_identity="other", payload={}))
    assert adapter.rows == {}


def test_composite_identity_uses_schema_order(facade: tuple[Meridian, WriterAdapter]) -> None:
    from meridian_storage.transactions.manager import current_transaction

    runtime, adapter = facade
    with runtime.transaction("investigation.cases"):
        frame = current_transaction()
        assert frame is not None
        ref = frame.snapshot.resource(support.SOURCE).schema
        assert ref is not None
        schema = frame.snapshot.schemas[ref]
        frame.snapshot = replace(
            frame.snapshot,
            schemas={
                **frame.snapshot.schemas,
                ref: replace(schema, definition={**schema.definition, "identity": ["title", "id"]}),
            },
        )
        writer(runtime).commit(mutation(runtime), intent(source_identity=("Case", "case-1")))
    assert adapter.commits == 1


def test_unversioned_data_requires_no_fabricated_version(
    facade: tuple[Meridian, WriterAdapter],
) -> None:
    runtime, adapter = facade
    adapter.returned = {"id": "case-1", "title": "Case"}
    writer(runtime).commit(mutation(runtime), intent(source_version=None))
    assert adapter.commits == 1
