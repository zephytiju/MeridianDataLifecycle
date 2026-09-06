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

"""Atomic mutation plus outbox composition through public Meridian operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, cast

from meridian_storage.transactions.manager import current_transaction

from meridian_storage import Expression, Meridian, Operation, OperationResult, ResourceRef
from meridian_storage.projection._canonical import sha256_digest
from meridian_storage.projection.errors import DataLifecycleValidationError
from meridian_storage.projection.outbox.model import OutboxDataV1


class _StructuredSurface(Protocol):
    def put(self, *, resource: str, data: object, mode: str) -> Expression: ...


def _same(left: object, right: object) -> bool:
    # Canonical comparison keeps boolean, integer and string identities distinct.
    return sha256_digest(left) == sha256_digest(right)


def _record(data: object) -> Mapping[str, object]:
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)) and len(data) == 1:
        data = data[0]
    if not isinstance(data, Mapping):
        raise DataLifecycleValidationError("source result must identify exactly one Data value")
    return cast(Mapping[str, object], data)


class TransactionalOutboxWriter:
    """Commit a validated source mutation and immutable intent in one transaction.

    Existing adapter idempotency may recognize a replay. This writer never
    catches a duplicate conflict, reconciles persisted intent, or replays a group.
    """

    def __init__(self, meridian: Meridian, *, outbox_resource: str) -> None:
        if not outbox_resource:
            raise ValueError("outbox_resource must be non-empty")
        self._meridian = meridian
        self._outbox_resource = outbox_resource

    def commit(self, mutation: Expression, intent: OutboxDataV1) -> OperationResult:
        structured = cast(_StructuredSurface, self._meridian.catalog("structured"))
        source = ResourceRef.parse(intent.source_resource, catalog=intent.source_catalog)
        with self._meridian.transaction(source):
            # Use the same registered normalizer and validation as execute. These
            # internal Core reads are tested against the exact released Core pin.
            provider = self._meridian._catalog_providers[mutation.catalog]
            contract = self._meridian._catalog_manifests[mutation.catalog].operation_for(
                mutation.method
            )
            operation = self._meridian._validate_operation(
                provider.normalize(mutation), mutation, contract
            )
            source_result = self._meridian.execute(mutation)
            self._validate_source(operation, mutation, source_result, source, intent)
            self._meridian.execute(
                structured.put(
                    resource=self._outbox_resource,
                    data=intent.to_mapping(),
                    mode="if_absent",
                )
            )
        return source_result

    def _validate_source(
        self,
        operation: Operation,
        mutation: Expression,
        result: OperationResult,
        source: ResourceRef,
        intent: OutboxDataV1,
    ) -> None:
        if operation.catalog != intent.source_catalog or result.catalog != operation.catalog:
            raise DataLifecycleValidationError("source Catalog does not match outbox intent")
        if operation.resources != (source,) or result.resources != operation.resources:
            raise DataLifecycleValidationError("source Resource does not match outbox intent")
        if operation.read_only or mutation.method != intent.mutation_kind:
            raise DataLifecycleValidationError("source mutation kind does not match outbox intent")
        frame = current_transaction()
        assert frame is not None
        snapshot = frame.snapshot
        if (
            result.operation_contract != operation.operation_contract
            or result.operation_version != operation.operation_version
            or result.operation_fingerprint != operation.request_fingerprint
            or result.registry_fingerprint != snapshot.fingerprint
        ):
            raise DataLifecycleValidationError("source result does not match normalized mutation")
        schema_ref = snapshot.resource(source).schema
        if schema_ref is None or intent.source_schema not in (
            str(schema_ref),
            f"{schema_ref.logical_name}@{schema_ref.version}",
        ):
            raise DataLifecycleValidationError("source Schema does not match outbox intent")
        schema = snapshot.schema(
            schema_ref.catalog, schema_ref.namespace, schema_ref.name, schema_ref.version
        )
        fields = schema.definition.get("identity", ())
        if not isinstance(fields, (list, tuple)) or any(not isinstance(f, str) for f in fields):
            raise DataLifecycleValidationError("source Schema identity is invalid")
        record = _record(result.data)
        values = tuple(record.get(cast(str, name)) for name in fields)
        identity = values[0] if len(values) == 1 else values or None
        if any(value is None for value in values) or not _same(identity, intent.source_identity):
            raise DataLifecycleValidationError("source identity does not match outbox intent")
        version = record.get("recordVersion")
        if isinstance(version, bool) or not isinstance(version, (str, int, type(None))):
            raise DataLifecycleValidationError("source version is invalid")
        if not _same(version, intent.source_version):
            raise DataLifecycleValidationError("source version does not match outbox intent")
        data = operation.input.get("data")
        if isinstance(data, Mapping):
            for name in fields:
                if name in data and not _same(data[name], record.get(cast(str, name))):
                    raise DataLifecycleValidationError("source identity differs from mutation Data")
        if intent.payload is not None:
            for name, value in intent.payload.items():
                if name not in record or not _same(value, record[name]):
                    raise DataLifecycleValidationError("source payload does not match result Data")
