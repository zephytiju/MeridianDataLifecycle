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

from collections.abc import Mapping
from typing import Protocol, cast

from meridian_storage import Expression, Meridian, OperationResult
from meridian_storage.projection.errors import DataLifecycleValidationError
from meridian_storage.projection.outbox.model import OutboxDataV1


class _StructuredSurface(Protocol):
    def put(self, *, resource: str, data: object) -> Expression: ...


class TransactionalOutboxWriter:
    """Commit a source mutation and its outbox intent in one Binding transaction."""

    def __init__(self, meridian: Meridian, *, outbox_resource: str) -> None:
        if not outbox_resource:
            raise ValueError("outbox_resource must be non-empty")
        self._meridian = meridian
        self._outbox_resource = outbox_resource

    def commit(self, mutation: Expression, intent: OutboxDataV1) -> OperationResult:
        if mutation.catalog != intent.source_catalog:
            raise DataLifecycleValidationError(
                "mutation Catalog does not match the outbox source Catalog"
            )
        resource = mutation.arguments.get("resource")
        if isinstance(resource, str):
            matches = resource == intent.source_resource
        elif isinstance(resource, Mapping):
            catalog = resource.get("catalog", intent.source_catalog)
            namespace = resource.get("namespace")
            name = resource.get("name")
            matches = (
                catalog == intent.source_catalog
                and isinstance(namespace, str)
                and isinstance(name, str)
                and f"{namespace}.{name}" == intent.source_resource
            )
        else:
            matches = False
        if not matches:
            raise DataLifecycleValidationError(
                "mutation Resource does not match the outbox source Resource"
            )
        structured = cast(_StructuredSurface, self._meridian.catalog("structured"))
        with self._meridian.transaction(intent.source_resource):
            source_result = self._meridian.execute(mutation)
            outbox_expression = structured.put(
                resource=self._outbox_resource,
                data=intent.to_mapping(),
            )
            self._meridian.execute(outbox_expression)
        return source_result
