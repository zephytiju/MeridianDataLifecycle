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

from contextlib import AbstractContextManager
from datetime import datetime
from types import TracebackType
from typing import cast

import pytest

from meridian_storage import Expression, Meridian, OperationResult
from meridian_storage.projection.errors import DataLifecycleValidationError
from meridian_storage.projection.outbox import OutboxDataV1, TransactionalOutboxWriter
from tests.conftest import result


class Transaction(AbstractContextManager["Transaction"]):
    def __init__(self, log: list[str]) -> None:
        self.log = log

    def __enter__(self) -> Transaction:
        self.log.append("begin")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.log.append("end")


class Surface:
    def put(self, *, resource: str, data: object) -> Expression:
        return Expression("structured", "put", {"resource": resource, "data": data})


class Runtime:
    def __init__(self) -> None:
        self.log: list[str] = []

    def catalog(self, name: str) -> Surface:
        assert name == "structured"
        return Surface()

    def transaction(self, resource: str) -> Transaction:
        self.log.append(f"transaction:{resource}")
        return Transaction(self.log)

    def execute(self, expression: Expression) -> OperationResult:
        self.log.append(f"execute:{expression.arguments['resource']}")
        return result({"ok": True})


def test_transactional_outbox_uses_one_public_transaction(fixed_time: datetime) -> None:
    runtime = Runtime()
    writer = TransactionalOutboxWriter(cast(Meridian, runtime), outbox_resource="system.outbox")
    intent = OutboxDataV1(
        "cases",
        "case@1",
        "case-1",
        "put",
        payload={"id": "case-1"},
        occurred_at=fixed_time,
        event_id="event",
    )
    mutation = Expression("structured", "put", {"resource": "cases", "data": {}})
    writer.commit(mutation, intent)
    assert runtime.log == [
        "transaction:cases",
        "begin",
        "execute:cases",
        "execute:system.outbox",
        "end",
    ]
    with pytest.raises(DataLifecycleValidationError, match="Catalog"):
        writer.commit(Expression("object", "put", {"resource": "cases"}), intent)
    with pytest.raises(DataLifecycleValidationError, match="Resource"):
        writer.commit(Expression("structured", "put", {"resource": "other"}), intent)
    with pytest.raises(ValueError, match="outbox_resource"):
        TransactionalOutboxWriter(cast(Meridian, runtime), outbox_resource="")
