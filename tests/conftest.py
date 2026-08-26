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

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from meridian_storage.semantics import JsonValue

from meridian_storage import Operation, OperationResult, ResourceRef


@pytest.fixture
def fixed_time() -> datetime:
    return datetime(2026, 8, 25, 12, 0, tzinfo=UTC)


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value

    def advance(self, *, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


def result(
    data: JsonValue,
    *,
    fingerprint: str = "sha256:" + "a" * 64,
    catalog: str = "structured",
    resource: str = "structured:investigation.cases",
) -> OperationResult:
    return OperationResult(
        data=data,
        catalog=catalog,
        operation_contract=f"{catalog}.test.v1",
        operation_version="1.0.0",
        resources=(resource,),
        request_id="request-1",
        execution_id="execution-1",
        operation_fingerprint=fingerprint,
        registry_fingerprint="sha256:" + "b" * 64,
        capability_fingerprint="sha256:" + "c" * 64,
    )


def serialized_operation(name: str) -> dict[str, object]:
    return cast(
        dict[str, object],
        Operation(
            catalog="structured",
            operation_contract="structured.query.v1",
            operation_version="1.0.0",
            resources=(ResourceRef("structured", "investigation", "cases"),),
            input={"condition": name},
            read_only=True,
            idempotent=True,
        ).to_dict(),
    )
