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
from enum import StrEnum
from typing import Protocol, cast

import pytest

from meridian_storage import Expression, Meridian, OperationResult
from meridian_storage.projection._canonical import (
    canonical_json_bytes,
    ensure_utc,
    parse_rfc3339,
    rfc3339,
    sha256_digest,
    to_json_value,
)
from meridian_storage.projection.control import CancellationToken
from meridian_storage.projection.errors import OperationCancelled
from meridian_storage.projection.evidence import (
    InMemoryEvidenceSink,
    LifecycleEvidence,
    MeridianEvidenceSink,
    NullEvidenceSink,
)
from tests.conftest import result


class State(StrEnum):
    READY = "ready"


def test_canonical_json_and_time_validation(fixed_time: datetime) -> None:
    assert canonical_json_bytes({"b": 1, "a": State.READY}) == b'{"a":"ready","b":1}'
    assert sha256_digest({"a": 1}) == sha256_digest({"a": 1})
    assert rfc3339(fixed_time).endswith("Z")
    assert parse_rfc3339(rfc3339(fixed_time), field="value") == fixed_time
    assert to_json_value((1, "two")) == [1, "two"]
    with pytest.raises(ValueError, match="timezone-aware"):
        ensure_utc(datetime(2026, 1, 1), field="value")
    with pytest.raises(ValueError, match="RFC 3339"):
        parse_rfc3339("not-a-date", field="value")
    with pytest.raises(ValueError, match="non-finite"):
        canonical_json_bytes(float("nan"))
    with pytest.raises(TypeError, match="keys"):
        canonical_json_bytes({1: "bad"})
    with pytest.raises(TypeError, match="unsupported"):
        canonical_json_bytes(object())


def test_cancellation_and_evidence(fixed_time: datetime) -> None:
    token = CancellationToken()
    token.raise_if_cancelled()
    token.cancel()
    assert token.cancelled
    with pytest.raises(OperationCancelled):
        token.raise_if_cancelled()

    evidence = LifecycleEvidence(
        kind="projection",
        subject="cases",
        state="COMPLETED",
        details={"count": 1},
        occurred_at=fixed_time,
    )
    assert evidence.fingerprint.startswith("sha256:")
    assert evidence.to_mapping()["occurredAt"] == rfc3339(fixed_time)
    with pytest.raises(ValueError, match="non-empty"):
        LifecycleEvidence(kind="", subject="x", state="y")
    with pytest.raises(ValueError, match="fingerprint"):
        LifecycleEvidence(
            kind="x",
            subject="y",
            state="z",
            fingerprint="sha256:wrong",
        )

    sink = InMemoryEvidenceSink()
    sink.record(evidence)
    assert sink.events == (evidence,)
    NullEvidenceSink().record(evidence)


class _EvidenceSurface(Protocol):
    def append(self, *, resource: str, data: object) -> Expression: ...


class _FakeMeridian:
    def __init__(self) -> None:
        self.expressions: list[Expression] = []

    def catalog(self, name: str) -> _EvidenceSurface:
        assert name == "evidence"

        class Surface:
            def append(self, *, resource: str, data: object) -> Expression:
                return Expression("evidence", "append", {"resource": resource, "data": data})

        return Surface()

    def execute(self, expression: Expression) -> OperationResult:
        self.expressions.append(expression)
        return result(None)


def test_meridian_evidence_sink(fixed_time: datetime) -> None:
    runtime = _FakeMeridian()
    with pytest.raises(ValueError, match="resource"):
        MeridianEvidenceSink(cast(Meridian, runtime), resource="")
    sink = MeridianEvidenceSink(cast(Meridian, runtime), resource="lifecycle.events")
    sink.record(
        LifecycleEvidence(
            kind="migration", subject="binding", state="PLANNED", occurred_at=fixed_time
        )
    )
    assert runtime.expressions[0].catalog == "evidence"
