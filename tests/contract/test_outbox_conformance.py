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

import inspect
from datetime import datetime

import pytest

from meridian_storage.projection import InMemoryOutboxStore
from meridian_storage.projection.outbox import Checkpoint
from meridian_storage.projection.testing import (
    OutboxConformanceTarget,
    run_outbox_conformance,
)


def reference_target(store: InMemoryOutboxStore) -> OutboxConformanceTarget:
    # Reusing this reference store exercises lifecycle assertions, not durability.
    return OutboxConformanceTarget(store, store.append, store.get, store.checkpoint, lambda: store)


def test_portable_outbox_lifecycle_fixtures() -> None:
    report = run_outbox_conformance(reference_target(InMemoryOutboxStore(poison_threshold=2)))
    assert len(report.checks) == 5
    assert report.same_owner_completion == "accepted-indistinguishable-owner"
    assert report.same_owner_release == "accepted-indistinguishable-owner"
    assert report.to_mapping()["generationFencing"] == "unsupported-by-owner-only-contract"


class WrongVersionStore(InMemoryOutboxStore):
    def complete(
        self,
        event_id: str,
        *,
        owner: str,
        acknowledged_source_version: str | int | None,
        target_fingerprint: str,
        now: datetime | None = None,
    ) -> Checkpoint:
        return super().complete(
            event_id,
            owner=owner,
            acknowledged_source_version=self.get(event_id).data.source_version,
            target_fingerprint=target_fingerprint,
            now=now,
        )


def test_fixtures_detect_adapter_that_ignores_exact_acknowledgement() -> None:
    with pytest.raises(AssertionError, match="MERIDIAN_CHECKPOINT_CONFLICT"):
        run_outbox_conformance(reference_target(WrongVersionStore(poison_threshold=2)))


def test_existing_outbox_port_signatures_are_unchanged() -> None:
    from meridian_storage.projection.outbox import OutboxPort

    expected = {
        "atomic_claim": ("self", "owner", "limit", "lease_duration", "now"),
        "complete": (
            "self",
            "event_id",
            "owner",
            "acknowledged_source_version",
            "target_fingerprint",
            "now",
        ),
        "release": ("self", "event_id", "owner", "error", "retryable", "now"),
        "lag": ("self", "now"),
    }
    for name, parameters in expected.items():
        signature = inspect.signature(getattr(OutboxPort, name))
        assert tuple(signature.parameters) == parameters
        for key, parameter in signature.parameters.items():
            if key not in ("self", "event_id"):
                assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters["now"].default is None
