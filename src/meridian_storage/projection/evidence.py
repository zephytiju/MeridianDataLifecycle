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

"""Evidence hooks; the registered evidence Catalog retains public authority."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Protocol

from meridian_storage import Meridian
from meridian_storage.projection._canonical import (
    ensure_utc,
    immutable_mapping,
    rfc3339,
    sha256_digest,
    to_json_value,
    utc_now,
)


@dataclass(frozen=True, slots=True)
class LifecycleEvidence:
    """One redacted lifecycle state transition suitable for Evidence Data."""

    kind: str
    subject: str
    state: str
    details: Mapping[str, object] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=utc_now)
    fingerprint: str = ""

    def __post_init__(self) -> None:
        if not self.kind or not self.subject or not self.state:
            raise ValueError("evidence kind, subject, and state must be non-empty")
        object.__setattr__(self, "details", immutable_mapping(self.details))
        object.__setattr__(self, "occurred_at", ensure_utc(self.occurred_at, field="occurred_at"))
        expected = sha256_digest(
            {
                "kind": self.kind,
                "subject": self.subject,
                "state": self.state,
                "details": self.details,
            }
        )
        if self.fingerprint and self.fingerprint != expected:
            raise ValueError("evidence fingerprint does not match canonical content")
        object.__setattr__(self, "fingerprint", expected)

    def to_mapping(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "subject": self.subject,
            "state": self.state,
            "details": to_json_value(self.details),
            "occurredAt": rfc3339(self.occurred_at),
            "fingerprint": self.fingerprint,
        }


class EvidenceSink(Protocol):
    """Injected evidence writer; implementations choose the Evidence Resource."""

    def record(self, evidence: LifecycleEvidence) -> None: ...


class NullEvidenceSink:
    """No-op evidence hook for deployments where evidence is not required."""

    def record(self, evidence: LifecycleEvidence) -> None:
        del evidence


class InMemoryEvidenceSink:
    """Thread-safe evidence collector for deterministic tests and local proofs."""

    def __init__(self) -> None:
        self._events: list[LifecycleEvidence] = []
        self._lock = Lock()

    @property
    def events(self) -> tuple[LifecycleEvidence, ...]:
        with self._lock:
            return tuple(self._events)

    def record(self, evidence: LifecycleEvidence) -> None:
        with self._lock:
            self._events.append(evidence)


class MeridianEvidenceSink:
    """Write lifecycle evidence through the registered public Evidence Catalog."""

    def __init__(
        self,
        meridian: Meridian,
        *,
        resource: str,
        map_data: Callable[[LifecycleEvidence], Mapping[str, object]] | None = None,
    ) -> None:
        if not resource:
            raise ValueError("evidence resource must be non-empty")
        self._meridian = meridian
        self._resource = resource
        self._map_data = map_data or LifecycleEvidence.to_mapping

    def record(self, evidence: LifecycleEvidence) -> None:
        surface = self._meridian.catalog("evidence")
        expression = surface.append(resource=self._resource, data=self._map_data(evidence))
        self._meridian.execute(expression)
