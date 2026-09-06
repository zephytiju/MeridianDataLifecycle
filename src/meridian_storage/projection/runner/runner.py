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

"""Host-driven projection runner over transactional outbox records."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite
from threading import Event
from typing import Protocol
from uuid import uuid4

from meridian_storage.semantics import CatalogName
from meridian_storage.spi.capabilities import capability_violations

from meridian_storage import Expression, Meridian, MeridianError, OperationResult, ResourceRef
from meridian_storage.projection._canonical import ensure_utc, sha256_digest, utc_now
from meridian_storage.projection.errors import ProjectionRejected
from meridian_storage.projection.evidence import EvidenceSink, LifecycleEvidence, NullEvidenceSink
from meridian_storage.projection.outbox import OutboxDataV1, OutboxPort, OutboxRecord, ProjectionLag

_CATALOG_NAMES = frozenset(catalog.value for catalog in CatalogName)


@dataclass(frozen=True, slots=True)
class ProjectionSpec:
    """Catalog-specific mapping from one logical Resource to a derived Resource."""

    name: str
    source_catalog: str
    source: str
    target_catalog: str
    target: str
    source_schema: str
    target_schema: str
    target_labels: tuple[str, ...] = ()
    eventual_visibility_seconds: float = 30.0

    def __post_init__(self) -> None:
        for field_name in (
            "name",
            "source_catalog",
            "source",
            "target_catalog",
            "target",
            "source_schema",
            "target_schema",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must be non-empty")
        if self.source_catalog not in _CATALOG_NAMES:
            raise ValueError(f"unknown source Catalog {self.source_catalog!r}")
        if self.target_catalog not in _CATALOG_NAMES:
            raise ValueError(f"unknown target Catalog {self.target_catalog!r}")
        if not isfinite(self.eventual_visibility_seconds) or self.eventual_visibility_seconds <= 0:
            raise ValueError("eventual_visibility_seconds must be positive")
        if len(set(self.target_labels)) != len(self.target_labels):
            raise ValueError("target_labels must not contain duplicates")
        object.__setattr__(self, "target_labels", tuple(self.target_labels))

    def to_mapping(self) -> dict[str, object]:
        return {
            "name": self.name,
            "sourceCatalog": self.source_catalog,
            "source": self.source,
            "targetCatalog": self.target_catalog,
            "target": self.target,
            "sourceSchema": self.source_schema,
            "targetSchema": self.target_schema,
            "targetLabels": list(self.target_labels),
            "eventualVisibilitySeconds": self.eventual_visibility_seconds,
        }


@dataclass(frozen=True, slots=True)
class ProjectionContext:
    projection: str
    event_id: str
    source_catalog: str
    source_resource: str
    source_schema: str
    source_identity: object
    source_version: str | int | None
    mutation_kind: str
    occurred_at: datetime
    operation_context: Mapping[str, str]


class Projector(Protocol):
    def __call__(self, source: Mapping[str, object], context: ProjectionContext) -> Expression: ...


class SourceVersionLoader(Protocol):
    def __call__(
        self,
        reference: Mapping[str, object],
        source_version: str | int | None,
    ) -> Mapping[str, object]: ...


class AcknowledgementExtractor(Protocol):
    def __call__(
        self,
        result: OperationResult,
        source: OutboxDataV1,
    ) -> tuple[str | int | None, str]: ...


@dataclass(frozen=True, slots=True)
class ProjectionRun:
    claimed: int = 0
    completed: int = 0
    retryable: int = 0
    quarantined: int = 0

    def __add__(self, other: ProjectionRun) -> ProjectionRun:
        return ProjectionRun(
            claimed=self.claimed + other.claimed,
            completed=self.completed + other.completed,
            retryable=self.retryable + other.retryable,
            quarantined=self.quarantined + other.quarantined,
        )


def _default_acknowledgement(
    result: OperationResult, source: OutboxDataV1
) -> tuple[str | int | None, str]:
    acknowledged: str | int | None = None
    if isinstance(result.data, Mapping):
        for key in ("acknowledgedSourceVersion", "acknowledged_source_version", "sourceVersion"):
            candidate = result.data.get(key)
            if isinstance(candidate, (str, int)) and not isinstance(candidate, bool):
                acknowledged = candidate
                break
    if source.source_version is None:
        acknowledged = None
    elif acknowledged is None:
        raise ProjectionRejected("target result did not acknowledge the source version")
    fingerprint = result.operation_fingerprint or sha256_digest(result.data)
    return acknowledged, fingerprint


class ProjectionRunner:
    """A synchronous library runner; the host owns scheduling and worker lifecycle."""

    def __init__(
        self,
        *,
        meridian: Meridian,
        spec: ProjectionSpec,
        project: Projector,
        outbox: OutboxPort,
        source_loader: SourceVersionLoader | None = None,
        acknowledgement: AcknowledgementExtractor = _default_acknowledgement,
        evidence: EvidenceSink | None = None,
        batch_size: int = 100,
        lease_seconds: float = 60.0,
        worker_id: str | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._meridian = meridian
        self.spec = spec
        self._project = project
        self._outbox = outbox
        self._source_loader = source_loader
        self._acknowledgement = acknowledgement
        self._evidence = evidence or NullEvidenceSink()
        self._batch_size = batch_size
        self._lease = timedelta(seconds=lease_seconds)
        self._worker_id = worker_id or f"projection-{uuid4()}"
        self._clock = clock
        self._validate_bindings()

    def _validate_bindings(self) -> None:
        # Catalog resolution requires an already-started runtime. Inspect one
        # immutable registry revision; never invoke the projector or open a session.
        # These internal Core reads are covered against our exact released Core pin.
        self._meridian.catalog(self.spec.source_catalog)
        self._meridian.catalog(self.spec.target_catalog)
        snapshot = self._meridian._snapshot_for_handle()
        for role, catalog, name, schema_name in (
            ("source", self.spec.source_catalog, self.spec.source, self.spec.source_schema),
            ("target", self.spec.target_catalog, self.spec.target, self.spec.target_schema),
        ):
            try:
                ref = ResourceRef.parse(name, catalog=catalog)
            except (ValueError, TypeError) as error:
                raise ProjectionRejected(f"invalid {role} Resource reference") from error
            if ref.catalog != catalog:
                raise ProjectionRejected(f"{role} Resource Catalog does not match projection spec")
            resource = snapshot.resource(ref)
            schema = resource.schema
            if schema is None or schema_name not in (
                str(schema),
                f"{schema.logical_name}@{schema.version}",
            ):
                raise ProjectionRejected(f"{role} Resource Schema does not match projection spec")
            snapshot.schema(schema.catalog, schema.namespace, schema.name, schema.version)
            try:
                binding = snapshot.binding_for(ref)
            except KeyError as error:
                raise ProjectionRejected(f"{role} Resource has no resolved Binding") from error
            manifest = self._meridian._capability_manifests.get(binding.binding_id)
            if (
                manifest is None
                or manifest.adapter_id != binding.adapter_id
                or manifest.fingerprint != binding.capability_fingerprint
            ):
                raise ProjectionRejected(f"{role} Resource Binding Capability is unresolved")
            if capability_violations(manifest, resource.requirements):
                raise ProjectionRejected(f"{role} Resource requires unavailable capabilities")

    def _source(self, record: OutboxRecord) -> Mapping[str, object]:
        if record.data.payload is not None:
            return record.data.payload
        assert record.data.immutable_reference is not None
        if self._source_loader is None:
            raise ProjectionRejected("referenced outbox data requires a source version loader")
        return self._source_loader(
            record.data.immutable_reference,
            record.data.source_version,
        )

    def _context(self, record: OutboxRecord) -> ProjectionContext:
        data = record.data
        return ProjectionContext(
            projection=self.spec.name,
            event_id=data.event_id,
            source_catalog=data.source_catalog,
            source_resource=data.source_resource,
            source_schema=data.source_schema,
            source_identity=data.source_identity,
            source_version=data.source_version,
            mutation_kind=data.mutation_kind,
            occurred_at=data.occurred_at,
            operation_context=data.operation_context,
        )

    def _validate_record(self, record: OutboxRecord) -> None:
        data = record.data
        if data.source_catalog != self.spec.source_catalog:
            raise ProjectionRejected("outbox source Catalog does not match projection spec")
        if data.source_resource != self.spec.source:
            raise ProjectionRejected("outbox source Resource does not match projection spec")
        if data.source_schema != self.spec.source_schema:
            raise ProjectionRejected("outbox source Schema does not match projection spec")
        if not set(self.spec.target_labels).issubset(data.target_labels):
            raise ProjectionRejected("outbox target labels do not match projection spec")

    def _validate_expression(self, expression: Expression) -> None:
        if expression.catalog != self.spec.target_catalog:
            raise ProjectionRejected("projector returned an Expression for the wrong Catalog")
        resource = expression.arguments.get("resource")
        if isinstance(resource, str):
            matches = resource == self.spec.target
        elif isinstance(resource, Mapping):
            catalog = resource.get("catalog", self.spec.target_catalog)
            namespace = resource.get("namespace")
            name = resource.get("name")
            matches = (
                catalog == self.spec.target_catalog
                and isinstance(namespace, str)
                and isinstance(name, str)
                and f"{namespace}.{name}" == self.spec.target
            )
        else:
            matches = False
        if not matches:
            raise ProjectionRejected("projector returned an Expression for the wrong Resource")

    def run_once(self, *, now: datetime | None = None) -> ProjectionRun:
        claim_time = ensure_utc(now or self._clock(), field="now")
        records = self._outbox.atomic_claim(
            owner=self._worker_id,
            limit=self._batch_size,
            lease_duration=self._lease,
            now=claim_time,
        )
        completed = retryable = quarantined = 0
        for record in records:
            try:
                self._validate_record(record)
                expression = self._project(self._source(record), self._context(record))
                self._validate_expression(expression)
                result = self._meridian.execute(expression)
                source_version, target_fingerprint = self._acknowledgement(result, record.data)
                self._outbox.complete(
                    record.data.event_id,
                    owner=self._worker_id,
                    acknowledged_source_version=source_version,
                    target_fingerprint=target_fingerprint,
                    now=self._clock(),
                )
                completed += 1
                self._evidence.record(
                    LifecycleEvidence(
                        kind="projection",
                        subject=self.spec.name,
                        state="COMPLETED",
                        details={
                            "eventId": record.data.event_id,
                            "sourceVersion": record.data.source_version,
                            "targetFingerprint": target_fingerprint,
                        },
                        occurred_at=self._clock(),
                    )
                )
            except Exception as error:
                is_retryable = isinstance(error, MeridianError) and error.retryable
                released = self._outbox.release(
                    record.data.event_id,
                    owner=self._worker_id,
                    error=error,
                    retryable=is_retryable,
                    now=self._clock(),
                )
                if released.state.value == "QUARANTINED":
                    quarantined += 1
                else:
                    retryable += 1
                self._evidence.record(
                    LifecycleEvidence(
                        kind="projection",
                        subject=self.spec.name,
                        state=released.state.value,
                        details={
                            "eventId": record.data.event_id,
                            "failureCode": released.failure.code if released.failure else "unknown",
                        },
                        occurred_at=self._clock(),
                    )
                )
        return ProjectionRun(
            claimed=len(records),
            completed=completed,
            retryable=retryable,
            quarantined=quarantined,
        )

    def run_until_idle(self, *, max_cycles: int = 10_000) -> ProjectionRun:
        if max_cycles < 1:
            raise ValueError("max_cycles must be positive")
        total = ProjectionRun()
        for _ in range(max_cycles):
            current = self.run_once()
            total += current
            if current.claimed == 0:
                return total
        raise RuntimeError("projection did not become idle within max_cycles")

    def run_until_stopped(
        self,
        stop: Event,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> ProjectionRun:
        if poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be non-negative")
        total = ProjectionRun()
        while not stop.is_set():
            current = self.run_once()
            total += current
            if current.claimed == 0:
                stop.wait(poll_interval_seconds)
        return total

    def lag(self, *, now: datetime | None = None) -> ProjectionLag:
        observed = self._outbox.lag(now=now)
        return ProjectionLag(
            incomplete_count=observed.incomplete_count,
            oldest_incomplete_at=observed.oldest_incomplete_at,
            lag_seconds=observed.lag_seconds,
            target_seconds=self.spec.eventual_visibility_seconds,
        )
