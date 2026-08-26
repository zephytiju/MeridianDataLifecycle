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

"""Bounded logical export/import coordination over injected adapter hooks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime
from threading import RLock
from typing import Protocol

from meridian_storage.projection._canonical import (
    canonical_json_bytes,
    require_sha256,
    sha256_digest,
    utc_now,
)
from meridian_storage.projection.control import CancellationToken
from meridian_storage.projection.errors import IntegrityError
from meridian_storage.projection.evidence import EvidenceSink, LifecycleEvidence, NullEvidenceSink
from meridian_storage.projection.portability.model import (
    ExportBoundary,
    ExportMetadata,
    ExportRequest,
    ImportReport,
    LogicalExportV1,
    LogicalRecord,
    MergePolicy,
    PartitionImportResult,
    PartitionManifest,
    RestoreValidationEvidence,
)


class LogicalExportSource(Protocol):
    def begin(self, request: ExportRequest) -> ExportBoundary: ...

    def metadata(self, request: ExportRequest, boundary: ExportBoundary) -> ExportMetadata: ...

    def partition_ids(self, request: ExportRequest, boundary: ExportBoundary) -> Sequence[str]: ...

    def records(
        self,
        request: ExportRequest,
        boundary: ExportBoundary,
        partition_id: str,
    ) -> Iterable[Sequence[LogicalRecord]]: ...

    def finish(self, request: ExportRequest, boundary: ExportBoundary) -> ExportBoundary: ...


class LogicalExportSink(Protocol):
    def begin_partition(self, partition_id: str) -> object: ...

    def write(self, handle: object, chunk: bytes) -> None: ...

    def finish_partition(self, handle: object, partition_id: str) -> str: ...


class LogicalArtifactSource(Protocol):
    """Immutable content-addressed logical artifacts, repeatable by reference."""

    def chunks(self, artifact_ref: str) -> Iterable[bytes]: ...


class LogicalImportTarget(Protocol):
    def preflight(self, metadata: ExportMetadata, merge_policy: MergePolicy) -> None: ...

    def write(self, records: Sequence[LogicalRecord], merge_policy: MergePolicy) -> None: ...

    def finalize(self, manifest: LogicalExportV1) -> str: ...


class ObjectTransferHook(Protocol):
    def transfer(self, reference: Mapping[str, object]) -> str: ...


class RecoveryValidationHook(Protocol):
    """IaC-owned backup/restore validation; no Engine lifecycle operation is exposed."""

    def validate(
        self,
        backup_reference: str,
        manifest: LogicalExportV1,
        import_report: ImportReport,
    ) -> str: ...


class InMemoryLogicalArtifacts:
    """Reference artifact sink/source used by conformance and round-trip tests."""

    def __init__(self) -> None:
        self._pending: dict[int, bytearray] = {}
        self._artifacts: dict[str, bytes] = {}
        self._next_handle = 0
        self._lock = RLock()

    def begin_partition(self, partition_id: str) -> object:
        del partition_id
        with self._lock:
            self._next_handle += 1
            self._pending[self._next_handle] = bytearray()
            return self._next_handle

    def write(self, handle: object, chunk: bytes) -> None:
        if not isinstance(handle, int):
            raise TypeError("in-memory artifact handle must be an integer")
        with self._lock:
            self._pending[handle].extend(chunk)

    def finish_partition(self, handle: object, partition_id: str) -> str:
        if not isinstance(handle, int):
            raise TypeError("in-memory artifact handle must be an integer")
        with self._lock:
            content = bytes(self._pending.pop(handle))
            reference = f"logical://{partition_id}/{hashlib.sha256(content).hexdigest()}"
            self._artifacts[reference] = content
            return reference

    def chunks(self, artifact_ref: str) -> Iterable[bytes]:
        with self._lock:
            content = self._artifacts[artifact_ref]
        if content:
            yield content


class LogicalExportCoordinator:
    """Stream canonical NDJSON partitions without owning Engine backup lifecycle."""

    def __init__(
        self,
        *,
        source: LogicalExportSource,
        sink: LogicalExportSink,
        evidence: EvidenceSink | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._source = source
        self._sink = sink
        self._evidence = evidence or NullEvidenceSink()
        self._clock = clock

    def export(
        self,
        request: ExportRequest,
        *,
        cancellation: CancellationToken | None = None,
        export_id: str,
    ) -> LogicalExportV1:
        token = cancellation or CancellationToken()
        boundary = self._source.begin(request)
        metadata = self._source.metadata(request, boundary)
        partition_ids = tuple(self._source.partition_ids(request, boundary))
        if len(set(partition_ids)) != len(partition_ids):
            raise ValueError("export source returned duplicate partition ids")
        manifests: list[PartitionManifest] = []
        for partition_id in partition_ids:
            token.raise_if_cancelled()
            handle = self._sink.begin_partition(partition_id)
            digest = hashlib.sha256()
            count = byte_count = 0
            minimum: str | None = None
            maximum: str | None = None
            previous: str | None = None
            for batch in self._source.records(request, boundary, partition_id):
                if len(batch) > request.batch_size:
                    raise ValueError("export source exceeded the requested batch size")
                token.raise_if_cancelled()
                for record in batch:
                    if previous is not None and record.logical_id <= previous:
                        raise ValueError("logical export records must be strictly ordered by id")
                    previous = record.logical_id
                    minimum = minimum or record.logical_id
                    maximum = record.logical_id
                    chunk = canonical_json_bytes(record.to_mapping()) + b"\n"
                    self._sink.write(handle, chunk)
                    digest.update(chunk)
                    count += 1
                    byte_count += len(chunk)
            artifact_ref = self._sink.finish_partition(handle, partition_id)
            manifests.append(
                PartitionManifest(
                    partition_id=partition_id,
                    artifact_ref=artifact_ref,
                    count=count,
                    bytes=byte_count,
                    minimum_logical_id=minimum,
                    maximum_logical_id=maximum,
                    sha256=f"sha256:{digest.hexdigest()}",
                )
            )
        final_boundary = self._source.finish(request, boundary)
        if final_boundary.boundary_id != boundary.boundary_id:
            raise ValueError("export source changed the boundary identity during export")
        if final_boundary.start_watermark != boundary.start_watermark:
            raise ValueError("export source changed the start watermark during export")
        manifest = LogicalExportV1(
            metadata=metadata,
            boundary=final_boundary,
            partitions=tuple(manifests),
            export_id=export_id,
            created_at=self._clock(),
        )
        self._evidence.record(
            LifecycleEvidence(
                kind="logical-export",
                subject=manifest.export_id,
                state="COMPLETED",
                details={
                    "exportDigest": manifest.digest,
                    "partitionCount": len(manifest.partitions),
                    "consistency": manifest.boundary.consistency.value,
                },
                occurred_at=self._clock(),
            )
        )
        return manifest


class LogicalImportCoordinator:
    """Validate and stream a logical export into an adapter-owned target."""

    def __init__(
        self,
        *,
        artifacts: LogicalArtifactSource,
        target: LogicalImportTarget,
        object_transfer: ObjectTransferHook | None = None,
        evidence: EvidenceSink | None = None,
        maximum_record_bytes: int = 8 * 1024 * 1024,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if maximum_record_bytes < 1024:
            raise ValueError("maximum_record_bytes must be at least 1024")
        self._artifacts = artifacts
        self._target = target
        self._object_transfer = object_transfer
        self._evidence = evidence or NullEvidenceSink()
        self._maximum_record_bytes = maximum_record_bytes
        self._clock = clock

    def _records(
        self,
        manifest: PartitionManifest,
        *,
        batch_size: int,
        merge_policy: MergePolicy,
        cancellation: CancellationToken,
        write: bool,
    ) -> PartitionImportResult:
        digest = hashlib.sha256()
        byte_count = count = 0
        minimum: str | None = None
        maximum: str | None = None
        previous: str | None = None
        buffer = bytearray()
        batch: list[LogicalRecord] = []
        cancellation.raise_if_cancelled()

        def consume(line: bytes) -> None:
            nonlocal count, minimum, maximum, previous
            if len(line) > self._maximum_record_bytes:
                raise IntegrityError("logical Record exceeds maximum_record_bytes")
            try:
                raw = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise IntegrityError("logical partition contains invalid NDJSON") from error
            if not isinstance(raw, Mapping):
                raise IntegrityError("logical partition line must be a JSON object")
            record = LogicalRecord.from_mapping(raw)
            if canonical_json_bytes(record.to_mapping()) != line:
                raise IntegrityError("logical partition Record is not canonical JSON")
            if previous is not None and record.logical_id <= previous:
                raise IntegrityError("logical partition records are not strictly ordered")
            previous = record.logical_id
            minimum = minimum or record.logical_id
            maximum = record.logical_id
            if write and self._object_transfer is not None:
                for reference in record.object_references:
                    transferred = self._object_transfer.transfer(reference)
                    try:
                        require_sha256(transferred, field="object transfer digest")
                    except ValueError as error:
                        raise IntegrityError(
                            "object transfer hook returned an invalid digest"
                        ) from error
            if write:
                batch.append(record)
            count += 1
            if write and len(batch) == batch_size:
                self._target.write(tuple(batch), merge_policy)
                batch.clear()

        for chunk in self._artifacts.chunks(manifest.artifact_ref):
            cancellation.raise_if_cancelled()
            digest.update(chunk)
            byte_count += len(chunk)
            buffer.extend(chunk)
            while b"\n" in buffer:
                line, _, remainder = buffer.partition(b"\n")
                buffer = bytearray(remainder)
                if line:
                    consume(bytes(line))
            if len(buffer) > self._maximum_record_bytes:
                raise IntegrityError("logical partition contains an oversized unterminated Record")
        if buffer:
            raise IntegrityError("logical partition must end each Record with a newline")
        if write and batch:
            self._target.write(tuple(batch), merge_policy)
        observed_digest = f"sha256:{digest.hexdigest()}"
        if (
            count != manifest.count
            or byte_count != manifest.bytes
            or minimum != manifest.minimum_logical_id
            or maximum != manifest.maximum_logical_id
            or observed_digest != manifest.sha256
        ):
            raise IntegrityError(f"partition {manifest.partition_id!r} failed manifest validation")
        return PartitionImportResult(
            partition_id=manifest.partition_id,
            count=count,
            bytes=byte_count,
            sha256=observed_digest,
        )

    def import_export(
        self,
        manifest: LogicalExportV1,
        *,
        merge_policy: MergePolicy = MergePolicy.REQUIRE_EMPTY,
        batch_size: int = 500,
        cancellation: CancellationToken | None = None,
    ) -> ImportReport:
        if batch_size < 1 or batch_size > 10_000:
            raise ValueError("import batch_size must be between 1 and 10000")
        token = cancellation or CancellationToken()
        token.raise_if_cancelled()
        verified = tuple(
            self._records(
                partition,
                batch_size=batch_size,
                merge_policy=merge_policy,
                cancellation=token,
                write=False,
            )
            for partition in manifest.partitions
        )
        self._target.preflight(manifest.metadata, merge_policy)
        results = tuple(
            self._records(
                partition,
                batch_size=batch_size,
                merge_policy=merge_policy,
                cancellation=token,
                write=True,
            )
            for partition in manifest.partitions
        )
        if results != verified:
            raise IntegrityError("logical artifacts changed after preflight verification")
        target_fingerprint = self._target.finalize(manifest)
        try:
            require_sha256(target_fingerprint, field="target fingerprint")
        except ValueError as error:
            raise IntegrityError("import target returned an invalid fingerprint") from error
        comparison_digest = sha256_digest(
            {
                "exportDigest": manifest.digest,
                "targetFingerprint": target_fingerprint,
                "partitions": [
                    {
                        "partitionId": item.partition_id,
                        "count": item.count,
                        "bytes": item.bytes,
                        "sha256": item.sha256,
                    }
                    for item in results
                ],
            }
        )
        report = ImportReport(
            export_digest=manifest.digest,
            target_fingerprint=target_fingerprint,
            partitions=results,
            comparison_digest=comparison_digest,
        )
        self._evidence.record(
            LifecycleEvidence(
                kind="logical-import",
                subject=manifest.export_id,
                state="COMPLETED",
                details={
                    "exportDigest": manifest.digest,
                    "targetFingerprint": target_fingerprint,
                    "comparisonDigest": comparison_digest,
                },
                occurred_at=self._clock(),
            )
        )
        return report


class PortableRecoveryCoordinator:
    """Correlate logical portability evidence with an IaC-owned restore validation."""

    def __init__(
        self,
        *,
        validation: RecoveryValidationHook,
        evidence: EvidenceSink | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._validation = validation
        self._evidence = evidence or NullEvidenceSink()
        self._clock = clock

    def validate_restore(
        self,
        *,
        backup_reference: str,
        manifest: LogicalExportV1,
        import_report: ImportReport,
    ) -> RestoreValidationEvidence:
        if not backup_reference:
            raise ValueError("backup_reference must be non-empty")
        if import_report.export_digest != manifest.digest:
            raise IntegrityError("import report does not reference the supplied logical export")
        validation_digest = self._validation.validate(
            backup_reference,
            manifest,
            import_report,
        )
        try:
            require_sha256(validation_digest, field="restore validation digest")
        except ValueError as error:
            raise IntegrityError("restore validation hook must return a SHA-256 digest") from error
        result = RestoreValidationEvidence(
            backup_reference=backup_reference,
            export_digest=manifest.digest,
            target_fingerprint=import_report.target_fingerprint,
            validation_digest=validation_digest,
            validated_at=self._clock(),
        )
        self._evidence.record(
            LifecycleEvidence(
                kind="restore-validation",
                subject=backup_reference,
                state="VALIDATED",
                details=result.to_mapping(),
                occurred_at=result.validated_at,
            )
        )
        return result
