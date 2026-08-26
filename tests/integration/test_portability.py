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

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime

import pytest

from meridian_storage.projection.control import CancellationToken
from meridian_storage.projection.errors import IntegrityError, OperationCancelled
from meridian_storage.projection.evidence import InMemoryEvidenceSink
from meridian_storage.projection.portability import (
    ExportBoundary,
    ExportConsistency,
    ExportMetadata,
    ExportRequest,
    ImportReport,
    InMemoryLogicalArtifacts,
    LogicalExportCoordinator,
    LogicalExportV1,
    LogicalImportCoordinator,
    LogicalRecord,
    MergePolicy,
    PartitionManifest,
    PortableRecoveryCoordinator,
)

TARGET_FINGERPRINT = "sha256:" + "d" * 64
COMPARISON_DIGEST = "sha256:" + "e" * 64


def _record(logical_id: str) -> LogicalRecord:
    return LogicalRecord(
        catalog="structured",
        namespace="investigation",
        resource="cases",
        schema_ref="investigation.case@1",
        logical_id=logical_id,
        version=1,
        values={"id": logical_id},
        object_references=({"resource": "objects", "id": f"object-{logical_id}"},),
    )


class Source:
    def __init__(self, *, reversed_records: bool = False) -> None:
        self.reversed_records = reversed_records

    def begin(self, request: ExportRequest) -> ExportBoundary:
        del request
        return ExportBoundary("boundary", ExportConsistency.CONSISTENT_BOUNDARY, "w1", "w1")

    def metadata(self, request: ExportRequest, boundary: ExportBoundary) -> ExportMetadata:
        del request, boundary
        return ExportMetadata(
            namespaces=({"name": "investigation"},),
            schemas=({"ref": "investigation.case@1"},),
            resources=({"catalog": "structured", "name": "cases"},),
        )

    def partition_ids(self, request: ExportRequest, boundary: ExportBoundary) -> Sequence[str]:
        del request, boundary
        return ("p0", "p1")

    def records(
        self,
        request: ExportRequest,
        boundary: ExportBoundary,
        partition_id: str,
    ) -> Iterable[Sequence[LogicalRecord]]:
        del request, boundary
        if partition_id == "p0":
            values = (
                (_record("002"), _record("001"))
                if self.reversed_records
                else (
                    _record("001"),
                    _record("002"),
                )
            )
            yield values
        else:
            yield (_record("003"),)

    def finish(self, request: ExportRequest, boundary: ExportBoundary) -> ExportBoundary:
        del request
        return boundary


class Target:
    def __init__(self) -> None:
        self.records: list[LogicalRecord] = []
        self.preflight_policy: MergePolicy | None = None

    def preflight(self, metadata: ExportMetadata, merge_policy: MergePolicy) -> None:
        assert metadata.resources[0]["catalog"] == "structured"
        self.preflight_policy = merge_policy

    def write(self, records: Sequence[LogicalRecord], merge_policy: MergePolicy) -> None:
        assert merge_policy is self.preflight_policy
        self.records.extend(records)

    def finalize(self, manifest: LogicalExportV1) -> str:
        assert manifest.partitions
        return TARGET_FINGERPRINT


class ObjectTransfer:
    def __init__(self) -> None:
        self.references: list[Mapping[str, object]] = []

    def transfer(self, reference: Mapping[str, object]) -> str:
        self.references.append(reference)
        return "sha256:" + "f" * 64


def test_logical_export_import_round_trip(fixed_time: datetime) -> None:
    artifacts = InMemoryLogicalArtifacts()
    evidence = InMemoryEvidenceSink()
    manifest = LogicalExportCoordinator(
        source=Source(), sink=artifacts, evidence=evidence, clock=lambda: fixed_time
    ).export(ExportRequest(("investigation.cases",), batch_size=2), export_id="export-1")
    assert manifest.digest.startswith("sha256:")
    assert sum(item.count for item in manifest.partitions) == 3
    assert evidence.events[0].state == "COMPLETED"

    target = Target()
    objects = ObjectTransfer()
    report = LogicalImportCoordinator(
        artifacts=artifacts,
        target=target,
        object_transfer=objects,
        evidence=evidence,
        clock=lambda: fixed_time,
    ).import_export(manifest, batch_size=2)
    assert [record.logical_id for record in target.records] == ["001", "002", "003"]
    assert len(objects.references) == 3
    assert report.export_digest == manifest.digest
    assert report.comparison_digest.startswith("sha256:")
    assert target.preflight_policy is MergePolicy.REQUIRE_EMPTY


class CorruptArtifacts:
    def __init__(self, delegate: InMemoryLogicalArtifacts) -> None:
        self.delegate = delegate

    def chunks(self, artifact_ref: str) -> Iterable[bytes]:
        for chunk in self.delegate.chunks(artifact_ref):
            yield chunk + b"corrupt"


class BytesArtifacts:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def chunks(self, artifact_ref: str) -> Iterable[bytes]:
        del artifact_ref
        yield self.content


def test_portability_integrity_order_and_cancellation(fixed_time: datetime) -> None:
    artifacts = InMemoryLogicalArtifacts()
    with pytest.raises(ValueError, match="strictly ordered"):
        LogicalExportCoordinator(source=Source(reversed_records=True), sink=artifacts).export(
            ExportRequest(("cases",)), export_id="bad-order"
        )

    manifest = LogicalExportCoordinator(
        source=Source(), sink=artifacts, clock=lambda: fixed_time
    ).export(ExportRequest(("cases",)), export_id="export")
    corrupt_target = Target()
    with pytest.raises(IntegrityError):
        importer = LogicalImportCoordinator(
            artifacts=CorruptArtifacts(artifacts), target=corrupt_target
        )
        importer.import_export(manifest)
    assert not corrupt_target.records
    assert corrupt_target.preflight_policy is None

    noncanonical = json.dumps(_record("001").to_mapping()).encode() + b"\n"
    noncanonical_manifest = LogicalExportV1(
        ExportMetadata((), (), ()),
        ExportBoundary("b", ExportConsistency.CONSISTENT_BOUNDARY, "w", "w"),
        (
            PartitionManifest(
                "p",
                "logical://noncanonical",
                1,
                len(noncanonical),
                "001",
                "001",
                "sha256:" + hashlib.sha256(noncanonical).hexdigest(),
            ),
        ),
        export_id="noncanonical",
        created_at=fixed_time,
    )
    noncanonical_target = Target()
    with pytest.raises(IntegrityError, match="canonical"):
        LogicalImportCoordinator(
            artifacts=BytesArtifacts(noncanonical), target=noncanonical_target
        ).import_export(noncanonical_manifest)
    assert not noncanonical_target.records

    cancelled = CancellationToken()
    cancelled.cancel()
    with pytest.raises(OperationCancelled):
        LogicalImportCoordinator(artifacts=artifacts, target=Target()).import_export(
            manifest, cancellation=cancelled
        )
    with pytest.raises(ValueError, match="batch_size"):
        LogicalImportCoordinator(artifacts=artifacts, target=Target()).import_export(
            manifest, batch_size=0
        )


def test_portability_model_validation(fixed_time: datetime) -> None:
    with pytest.raises(ValueError, match="cache state"):
        LogicalRecord("cache", "n", "r", "s", "id", 1, {})
    with pytest.raises(ValueError, match="cache resources"):
        ExportMetadata((), (), ({"catalog": "cache"},))
    with pytest.raises(ValueError, match="watermark"):
        ExportBoundary("b", ExportConsistency.CONSISTENT_BOUNDARY, "one", "two")
    with pytest.raises(ValueError, match="logical id bounds"):
        PartitionManifest("p", "ref", 1, 1, None, None, "sha256:" + "a" * 64)
    with pytest.raises(ValueError, match="sha256"):
        PartitionManifest("p", "ref", 0, 0, None, None, "bad")
    with pytest.raises(ValueError, match="resources"):
        ExportRequest(())
    with pytest.raises(ValueError, match="version"):
        LogicalRecord.from_mapping(
            {
                "catalog": "structured",
                "namespace": "n",
                "resource": "r",
                "schemaRef": "s@1",
                "logicalId": "id",
                "version": True,
                "values": {},
                "objectReferences": [],
            }
        )

    boundary = ExportBoundary("b", ExportConsistency.LIVE_READ, "one", "two")
    metadata = ExportMetadata((), (), ())
    manifest = LogicalExportV1(metadata, boundary, (), export_id="e", created_at=fixed_time)
    with pytest.raises(ValueError, match="digest"):
        LogicalExportV1(
            metadata,
            boundary,
            (),
            export_id="e",
            created_at=fixed_time,
            digest="bad",
        )
    assert manifest.to_mapping()["formatVersion"] == "meridian.logical-export.v1"


class RecoveryValidation:
    def validate(
        self,
        backup_reference: str,
        manifest: LogicalExportV1,
        import_report: ImportReport,
    ) -> str:
        assert backup_reference == "backup-1"
        assert import_report.export_digest == manifest.digest
        return "sha256:" + "c" * 64


def test_portable_restore_validation(fixed_time: datetime) -> None:
    metadata = ExportMetadata((), (), ())
    manifest = LogicalExportV1(
        metadata,
        ExportBoundary("b", ExportConsistency.CONSISTENT_BOUNDARY, "w", "w"),
        (),
        export_id="export",
        created_at=fixed_time,
    )
    report = ImportReport(manifest.digest, TARGET_FINGERPRINT, (), COMPARISON_DIGEST)
    evidence = InMemoryEvidenceSink()
    restored = PortableRecoveryCoordinator(
        validation=RecoveryValidation(), evidence=evidence, clock=lambda: fixed_time
    ).validate_restore(backup_reference="backup-1", manifest=manifest, import_report=report)
    assert restored.validation_digest == "sha256:" + "c" * 64
    assert evidence.events[0].kind == "restore-validation"
    with pytest.raises(IntegrityError, match="does not reference"):
        PortableRecoveryCoordinator(validation=RecoveryValidation()).validate_restore(
            backup_reference="backup-1",
            manifest=manifest,
            import_report=ImportReport(
                "sha256:" + "0" * 64,
                TARGET_FINGERPRINT,
                (),
                COMPARISON_DIGEST,
            ),
        )
