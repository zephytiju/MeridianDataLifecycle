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

"""Logical export, import, and portable recovery evidence contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Final, cast
from uuid import uuid4

from meridian_storage.semantics import CatalogName

from meridian_storage.projection._canonical import (
    JsonValue,
    ensure_utc,
    immutable_mapping,
    require_sha256,
    rfc3339,
    sha256_digest,
    to_json_value,
    utc_now,
)

LOGICAL_EXPORT_FORMAT_VERSION: Final = "meridian.logical-export.v1"
_EXPORT_CATALOGS: Final = frozenset(
    catalog.value for catalog in CatalogName if catalog is not CatalogName.CACHE
)


class ExportConsistency(StrEnum):
    CONSISTENT_BOUNDARY = "consistent-boundary"
    LIVE_READ = "live-read"


class MergePolicy(StrEnum):
    REQUIRE_EMPTY = "require-empty"
    PRESERVE_NEWER = "preserve-newer"
    REPLACE = "replace"


@dataclass(frozen=True, slots=True)
class LogicalRecord:
    catalog: str
    namespace: str
    resource: str
    schema_ref: str
    logical_id: str
    version: str | int
    values: Mapping[str, object]
    object_references: tuple[Mapping[str, object], ...] = ()

    def __post_init__(self) -> None:
        for name in ("catalog", "namespace", "resource", "schema_ref", "logical_id"):
            if not getattr(self, name):
                raise ValueError(f"{name} must be non-empty")
        if self.catalog == "cache":
            raise ValueError("cache state is excluded from logical export")
        if self.catalog not in _EXPORT_CATALOGS:
            raise ValueError(f"unknown export Catalog {self.catalog!r}")
        if not isinstance(self.version, (str, int)) or isinstance(self.version, bool):
            raise ValueError("logical version must be a string or integer")
        object.__setattr__(self, "values", immutable_mapping(self.values))
        object.__setattr__(
            self,
            "object_references",
            tuple(immutable_mapping(value) for value in self.object_references),
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "catalog": self.catalog,
            "namespace": self.namespace,
            "resource": self.resource,
            "schemaRef": self.schema_ref,
            "logicalId": self.logical_id,
            "version": self.version,
            "values": to_json_value(self.values),
            "objectReferences": [to_json_value(value) for value in self.object_references],
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> LogicalRecord:
        object_refs = value.get("objectReferences", ())
        if not isinstance(object_refs, list) or not all(
            isinstance(item, Mapping) for item in object_refs
        ):
            raise ValueError("objectReferences must be an array of mappings")
        values = value.get("values")
        if not isinstance(values, Mapping):
            raise ValueError("logical Record values must be a mapping")
        version = value.get("version")
        if not isinstance(version, (str, int)) or isinstance(version, bool):
            raise ValueError("logical Record version must be a string or integer")
        required = {
            name: value.get(name)
            for name in ("catalog", "namespace", "resource", "schemaRef", "logicalId")
        }
        if not all(isinstance(item, str) for item in required.values()):
            raise ValueError("logical Record identity fields must be strings")
        return cls(
            catalog=cast(str, required["catalog"]),
            namespace=cast(str, required["namespace"]),
            resource=cast(str, required["resource"]),
            schema_ref=cast(str, required["schemaRef"]),
            logical_id=cast(str, required["logicalId"]),
            version=version,
            values=cast(Mapping[str, object], values),
            object_references=tuple(cast(Mapping[str, object], item) for item in object_refs),
        )


@dataclass(frozen=True, slots=True)
class ExportBoundary:
    boundary_id: str
    consistency: ExportConsistency
    start_watermark: str
    end_watermark: str

    def __post_init__(self) -> None:
        if not self.boundary_id or not self.start_watermark or not self.end_watermark:
            raise ValueError("export boundary fields must be non-empty")
        if (
            self.consistency is ExportConsistency.CONSISTENT_BOUNDARY
            and self.start_watermark != self.end_watermark
        ):
            raise ValueError("a consistent export must use one start/end watermark")

    def to_mapping(self) -> dict[str, str]:
        return {
            "boundaryId": self.boundary_id,
            "consistency": self.consistency.value,
            "startWatermark": self.start_watermark,
            "endWatermark": self.end_watermark,
        }


@dataclass(frozen=True, slots=True)
class ExportMetadata:
    namespaces: tuple[Mapping[str, object], ...]
    schemas: tuple[Mapping[str, object], ...]
    resources: tuple[Mapping[str, object], ...]
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for resource in self.resources:
            if resource.get("catalog") == "cache":
                raise ValueError("cache resources are excluded from logical export")
            catalog = resource.get("catalog")
            if catalog is not None and catalog not in _EXPORT_CATALOGS:
                raise ValueError(f"unknown export Resource Catalog {catalog!r}")
        object.__setattr__(
            self, "namespaces", tuple(immutable_mapping(value) for value in self.namespaces)
        )
        object.__setattr__(
            self, "schemas", tuple(immutable_mapping(value) for value in self.schemas)
        )
        object.__setattr__(
            self, "resources", tuple(immutable_mapping(value) for value in self.resources)
        )
        object.__setattr__(self, "extensions", immutable_mapping(self.extensions))

    def to_mapping(self) -> dict[str, object]:
        return {
            "namespaces": [to_json_value(value) for value in self.namespaces],
            "schemas": [to_json_value(value) for value in self.schemas],
            "resources": [to_json_value(value) for value in self.resources],
            "extensions": to_json_value(self.extensions),
        }


@dataclass(frozen=True, slots=True)
class PartitionManifest:
    partition_id: str
    artifact_ref: str
    count: int
    bytes: int
    minimum_logical_id: str | None
    maximum_logical_id: str | None
    sha256: str

    def __post_init__(self) -> None:
        if not self.partition_id or not self.artifact_ref:
            raise ValueError("partition identity and artifact reference must be non-empty")
        if self.count < 0 or self.bytes < 0:
            raise ValueError("partition count and bytes cannot be negative")
        if self.count == 0 and (
            self.minimum_logical_id is not None or self.maximum_logical_id is not None
        ):
            raise ValueError("empty partitions cannot have logical id bounds")
        if self.count > 0 and (self.minimum_logical_id is None or self.maximum_logical_id is None):
            raise ValueError("non-empty partitions require logical id bounds")
        if self.count > 0 and self.bytes == 0:
            raise ValueError("non-empty partitions require non-zero bytes")
        if (
            self.minimum_logical_id is not None
            and self.maximum_logical_id is not None
            and self.minimum_logical_id > self.maximum_logical_id
        ):
            raise ValueError("partition logical id bounds are reversed")
        require_sha256(self.sha256, field="partition sha256")

    def to_mapping(self) -> dict[str, object]:
        return {
            "partitionId": self.partition_id,
            "artifactRef": self.artifact_ref,
            "count": self.count,
            "bytes": self.bytes,
            "minimumLogicalId": self.minimum_logical_id,
            "maximumLogicalId": self.maximum_logical_id,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class LogicalExportV1:
    metadata: ExportMetadata
    boundary: ExportBoundary
    partitions: tuple[PartitionManifest, ...]
    export_id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=utc_now)
    digest: str = ""
    format_version: str = LOGICAL_EXPORT_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != LOGICAL_EXPORT_FORMAT_VERSION:
            raise ValueError(f"format_version must be {LOGICAL_EXPORT_FORMAT_VERSION!r}")
        if not self.export_id:
            raise ValueError("export_id must be non-empty")
        ids = [item.partition_id for item in self.partitions]
        if len(set(ids)) != len(ids):
            raise ValueError("partition ids must be unique")
        object.__setattr__(self, "created_at", ensure_utc(self.created_at, field="created_at"))
        expected = sha256_digest(self._digest_material())
        if self.digest and self.digest != expected:
            raise ValueError("logical export digest does not match canonical content")
        object.__setattr__(self, "digest", expected)

    def _digest_material(self) -> dict[str, object]:
        return {
            "formatVersion": self.format_version,
            "exportId": self.export_id,
            "createdAt": rfc3339(self.created_at),
            "metadata": self.metadata.to_mapping(),
            "boundary": self.boundary.to_mapping(),
            "partitions": [item.to_mapping() for item in self.partitions],
        }

    def to_mapping(self) -> dict[str, object]:
        return {**self._digest_material(), "digest": self.digest}


@dataclass(frozen=True, slots=True)
class ExportRequest:
    resources: tuple[str, ...]
    batch_size: int = 500

    def __post_init__(self) -> None:
        if (
            not self.resources
            or not all(self.resources)
            or len(set(self.resources)) != len(self.resources)
        ):
            raise ValueError("export resources must be non-empty and unique")
        if self.batch_size < 1 or self.batch_size > 10_000:
            raise ValueError("export batch_size must be between 1 and 10000")


@dataclass(frozen=True, slots=True)
class PartitionImportResult:
    partition_id: str
    count: int
    bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if not self.partition_id or self.count < 0 or self.bytes < 0:
            raise ValueError("partition import result fields are invalid")
        require_sha256(self.sha256, field="partition import sha256")


@dataclass(frozen=True, slots=True)
class ImportReport:
    export_digest: str
    target_fingerprint: str
    partitions: tuple[PartitionImportResult, ...]
    comparison_digest: str

    def __post_init__(self) -> None:
        require_sha256(self.export_digest, field="export_digest")
        require_sha256(self.target_fingerprint, field="target_fingerprint")
        require_sha256(self.comparison_digest, field="comparison_digest")
        ids = [partition.partition_id for partition in self.partitions]
        if len(ids) != len(set(ids)):
            raise ValueError("import report partition ids must be unique")


@dataclass(frozen=True, slots=True)
class RestoreValidationEvidence:
    backup_reference: str
    export_digest: str
    target_fingerprint: str
    validation_digest: str
    validated_at: datetime

    def __post_init__(self) -> None:
        if not self.backup_reference:
            raise ValueError("backup_reference must be non-empty")
        require_sha256(self.export_digest, field="export_digest")
        require_sha256(self.target_fingerprint, field="target_fingerprint")
        require_sha256(self.validation_digest, field="validation_digest")
        object.__setattr__(
            self, "validated_at", ensure_utc(self.validated_at, field="validated_at")
        )

    def to_mapping(self) -> dict[str, JsonValue]:
        return cast(
            dict[str, JsonValue],
            to_json_value(
                {
                    "backupReference": self.backup_reference,
                    "exportDigest": self.export_digest,
                    "targetFingerprint": self.target_fingerprint,
                    "validationDigest": self.validation_digest,
                    "validatedAt": rfc3339(self.validated_at),
                }
            ),
        )
