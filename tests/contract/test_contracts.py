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
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]
from meridian_storage.semantics import CacheCatalogSurface, CacheEntry

from meridian_storage.projection import (
    CACHE_ENVELOPE_FORMAT_VERSION,
    CachePolicy,
    ExportBoundary,
    ExportConsistency,
    ExportMetadata,
    LogicalExportV1,
    MigrationBundleV1,
    MigrationCondition,
    MigrationStep,
    MigrationStepKind,
    OutboxDataV1,
    ProjectionSpec,
)
from tests.conftest import serialized_operation

ROOT = Path(__file__).parents[2]
CONTRACTS = ROOT / "contracts" / "data-lifecycle"


def _json(name: str) -> dict[str, object]:
    return cast(dict[str, object], json.loads((CONTRACTS / name).read_text(encoding="utf-8")))


def _validate(name: str, value: object) -> None:
    schema = _json(name)
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(value)


def test_contract_schemas_validate_public_models(fixed_time: datetime) -> None:
    outbox = OutboxDataV1(
        "cases",
        "case@1",
        "case-1",
        "put",
        source_version=1,
        payload={"id": "case-1"},
        event_id="event",
        occurred_at=fixed_time,
    )
    _validate("meridian-outbox.v1.schema.json", outbox.to_mapping())

    spec = ProjectionSpec("p", "structured", "cases", "structured", "derived", "c@1", "d@1")
    _validate("meridian-projection-spec.v1.schema.json", spec.to_mapping())

    policy = CachePolicy(
        "cases",
        "case-cache",
        ("id",),
        "canonical-json-v1",
        "sha256:" + "a" * 64,
        timedelta(seconds=30),
        timedelta(minutes=5),
        timedelta(seconds=30),
    )
    _validate("meridian-cache-policy.v1.schema.json", policy.to_mapping())
    cache_entry = CacheEntry(
        key=("cases", "case-1"),
        value={"$meridian.cache.value": {"id": "case-1"}},
        serializer_id="canonical-json-v1",
        schema_fingerprint="sha256:" + "a" * 64,
        created_at=fixed_time,
        expires_at=fixed_time + timedelta(seconds=30),
        source_version=1,
    )
    _validate(
        "meridian-cache-envelope.v1.schema.json",
        {"formatVersion": CACHE_ENVELOPE_FORMAT_VERSION, **cache_entry.to_dict()},
    )

    bundle = MigrationBundleV1(
        "sha256:" + "a" * 64,
        "sha256:" + "b" * 64,
        (MigrationStep("activate", MigrationStepKind.ACTIVATE_SCHEMA_VERSION),),
        preconditions=(
            MigrationCondition(
                "source-is-ready",
                serialized_operation("source-is-ready"),
                {"kind": "equals", "value": True},
            ),
        ),
        bundle_id="bundle",
    )
    _validate("meridian-migration-bundle.v1.schema.json", bundle.to_mapping())

    export = LogicalExportV1(
        ExportMetadata((), (), ()),
        ExportBoundary("boundary", ExportConsistency.CONSISTENT_BOUNDARY, "w", "w"),
        (),
        export_id="export",
        created_at=fixed_time,
    )
    _validate("meridian-logical-export.v1.schema.json", export.to_mapping())


def test_locked_catalog_and_cache_surface() -> None:
    contract = _json("meridian-data-lifecycle.v1.json")
    assert contract["catalogsOwned"] == []
    assert contract["catalogRegistry"] == [
        "structured",
        "object",
        "cache",
        "evidence",
        "streaming",
    ]
    assert "projection" not in cast(list[str], contract["catalogRegistry"])
    assert contract["cacheEnvelopeFormat"] == CACHE_ENVELOPE_FORMAT_VERSION
    assert contract["migrationConditionOperationFormat"] == "meridian-operation.v1"
    public = {
        name
        for name, member in inspect.getmembers(CacheCatalogSurface, predicate=callable)
        if not name.startswith("_")
    }
    assert public == set(cast(list[str], contract["cacheCatalogSurface"]))


def test_compatibility_manifest_is_exact() -> None:
    compatibility = cast(
        dict[str, object], json.loads((ROOT / "compatibility.json").read_text(encoding="utf-8"))
    )
    assert compatibility["package"] == "meridian-storage-projection"
    assert compatibility["version"] == "1.0.3"
    assert compatibility["catalogsOwned"] == []
    assert compatibility["dependencies"] == {
        "meridian-storage-core": "<2,>=1.0.1",
        "meridian-storage-semantics": "<3,>=2.0.0",
        "meridian-storage-query": "<2,>=1.0.2",
    }
