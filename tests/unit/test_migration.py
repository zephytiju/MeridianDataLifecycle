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

import pytest
from meridian_storage.semantics import (
    CatalogName,
    CompatibilityClass,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
)

from meridian_storage.projection.errors import IncompatibleMigration
from meridian_storage.projection.migration import (
    LockClass,
    MigrationBundleV1,
    MigrationCondition,
    MigrationPlanner,
    MigrationStep,
    MigrationStepKind,
    Reversibility,
)
from tests.conftest import serialized_operation


def schema(version: str, *, include_name: bool, nullable_name: bool = True) -> SchemaDocument:
    fields = [FieldDefinition("id", LogicalType(LogicalKind.STRING))]
    if include_name:
        fields.append(
            FieldDefinition("name", LogicalType(LogicalKind.STRING), nullable=nullable_name)
        )
    return SchemaDocument(
        ref=SchemaReference(CatalogName.STRUCTURED, "investigation", "case", version),
        semantic_kind=SemanticKind.RELATIONAL,
        fields=tuple(fields),
        identity=("id",),
    )


def condition(name: str) -> MigrationCondition:
    return MigrationCondition(
        name,
        operation=serialized_operation(name),
        expectation={"kind": "equals", "value": 0},
    )


def test_planner_accepts_compatible_and_rejects_unproven_breaking() -> None:
    planner = MigrationPlanner()
    source = schema("1.0.0", include_name=False)
    target = schema("2.0.0", include_name=True)
    assert planner.classify(source, target).classification is CompatibilityClass.BACKWARD
    bundle = planner.create_bundle(
        source=source,
        target=target,
        steps=(
            MigrationStep("add-name", MigrationStepKind.ADD_FIELD, {"field": "name"}),
            MigrationStep("activate", MigrationStepKind.ACTIVATE_SCHEMA_VERSION),
        ),
        bundle_id="case-1-to-2",
    )
    assert bundle.source_schema_fingerprint == source.fingerprint
    assert bundle.digest.startswith("sha256:")

    breaking_source = schema("2.0.0", include_name=True, nullable_name=False)
    breaking_target = schema("3.0.0", include_name=False)
    assert (
        planner.classify(breaking_source, breaking_target).classification
        is CompatibilityClass.BREAKING
    )
    with pytest.raises(IncompatibleMigration, match="transform-record"):
        planner.create_bundle(
            source=breaking_source,
            target=breaking_target,
            steps=(MigrationStep("activate", MigrationStepKind.ACTIVATE_SCHEMA_VERSION),),
            bundle_id="breaking",
        )
    with pytest.raises(IncompatibleMigration, match="preconditions"):
        planner.create_bundle(
            source=breaking_source,
            target=breaking_target,
            steps=(
                MigrationStep("transform", MigrationStepKind.TRANSFORM_RECORD),
                MigrationStep("activate", MigrationStepKind.ACTIVATE_SCHEMA_VERSION),
            ),
            bundle_id="breaking",
        )
    proved = planner.create_bundle(
        source=breaking_source,
        target=breaking_target,
        steps=(
            MigrationStep("transform", MigrationStepKind.TRANSFORM_RECORD),
            MigrationStep("activate", MigrationStepKind.ACTIVATE_SCHEMA_VERSION),
        ),
        preconditions=(condition("no-null-names"),),
        validations=(condition("record-count"),),
        bundle_id="breaking-proved",
    )
    assert proved.reversibility is Reversibility.RESTORE_REQUIRED


def test_migration_contract_validation() -> None:
    fingerprint = "sha256:" + "a" * 64
    step = MigrationStep("activate", MigrationStepKind.ACTIVATE_SCHEMA_VERSION)
    bundle = MigrationBundleV1(
        fingerprint,
        "sha256:" + "b" * 64,
        (step,),
        estimated_lock_class=LockClass.EXCLUSIVE,
        bundle_id="bundle",
    )
    assert bundle.to_mapping()["formatVersion"] == "meridian.migration-bundle.v1"
    assert step.digest.startswith("sha256:")
    with pytest.raises(ValueError, match="sha256"):
        MigrationBundleV1("bad", fingerprint, (step,), bundle_id="x")
    with pytest.raises(ValueError, match="steps"):
        MigrationBundleV1(fingerprint, fingerprint, (), bundle_id="x")
    with pytest.raises(ValueError, match="unique"):
        MigrationBundleV1(fingerprint, fingerprint, (step, step), bundle_id="x")
    with pytest.raises(ValueError, match="digest"):
        MigrationBundleV1(fingerprint, fingerprint, (step,), bundle_id="x", digest="bad")
    with pytest.raises(ValueError, match="name"):
        MigrationCondition("", {}, {})
    with pytest.raises(ValueError, match="serialized Meridian Operation"):
        MigrationCondition("invalid-operation", {}, {})
    with pytest.raises(ValueError, match="step_id"):
        MigrationStep("", MigrationStepKind.ADD_FIELD)
