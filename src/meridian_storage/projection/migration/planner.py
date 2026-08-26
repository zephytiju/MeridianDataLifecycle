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

"""Schema compatibility checks and deterministic logical bundle planning."""

from __future__ import annotations

from meridian_storage.semantics import (
    CompatibilityClass,
    CompatibilityReport,
    SchemaDocument,
    classify_compatibility,
)

from meridian_storage.projection.errors import IncompatibleMigration
from meridian_storage.projection.migration.model import (
    LockClass,
    MigrationBundleV1,
    MigrationCondition,
    MigrationStep,
    MigrationStepKind,
    Reversibility,
)


class MigrationPlanner:
    """Validate caller-authored logical steps against released Schema semantics."""

    def classify(self, source: SchemaDocument, target: SchemaDocument) -> CompatibilityReport:
        return classify_compatibility(source, target)

    def create_bundle(
        self,
        *,
        source: SchemaDocument,
        target: SchemaDocument,
        steps: tuple[MigrationStep, ...],
        preconditions: tuple[MigrationCondition, ...] = (),
        validations: tuple[MigrationCondition, ...] = (),
        postconditions: tuple[MigrationCondition, ...] = (),
        compiler_requirements: tuple[str, ...] = (),
        estimated_lock_class: LockClass = LockClass.EXCLUSIVE,
        reversibility: Reversibility = Reversibility.RESTORE_REQUIRED,
        bundle_id: str,
    ) -> MigrationBundleV1:
        report = self.classify(source, target)
        kinds = {step.kind for step in steps}
        if report.classification is CompatibilityClass.BREAKING:
            if MigrationStepKind.TRANSFORM_RECORD not in kinds:
                raise IncompatibleMigration(
                    "breaking Schema changes require an explicit transform-record step"
                )
            if not preconditions or not validations:
                raise IncompatibleMigration(
                    "breaking Schema changes require preconditions and validation Operations"
                )
        if report.classification is CompatibilityClass.CONDITIONAL and not compiler_requirements:
            raise IncompatibleMigration(
                "conditionally compatible changes require an Adapter compiler requirement"
            )
        if MigrationStepKind.ACTIVATE_SCHEMA_VERSION not in kinds:
            raise IncompatibleMigration(
                "every migration must explicitly activate the target Schema"
            )
        return MigrationBundleV1(
            source_schema_fingerprint=source.fingerprint,
            target_schema_fingerprint=target.fingerprint,
            steps=steps,
            preconditions=preconditions,
            validations=validations,
            postconditions=postconditions,
            compiler_requirements=compiler_requirements,
            estimated_lock_class=estimated_lock_class,
            reversibility=reversibility,
            bundle_id=bundle_id,
        )
