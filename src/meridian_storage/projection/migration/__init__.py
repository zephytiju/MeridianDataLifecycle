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

"""Schema migration planning and explicit execution public API."""

from meridian_storage.projection.migration.executor import (
    InMemoryMigrationRecorder,
    MigrationAdapterHooks,
    MigrationExecutor,
    MigrationRecorder,
    MigrationValidationRunner,
)
from meridian_storage.projection.migration.model import (
    MIGRATION_FORMAT_VERSION,
    ActivationResult,
    AppliedMigrationStep,
    BindingStateEvidence,
    CompiledMigrationPlan,
    CompiledMigrationStep,
    CutoverEvidence,
    LockClass,
    MigrationBundleV1,
    MigrationCheckpoint,
    MigrationCondition,
    MigrationLock,
    MigrationReport,
    MigrationState,
    MigrationStep,
    MigrationStepKind,
    Reversibility,
    ValidationOutcome,
)
from meridian_storage.projection.migration.planner import MigrationPlanner

__all__ = [
    "MIGRATION_FORMAT_VERSION",
    "ActivationResult",
    "AppliedMigrationStep",
    "BindingStateEvidence",
    "CompiledMigrationPlan",
    "CompiledMigrationStep",
    "CutoverEvidence",
    "InMemoryMigrationRecorder",
    "LockClass",
    "MigrationAdapterHooks",
    "MigrationBundleV1",
    "MigrationCheckpoint",
    "MigrationCondition",
    "MigrationExecutor",
    "MigrationLock",
    "MigrationPlanner",
    "MigrationRecorder",
    "MigrationReport",
    "MigrationState",
    "MigrationStep",
    "MigrationStepKind",
    "MigrationValidationRunner",
    "Reversibility",
    "ValidationOutcome",
]
