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

"""Stable lifecycle failures mapped onto the public Meridian error hierarchy."""

from __future__ import annotations

from meridian_storage import (
    ConflictError,
    CorruptionError,
    LifecycleError,
    TransientError,
    UnavailableError,
    ValidationError,
)


class DataLifecycleValidationError(ValidationError):
    """A lifecycle contract failed deterministic validation."""

    def __init__(self, message: str, *, code: str = "MERIDIAN_LIFECYCLE_INVALID") -> None:
        super().__init__(code, message)


class LeaseLostError(ConflictError):
    """An outbox lease is absent, expired, or owned by another worker."""

    def __init__(self, message: str) -> None:
        super().__init__("MERIDIAN_OUTBOX_LEASE_LOST", message)


class CheckpointConflict(ConflictError):
    """A checkpoint compare-and-set precondition failed."""

    def __init__(self, message: str) -> None:
        super().__init__("MERIDIAN_CHECKPOINT_CONFLICT", message)


class ProjectionRejected(DataLifecycleValidationError):
    """A projector returned an expression outside its locked specification."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="MERIDIAN_PROJECTION_REJECTED")


class CacheUnavailable(UnavailableError):
    """An explicit cache coordination operation cannot proceed."""

    def __init__(self, message: str) -> None:
        super().__init__("MERIDIAN_CACHE_UNAVAILABLE", message, retryable=True)


class IncompatibleMigration(DataLifecycleValidationError):
    """A Schema migration cannot preserve the declared contract."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="MERIDIAN_MIGRATION_INCOMPATIBLE")


class MigrationConflict(ConflictError):
    """Migration state or lock evidence conflicts with the requested bundle."""

    def __init__(self, message: str) -> None:
        super().__init__("MERIDIAN_MIGRATION_CONFLICT", message)


class MigrationFailed(LifecycleError):
    """An explicit migration execution failed after planning."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__("MERIDIAN_MIGRATION_FAILED", message, retryable=retryable)


class IntegrityError(CorruptionError):
    """Portable content or evidence failed digest validation."""

    def __init__(self, message: str) -> None:
        super().__init__("MERIDIAN_PORTABILITY_INTEGRITY", message)


class OperationCancelled(TransientError):
    """A bounded lifecycle operation was cancelled at a safe checkpoint."""

    def __init__(self, message: str = "lifecycle operation cancelled") -> None:
        super().__init__("MERIDIAN_LIFECYCLE_CANCELLED", message, retryable=True)
