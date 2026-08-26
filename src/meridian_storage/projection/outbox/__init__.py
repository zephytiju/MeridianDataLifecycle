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

"""Transactional outbox contracts."""

from meridian_storage.projection.outbox.model import (
    OUTBOX_FORMAT_VERSION,
    Checkpoint,
    OutboxDataV1,
    OutboxFailure,
    OutboxLease,
    OutboxRecord,
    OutboxState,
    ProjectionLag,
)
from meridian_storage.projection.outbox.store import InMemoryOutboxStore, OutboxPort
from meridian_storage.projection.outbox.transactional import TransactionalOutboxWriter

__all__ = [
    "OUTBOX_FORMAT_VERSION",
    "Checkpoint",
    "InMemoryOutboxStore",
    "OutboxDataV1",
    "OutboxFailure",
    "OutboxLease",
    "OutboxPort",
    "OutboxRecord",
    "OutboxState",
    "ProjectionLag",
    "TransactionalOutboxWriter",
]
