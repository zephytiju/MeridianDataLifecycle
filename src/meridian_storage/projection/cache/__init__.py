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

"""Cache policy, backend, and coordination public API."""

from meridian_storage.projection.cache.backends import (
    CACHE_ENVELOPE_FORMAT_VERSION,
    CacheBackend,
    InMemoryCacheBackend,
    MeridianCacheBackend,
    VersionedCacheEntry,
)
from meridian_storage.projection.cache.coordinator import (
    CacheCoordinator,
    CacheSerializer,
    CanonicalJsonSerializer,
)
from meridian_storage.projection.cache.model import (
    CacheOutcome,
    CachePolicy,
    CacheRead,
    InvalidationResult,
    LoadedValue,
    SourceVersionStrategy,
    VersionComparator,
)

__all__ = [
    "CACHE_ENVELOPE_FORMAT_VERSION",
    "CacheBackend",
    "CacheCoordinator",
    "CacheOutcome",
    "CachePolicy",
    "CacheRead",
    "CacheSerializer",
    "CanonicalJsonSerializer",
    "InMemoryCacheBackend",
    "InvalidationResult",
    "LoadedValue",
    "MeridianCacheBackend",
    "SourceVersionStrategy",
    "VersionComparator",
    "VersionedCacheEntry",
]
