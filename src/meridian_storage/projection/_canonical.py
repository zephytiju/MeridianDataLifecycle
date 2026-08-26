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

"""Deterministic serialization shared by lifecycle contracts."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from enum import Enum
from types import MappingProxyType
from typing import cast

type JsonScalar = bool | int | float | str | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]

_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(UTC)


def ensure_utc(value: datetime, *, field: str) -> datetime:
    """Validate and normalize an aware timestamp to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def rfc3339(value: datetime) -> str:
    """Serialize an aware timestamp using the Meridian UTC representation."""

    return (
        ensure_utc(value, field="timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def parse_rfc3339(value: str, *, field: str) -> datetime:
    """Parse an RFC 3339 timestamp and normalize it to UTC."""

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an RFC 3339 timestamp") from error
    return ensure_utc(parsed, field=field)


def _json_value(value: object) -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floating-point values are not canonical JSON")
        return value
    if isinstance(value, datetime):
        return rfc3339(value)
    if isinstance(value, Enum):
        return _json_value(value.value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _json_value(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            normalized[key] = _json_value(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def to_json_value(value: object) -> JsonValue:
    """Convert a supported Python value into deterministic JSON data."""

    return _json_value(value)


def canonical_json_bytes(value: object) -> bytes:
    """Return canonical UTF-8 JSON bytes with sorted object keys."""

    normalized = _json_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_digest(value: object) -> str:
    """Return a prefixed SHA-256 digest of a canonical value."""

    return f"sha256:{hashlib.sha256(canonical_json_bytes(value)).hexdigest()}"


def require_sha256(value: str, *, field: str) -> str:
    """Validate one canonical prefixed SHA-256 digest."""

    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must use the sha256:<lowercase hex> form")
    return value


def _immutable_json(value: JsonValue) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _immutable_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_immutable_json(item) for item in value)
    return value


def immutable_value(value: object) -> object:
    """Return a deeply immutable canonical-JSON copy."""

    return _immutable_json(_json_value(value))


def immutable_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    """Create a deeply immutable canonical-JSON mapping copy."""

    frozen = immutable_value(value)
    return cast(Mapping[str, object], frozen)
