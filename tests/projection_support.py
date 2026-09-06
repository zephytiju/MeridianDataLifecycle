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

from meridian_storage.registry import (
    BindingRecord,
    NamespaceDefinition,
    RegistrySnapshot,
    ResourceBundle,
    ResourceDefinition,
    SchemaDefinition,
    SchemaRef,
)
from meridian_storage.runtime.config import SecretReference
from meridian_storage.semantics import StructuredCatalogProvider
from meridian_storage.spi import (
    AdapterCreateContext,
    AdapterDescriptor,
    AdapterProbe,
    CapabilityManifest,
    ExecutionRequest,
    ExecutionResult,
    OperationCapability,
    PhysicalResource,
    PhysicalVerification,
    SecretValue,
)

from meridian_storage import Meridian, ResourceRef, RuntimeConfig

FINGERPRINT = "sha256:" + "a" * 64
SOURCE = ResourceRef("structured", "investigation", "cases")
TARGET = ResourceRef("structured", "investigation", "case_search")


def manifest() -> CapabilityManifest:
    operations = StructuredCatalogProvider().manifest().operations
    return CapabilityManifest(
        AdapterDescriptor(
            "projection.test",
            "1.0.0",
            "recording-1.0.0",
            {"test": ("1.0.0",)},
            tuple(
                OperationCapability(
                    item.operation_contract,
                    (item.operation_version,),
                    guarantees=item.requirement.guarantees,
                    limits=item.requirement.minimum_limits,
                )
                for item in operations
            ),
        ),
        "test",
        "1.0.0",
    )


def bundle() -> ResourceBundle:
    provider = StructuredCatalogProvider()
    schemas = tuple(
        SchemaDefinition(
            SchemaRef("structured", "investigation", name, "1"),
            {"fields": {"id": {"type": "string"}}, "identity": ["id"]},
        )
        for name in ("case", "case_search")
    )
    return ResourceBundle(
        "projection.schemas",
        "1.0.0",
        "1.0.0",
        namespaces=(NamespaceDefinition("structured", "investigation"),),
        schemas=schemas,
        resources=tuple(
            ResourceDefinition(
                ref,
                "relational",
                schema.ref,
                requirements=(provider.manifest().operation_for(method).requirement,),
            )
            for ref, schema, method in zip((SOURCE, TARGET), schemas, ("get", "put"), strict=True)
        ),
    )


class ProjectionMetadata:
    """Synthetic metadata for focused runner tests, using released Core models."""

    def __init__(self) -> None:
        data = bundle()
        capability = manifest()
        self._capability_manifests = {"test": capability}
        self.snapshot = RegistrySnapshot(
            1,
            FINGERPRINT,
            {"structured": StructuredCatalogProvider().manifest()},
            {(item.catalog, item.name): item for item in data.namespaces},
            {item.ref: item for item in data.schemas},
            {item.ref: item for item in data.resources},
            {
                item.ref: BindingRecord(
                    "test",
                    capability.adapter_id,
                    capability.fingerprint,
                    FINGERPRINT,
                    "opaque-test",
                )
                for item in data.resources
            },
        )

    def _snapshot_for_handle(self) -> RegistrySnapshot:
        return self.snapshot


class RecordingSession:
    def __init__(self, runtime: RecordingAdapter) -> None:
        self.runtime = runtime

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        self.runtime.requests.append(request)
        return ExecutionResult({"acknowledgedSourceVersion": 3}, 64)


class RecordingAdapter:
    def __init__(self) -> None:
        self.requests: list[ExecutionRequest] = []
        self.sessions = 0
        self.opens = 0

    def open(self) -> None:
        self.opens += 1

    def close(self) -> None:
        pass

    def probe(self) -> AdapterProbe:
        return AdapterProbe(manifest())

    def verify_physical(self, resources: tuple[PhysicalResource, ...]) -> PhysicalVerification:
        return PhysicalVerification(
            FINGERPRINT, {str(item.resource_ref): "opaque-test" for item in resources}
        )

    def open_session(self, *, transactional: bool) -> RecordingSession:
        del transactional
        self.sessions += 1
        return RecordingSession(self)


class RecordingFactory:
    adapter_id = "projection.test"

    def __init__(self) -> None:
        self.runtime = RecordingAdapter()

    def create(self, context: AdapterCreateContext) -> RecordingAdapter:
        del context
        return self.runtime


class SchemaProvider:
    provider_id = "projection.schemas"
    provider_contract_version = "1.0.0"

    def load(self) -> ResourceBundle:
        return bundle()


class SecretResolver:
    def resolve(self, reference: SecretReference) -> SecretValue:
        del reference
        return SecretValue(b"synthetic-test-value")


def real_facade() -> tuple[Meridian, RecordingAdapter]:
    """Released Meridian/Core + Structured Catalog, recording SPI, no Engine I/O."""
    data = bundle()
    provider = StructuredCatalogProvider()
    capability = manifest()
    factory = RecordingFactory()
    config = RuntimeConfig.from_mapping(
        {
            "formatVersion": "meridian-config.v1",
            "profile": "test",
            "catalogs": {
                "providers": [
                    {
                        "name": "structured",
                        "package": provider.manifest().package_name,
                        "contract": "1.x",
                        "requiredFingerprint": provider.manifest().fingerprint,
                    }
                ],
                "extensions": {},
            },
            "resources": {
                "pins": [
                    {
                        "ref": item.ref.to_dict(),
                        "providerId": data.provider_id,
                        "requiredFingerprint": item.fingerprint,
                    }
                    for item in data.resources
                ],
                "extensions": {},
            },
            "schemas": {
                "providers": [
                    {
                        "id": data.provider_id,
                        "package": "test-schemas",
                        "contract": "1.x",
                        "requiredFingerprint": data.fingerprint,
                    }
                ],
                "live": {"enabled": False, "required": False, "providerId": None},
                "extensions": {},
            },
            "bindings": [
                {
                    "id": "test",
                    "adapterId": factory.adapter_id,
                    "adapterContract": "1.x",
                    "engineProfile": "test",
                    "engineVersion": "1.0.0",
                    "endpoint": "memory://test",
                    "serviceRef": None,
                    "physicalNamespace": "synthetic",
                    "tls": {
                        "mode": "disabled",
                        "serverName": None,
                        "caRef": None,
                        "clientCertificateRef": None,
                    },
                    "identityRef": {"provider": "test", "reference": "synthetic"},
                    "secretRef": {"provider": "test", "reference": "synthetic"},
                    "client": {
                        "minSize": 0,
                        "maxSize": 2,
                        "acquireTimeoutMs": 1000,
                        "idleTimeoutMs": 1000,
                        "operationTimeoutMs": 5000,
                        "maxResultBytes": 1024,
                        "iteratorLifetimeMs": 5000,
                    },
                    "requiredCapabilityFingerprint": capability.fingerprint,
                    "requiredPhysicalFingerprint": FINGERPRINT,
                    "compatibilityPins": {},
                    "settings": {},
                    "extensions": {},
                }
            ],
            "placements": [
                {
                    "id": "test",
                    "bindingId": "test",
                    "selector": {
                        "resources": [item.ref.to_dict() for item in data.resources],
                        "catalog": None,
                        "labels": {},
                    },
                    "extensions": {},
                }
            ],
            "validation": {
                "strict": True,
                "requirePhysicalFingerprints": True,
                "defaultOperationTimeoutMs": 5000,
                "idempotencyCacheEntries": 64,
                "retry": {"maxAttempts": 1, "baseDelayMs": 0, "maxDelayMs": 0, "jitterRatio": 0},
            },
        }
    )
    runtime = Meridian.from_config(
        config,
        adapter_factories=(factory,),
        schema_providers=(SchemaProvider(),),
        secret_resolver=SecretResolver(),
    )
    return runtime, factory.runtime
