# Meridian Storage Projection

`meridian-storage-projection` is the Apache-2.0 Meridian V1 lifecycle library
for transactional outbox processing, derived projections, cache policy and
normal-read reuse, explicit Schema migration, logical export/import, and
projection rebuild.

It is a library, not a Projection Catalog. Meridian still has exactly five
Catalogs: `structured`, `object`, `cache`, `evidence`, and `streaming`. This
package registers none of them. The released `cache` Catalog retains its exact
public surface and this package supplies policy and coordination helpers around
it.

## Install

```console
python -m pip install meridian-storage-projection==1.0.2
```

Python 3.12 through 3.14 is supported. Runtime dependencies are pinned to the
released Meridian Core 1.0.1, Semantics 2.0.0, and Query 1.0.2 contracts.

## Projection

```python
from meridian_storage.projection import ProjectionRunner, ProjectionSpec

spec = ProjectionSpec(
    name="case-search-v1",
    source_catalog="structured",
    source="investigation.cases",
    target_catalog="structured",
    target="investigation.case_search",
    source_schema="investigation.case@3",
    target_schema="investigation.case_search@1",
)

runner = ProjectionRunner(
    meridian=meridian,
    spec=spec,
    project=project_case_for_search,
    outbox=outbox_adapter,
    batch_size=100,
    lease_seconds=60,
)

# The host process owns scheduling and shutdown.
runner.run_once()
```

The runner advances a checkpoint only after the derived target acknowledges the
exact source version. Crashed workers reclaim expired leases, duplicate delivery
requires idempotent target writes, and poison records enter quarantine until an
explicit operator retry.

Construct the runner after `meridian.start()`. Both Resources must have the exact
Schemas in the spec and resolved Bindings satisfying their declared Capability
requirements. Construction performs no target writes or projector calls.

Adapter authors can run the wheel's shared lifecycle fixtures via
`meridian_storage.projection.testing`; see [conformance](docs/CONFORMANCE.md).
The owner-only port does not fence an older attempt after the same owner is
reused. Hosts must prevent overlapping owner reuse.

## Transparent read cache

```python
from datetime import timedelta

from meridian_storage.projection import (
    CacheCoordinator,
    CachePolicy,
    LoadedValue,
    MeridianCacheBackend,
)

policy = CachePolicy(
    source_resource="investigation.cases",
    cache_resource="investigation.case_cache",
    key_fields=("case_id",),
    serializer_id="canonical-json-v1",
    schema_fingerprint="sha256:" + schema_fingerprint_hex,
    default_ttl=timedelta(seconds=30),
    maximum_ttl=timedelta(minutes=5),
    maximum_staleness=timedelta(seconds=30),
)
cache = CacheCoordinator(policy=policy, backend=MeridianCacheBackend(meridian))

result = cache.read(
    {"case_id": case_id},
    lambda: LoadedValue(load_authoritative_case(), source_version=record_version),
    required_source_version=record_version,
)
```

Every stored value carries a versioned cache envelope with its logical key,
serializer, Schema fingerprint, creation/expiry timestamps, and source version.
Positive and negative values use distinct tagged payloads, so user Data cannot
collide with the negative-cache marker. Cache outage, corruption, and invalidation failure never change authoritative
correctness. Explicit CAS/unavailable operations fail retryably; normal reads
may fall back according to policy.

## Migration and portability

`MigrationPlanner` classifies released `SchemaDocument` values and verifies an
explicit logical `MigrationBundleV1`. Preconditions, validations, and
postconditions carry complete serialized Meridian V1 Operations, never Adapter
or Engine concepts. `MigrationExecutor` runs only when called by a deployment
job, against injected Adapter compilation/lock/apply/activation hooks. Runtime
startup remains read-only.

`LogicalExportCoordinator` and `LogicalImportCoordinator` stream bounded,
canonical NDJSON partitions with counts, byte sizes, logical-id bounds, and
SHA-256 verification. Import verifies every immutable artifact before target
preflight or writes, then detects any artifact change between verification and
consumption. Cache state is excluded. `PortableRecoveryCoordinator`
only correlates logical evidence with an IaC-owned backup/restore validation;
it performs no Engine lifecycle action.

## V1 boundaries

- No service, scheduler, broker, worker deployment, or Engine client is started.
- No physical table, index, topic, bucket, endpoint, or credential is public.
- Streaming replay/group positions remain versioned Streaming Operations.
- Online dual-write, CDC catch-up, zero-downtime cross-engine cutover,
  snapshot-stable pagination, and high-volume specialization are forward-looking.
- Platform/Vangu IaC owns provisioning, identity, ACL, migration jobs, backup,
  restore, recovery, topology, scaling, and worker lifecycle.

Normative JSON Schemas are in `contracts/data-lifecycle/`. Unit, integration,
contract, crash-boundary, and packaging tests are included in the source
distribution.

## Verification

Run the same release gate used by CI:

```console
python scripts/verify.py
```

It runs formatting, lint, strict typing, unit/integration/contract/crash and
packaging tests, branch coverage, dependency consistency, reproducible builds,
wheel installation, and Twine metadata validation. Deterministic hashes and
results are written to `build/evidence/verification.json`. See
[`docs/CONFORMANCE.md`](docs/CONFORMANCE.md) for acceptance traceability.

## Released durable PostgreSQL integration

The [version-addressed target and bounded host-drain recipe](docs/RELEASED_PROVIDER.md)
uses the released Projection 1.0.2 and PostgreSQL 2.1.0 composition. It includes
latest-version/tombstone reads and real-provider CI for restart, replay, exact
acknowledgement, graceful drain and failed-drain recovery. The executable host
retains lifecycle and termination ownership.
