# Meridian Storage Projection V1 conformance

This ledger maps the V1 acceptance surface to executable evidence. The package
is a library and owns no Catalog. Production Adapters implement the injected
ports; Platform and Vangu IaC own deployment jobs, bindings, Engine lifecycle,
identity, ACL, backup, restore, and recovery.

| Acceptance area | Package implementation | Executable evidence |
|---|---|---|
| Atomic source mutation and outbox intent | `TransactionalOutboxWriter`, `OutboxDataV1` | `tests/integration/test_transactional_outbox.py` |
| Lease, ordering, exact acknowledgement, retry and quarantine | `InMemoryOutboxStore`, `ProjectionRunner` | `tests/unit/test_outbox.py`, `tests/integration/test_runner.py` |
| Checkpoint crash boundary and duplicate replay | `ProjectionRunner`, `OutboxPort` | `tests/crash/test_projection_crash_boundaries.py` |
| Cache policy, normal-read reuse, single-flight and invalidation | `CacheCoordinator` | `tests/unit/test_cache.py` |
| Exact public Cache Catalog composition and envelope preservation | `MeridianCacheBackend` | `tests/contract/test_contracts.py`, `tests/integration/test_meridian_cache_backend.py` |
| Schema compatibility and deterministic migration compilation | `MigrationPlanner`, `MigrationBundleV1` | `tests/unit/test_migration.py` |
| Locking, durable checkpoints, resume, validation and cutover evidence | `MigrationExecutor` | `tests/integration/test_migration_executor.py` |
| Bounded logical export/import and digest-first validation | `LogicalExportCoordinator`, `LogicalImportCoordinator` | `tests/integration/test_portability.py` |
| Backup/restore evidence without Engine ownership | `PortableRecoveryCoordinator` | `tests/integration/test_portability.py` |
| Scan, concurrent tail, validation, activation and rollback retention | `RebuildCoordinator` | `tests/integration/test_rebuild.py` |
| JSON contracts and exactly five Catalogs | `contracts/data-lifecycle/` | `tests/contract/test_contracts.py` |
| Apache-2.0 metadata, wheel/sdist contents and reproducibility | package metadata and `scripts/verify.py` | `tests/packaging/test_distribution.py` |

The compatibility manifest locks HLD revision 56, Catalogs/Public Interfaces
revision 70, Engine Adapters revision 24, Kafka Adapter revision 6, Constructs
revision 45, and the observed package LLD revision 53 / Core runtime revision 69. Runtime dependencies are
only the released Core, Semantics, and Query 1.0.0 distributions.

The release gate writes artifact, contract, dependency, test, and coverage
evidence to `build/evidence/verification.json`; CI retains it with the exact
wheel and source distribution.

## Projection construction (1.0.1)

Start Meridian before constructing `ProjectionRunner`. The constructor resolves
both registered Catalogs and checks both logical Resources on one immutable
registry revision. Each Resource must reference the exact Schema named by the
specification, that Schema must exist, and its resolved Binding must identify the
authenticated Capability manifest satisfying all declared Resource requirements
(operation versions, guarantees and minimum limits). Source and target may use
different Bindings: projection completion remains asynchronous.

Validation is read-only. It invokes neither the projector, OutboxPort, adapter
session nor external connection. The host still starts/stops Meridian and the
runner, and injects its production OutboxPort. No factory is required. Internally,
this library reads the pinned Core 1.0.0 registry snapshot and capability map;
consumers receive no Binding IDs, Engine clients or physical locators. Actual
projector Expression requirements are validated by Core when executed, since a
pure application projector's future Expression cannot be known at construction.

`tests/integration/test_runner_bindings.py` covers failure before work and a
started, installed Core + Structured Catalog composition with a recording SPI
adapter. The latter verifies released interfaces and performs no database I/O;
it does not claim production adapter conformance.

## Shared OutboxPort lifecycle fixtures (1.0.1)

Install `meridian-storage-projection==1.0.1` from PyPI. The wheel exports
`OutboxConformanceTarget` and `run_outbox_conformance` from
`meridian_storage.projection.testing`; no test source checkout or pytest
dependency is needed. The adapter test owns an empty isolated Resource/projection
scope, sets its poison threshold to **2**, and cleans up all resulting records.
Its test callbacks seed immutable intents, inspect records/checkpoints and reopen
the production port over the same storage:

```python
from meridian_storage.projection.testing import (
    OutboxConformanceTarget,
    run_outbox_conformance,
)

# All variables below are supplied by the adapter's disposable test environment.
# seed_intent can use its supported TransactionalOutboxWriter composition.
report = run_outbox_conformance(
    OutboxConformanceTarget(
        outbox=durable_outbox,
        seed=seed_intent,
        inspect_record=read_current_record,
        checkpoint=read_current_checkpoint,
        reopen=reopen_same_storage,
        source_resource="investigation.cases",
        source_schema="investigation.case@1",
        target_labels=("search",),
    )
)
print(report.to_mapping())
```

The callbacks are test setup and inspection, not a new production lifecycle SPI.
They must always read the current storage after reopening. `reopen` must preserve
storage while recreating adapter connections/instances; an adapter's own crash
tests additionally demonstrate process/connection loss. The runner uses explicit
UTC `now` arguments, fixed event IDs and versions, bounded claims, and unique
source identities. Do not run it with Python `-O`, which disables assertions.

Assertions cover exclusive bounded claims, per-source ordering, exact expiry,
non-owner complete/release, restart callbacks, reclaim attempts, wrong version
(including boolean/integer and string/integer confusion), completion/checkpoint
consistency, monotonic revisions, retry/quarantine persistence, redaction and
quarantine blocking without silent skips. Runner tests also verify target failure
and crash after target acknowledgement leave the source checkpoint unchanged.

**Owner semantics remain unchanged.** The four OutboxPort signatures carry no
generation token. `OutboxLease.attempt` is diagnostic metadata and is not supplied
to complete/release. After the same owner reclaims, an older caller may submit an
indistinguishable completion/release. The fixture reports that observation
separately and never claims stale-generation rejection. Hosts must prevent older
attempts from remaining active when reusing an owner; distinct owner identities
allow the existing owner check to reject old callers.

`InMemoryOutboxStore` runs the same fixture in library CI, and a deliberately
incorrect acknowledgement implementation proves the checks detect violations.
Reusing the reference instance in `reopen` is only a lifecycle test. Durable
storage, independent processes, atomicity under interruption and real engine
proof remain the adapter task's acceptance gates. The report explicitly preserves
this distinction.
