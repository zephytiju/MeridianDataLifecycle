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
revision 124, Engine Adapters revision 24, Kafka Adapter revision 6, Constructs
revision 45, and the observed package LLD revision 53 / Core runtime revision 69. Runtime dependencies are
only the released Core 1.0.1, Semantics 2.0.0 and Query 1.0.2 distributions.

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
this library reads the pinned Core 1.0.1 registry snapshot and capability map;
consumers receive no Binding IDs, Engine clients or physical locators. Actual
projector Expression requirements are validated by Core when executed, since a
pure application projector's future Expression cannot be known at construction.

`tests/integration/test_runner_bindings.py` covers failure before work and a
started, installed Core + Structured Catalog composition with a recording SPI
adapter. The latter verifies released interfaces and performs no database I/O;
it does not claim production adapter conformance.

## Shared OutboxPort lifecycle fixtures (1.0.1)

Install `meridian-storage-projection==1.0.2` from PyPI. The wheel exports
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


## Transactional writer validation (1.0.2)

`TransactionalOutboxWriter.commit(mutation, intent)` keeps its existing signature.
It joins the source Binding transaction, executes the mutation through Meridian,
and checks the actual OperationResult before inserting intent. Resource and
Catalog must match the single normalized source; the exact Schema comes from the
transaction-pinned registry snapshot. Result contract/version, request fingerprint
and registry fingerprint must match that normalized operation.

The result must contain one record (a mapping or a singleton record sequence).
Identity follows the Schema's ordered `identity` fields: one field is a scalar;
multiple fields form an ordered tuple. Returned `recordVersion` must match the
intent exactly, including type; absent version metadata requires `None`, never a
fabricated zero/create sentinel. Included payload fields must agree with returned
Data; immutable-reference intents still validate source identity and version.
A put's supplied identity must also agree with its actual result. Mismatches raise
`DataLifecycleValidationError` inside the transaction. Required write failures
propagate; nested transactions retain Core's existing rollback behavior.

Outbox creation calls Structured `put(mode="if_absent")` without an expected
version. There is no read-before-write reconciliation and no group replay. Existing
adapter recognition of the same request may return its original result without
writing again; equal content alone never establishes replay. Independent duplicate
creates conflict and roll back the source. Writer tests use installed Core and
Semantics with a transactional recording SPI to verify rollback, replay, immutable
intent and preservation of externally advanced processing state. They do not claim
durable SQL or process-restart proof, which belongs to the PostgreSQL adapter.

The shared lifecycle fixtures published in 1.0.1 remain unchanged and run in this
package's contract tests; there is no competing fixture suite or new owner protocol.
