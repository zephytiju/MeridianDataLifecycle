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
revision 45, and the observed package LLD revision 27. Runtime dependencies are
only the released Core, Semantics, and Query 1.0.0 distributions.

The release gate writes artifact, contract, dependency, test, and coverage
evidence to `build/evidence/verification.json`; CI retains it with the exact
wheel and source distribution.
