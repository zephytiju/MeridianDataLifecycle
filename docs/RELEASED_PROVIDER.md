<!-- SPDX-License-Identifier: Apache-2.0 -->

# Versioned target and bounded host drain

This is the approved initial integration profile from
[Projection, Cache & Migration §3.5.4 and §4.2, revision 55](https://qcnwge0wy4s0.feishu.cn/docx/AyTkdScgUofjYVx406Fca9conJg#doxcnFCcbNzqnmVZkgmFvF2Vwuh).
The executable example is [`examples/versioned_target.py`](../examples/versioned_target.py).
It uses these released distributions, which install together with ordinary
resolver validation and `pip check`:

| Distribution | Version |
| --- | --- |
| meridian-storage-core | 1.0.1 |
| meridian-storage-semantics | 2.0.0 |
| meridian-storage-query | 1.0.2 |
| meridian-storage-projection | 1.0.2 |
| meridian-storage-postgresql | 2.1.0 |

The adapter pins Projection 1.0.2 exactly. This delivery publishes the example,
read recipe and CI harness in this repository; the runtime APIs and package
payload remain the released 1.0.2 implementation. No dependency override,
editable installation, adapter source checkout or new release is needed. Pin this
repository's merged commit when consuming its example and harness separately.

## Construct and inject the durable provider

Deployment creates a started Meridian facade and an opened PostgreSQL adapter
runtime using `PostgreSQLAdapterFactory().create(AdapterCreateContext(...))`.
The context holds its validated Binding and resolved secret values. Use the
[released provider's construction and migration instructions](https://github.com/zephytiju/MeridianPostgreSQLAdapter/blob/27fad75ae2bc9ae705cee10b2729ed42de5484fa/README.md#durable-projection-outbox-210)
to provision the immutable Structured outbox Resource, metadata and physical
fingerprints **before** runtime startup. The source and outbox must share a
transaction-capable Binding. Startup is read-only. Adapter/Engine clients and SQL
stay out of the projector and business producer.

```python
from meridian_storage.adapters.postgresql import PostgreSQLOutbox
from meridian_storage.projection import ProjectionRunner, TransactionalOutboxWriter

# spec, worker_context, opened_adapter and started meridian come from the host.
# make_projector is the pinned examples/versioned_target.py recipe.
outbox = PostgreSQLOutbox(
    opened_adapter,
    resource="platform.outbox",
    spec=spec,
    context=worker_context,
    poison_threshold=5,
    max_batch_size=100,
)
project = make_projector(
    meridian,
    target=spec.target,
    scope={"tenant": worker_context.tenant, **worker_context.scope},
)
runner = ProjectionRunner(
    meridian=meridian,
    spec=spec,
    project=project,
    outbox=outbox,
    batch_size=2,
    lease_seconds=120,
)
with meridian.context(worker_context):
    runner.run_until_stopped(stop_event, poll_interval_seconds=1)

# The producer uses the actual source Resource/Schema/identity/version in intent.
with meridian.context(producer_context):
    TransactionalOutboxWriter(meridian, outbox_resource="platform.outbox").commit(
        source_mutation,
        intent,
    )
```

The host supplies the same admitted logical tenant/scope for its outbox,
projector closure and Meridian execution context, and closes both runtimes.
Use a distinct worker owner on restart; the unchanged owner-only protocol has no
generation token and cannot distinguish overlapping attempts sharing an owner.

## Version-addressed writes and exact acknowledgement

The initial profile supports ordered integer source versions. Its target Schema
has `id` (string identity), `sourceKey` (string), `sourceVersion` (int64), `deleted`
(boolean) and `document` (JSON). The source Schema used by the executable harness
has `id`, `name` and `deleted`; a deletion is an authoritative versioned tombstone
mutation committed together with its intent.

The example's source key hashes a canonical JSON tuple containing the pinned
projection name, logical tenant/scope, source Catalog, Resource and Data identity.
The target row identity hashes that key plus the **exact integer source version**.
Canonical JSON preserves typed JSON identities and avoids delimiter collisions;
composite JSON identities are supported. Boolean and opaque source versions fail
validation. This is not a mutable-current-row version guard.

The pure projector always returns the same logical Data for the same scoped source
version under its pinned projector and Schema. Event IDs, clock time and attempt
numbers do not change that Data. `structured.put(mode="upsert")` addresses only
that version's row. PostgreSQL may advance its own `recordVersion` on replay;
that is not the source version. The actual returned target Data carries
`sourceVersion`; the runner's released acknowledgement and durable provider
validate it before completion/checkpoint advancement. Do not synthesize an
acknowledgement from the requested input or treat a conflict as success.

A crash after writing v1, followed by writing v2 and restarting to replay v1,
rewrites only v1. The conformance suite checks actual returned/checkpoint version
1, the unchanged full v2 row, immutable source intent and a second claim attempt.
It recreates both runtime pools and the Meridian facade to exclude in-process
idempotency caches. Three additional subprocess tests abruptly exit after claim,
after target commit, and immediately before calling source completion.

## Latest version first, then tombstones and business filters

Collect all versions in the authorized scope for the bounded read set, including
all pages and tombstones. The `latest_visible` recipe groups by `sourceKey`,
selects the largest integer `sourceVersion`, then removes tombstones and applies
the supplied predicate. It does not return an older matching version when the
latest version fails a search/business predicate.

```python
# rows contains the complete bounded version set fetched through structured.query.
visible = latest_visible(rows, matches=lambda row: row["document"]["name"] == query_name)
```

Do not push `deleted=false` or a business/search predicate into the underlying
version scan. Do not reduce each page independently. The executable tests force
one-row pages and prove that v2 suppresses matching v1 and a v3 tombstone continues
to suppress older versions after v1 is replayed again. Production must enforce a
bounded read set and its required read consistency; live keyset pagination is not
a snapshot guarantee. This small recipe is not a general search query planner.
Retention must preserve each latest row/tombstone while any older event can replay.
Pin Schema and projector behavior; changing either requires a reviewed new target
projection/generation rather than changing existing version Data.

## Host admission, successful drain and failed drain

Public `Projector`, `ProjectionRunner`, `run_until_stopped` and `OutboxPort`
signatures are unchanged. The host admits only bounded source loading, projection,
target execution, acknowledgement, provider calls and evidence emission. Account
for the **whole claimed batch**, including release/error paths, in its worst-case
cycle budget. Every admitted callback and provider operation must meet its bound;
an operation timeout alone does not bound arbitrary Python callbacks.

An illustrative budget used by the two-record conformance host is:

| Stage | Maximum allowance |
| --- | ---: |
| Claim call | 10 s |
| Each record: loader + projector + acknowledgement + evidence | 0.4 s |
| Each record: target call + source completion or release | 20 s |
| Full two-record cycle | 50.8 s |
| Host drain deadline | 60 s |
| Lease with margin | 120 s |

These are admission requirements, not deadlines imposed by the runner. Deployment
must configure/validate its actual provider call bounds and include all retry,
release and evidence costs; these numbers are not a universal PostgreSQL SLA.
After stop is observed the existing loop finishes its admitted cycle and starts
no next cycle. The success test sets stop inside the first callback of a claimed
batch of two, verifies both records complete, and proves a third remains pending.
It records elapsed graceful drain against the configured 60 s host deadline.

A separate deliberately stuck callback violates its admitted bound. The test
sets stop, records `graceful_drain=false` at a short host deadline, terminates its
own disposable subprocess, then recreates runtimes and reclaims the expired lease.
There is no target row, completion or checkpoint before recovery. Forced
termination is never reported as graceful drain. Conformance uses explicit UTC
clock arguments to make lease expiry/reclaim deterministic; the deployment host
owns real clock, stop signals and termination policy.

## Reproduce and retain evidence

The `Released provider conformance` workflow tests PostgreSQL 16/PostGIS 3.4 and
PostgreSQL 17/PostGIS 3.5. It installs only exact released runtime packages and runs
`python -I -m pytest conformance/postgresql`; it never installs the checkout.
The example and test harness are the checked-in subject of verification.
The separate standard CI still verifies the repository's existing package gates.

```sh
python -m venv .venv
.venv/bin/python -m pip install -r conformance/requirements.txt
.venv/bin/python -m pip check
# Set MERIDIAN_POSTGRESQL_TEST_DSN to an isolated disposable test database.
# Set MERIDIAN_POSTGRESQL_ENGINE_VERSION to 16-postgis-3.4 or 17-postgis-3.5.
.venv/bin/python -I -m pytest conformance/postgresql -q -s \
  --junitxml=build/evidence/released-provider.xml
.venv/bin/python -I conformance/record_artifacts.py
```

The fixture uses the **installed adapter's** Schema compiler/migration code to
create disposable test resources. Its DDL setup and cleanup are test-only and
adapted from the public PostgreSQL 2.1.0 conformance source. Business/example code
contains no SQL, driver calls or provider factory. The only fault proxy delegates
to the durable port and exits at the checkpoint boundary; it supplies no storage.
The installed shared Outbox conformance fixture also runs, covering wrong/expired
owners, retries, quarantine, immutable intent and progress across reopened storage.

CI retains JUnit, host outcome output and an installed-artifact manifest containing
exact versions, package source digests, origin paths, example/harness digests and
Git commit. All production classes must originate in site-packages. This evidence
must be considered alongside the successful provider publication and the merged
example commit; an old counterexample report is not final acceptance.
