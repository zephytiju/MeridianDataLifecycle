# Dependency compatibility and validation

Projection 1.0.3 repairs distribution metadata without changing lifecycle
implementation, serialized Schema formats, Catalog names, or public signatures.
Its dependency bounds describe the APIs it consumes:

| Dependency | Bound | Required surface |
| --- | --- | --- |
| Core | `>=1.0.1,<2` | Meridian facade, Expression/Operation/OperationResult, resource resolution, Capability validation, transaction context and error contracts. |
| Semantics | `>=2.0.0,<3` | Structured explicit write modes, Catalog names, CacheEntry/FrozenJson and deterministic Schema migration models. The 2.0 boundary retains the explicit `if_absent` producer contract. |
| Query | `>=1.0.2,<2` | Released structured query composition used by logical resource and latest-version reads; the existing minimum is retained. |

The lower bounds retain the previously shipped API floor. The next major
versions are excluded because their API compatibility has not been established.
Core 1.1.0 retains the consumed interfaces, and the repaired Semantics 2.0.1 and
Query 1.0.3 releases admit it normally. A range admits resolution; it does not
prove behavior for every future release within that range.

`validation/dependencies.json` records exact public wheel and sdist URLs and
SHA-256 hashes. Its pip requirements files select two release-validation profiles:

| Profile | Core | Semantics | Query |
| --- | --- | --- | --- |
| `core-1.0.1` | 1.0.1 | 2.0.0 | 1.0.2 |
| `core-1.1.0` | 1.1.0 | 2.0.1 | 1.0.3 |

CI runs all release gates on both profiles for Python 3.12, 3.13 and 3.14.
Publication uses the Core 1.1.0 profile. The verifier rejects drift from the
selected validation lock and records the exact dependency artifacts in release
evidence. These are build and deployment inputs, never runtime release
allowlists. Consumers own their deployment locks; untested combinations remain
unverified.

From a fresh environment, run the new profile with:

```console
python -m pip install --require-hashes -r validation/core-1.1.0.txt
python -m pip install '.[test]'
python scripts/verify.py
```

To validate the API floor, use `validation/core-1.0.1.txt` in another fresh
environment and set `MERIDIAN_VALIDATION_PROFILE=core-1.0.1` when running the
verifier. Packaging tests install the candidate wheel and selected public
prerequisites using normal dependency resolution in a clean environment, run
`pip check`, and exercise the installed OutboxPort conformance fixture under
isolated Python. No sibling implementation or import-path injection is used.

The existing released-provider workflow remains on its historical public lock
in `conformance/requirements.txt`: Projection 1.0.2 and PostgreSQL 2.1.0 with
Core 1.0.1. It continues to check source/outbox atomicity, durable restart, exact
acknowledgement, v1 replay after v2, latest/tombstones, graceful drain and failed
drain/termination/expiry recovery on both recorded PostGIS profiles. This is
historical provider evidence, not physical conformance for the new closure.
At this repair, public PostgreSQL 2.1.1 still requires the historical package
versions; the new provider composition is unverified until the downstream
PostgreSQL release and final closure conformance run. Dependency overrides or
skipped gates cannot bridge that boundary.

Upgrading this package requires normal resolution and a new deployment lock;
it requires no outbox migration or persisted-contract reinterpretation. Keep
the previous exact deployment lock for rollback. Publication always uses a new
version and CI-built artifacts from the verified merged commit.
