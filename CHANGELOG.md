# Changelog

All notable changes to this project are documented here.

## 1.0.0 - 2026-08-25

- Add transactional outbox models, atomic mutation composition, leases,
  quarantine, exact-version checkpoints, and lag contracts.
- Add host-driven projection execution and deterministic cutover evidence.
- Add cache policy, normal read-through reuse, invalidation, single-flight, and
  explicit cache coordination over the released Cache Catalog, including a
  collision-safe versioned validation envelope.
- Add Schema compatibility planning and deployment-controlled migration hooks.
- Add bounded logical export/import, integrity verification, portable restore
  validation, pre-write artifact verification, and projection generation
  rebuild coordination with activation-safe cleanup.
- Add V1 JSON contracts, conformance fixtures, CI, packaging, and release gates.
