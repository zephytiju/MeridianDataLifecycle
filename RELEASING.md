# Releasing

Releases are ordinary public Python package releases under Apache-2.0.

1. Run `python scripts/verify.py` from a clean checkout.
2. Confirm the version agrees in `pyproject.toml`, `_version.py`, changelog, and
   compatibility manifest.
3. Merge a green pull request to protected `main`.
4. Before the first release, the PyPI owner configures the pending trusted
   publisher for owner `zephytiju`, repository
   `meridian-storage-projection`, workflow `release.yml`, and environment
   `pypi`. This is the only owner-assisted namespace gate; credentials and MFA
   are never bypassed.
5. Push a signed `vX.Y.Z` tag at the verified merge commit. GitHub Actions
   rebuilds and attests the artifacts, creates the GitHub release, and publishes
   through PyPI trusted publishing.
6. Independently install the published wheel and verify its hashes and imports.
