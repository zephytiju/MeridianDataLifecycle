# Contributing

Contributions are accepted under the Apache License 2.0. Create a focused
branch, add tests for behavioral changes, and run the repository quality gates
before opening a pull request.

```console
python3 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r validation/core-1.1.0.txt
.venv/bin/python -m pip install '.[test]'
.venv/bin/python scripts/verify.py
```
