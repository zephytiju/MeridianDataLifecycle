#!/usr/bin/env python3
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

"""Run all release gates and emit deterministic verification evidence."""

from __future__ import annotations

import ast
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import tomllib
import xml.etree.ElementTree as element_tree
from collections.abc import Sequence
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "build" / "evidence"
RELEASE = ROOT / "build" / "release"
DEPENDENCIES = (
    "meridian-storage-core",
    "meridian-storage-semantics",
    "meridian-storage-query",
)


def _run(arguments: Sequence[str], *, environment: dict[str, str]) -> None:
    rendered = " ".join(arguments)
    print(f"+ {rendered}", flush=True)
    subprocess.run(arguments, cwd=ROOT, env=environment, check=True)


def _output(arguments: Sequence[str]) -> str:
    return subprocess.run(
        arguments,
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _version_contract() -> tuple[str, dict[str, object]]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    project = cast(dict[str, object], project)
    version = cast(str, project["version"])
    compatibility = cast(
        dict[str, object],
        json.loads((ROOT / "compatibility.json").read_text(encoding="utf-8")),
    )
    version_tree = ast.parse(
        (ROOT / "src/meridian_storage/projection/_version.py").read_text(encoding="utf-8")
    )
    source_version = next(
        ast.literal_eval(node.value)
        for node in version_tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        )
    )
    if compatibility.get("version") != version or source_version != version:
        raise RuntimeError("package versions disagree across release metadata")
    dependencies = cast(dict[str, str], compatibility["dependencies"])
    project_dependencies = set(cast(list[str], project["dependencies"]))
    if {f"{name}{constraint}" for name, constraint in dependencies.items()} != project_dependencies:
        raise RuntimeError("dependency constraints disagree with compatibility.json")
    if project.get("license") != "Apache-2.0":
        raise RuntimeError("project license expression must be Apache-2.0")
    return version, compatibility


def _license_gate() -> None:
    marker = "Licensed under the Apache License, Version 2.0"
    paths = sorted((ROOT / "src").rglob("*.py"))
    paths += sorted((ROOT / "tests").rglob("*.py"))
    paths += sorted((ROOT / "scripts").rglob("*.py"))
    missing = [str(path.relative_to(ROOT)) for path in paths if marker not in path.read_text()]
    if missing:
        raise RuntimeError(f"Python sources missing Apache-2.0 headers: {missing}")
    for path in [
        *sorted((ROOT / "contracts").rglob("*.json")),
        ROOT / "compatibility.json",
    ]:
        value = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
        if value.get("$comment") != "SPDX-License-Identifier: Apache-2.0":
            raise RuntimeError(f"{path.relative_to(ROOT)} is missing its SPDX marker")


def main() -> int:
    version, compatibility = _version_contract()
    _license_gate()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    if RELEASE.exists():
        shutil.rmtree(RELEASE)
    RELEASE.mkdir(parents=True)
    environment = os.environ.copy()
    source_date_epoch = environment.get("SOURCE_DATE_EPOCH") or _output(
        ["git", "log", "-1", "--format=%ct"]
    )
    environment["SOURCE_DATE_EPOCH"] = source_date_epoch

    commands = (
        [sys.executable, "-m", "ruff", "check", "."],
        [sys.executable, "-m", "ruff", "format", "--check", "."],
        [sys.executable, "-m", "mypy", "src", "tests", "scripts"],
        [
            sys.executable,
            "-m",
            "pytest",
            "--cov",
            "--cov-report=term-missing",
            "--cov-report=xml:build/evidence/coverage.xml",
            "--cov-report=json:build/evidence/coverage.json",
            "--junitxml=build/evidence/junit.xml",
        ],
        [sys.executable, "-m", "pip", "check"],
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--sdist",
            "--wheel",
            "--outdir",
            str(RELEASE),
            ".",
        ],
    )
    for command in commands:
        _run(command, environment=environment)
    artifacts = sorted(path for path in RELEASE.iterdir() if path.is_file())
    _run(
        [sys.executable, "-m", "twine", "check", *map(str, artifacts)],
        environment=environment,
    )

    contract_hashes = {
        str(path.relative_to(ROOT)): _sha256(path)
        for path in sorted((ROOT / "contracts" / "data-lifecycle").glob("*.json"))
    }
    artifact_evidence = [
        {"file": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)}
        for path in artifacts
    ]
    coverage = cast(
        dict[str, object],
        json.loads((EVIDENCE / "coverage.json").read_text(encoding="utf-8")),
    )
    coverage_totals = cast(dict[str, object], coverage["totals"])
    junit_root = element_tree.parse(EVIDENCE / "junit.xml").getroot()
    suites = [junit_root] if junit_root.tag == "testsuite" else list(junit_root)
    test_totals = {
        field: sum(int(suite.attrib.get(field, "0")) for suite in suites)
        for field in ("tests", "failures", "errors", "skipped")
    }
    dirty = bool(_output(["git", "status", "--porcelain", "--untracked-files=normal"]))
    report = {
        "formatVersion": "meridian.package-verification.v1",
        "package": "meridian-storage-projection",
        "version": version,
        "gitHead": _output(["git", "rev-parse", "HEAD"]),
        "gitDirty": dirty,
        "sourceDateEpoch": int(source_date_epoch),
        "python": ".".join(map(str, sys.version_info[:3])),
        "dependencies": {name: importlib.metadata.version(name) for name in DEPENDENCIES},
        "compatibility": compatibility,
        "contracts": contract_hashes,
        "artifacts": artifact_evidence,
        "coverage": {
            key: coverage_totals[key]
            for key in (
                "num_statements",
                "missing_lines",
                "num_branches",
                "missing_branches",
                "percent_covered",
            )
        },
        "tests": test_totals,
        "gates": ["ruff", "format", "mypy", "pytest-coverage", "pip-check", "twine"],
    }
    output = EVIDENCE / "verification.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"verified {version}; evidence: {output.relative_to(ROOT)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
