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

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tarfile
import venv
import zipfile
from email.parser import BytesParser
from email.policy import default
from pathlib import Path

import pytest

import meridian_storage

ROOT = Path(__file__).parents[2]
EXPECTED_REQUIREMENTS = {
    "meridian-storage-core==1.0.1",
    "meridian-storage-query==1.0.2",
    "meridian-storage-semantics==2.0.0",
}


def _build(output: Path) -> tuple[Path, Path]:
    output.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["SOURCE_DATE_EPOCH"] = "1787616000"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--sdist",
            "--wheel",
            "--outdir",
            str(output),
            ".",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return next(output.glob("*.whl")), next(output.glob("*.tar.gz"))


@pytest.fixture(scope="session")
def distributions(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    return _build(tmp_path_factory.mktemp("distribution"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_wheel_metadata_and_contents(distributions: tuple[Path, Path]) -> None:
    wheel, _ = distributions
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = BytesParser(policy=default).parsebytes(archive.read(metadata_name))

    assert metadata["Name"] == "meridian-storage-projection"
    assert metadata["Version"] == "1.0.2"
    assert metadata["License-Expression"] == "Apache-2.0"
    assert set(metadata["Requires-Python"].split(",")) == {">=3.12", "<3.15"}
    assert set(metadata.get_all("Requires-Dist", [])) >= EXPECTED_REQUIREMENTS
    assert "meridian_storage/projection/py.typed" in names
    assert "meridian_storage/projection/testing/outbox_conformance.py" in names
    assert "meridian_storage/projection/compatibility.json" in names
    assert any(name.endswith(".dist-info/licenses/LICENSE") for name in names)
    assert any(name.endswith(".dist-info/licenses/NOTICE") for name in names)
    contract_prefix = "meridian_storage/projection/contracts/data-lifecycle/"
    assert {
        contract_prefix + "meridian-cache-envelope.v1.schema.json",
        contract_prefix + "meridian-cache-policy.v1.schema.json",
        contract_prefix + "meridian-logical-export.v1.schema.json",
        contract_prefix + "meridian-migration-bundle.v1.schema.json",
        contract_prefix + "meridian-outbox.v1.schema.json",
        contract_prefix + "meridian-projection-spec.v1.schema.json",
    } <= names
    assert not any(name.startswith("meridian_storage/query/") for name in names)
    assert not any(name.startswith("meridian_storage/semantics/") for name in names)


def test_sdist_contains_source_tests_contracts_and_release_material(
    distributions: tuple[Path, Path],
) -> None:
    _, sdist = distributions
    with tarfile.open(sdist, "r:gz") as archive:
        names = {name.split("/", 1)[1] for name in archive.getnames() if "/" in name}
    assert {
        "LICENSE",
        "NOTICE",
        "README.md",
        "RELEASING.md",
        "compatibility.json",
        "contracts/data-lifecycle/meridian-cache-envelope.v1.schema.json",
        "src/meridian_storage/projection/__init__.py",
        "tests/packaging/test_distribution.py",
    } <= names


def test_build_is_reproducible(distributions: tuple[Path, Path], tmp_path: Path) -> None:
    first_wheel, first_sdist = distributions
    second_wheel, second_sdist = _build(tmp_path / "second")
    assert _sha256(first_wheel) == _sha256(second_wheel)
    assert _sha256(first_sdist) == _sha256(second_sdist)


def test_wheel_installs_and_imports_outside_source_tree(
    distributions: tuple[Path, Path], tmp_path: Path
) -> None:
    wheel, _ = distributions
    environment = tmp_path / "venv"
    # Standalone macOS Python needs its linked executable to locate its runtime.
    venv.EnvBuilder(with_pip=True, symlinks=os.name != "nt").create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run(
        [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    process_environment = os.environ.copy()
    assert meridian_storage.__file__ is not None
    process_environment["PYTHONPATH"] = str(Path(meridian_storage.__file__).resolve().parent.parent)
    completed = subprocess.run(
        [
            str(python),
            "-c",
            (
                "import pathlib,site,sys; import meridian_storage; "
                "root=next(pathlib.Path(item)/'meridian_storage' for item in "
                "site.getsitepackages() if (pathlib.Path(item)/'meridian_storage'/'projection'"
                "/'__init__.py').is_file()); "
                "meridian_storage.__path__ = [str(root), *meridian_storage.__path__]; "
                "import meridian_storage.projection as p; "
                "path=pathlib.Path(p.__file__).resolve(); "
                "assert p.__version__ == '1.0.2'; "
                "from meridian_storage.projection.testing import "
                "OutboxConformanceTarget, run_outbox_conformance; "
                "store=p.InMemoryOutboxStore(poison_threshold=2); "
                "report=run_outbox_conformance(OutboxConformanceTarget("
                "store,store.append,store.get,store.checkpoint,lambda:store)); "
                "assert report.same_owner_completion == 'accepted-indistinguishable-owner'; "
                "assert path.is_relative_to(pathlib.Path(sys.prefix).resolve()), path"
            ),
        ],
        cwd=tmp_path,
        env=process_environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stderr == ""
