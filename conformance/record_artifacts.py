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

"""Record installed release identities and the separately versioned example/harness."""

import hashlib
import inspect
import json
import subprocess
from importlib.metadata import distribution
from pathlib import Path

from meridian_storage.adapters.postgresql import PostgreSQLOutbox

from meridian_storage.projection import ProjectionRunner, TransactionalOutboxWriter

root = Path(__file__).resolve().parents[1]
packages = {}
for name in ("core", "semantics", "query", "projection", "postgresql"):
    d = distribution("meridian-storage-" + name)
    files = {}
    for item in d.files:
        if item.suffix in (".py", ".json"):
            path = Path(d.locate_file(item)).resolve()
            assert "site-packages" in path.parts, path
            files[str(item)] = hashlib.sha256(path.read_bytes()).hexdigest()
    packages[d.metadata["Name"]] = {"version": d.version, "files": files}
origins = {
    cls.__name__: inspect.getfile(cls)
    for cls in (PostgreSQLOutbox, ProjectionRunner, TransactionalOutboxWriter)
}
for path in origins.values():
    assert "site-packages" in Path(path).parts
sources = {
    str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
    for folder in ("examples", "conformance")
    for p in (root / folder).rglob("*.py")
}
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
output = root / "build/evidence/installed-artifacts.json"
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(
    json.dumps(
        {"gitHead": commit, "packages": packages, "origins": origins, "harness": sources}, indent=2
    )
    + "\n"
)
print(output)
