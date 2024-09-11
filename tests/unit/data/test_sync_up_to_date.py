# Copyright 2024 Google LLC
#
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
import os
import sys
import hashlib
import pytest
from difflib import unified_diff

# add cross_sync to path
test_dir_name = os.path.dirname(__file__)
repo_root = os.path.join(test_dir_name, "..", "..", "..")
cross_sync_path = os.path.join(repo_root, ".cross_sync")
sys.path.append(cross_sync_path)

from generate import convert_files_in_dir  # noqa: E402


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="ast.unparse is only available in 3.9+"
)
@pytest.mark.parametrize(
    "artifact", convert_files_in_dir(repo_root), ids=lambda a: a.file_path
)
def test_sync_up_to_date(artifact):
    """
    Generate a fresh copy of each cross_sync file, and compare hashes with the existing file.

    If this test fails, run `nox -s generate_sync` to update the sync files.
    """
    path = artifact.file_path
    new_render = artifact.render()
    found_render = open(path).read()
    # compare by content
    diff = unified_diff(found_render.splitlines(), new_render.splitlines(), lineterm="")
    diff_str = "\n".join(diff)
    assert not diff_str, f"Found differences:\n{diff_str}"
    # compare by hash
    new_hash = hashlib.md5(new_render.encode()).hexdigest()
    found_hash = hashlib.md5(found_render.encode()).hexdigest()
    assert new_hash == found_hash, f"md5 mismatch for {path}"
