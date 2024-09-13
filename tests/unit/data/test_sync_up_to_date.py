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
import ast
import re
from difflib import unified_diff

# add cross_sync to path
test_dir_name = os.path.dirname(__file__)
repo_root = os.path.join(test_dir_name, "..", "..", "..")
cross_sync_path = os.path.join(repo_root, ".cross_sync")
sys.path.append(cross_sync_path)

from generate import convert_files_in_dir, CrossSyncOutputFile  # noqa: E402

sync_files = list(convert_files_in_dir(repo_root))


@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="ast.unparse is only available in 3.9+"
)
@pytest.mark.parametrize("sync_file", sync_files, ids=lambda f: f.output_path)
def test_sync_up_to_date(sync_file):
    """
    Generate a fresh copy of each cross_sync file, and compare hashes with the existing file.

    If this test fails, run `nox -s generate_sync` to update the sync files.
    """
    path = sync_file.output_path
    new_render = sync_file.render(with_black=True, save_to_disk=False)
    found_render = CrossSyncOutputFile(
        output_path="", ast_tree=ast.parse(open(path).read()), header=sync_file.header
    ).render(with_black=True, save_to_disk=False)
    # compare by content
    diff = unified_diff(found_render.splitlines(), new_render.splitlines(), lineterm="")
    diff_str = "\n".join(diff)
    assert not diff_str, f"Found differences:\n{diff_str}"
    # compare by hash
    new_hash = hashlib.md5(new_render.encode()).hexdigest()
    found_hash = hashlib.md5(found_render.encode()).hexdigest()
    assert new_hash == found_hash, f"md5 mismatch for {path}"


@pytest.mark.parametrize("sync_file", sync_files, ids=lambda f: f.output_path)
def test_verify_headers(sync_file):
    license_regex = r"""
        \#\ Copyright\ \d{4}\ Google\ LLC\n
        \#\n
        \#\ Licensed\ under\ the\ Apache\ License,\ Version\ 2\.0\ \(the\ \"License\"\);\n
        \#\ you\ may\ not\ use\ this\ file\ except\ in\ compliance\ with\ the\ License\.\n
        \#\ You\ may\ obtain\ a\ copy\ of\ the\ License\ at\
        \#\n
        \#\s+http:\/\/www\.apache\.org\/licenses\/LICENSE-2\.0\n
        \#\n
        \#\ Unless\ required\ by\ applicable\ law\ or\ agreed\ to\ in\ writing,\ software\n
        \#\ distributed\ under\ the\ License\ is\ distributed\ on\ an\ \"AS\ IS\"\ BASIS,\n
        \#\ WITHOUT\ WARRANTIES\ OR\ CONDITIONS\ OF\ ANY\ KIND,\ either\ express\ or\ implied\.\n
        \#\ See\ the\ License\ for\ the\ specific\ language\ governing\ permissions\ and\n
        \#\ limitations\ under\ the\ License\.
    """
    pattern = re.compile(license_regex, re.VERBOSE)

    with open(sync_file.output_path, "r") as f:
        content = f.read()
        assert pattern.search(content), "Missing license header"
