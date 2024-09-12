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
from __future__ import annotations
from typing import Sequence
import ast
"""
Entrypoint for initiating an async -> sync conversion using CrossSync

Finds all python files rooted in a given directory, and uses
transformers.CrossSyncFileProcessor to handle any files marked with
__CROSS_SYNC_OUTPUT__
"""


def extract_header_comments(file_path) -> str:
    """
    Extract the file header. Header is defined as the top-level
    comments before any code or imports
    """
    header = []
    with open(file_path, "r") as f:
        for line in f:
            if line.startswith("#") or line.strip() == "":
                header.append(line)
            else:
                break
    return "".join(header)


class CrossSyncOutputFile:

    def __init__(self, output_path: str, ast_tree, header: str | None = None):
        self.output_path = output_path
        self.tree = ast_tree
        self.header = header or ""

    def render(self, with_black=True, save_to_disk: bool = False) -> str:
        """
        Render the file to a string, and optionally save to disk

        Args:
            with_black: whether to run the output through black before returning
            save_to_disk: whether to write the output to the file path
        """
        full_str = self.header + ast.unparse(self.tree)
        if with_black:
            import black  # type: ignore
            import autoflake  # type: ignore

            full_str = black.format_str(
                autoflake.fix_code(full_str, remove_all_unused_imports=True),
                mode=black.FileMode(),
            )
        if save_to_disk:
            import os
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            with open(self.output_path, "w") as f:
                f.write(full_str)
        return full_str


def convert_files_in_dir(directory: str) -> set[CrossSyncOutputFile]:
    import glob
    from transformers import CrossSyncFileProcessor

    # find all python files in the directory
    files = glob.glob(directory + "/**/*.py", recursive=True)
    # keep track of the output files pointed to by the annotated classes
    artifacts: set[CrossSyncOutputFile] = set()
    file_transformer = CrossSyncFileProcessor()
    # run each file through ast transformation to find all annotated classes
    for file_path in files:
        ast_tree = ast.parse(open(file_path).read())
        output_path = file_transformer.get_output_path(ast_tree)
        if output_path is not None:
            # contains __CROSS_SYNC_OUTPUT__ annotation
            converted_tree = file_transformer.visit(ast_tree)
            header = extract_header_comments(file_path)
            artifacts.add(CrossSyncOutputFile(output_path, converted_tree, header))
    # return set of output artifacts
    return artifacts


def save_artifacts(artifacts: Sequence[CrossSyncOutputFile]):
    for a in artifacts:
        a.render(save_to_disk=True)


if __name__ == "__main__":
    import sys

    search_root = sys.argv[1]
    outputs = convert_files_in_dir(search_root)
    print(f"Generated {len(outputs)} artifacts: {[a.file_path for a in outputs]}")
    save_artifacts(outputs)
