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

import ast

from dataclasses import dataclass, field


class SymbolReplacer(ast.NodeTransformer):
    def __init__(self, replacements: dict[str, str]):
        self.replacements = replacements

    def visit_Name(self, node):
        if node.id in self.replacements:
            node.id = self.replacements[node.id]
        return node

    def visit_Attribute(self, node):
        return ast.copy_location(
            ast.Attribute(
                self.visit(node.value),
                self.replacements.get(node.attr, node.attr),
                node.ctx,
            ),
            node,
        )

    def update_docstring(self, docstring):
        """
        Update docstring to replace any key words in the replacements dict
        """
        if not docstring:
            return docstring
        for key_word, replacement in self.replacements.items():
            docstring = docstring.replace(f" {key_word} ", f" {replacement} ")
        return docstring

    def visit_FunctionDef(self, node):
        # replace docstring
        docstring = self.update_docstring(ast.get_docstring(node))
        if isinstance(node.body[0], ast.Expr) and isinstance(
            node.body[0].value, ast.Str
        ):
            node.body[0].value.s = docstring
        return self.generic_visit(node)

    def visit_Str(self, node):
        """Used to replace string type annotations"""
        node.s = self.replacements.get(node.s, node.s)
        return node


class AsyncToSync(ast.NodeTransformer):
    def visit_Await(self, node):
        return self.visit(node.value)

    def visit_AsyncFor(self, node):
        return ast.copy_location(
            ast.For(
                self.visit(node.target),
                self.visit(node.iter),
                [self.visit(stmt) for stmt in node.body],
                [self.visit(stmt) for stmt in node.orelse],
            ),
            node,
        )

    def visit_AsyncWith(self, node):
        return ast.copy_location(
            ast.With(
                [self.visit(item) for item in node.items],
                [self.visit(stmt) for stmt in node.body],
            ),
            node,
        )

    def visit_AsyncFunctionDef(self, node):
        return ast.copy_location(
            ast.FunctionDef(
                node.name,
                self.visit(node.args),
                [self.visit(stmt) for stmt in node.body],
                [self.visit(decorator) for decorator in node.decorator_list],
                node.returns and self.visit(node.returns),
            ),
            node,
        )

    def visit_ListComp(self, node):
        # replace [x async for ...] with [x for ...]
        for generator in node.generators:
            generator.is_async = False
        return self.generic_visit(node)


class HandleCrossSyncDecorators(ast.NodeTransformer):
    def visit_FunctionDef(self, node):
        if hasattr(node, "decorator_list"):
            found_list, node.decorator_list = node.decorator_list, []
            for decorator in found_list:
                if "CrossSync" in ast.dump(decorator):
                    decorator_type = (
                        decorator.func.attr
                        if hasattr(decorator, "func")
                        else decorator.attr
                    )
                    if decorator_type == "convert":
                        for subcommand in decorator.keywords:
                            if subcommand.arg == "sync_name":
                                node.name = subcommand.value.s
                            if subcommand.arg == "replace_symbols":
                                replacements = {
                                    subcommand.value.keys[i]
                                    .s: subcommand.value.values[i]
                                    .s
                                    for i in range(len(subcommand.value.keys))
                                }
                                node = SymbolReplacer(replacements).visit(node)
                    elif decorator_type == "pytest":
                        pass
                    elif decorator_type == "pytest_fixture":
                        # keep decorator
                        node.decorator_list.append(decorator)
                    elif decorator_type == "Retry":
                        node.decorator_list.append(decorator)
                    elif decorator_type == "drop_method":
                        return None
                    else:
                        raise ValueError(
                            f"Unsupported CrossSync decorator: {decorator_type}"
                        )
                else:
                    # add non-crosssync decorators back
                    node.decorator_list.append(decorator)
        return node


@dataclass
class CrossSyncFileArtifact:
    """
    Used to track an output file location. Collects a number of converted classes, and then
    writes them to disk
    """

    file_path: str
    imports: list[ast.Import | ast.ImportFrom | ast.Try | ast.If] = field(
        default_factory=list
    )
    converted_classes: list[ast.ClassDef] = field(default_factory=list)
    contained_classes: set[str] = field(default_factory=set)
    mypy_ignore: list[str] = field(default_factory=list)

    def __hash__(self):
        return hash(self.file_path)

    def __repr__(self):
        return f"CrossSyncFileArtifact({self.file_path}, classes={[c.name for c in self.converted_classes]})"

    def render(self, with_black=True, save_to_disk=False) -> str:
        full_str = (
            "# Copyright 2024 Google LLC\n"
            "#\n"
            '# Licensed under the Apache License, Version 2.0 (the "License");\n'
            "# you may not use this file except in compliance with the License.\n"
            "# You may obtain a copy of the License at\n"
            "#\n"
            "#     http://www.apache.org/licenses/LICENSE-2.0\n"
            "#\n"
            "# Unless required by applicable law or agreed to in writing, software\n"
            '# distributed under the License is distributed on an "AS IS" BASIS,\n'
            "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
            "# See the License for the specific language governing permissions and\n"
            "# limitations under the License.\n"
            "#\n"
            "# This file is automatically generated by CrossSync. Do not edit manually.\n"
        )
        if self.mypy_ignore:
            full_str += (
                f'\n# mypy: disable-error-code="{",".join(self.mypy_ignore)}"\n\n'
            )
        full_str += "\n".join([ast.unparse(node) for node in self.imports])  # type: ignore
        full_str += "\n\n"
        full_str += "\n".join([ast.unparse(node) for node in self.converted_classes])  # type: ignore
        if with_black:
            import black  # type: ignore
            import autoflake  # type: ignore

            full_str = black.format_str(
                autoflake.fix_code(full_str, remove_all_unused_imports=True),
                mode=black.FileMode(),
            )
        if save_to_disk:
            with open(self.file_path, "w") as f:
                f.write(full_str)
        return full_str


class CrossSyncClassParser(ast.NodeTransformer):
    def __init__(self, file_path):
        self.in_path = file_path
        self._artifact_dict: dict[str, CrossSyncFileArtifact] = {}
        self.imports: list[ast.Import | ast.ImportFrom | ast.Try | ast.If] = []
        self.cross_sync_converter = SymbolReplacer(
            {"CrossSync": "CrossSync._Sync_Impl"}
        )

    def convert_file(
        self, artifacts: set[CrossSyncFileArtifact] | None = None
    ) -> set[CrossSyncFileArtifact]:
        """
        Called to run a file through the transformer. If any classes are marked with a CrossSync decorator,
        they will be transformed and added to an artifact for the output file
        """
        tree = ast.parse(open(self.in_path).read())
        self._artifact_dict = {f.file_path: f for f in artifacts or []}
        self.imports = self._get_imports(tree)
        self.visit(tree)
        found = set(self._artifact_dict.values())
        if artifacts is not None:
            artifacts.update(found)
        return found

    def visit_ClassDef(self, node):
        """
        Called for each class in file. If class has a CrossSync decorator, it will be transformed
        according to the decorator arguments
        """
        for decorator in node.decorator_list:
            if "CrossSync" in ast.dump(decorator):
                kwargs = {
                    kw.arg: self._convert_ast_to_py(kw.value)
                    for kw in decorator.keywords
                }
                # find the path to write the sync class to
                sync_path = kwargs.pop("sync_path", None)
                if not sync_path:
                    sync_path = decorator.args[0].s
                out_file = "/".join(sync_path.rsplit(".")[:-1]) + ".py"
                sync_cls_name = sync_path.rsplit(".", 1)[-1]
                # find the artifact file for the save location
                output_artifact = self._artifact_dict.get(
                    out_file, CrossSyncFileArtifact(out_file)
                )
                # write converted class details if not already present
                if sync_cls_name not in output_artifact.contained_classes:
                    converted = self._transform_class(node, sync_cls_name, **kwargs)
                    output_artifact.converted_classes.append(converted)
                    # handle file-level mypy ignores
                    mypy_ignores = [
                        s
                        for s in kwargs.get("mypy_ignore", [])
                        if s not in output_artifact.mypy_ignore
                    ]
                    output_artifact.mypy_ignore.extend(mypy_ignores)
                    # handle file-level imports
                    if not output_artifact.imports and kwargs.get(
                        "include_file_imports", True
                    ):
                        output_artifact.imports = self.imports
                self._artifact_dict[out_file] = output_artifact
        return node

    def _transform_class(
        self,
        cls_ast: ast.ClassDef,
        new_name: str,
        replace_symbols: dict[str, str] | None = None,
        **kwargs,
    ) -> ast.ClassDef:
        """
        Transform async class into sync one, by running through a series of transformers
        """
        # update name
        cls_ast.name = new_name
        # strip CrossSync decorators
        if hasattr(cls_ast, "decorator_list"):
            cls_ast.decorator_list = [
                d for d in cls_ast.decorator_list if "CrossSync" not in ast.dump(d)
            ]
        # convert class contents
        cls_ast = AsyncToSync().visit(cls_ast)
        cls_ast = self.cross_sync_converter.visit(cls_ast)
        if replace_symbols:
            cls_ast = SymbolReplacer(replace_symbols).visit(cls_ast)
        cls_ast = HandleCrossSyncDecorators().visit(cls_ast)
        return cls_ast

    def _get_imports(
        self, tree: ast.Module
    ) -> list[ast.Import | ast.ImportFrom | ast.Try | ast.If]:
        """
        Grab the imports from the top of the file
        """
        imports = []
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.Try, ast.If)):
                imports.append(self.cross_sync_converter.visit(node))
        return imports

    def _convert_ast_to_py(self, ast_node):
        """
        Helper to convert ast primitives to python primitives. Used when unwrapping kwargs
        """
        if isinstance(ast_node, ast.Constant):
            return ast_node.value
        if isinstance(ast_node, ast.List):
            return [self._convert_ast_to_py(node) for node in ast_node.elts]
        if isinstance(ast_node, ast.Dict):
            return {
                self._convert_ast_to_py(k): self._convert_ast_to_py(v)
                for k, v in zip(ast_node.keys, ast_node.values)
            }
        raise ValueError(f"Unsupported type {type(ast_node)}")
