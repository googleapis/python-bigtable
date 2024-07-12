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

from google.cloud.bigtable.data._sync.cross_sync import CrossSync


class SymbolReplacer(ast.NodeTransformer):
    """
    Replaces all instances of a symbol in an AST with a replacement

    Works for function signatures, method calls, docstrings, and type annotations
    """
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

    def visit_AsyncFunctionDef(self, node):
        """
        Replace async function docstrings
        """
        # use same logic as FunctionDef
        return self.visit_FunctionDef(node)

    def visit_FunctionDef(self, node):
        """
        Replace function docstrings
        """
        docstring = ast.get_docstring(node)
        if docstring and isinstance(node.body[0], ast.Expr) and isinstance(
            node.body[0].value, ast.Str
        ):
            for key_word, replacement in self.replacements.items():
                docstring = docstring.replace(f" {key_word} ", f" {replacement} ")
            node.body[0].value.s = docstring
        return self.generic_visit(node)

    def visit_Str(self, node):
        """Replace string type annotations"""
        node.s = self.replacements.get(node.s, node.s)
        return node


class AsyncToSync(ast.NodeTransformer):
    """
    Replaces or strips all async keywords from a given AST
    """
    def visit_Await(self, node):
        """
        Strips await keyword
        """
        return self.visit(node.value)

    def visit_AsyncFor(self, node):
        """
        Replaces `async for` with `for`
        """
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
        """
        Replaces `async with` with `with`
        """
        return ast.copy_location(
            ast.With(
                [self.visit(item) for item in node.items],
                [self.visit(stmt) for stmt in node.body],
            ),
            node,
        )

    def visit_AsyncFunctionDef(self, node):
        """
        Replaces `async def` with `def`
        """
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
        """
        Replaces `async for` with `for` in list comprehensions
        """
        for generator in node.generators:
            generator.is_async = False
        return self.generic_visit(node)


class CrossSyncMethodDecoratorHandler(ast.NodeTransformer):
    """
    Visits each method in a class, and handles any CrossSync decorators found
    """

    def visit_FunctionDef(self, node):
        return self.visit_AsyncFunctionDef(node)

    def visit_AsyncFunctionDef(self, node):
        try:
            if hasattr(node, "decorator_list"):
                found_list, node.decorator_list = node.decorator_list, []
                for decorator in found_list:
                    if decorator == CrossSync.convert:
                        # convert async to sync
                        kwargs = CrossSync.convert.parse_ast_keywords(decorator)
                        node = AsyncToSync().visit(node)
                        # replace method name if specified
                        if kwargs["sync_name"] is not None:
                            node.name = kwargs["sync_name"]
                        # replace symbols if specified
                        if kwargs["replace_symbols"]:
                            node = SymbolReplacer(kwargs["replace_symbols"]).visit(node)
                    elif decorator == CrossSync.drop_method:
                        # drop method entirely from class
                        return None
                    elif decorator == CrossSync.pytest:
                        # also convert pytest methods to sync
                        node = AsyncToSync().visit(node)
                    elif decorator == CrossSync.pytest_fixture:
                        # add pytest.fixture decorator
                        decorator.func.value = ast.Name(id="pytest", ctx=ast.Load())
                        decorator.func.attr = "fixture"
                        node.decorator_list.append(decorator)
                    else:
                        # keep unknown decorators
                        node.decorator_list.append(decorator)
            return node
        except ValueError as e:
            raise ValueError(f"node {node.name} failed") from e


class CrossSyncClassDecoratorHandler(ast.NodeTransformer):
    """
    Visits each class in the file, and if it has a CrossSync decorator, it will be transformed.

    Uses CrossSyncMethodDecoratorHandler to visit and (potentially) convert each method in the class
    """
    def __init__(self, file_path):
        self.in_path = file_path
        self._artifact_dict: dict[str, CrossSyncFileArtifact] = {}
        self.imports: list[ast.Import | ast.ImportFrom | ast.Try | ast.If] = []
        self.cross_sync_symbol_transformer = SymbolReplacer(
            {"CrossSync": "CrossSync._Sync_Impl"}
        )
        self.cross_sync_method_handler = CrossSyncMethodDecoratorHandler()

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
        try:
            for decorator in node.decorator_list:
                if decorator == CrossSync.export_sync:
                    kwargs = CrossSync.export_sync.parse_ast_keywords(decorator)
                    # find the path to write the sync class to
                    sync_path = kwargs["path"]
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
                            for s in kwargs["mypy_ignore"]
                            if s not in output_artifact.mypy_ignore
                        ]
                        output_artifact.mypy_ignore.extend(mypy_ignores)
                        # handle file-level imports
                        if not output_artifact.imports and kwargs["include_file_imports"]:
                            output_artifact.imports = self.imports
                    self._artifact_dict[out_file] = output_artifact
            return node
        except ValueError as e:
            raise ValueError(f"failed for class: {node.name}") from e

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
        cls_ast = self.cross_sync_symbol_transformer.visit(cls_ast)
        if replace_symbols:
            cls_ast = SymbolReplacer(replace_symbols).visit(cls_ast)
        cls_ast = self.cross_sync_method_handler.visit(cls_ast)
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
                imports.append(self.cross_sync_symbol_transformer.visit(node))
        return imports


