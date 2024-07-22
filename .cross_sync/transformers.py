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
"""
Provides a set of ast.NodeTransformer subclasses that are composed to generate
async code into sync code.

At a high level:
- The main entrypoint is CrossSyncClassDecoratorHandler, which is used to find classes
annotated with @CrossSync.export_sync.
- SymbolReplacer is used to swap out CrossSync.X with CrossSync._Sync_Impl.X
- RmAioFunctions is then called on the class, to strip out asyncio keywords
marked with CrossSync.rm_aio (using AsyncToSync to handle the actual transformation)
- Finally, CrossSyncMethodDecoratorHandler is called to find methods annotated
with AstDecorators, and call decorator.sync_ast_transform on each one to fully transform the class.
"""
from __future__ import annotations

import ast

import sys
# add cross_sync to path
sys.path.append("google/cloud/bigtable/data/_sync/_cross_sync")
from _decorators import AstDecorator, ExportSync
from generate import CrossSyncOutputFile


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

class RmAioFunctions(ast.NodeTransformer):
    """
    Visits all calls marked with CrossSync.rm_aio, and removes asyncio keywords
    """

    def __init__(self):
        self.to_sync = AsyncToSync()

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and \
        node.func.attr == "rm_aio" and "CrossSync" in node.func.value.id:
            return self.visit(self.to_sync.visit(node.args[0]))
        return self.generic_visit(node)

    def visit_AsyncWith(self, node):
        """
        Async with statements are not fully wrapped by calls
        """
        found_rmaio = False
        for item in node.items:
            if isinstance(item.context_expr, ast.Call) and isinstance(item.context_expr.func, ast.Attribute) and isinstance(item.context_expr.func.value, ast.Name) and \
            item.context_expr.func.attr == "rm_aio" and "CrossSync" in item.context_expr.func.value.id:
                found_rmaio = True
                break
        if found_rmaio:
            new_node = ast.copy_location(
                ast.With(
                    [self.generic_visit(item) for item in node.items],
                    [self.generic_visit(stmt) for stmt in node.body],
                ),
                node,
            )
            return self.generic_visit(new_node)
        return self.generic_visit(node)

    def visit_AsyncFor(self, node):
        """
        Async for statements are not fully wrapped by calls
        """
        it = node.iter
        if isinstance(it, ast.Call) and isinstance(it.func, ast.Attribute) and isinstance(it.func.value, ast.Name) and \
        it.func.attr == "rm_aio" and "CrossSync" in it.func.value.id:
            return ast.copy_location(
                ast.For(
                    self.visit(node.target),
                    self.visit(it),
                    [self.visit(stmt) for stmt in node.body],
                    [self.visit(stmt) for stmt in node.orelse],
                ),
                node,
            )
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
                    try:
                        handler = AstDecorator.get_for_node(decorator)
                        node = handler.sync_ast_transform(node, globals())
                        if node is None:
                            return None
                        # recurse to any nested functions
                        node = self.generic_visit(node)
                    except ValueError:
                        # keep unknown decorators
                        node.decorator_list.append(decorator)
                        continue
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
        self._artifact_dict: dict[str, CrossSyncOutputFile] = {}
        self.imports: list[ast.Import | ast.ImportFrom | ast.Try | ast.If] = []
        self.cross_sync_symbol_transformer = SymbolReplacer(
            {"CrossSync": "CrossSync._Sync_Impl"}
        )
        self.cross_sync_method_handler = CrossSyncMethodDecoratorHandler()

    def convert_file(
        self, artifacts: set[CrossSyncOutputFile] | None = None
    ) -> set[CrossSyncOutputFile]:
        """
        Called to run a file through the ast transformer.

        If the file contains any classes marked with CrossSync.export_sync, the
        classes will be processed according to the decorator arguments, and
        a set of CrossSyncOutputFile objects will be returned for each output file.

        If no CrossSync annotations are found, no changes will occur and an
        empty set will be returned
        """
        tree = ast.parse(open(self.in_path).read())
        self._artifact_dict = {f.file_path: f for f in artifacts or []}
        self.imports = self._get_imports(tree)
        self.visit(tree)
        # return set of new artifacts
        return set(self._artifact_dict.values()).difference(artifacts or [])

    def visit_ClassDef(self, node):
        """
        Called for each class in file. If class has a CrossSync decorator, it will be transformed
        according to the decorator arguments. Otherwise, no changes will occur

        Uses a set of CrossSyncOutputFile objects to store the transformed classes
        and avoid duplicate writes
        """
        try:
            for decorator in node.decorator_list:
                try:
                    handler = AstDecorator.get_for_node(decorator)
                    if isinstance(handler, ExportSync):
                        # find the path to write the sync class to
                        out_file = "/".join(handler.path.rsplit(".")[:-1]) + ".py"
                        sync_cls_name = handler.path.rsplit(".", 1)[-1]
                        # find the artifact file for the save location
                        output_artifact = self._artifact_dict.get(
                            out_file, CrossSyncOutputFile(out_file)
                        )
                        # write converted class details if not already present
                        if sync_cls_name not in output_artifact.contained_classes:
                            # transformation is handled in sync_ast_transform method of the decorator
                            converted = handler.sync_ast_transform(node, globals())
                            output_artifact.converted_classes.append(converted)
                            # handle file-level mypy ignores
                            mypy_ignores = [
                                s
                                for s in handler.mypy_ignore
                                if s not in output_artifact.mypy_ignore
                            ]
                            output_artifact.mypy_ignore.extend(mypy_ignores)
                            # handle file-level imports
                            if not output_artifact.imports and handler.include_file_imports:
                                output_artifact.imports = self.imports
                        self._artifact_dict[out_file] = output_artifact
                except ValueError:
                    continue
            return node
        except ValueError as e:
            raise ValueError(f"failed for class: {node.name}") from e

    def _get_imports(
        self, tree: ast.Module
    ) -> list[ast.Import | ast.ImportFrom | ast.Try | ast.If]:
        """
        Grab the imports from the top of the file

        raw imports, as well as try and if statements at the top level are included
        """
        imports = []
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.Try, ast.If)):
                imports.append(self.cross_sync_symbol_transformer.visit(node))
        return imports


