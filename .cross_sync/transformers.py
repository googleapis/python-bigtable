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
from google.cloud.bigtable.data._sync.cross_sync_decorators import AstDecorator, ExportSyncDecorator
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
                        node = handler.sync_ast_transform(decorator, node, globals())
                        if node is None:
                            return None
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
        """
        try:
            for decorator in node.decorator_list:
                try:
                    handler = AstDecorator.get_for_node(decorator)
                    if handler == ExportSyncDecorator:
                        kwargs = CrossSync.export_sync.parse_ast_keywords(decorator)
                        # find the path to write the sync class to
                        sync_path = kwargs["path"]
                        out_file = "/".join(sync_path.rsplit(".")[:-1]) + ".py"
                        sync_cls_name = sync_path.rsplit(".", 1)[-1]
                        # find the artifact file for the save location
                        output_artifact = self._artifact_dict.get(
                            out_file, CrossSyncOutputFile(out_file)
                        )
                        # write converted class details if not already present
                        if sync_cls_name not in output_artifact.contained_classes:
                            converted = self._transform_class(node, sync_cls_name, **kwargs)
                            output_artifact.converted_classes.append(converted)
                            # add mapping decorator if specified
                            mapping_name = kwargs.get("add_mapping_for_name")
                            if mapping_name:
                                mapping_decorator = ast.Call(
                                    func=ast.Attribute(value=ast.Name(id='CrossSync._Sync_Impl', ctx=ast.Load()), attr='add_mapping_decorator', ctx=ast.Load()),
                                    args=[ast.Str(s=mapping_name)], keywords=[]
                                )
                                converted.decorator_list.append(mapping_decorator)
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
                except ValueError:
                    continue
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
        Transform async class into sync one, by applying the following ast transformations:
        - SymbolReplacer: to replace any class-level symbols specified in CrossSync.export_sync(replace_symbols={}) decorator
        - CrossSyncMethodDecoratorHandler: to visit each method in the class and apply any CrossSync decorators found
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

        raw imports, as well as try and if statements at the top level are included
        """
        imports = []
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.Try, ast.If)):
                imports.append(self.cross_sync_symbol_transformer.visit(node))
        return imports


