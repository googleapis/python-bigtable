# Copyright 2023 Google LLC
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
#

from __future__ import annotations

import inspect
import ast
import textwrap
import time
import queue
import os
import threading
import concurrent.futures

from black import format_str, FileMode
import autoflake
"""
This module allows us to generate a synchronous API surface from our asyncio surface.
"""

# This map defines replacements for asyncio API calls
asynciomap = {
    "sleep": ({"time": time}, "time.sleep"),
    "Queue": ({"queue": queue}, "queue.Queue"),
    "Condition": ({"threading": threading}, "threading.Condition"),
    "Future": ({"concurrent.futures": concurrent.futures}, "concurrent.futures.Future"),
}

# This map defines find/replace pairs for the generated code and docstrings
# replace async calls with corresponding sync ones
name_map = {
    "__anext__": "__next__",
    "__aiter__": "__iter__",
    "__aenter__": "__enter__",
    "__aexit__": "__exit__",
    "aclose": "close",
    "AsyncIterable": "Iterable",
    "AsyncIterator": "Iterator",
    "AsyncGenerator": "Generator",
    "StopAsyncIteration": "StopIteration",
    "BigtableAsyncClient": "BigtableClient",
    "AsyncRetry": "Retry",
    "PooledBigtableGrpcAsyncIOTransport": "BigtableGrpcTransport",
    "Awaitable": None,
    "pytest_asyncio": "pytest",
    "AsyncMock": "mock.Mock",
    "_ReadRowsOperation": "_ReadRowsOperation_Sync",
    "Table": "Table_Sync",
    "BigtableDataClient": "BigtableDataClient_Sync",
    "ReadRowsIterator": "ReadRowsIterator_Sync",
    "_MutateRowsOperation": "_MutateRowsOperation_Sync",
    "MutationsBatcher": "MutationsBatcher_Sync",
    "_FlowControl": "_FlowControl_Sync",
}

# This maps classes to the final sync surface location, so they can be instantiated in generated code
concrete_class_map = {
    "_ReadRowsOperation": "google.cloud.bigtable._sync._concrete._ReadRowsOperation_Sync_Concrete",
    "Table": "google.cloud.bigtable._sync._concrete.Table_Sync_Concrete",
    "BigtableDataClient": "google.cloud.bigtable._sync._concrete.BigtableDataClient_Sync_Concrete",
    "ReadRowsIterator": "google.cloud.bigtable._sync._concrete.ReadRowsIterator_Sync_Concrete",
    "_MutateRowsOperation": "google.cloud.bigtable._sync._concrete._MutateRowsOperation_Sync_Concrete",
    "MutationsBatcher": "google.cloud.bigtable._sync._concrete.MutationsBatcher_Threaded",
    "_FlowControl": "google.cloud.bigtable._sync._concrete._FlowControl_Sync_Concrete",
}

# This map defines import replacements for the generated code
# Note that "import ... as" statements keep the same name
import_map = {
    ("google.api_core", "retry_async"): ("google.api_core", "retry"),
    (
        "google.cloud.bigtable_v2.services.bigtable.async_client",
        "BigtableAsyncClient",
    ): ("google.cloud.bigtable_v2.services.bigtable.client", "BigtableClient"),
    ("typing", "AsyncIterable"): ("typing", "Iterable"),
    ("typing", "AsyncIterator"): ("typing", "Iterator"),
    ("typing", "AsyncGenerator"): ("typing", "Generator"),
    (
        "google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio",
        "PooledBigtableGrpcAsyncIOTransport",
    ): (
        "google.cloud.bigtable_v2.services.bigtable.transports.grpc",
        "BigtableGrpcTransport",
    ),
    ("grpc.aio", "Channel"): ("grpc", "Channel"),
}

# methods that are replaced with an empty implementation in sync surface
pass_methods=["__init__async__", "_prepare_stream", "_manage_channel", "_register_instance", "__init__transport__", "start_background_channel_refresh", "_start_idle_timer"]
# methods that are dropped from sync surface
drop_methods=["_buffer_to_generator", "_generator_to_buffer", "_idle_timeout_coroutine"]
# methods that raise a NotImplementedError in sync surface
error_methods=[]

class AsyncToSyncTransformer(ast.NodeTransformer):
    """
    This class is used to transform async classes into sync classes.

    Generated classes are abstract, and must be subclassed to be used.
    This is to ensure any required customizations from 
    outside of this autogeneration system are always applied
    """

    def __init__(self, *, import_replacements=None, asyncio_replacements=None, name_replacements=None, concrete_class_map=None, drop_methods=None, pass_methods=None, error_methods=None):
        """
        Args:
          - import_replacements: dict of (module, name) to (module, name) replacement import statements
                For example, {("foo", "bar"): ("baz", "qux")} will replace "from foo import bar" with "from baz import qux"
          - asyncio_replacements: dict of asyncio function names to the module/function name to replace them with
          - name_replacements: dict of names to replace directly in the source code and docstrings
          - concrete_class_map: dict of the concrete class names for all autogenerated classes 
                in this module, so that concrete versions of each class can be instantiated
          - drop_methods: list of method names to drop from the class
          - pass_methods: list of method names to replace with "pass" in the class
          - error_methods: list of method names to replace with "raise NotImplementedError" in the class
        """
        self.globals = {}
        self.import_replacements = import_replacements or {}
        self.asyncio_replacements = asyncio_replacements or {}
        self.name_replacements = name_replacements or {}
        self.concrete_class_map = concrete_class_map or {}
        self.drop_methods = drop_methods or []
        self.pass_methods = pass_methods or []
        self.error_methods = error_methods or []

    def update_docstring(self, docstring):
        """
        Update docstring toreplace any key words in the name_replacements dict
        """
        if not docstring:
            return docstring
        for key_word, replacement in self.name_replacements.items():
            docstring = docstring.replace(f" {key_word} ", f" {replacement} ")
        if "\n" in docstring:
            # if multiline docstring, add linebreaks to put the """ on a separate line
            docstring = "\n" + docstring + "\n\n"
        return docstring

    def visit_FunctionDef(self, node):
        """
        Re-use replacement logic for Async functions
        """
        return self.visit_AsyncFunctionDef(node)

    def visit_AsyncFunctionDef(self, node):
        """
        Replace async functions with sync functions
        """
        # replace docstring
        docstring = self.update_docstring(ast.get_docstring(node))
        if isinstance(node.body[0], ast.Expr) and isinstance(
            node.body[0].value, ast.Str
        ):
            node.body[0].value.s = docstring
        # drop or replace body as needed
        if node.name in self.drop_methods:
            return None
        elif node.name in self.pass_methods:
            # replace with pass
            node.body = [ast.Expr(value=ast.Str(s="Implementation purposely removed in sync mode"))]
        elif node.name in self.error_methods:
            self._create_error_node(node, "Function marked as unsupported in sync mode")
        else:
            # check if the function contains non-replaced usage of asyncio
            func_ast = ast.parse(ast.unparse(node))
            has_asyncio_calls = any(
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Attribute)
                and isinstance(n.func.value, ast.Name)
                and n.func.value.id == "asyncio"
                and n.func.attr not in self.asyncio_replacements
                for n in ast.walk(func_ast)
            )
            if has_asyncio_calls:
                self._create_error_node(
                    node,
                    "Corresponding Async Function contains unhandled asyncio calls",
                )
        # remove pytest.mark.asyncio decorator
        if hasattr(node, "decorator_list"):
            is_asyncio_decorator = lambda d: all(x in ast.dump(d) for x in ["pytest", "mark", "asyncio"])
            node.decorator_list = [
                d for d in node.decorator_list if not is_asyncio_decorator(d)
            ]
        return ast.copy_location(
            ast.FunctionDef(
                self.name_replacements.get(node.name, node.name),
                self.visit(node.args),
                [self.visit(stmt) for stmt in node.body],
                [self.visit(stmt) for stmt in node.decorator_list],
                node.returns and self.visit(node.returns),
            ),
            node,
        )

    def visit_Call(self, node):
        # name replacement for class method calls
        if isinstance(node.func, ast.Attribute) and isinstance(
            node.func.value, ast.Name
        ):
            node.func.value.id = self.name_replacements.get(node.func.value.id, node.func.value.id)
        # when initializing an auto-generated sync class, replace the class name with the patched version
        if isinstance(node.func, ast.Name) and node.func.id in self.concrete_class_map:
            node.func.id = self.concrete_class_map[node.func.id]
        return ast.copy_location(
            ast.Call(
                self.visit(node.func),
                [self.visit(arg) for arg in node.args],
                [self.visit(keyword) for keyword in node.keywords],
            ),
            node,
        )

    def visit_Await(self, node):
        return self.visit(node.value)

    def visit_Attribute(self, node):
        if (
            isinstance(node.value, ast.Name)
            and isinstance(node.value.ctx, ast.Load)
            and node.value.id == "asyncio"
            and node.attr in self.asyncio_replacements
        ):
            g, replacement = self.asyncio_replacements[node.attr]
            self.globals.update(g)
            return ast.copy_location(ast.parse(replacement, mode="eval").body, node)
        elif isinstance(node, ast.Attribute) and node.attr in self.name_replacements:
            new_node = ast.copy_location(
                ast.Attribute(node.value, self.name_replacements[node.attr], node.ctx), node
            )
            return new_node
        return node

    def visit_Name(self, node):
        node.id = self.name_replacements.get(node.id, node.id)
        return node

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

    def visit_ListComp(self, node):
        # replace [x async for ...] with [x for ...]
        new_generators = []
        for generator in node.generators:
            if generator.is_async:
                new_generators.append(
                    ast.copy_location(
                        ast.comprehension(
                            self.visit(generator.target),
                            self.visit(generator.iter),
                            [self.visit(i) for i in generator.ifs],
                            False,
                        ),
                        generator,
                    )
                )
            else:
                new_generators.append(generator)
        node.generators = new_generators
        return ast.copy_location(
            ast.ListComp(
                self.visit(node.elt),
                [self.visit(gen) for gen in node.generators],
            ),
            node,
        )

    def visit_Subscript(self, node):
        if (
            hasattr(node, "value")
            and isinstance(node.value, ast.Name)
            and node.value.id == "AsyncGenerator"
            and self.name_replacements.get(node.value.id, "") == "Generator"
        ):
            # Generator has different argument signature than AsyncGenerator
            return ast.copy_location(
                ast.Subscript(
                    ast.Name("Generator"),
                    ast.Index(
                        ast.Tuple(
                            [
                                self.visit(i)
                                for i in node.slice.elts + [ast.Constant("Any")]
                            ]
                        )
                    ),
                    node.ctx,
                ),
                node,
            )
        elif (
            hasattr(node, "value")
            and isinstance(node.value, ast.Name)
            and self.name_replacements.get(node.value.id, False) is None
        ):
            # needed for Awaitable
            return self.visit(node.slice)
        return ast.copy_location(
            ast.Subscript(
                self.visit(node.value),
                self.visit(node.slice),
                node.ctx,
            ),
            node,
        )

    @staticmethod
    def _create_error_node(node, error_msg):
        # replace function body with NotImplementedError
        exc_node = ast.Call(
            func=ast.Name(id="NotImplementedError", ctx=ast.Load()),
            args=[ast.Str(s=error_msg)],
            keywords=[],
        )
        raise_node = ast.Raise(exc=exc_node, cause=None)
        node.body = [raise_node]


    def get_imports(self, filename):
        """
        Get the imports from a file, and do a find-and-replace against import_replacements
        """
        imports = set()
        with open(filename, "r") as f:
            full_tree = ast.parse(f.read(), filename)
            for node in ast.walk(full_tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    for alias in node.names:
                        if isinstance(node, ast.Import):
                            # import statments
                            new_import = self.import_replacements.get((alias.name), (alias.name))
                            imports.add(ast.parse(f"import {new_import}").body[0])
                        else:
                            # import from statements
                            # break into individual components
                            module, name = self.import_replacements.get(
                                (node.module, alias.name), (node.module, alias.name)
                            )
                            # don't import from same file
                            if module == ".":
                                continue
                            asname_str = f" as {alias.asname}" if alias.asname else ""
                            imports.add(
                                ast.parse(f"from {module} import {name}{asname_str}").body[
                                    0
                                ]
                            )
        return imports


def transform_sync(class_list:list[Type], new_name_format="{}_Sync", add_imports=None, **kwargs):
    combined_tree = ast.parse("")
    combined_imports = set()
    for in_obj in class_list:
        filename = inspect.getfile(in_obj)
        lines, lineno = inspect.getsourcelines(in_obj)
        ast_tree = ast.parse(textwrap.dedent("".join(lines)), filename)
        if ast_tree.body and isinstance(ast_tree.body[0], ast.ClassDef):
            # update name
            old_name = ast_tree.body[0].name
            new_name = new_name_format.format(old_name)
            ast_tree.body[0].name = new_name
            ast.increment_lineno(ast_tree, lineno - 1)
            # add ABC as base class
            ast_tree.body[0].bases = ast_tree.body[0].bases + [
                ast.Name("ABC", ast.Load()),
            ]
        # remove top-level imports if any. Add them back later
        ast_tree.body = [n for n in ast_tree.body if not isinstance(n, (ast.Import, ast.ImportFrom))]
        # transform
        transformer = AsyncToSyncTransformer(**kwargs)
        transformer.visit(ast_tree)
        # find imports
        imports = transformer.get_imports(filename)
        imports.add(ast.parse("from abc import ABC").body[0])
        # add globals
        for g in transformer.globals:
            imports.add(ast.parse(f"import {g}").body[0])
        # add locals from file, in case they are needed
        if ast_tree.body and isinstance(ast_tree.body[0], ast.ClassDef):
            file_basename = os.path.splitext(os.path.basename(filename))[0]
            with open(filename, "r") as f:
                for node in ast.walk(ast.parse(f.read(), filename)):
                    if isinstance(node, ast.ClassDef):
                        imports.add(
                            ast.parse(
                                f"from google.cloud.bigtable.{file_basename} import {node.name}"
                            ).body[0]
                        )
        # update combined data
        combined_tree.body.extend(ast_tree.body)
        combined_imports.update(imports)
    # add extra imports
    if add_imports:
        for import_str in add_imports:
            combined_imports.add(ast.parse(import_str).body[0])
    # render tree as string of code
    import_unique = list(set([ast.unparse(i) for i in combined_imports]))
    import_unique.sort()
    google, non_google = [], []
    for i in import_unique:
        if "google" in i:
            google.append(i)
        else:
            non_google.append(i)
    import_str = "\n".join(non_google + [""] + google)
    # append clean tree
    header = """# Copyright 2023 Google LLC
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
    #

    # This file is automatically generated by sync_surface_generator.py. Do not edit.
    """
    full_code = f"{header}\n\n{import_str}\n\n{ast.unparse(combined_tree)}"
    full_code = autoflake.fix_code(full_code, remove_all_unused_imports=True)
    formatted_code = format_str(full_code, mode=FileMode())
    return formatted_code


def generate_full_surface(save_path=None):
    """
    Generate a sync surface from all async classes
    """
    from google.cloud.bigtable._read_rows import _ReadRowsOperation
    from google.cloud.bigtable._mutate_rows import _MutateRowsOperation
    from google.cloud.bigtable.client import Table
    from google.cloud.bigtable.client import BigtableDataClient
    from google.cloud.bigtable.iterators import ReadRowsIterator
    from google.cloud.bigtable.mutations_batcher import MutationsBatcher
    from google.cloud.bigtable.mutations_batcher import _FlowControl

    conversion_list = [_ReadRowsOperation, Table, BigtableDataClient, ReadRowsIterator, _MutateRowsOperation, MutationsBatcher, _FlowControl]
    code = transform_sync(conversion_list,
        concrete_class_map=concrete_class_map, name_replacements=name_map, asyncio_replacements=asynciomap, import_replacements=import_map,
        pass_methods=pass_methods, drop_methods=drop_methods, error_methods=error_methods,
        add_imports=["import google.cloud.bigtable.exceptions as bt_exceptions"],
    )
    if save_path is not None:
        with open(save_path, "w") as f:
            f.write(code)
    return code

def generate_system_tests(save_path=None):
    from tests.system import test_system
    conversion_list = [test_system]
    code = transform_sync(conversion_list,
        concrete_class_map=concrete_class_map, name_replacements=name_map, asyncio_replacements=asynciomap, import_replacements=import_map,
        drop_methods=["test_read_rows_stream_inactive_timer"],
        add_imports=["import google.cloud.bigtable"]
    )
    if save_path is not None:
        with open(save_path, "w") as f:
            f.write(code)
    return code

def generate_unit_tests(test_path="./tests/unit", save_path=None):
    """
    Unit tests should typically not be generated using this script.
    But this is useful to generate a starting point.
    """
    import importlib
    if save_path is None:
        save_path = os.path.join(test_path, "sync")
    updated_list = []
    # find files in test_path
    conversion_list = [f for f in os.listdir(test_path) if f.endswith(".py")]
    # attempt tp convert each file
    for f in conversion_list:
        old_code = open(os.path.join(test_path, f), "r").read()
        obj = importlib.import_module(f"tests.unit.{f[:-3]}")
        new_code = transform_sync([obj],
            concrete_class_map=concrete_class_map, name_replacements=name_map, asyncio_replacements=asynciomap, import_replacements=import_map,
            add_imports=["import google.cloud.bigtable"]
        )
        # only save files with async code
        if any([a in new_code for a in asynciomap]) or "async def" in old_code:
            with open(os.path.join(save_path, f), "w") as out:
                out.write(new_code)
            updated_list.append(f)
    print(f"Updated {len(updated_list)} files: {updated_list}")
    return updated_list

if __name__ == "__main__":
    generate_full_surface(save_path="./google/cloud/bigtable/_sync/_autogen.py")
    generate_system_tests("./tests/system/test_system_sync_autogen.py")
