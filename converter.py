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

from black import format_str, FileMode
import autoflake

asynciomap = {
    # asyncio function to (additional globals, replacement source) tuples
    "sleep": ({"time": time}, "time.sleep"),
    "Queue": ({"queue": queue}, "queue.Queue"),
    "Condition": ({"threading": threading}, "threading.Condition"),
}

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
}

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
}

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

# This file is automatically generated by converter.py. Do not edit.
"""


class AsyncToSync(ast.NodeTransformer):
    def __init__(self, *, import_replacements=None, asyncio_replacements=None, name_replacements=None, drop_methods=None, pass_methods=None, error_methods=None):
        self.globals = {}
        self.import_replacements = import_replacements or {}
        self.asyncio_replacements = asyncio_replacements or {}
        self.name_replacements = name_replacements or {}
        self.drop_methods = drop_methods or []
        self.pass_methods = pass_methods or []
        self.error_methods = error_methods or []

    def update_docstring(self, docstring):
        if not docstring:
            return docstring
        for key_word, replacement in self.name_replacements.items():
            docstring = docstring.replace(f" {key_word} ", f" {replacement} ")
        if "\n" in docstring:
            # if multiline docstring, add linebreaks to put the """ on a separate line
            docstring = "\n" + docstring + "\n\n"
        return docstring

    def visit_FunctionDef(self, node):
        return self.visit_AsyncFunctionDef(node)

    def visit_AsyncFunctionDef(self, node):
        docstring = self.update_docstring(ast.get_docstring(node))
        if isinstance(node.body[0], ast.Expr) and isinstance(
            node.body[0].value, ast.Str
        ):
            node.body[0].value.s = docstring
        # replace any references to Async objects, even in sync functions
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


def transform_sync(in_obj, new_class_name=None, **kwargs):
    filename = inspect.getfile(in_obj)
    lines, lineno = inspect.getsourcelines(in_obj)
    ast_tree = ast.parse(textwrap.dedent("".join(lines)), filename)
    # update name
    old_name = ast_tree.body[0].name
    if new_class_name is None:
        underscore_str = "_" if not old_name.startswith("_") else ""
        new_class_name = f"{underscore_str}{old_name}_SyncAutoGenerated"
    ast_tree.body[0].name = new_class_name
    ast.increment_lineno(ast_tree, lineno - 1)
    # transform
    transformer = AsyncToSync(**kwargs)
    transformer.visit(ast_tree)
    # find imports
    imports = transformer.get_imports(filename)
    # add globals
    for g in transformer.globals:
        imports.add(ast.parse(f"import {g}").body[0])
    # add locals from file, in case they are needed
    file_basename = os.path.splitext(os.path.basename(filename))[0]
    with open(filename, "r") as f:
        for node in ast.walk(ast.parse(f.read(), filename)):
            if isinstance(node, ast.ClassDef):
                imports.add(
                    ast.parse(
                        f"from google.cloud.bigtable.{file_basename} import {node.name}"
                    ).body[0]
                )
    return ast_tree, imports


if __name__ == "__main__":
    from google.cloud.bigtable._read_rows import _ReadRowsOperation
    from google.cloud.bigtable.client import Table
    from google.cloud.bigtable.client import BigtableDataClient
    from google.cloud.bigtable.iterators import ReadRowsIterator
    from google.cloud.bigtable._mutate_rows import _MutateRowsOperation
    from google.cloud.bigtable.mutations_batcher import _FlowControl
    from google.cloud.bigtable.mutations_batcher import MutationsBatcher

    tree, imports = None, set()
    # conversion_list = [_FlowControl, MutationsBatcher, _MutateRowsOperation]
    conversion_list = [_ReadRowsOperation, Table, BigtableDataClient, ReadRowsIterator]

    # register new sync versions
    for cls in conversion_list:
        name_map[cls.__name__] = f"{cls.__name__}_Sync"
        import_map[(cls.__module__, cls.__name__)] = (
            "google.cloud.bigtable.sync",
            f"{cls.__name__}_Sync",
        )
        imports.add(ast.parse(f"from google.cloud.bigtable.sync import {cls.__name__}_Sync").body[0])
    for cls in conversion_list:
        new_tree, new_imports = transform_sync(cls, name_replacements=name_map, asyncio_replacements=asynciomap, import_replacements=import_map,
            pass_methods=["mutations_batcher", "_manage_channel", "_register_instance", "__init__transport__", "start_background_channel_refresh", "_start_idle_timer"],
            drop_methods=["_buffer_to_generator", "_generator_to_buffer", "_idle_timeout_coroutine"])
        if tree is None:
            tree = new_tree
        else:
            tree.body.extend(new_tree.body)
        imports.update(new_imports)
    # add imports
    import_unique = list(set([ast.unparse(i) for i in imports]))
    import_unique.sort()
    google, non_google = [], []
    for i in import_unique:
        if "google" in i:
            google.append(i)
        else:
            non_google.append(i)
    import_str = "\n".join(non_google + [""] + google)
    # append clean tree
    full_code = f"{header}\n\n{import_str}\n\n{ast.unparse(tree)}"
    full_code = autoflake.fix_code(full_code, remove_all_unused_imports=True)
    formatted_code = format_str(full_code, mode=FileMode())
    # write to disk
    with open("./google/cloud/bigtable/_sync_autogen.py", "w") as f:
        f.write(formatted_code)
