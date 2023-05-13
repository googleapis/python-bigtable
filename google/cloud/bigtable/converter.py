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

from black import format_str, FileMode

asynciomap = {
    # asyncio function to (additional globals, replacement source) tuples
    "sleep": ({"time": time}, "time.sleep"),
    "InvalidStateError": ({ValueError: ValueError}, "ValueError"),
    "Queue": ({"queue": queue}, "queue.Queue"),
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
}

import_map = {
    ("google.api_core", "retry_async"): ("google.api_core", "retry"),
    ("google.cloud.bigtable_v2.services.bigtable.async_client", "BigtableAsyncClient"): ("google.cloud.bigtable_v2.services.bigtable.client", "BigtableClient"),
    ("typing", "AsyncIterable"): ("typing", "Iterable"),
    ("typing", "AsyncIterator"): ("typing", "Iterator"),
    ("typing", "AsyncGenerator"): ("typing", "Generator"),
    ("google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio", "PooledBigtableGrpcAsyncIOTransport"): ("google.cloud.bigtable_v2.services.bigtable.transports.grpc", "BigtableGrpcTransport"),
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
    def __init__(self):
        self.globals = {}

    def update_docstring(self, orig_docstring):
        if not orig_docstring:
            return orig_docstring
        orig_words = orig_docstring.split()
        new_words = [name_map.get(word, word) for word in orig_words]
        return " ".join(new_words)

    def visit_FunctionDef(self, node):
        # replace any references to Async objects, even in sync functions
        return self.visit_AsyncFunctionDef(node)


    def visit_AsyncFunctionDef(self, node):
        docstring = self.update_docstring(ast.get_docstring(node))
        if isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Str):
            node.body[0].value.s = docstring
        # check if the function contains non-replaced usage of asyncio
        func_ast = ast.parse(ast.unparse(node))
        has_asyncio_calls = any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name) and n.func.value.id == 'asyncio' and n.func.attr not in asynciomap for n in ast.walk(func_ast))
        if has_asyncio_calls:
            # replace function body with NotImplementedError
            exc_node = ast.Call(
                func=ast.Name(id='NotImplementedError', ctx=ast.Load()),
                args=[ast.Str(s='Corresponding Async Function contains unhandled asyncio calls')],
                keywords=[]
            )
            raise_node = ast.Raise(
                exc=exc_node,
                cause=None
            )
            node.body = [raise_node]
        return ast.copy_location(
            ast.FunctionDef(
                name_map.get(node.name, node.name),
                self.visit(node.args),
                [self.visit(stmt) for stmt in node.body],
                [self.visit(stmt) for stmt in node.decorator_list],
                node.returns and self.visit(node.returns),
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
            and node.attr in asynciomap
        ):
            g, replacement = asynciomap[node.attr]
            self.globals.update(g)
            return ast.copy_location(
                ast.parse(replacement, mode="eval").body,
                node
            )
        elif isinstance(node, ast.Attribute) and node.attr in name_map:
            new_node = ast.copy_location(ast.Attribute(node.value, name_map[node.attr], node.ctx), node)
            return new_node
        return node

    def visit_Name(self, node):
        node.id = name_map.get(node.id, node.id)
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

def get_imports(filename):
    imports = []
    with open(filename, "r") as f:
        full_tree = ast.parse(f.read(), filename)
        for node in ast.walk(full_tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    if isinstance(node, ast.Import):
                        # import statments
                        alias.name = import_map.get((alias.name), (alias.name))
                    else:
                        # import from statements
                        node.module, alias.name = import_map.get((node.module, alias.name), (node.module, alias.name))
                imports.append(node)
    return [ast.unparse(node) for node in imports]

def transform_sync(in_obj):
    filename = inspect.getfile(in_obj)
    lines, lineno = inspect.getsourcelines(in_obj)
    ast_tree = ast.parse(textwrap.dedent(''.join(lines)), filename)
    # update name
    old_name = ast_tree.body[0].name
    ast_tree.body[0].name = f"{old_name}_SyncAutoGenerated"
    ast.increment_lineno(ast_tree, lineno - 1)
    # add async version as subclass
    # ast_tree.body[0].bases.append(ast.Name(id=old_name, ctx=ast.Load()))
    # find imports
    imports = get_imports(filename)
    # add async base class
    file_basename = os.path.splitext(os.path.basename(filename))[0]
    imports.append(f"from google.cloud.bigtable.{file_basename} import {old_name}")

    transformer = AsyncToSync()
    transformer.visit(ast_tree)
    return ast_tree, imports
    # str_tree = ast.unparse(ast_tree)
    # if save_path:
    #     with open(save_path, 'w') as f:
    #         f.write(header)
    #         f.write('\n\n')
    #         f.write('\n'.join(imports))
    #         f.write('\n\n\n')
    #         f.write(str_tree)
    # tranformed_globals = {**f.__globals__, **transformer.globals}
    # exec(compile(ast_tree, filename, 'exec'), tranformed_globals)
    # return tranformed_globals[f.__name__]

if __name__ == "__main__":
    from google.cloud.bigtable._read_rows import _ReadRowsOperation
    from google.cloud.bigtable.client import Table
    from google.cloud.bigtable.client import BigtableDataClient
    tree, imports = None, []
    # register new sync versions
    for cls in [_ReadRowsOperation, Table, BigtableDataClient]:
        name_map[cls.__name__] = f"{cls.__name__}_SyncAutoGenerated"
        import_map[(cls.__module__, cls.__name__)] = (".", f"{cls.__name__}_SyncAutoGenerated")
    for cls in [Table, BigtableDataClient, _ReadRowsOperation]:
        new_tree, new_imports = transform_sync(cls)
        if tree is None:
            tree = new_tree
        else:
            tree.body.extend(new_tree.body)
        imports.extend(new_imports)
    # write to disk
    formatted_tree = format_str(ast.unparse(tree), mode=FileMode())
    with open('output.py', 'w') as f:
        f.write(header)
        f.write('\n\n')
        f.write('\n'.join(imports))
        f.write('\n\n\n')
        f.write(formatted_tree)
