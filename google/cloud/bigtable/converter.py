from __future__ import annotations

import inspect
import ast
import copy
import textwrap
import time

asynciomap = {
    # asyncio function to (additional globals, replacement source) tuples
    "sleep": ({"time": time}, "time.sleep"),
    "InvalidStateError": ({ValueError: ValueError}, "ValueError"),
}

name_map = {
    "__anext__": "__next__",
    "AsyncIterator": "Iterator",
}


class AsyncToSync(ast.NodeTransformer):
    def __init__(self):
        self.globals = {}

    def update_docstring(self, orig_docstring):
        orig_words = orig_docstring.split()
        new_words = [name_map.get(word, word) for word in orig_words]
        return " ".join(new_words)

    def visit_AsyncFunctionDef(self, node):
        docstring = self.update_docstring(ast.get_docstring(node))
        node.body[0].value.s = docstring
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


def transform_sync(f):
    filename = inspect.getfile(f)
    lines, lineno = inspect.getsourcelines(f)
    ast_tree = ast.parse(textwrap.dedent(''.join(lines)), filename)
    ast.increment_lineno(ast_tree, lineno - 1)

    transformer = AsyncToSync()
    transformer.visit(ast_tree)
    str_tree = ast.unparse(ast_tree)
    breakpoint()
    print(str_tree)
    # tranformed_globals = {**f.__globals__, **transformer.globals}
    # exec(compile(ast_tree, filename, 'exec'), tranformed_globals)
    # return tranformed_globals[f.__name__]

if __name__ == "__main__":
    from google.cloud.bigtable._read_rows import _ReadRowsOperation
    obj = transform_sync(_ReadRowsOperation.__anext__)

    # transformable = [_ReadRowsOperation]
    # for cls in transformable:
    #     for name, f in inspect.getmembers(cls, inspect.isfunction):
    #         setattr(cls, name, transform_sync(f))

    # sync_ = transform_sync(foo)
    # foo()
