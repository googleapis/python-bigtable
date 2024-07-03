import ast

class SymbolReplacer(ast.NodeTransformer):

    def __init__(self, replacements):
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
                if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute) and isinstance(decorator.func.value, ast.Name) and "CrossSync" in decorator.func.value.id:
                    decorator_type = decorator.func.attr
                    if decorator_type == "convert":
                        for subcommand in decorator.keywords:
                            if subcommand.arg == "sync_name":
                                node.name = subcommand.value.s
                            if subcommand.arg == "replace_symbols":
                                replacements = {subcommand.value.keys[i].s: subcommand.value.values[i].s for i in range(len(subcommand.value.keys))}
                                node = SymbolReplacer(replacements).visit(node)
                else:
                    # add non-crosssync decorators back
                    node.decorator_list.append(decorator)
        return node


