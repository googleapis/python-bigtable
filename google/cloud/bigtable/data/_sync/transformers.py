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

class HandleCrossSyncDecorators(ast.NodeTransformer):

    def visit_FunctionDef(self, node):
        if hasattr(node, "decorator_list"):
            revised_list = [d for d in node.decorator_list if "CrossSync" not in ast.dump(d)]
            node.decorator_list = revised_list
        return node


