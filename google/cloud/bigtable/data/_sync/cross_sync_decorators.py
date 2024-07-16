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
from typing import Sequence


class AstDecorator:
    """
    Helper class for CrossSync decorators used for guiding ast transformations.

    These decorators provide arguments that are used during the code generation process,
    but act as no-ops when encountered in live code
    """

    @classmethod
    def decorator(cls, *args, **kwargs):
        """
        Called when the decorator is used in code.

        Returns a no-op decorator function, or applies the async_impl decorator
        """
        # check for decorators with no arguments
        func = None
        if len(args) == 1 and callable(args[0]):
            func = args[0]
            args = args[1:]
        # create new instance from given arguments
        new_instance = cls(*args, **kwargs)
        wrapper = new_instance.async_decorator()
        # if we can, return single wrapped function
        if func:
            return wrapper(func)
        # otherwise, return wrap function
        return wrapper

    @classmethod
    def _convert_ast_to_py(cls, ast_node):
        """
        Helper to convert ast primitives to python primitives. Used when unwrapping kwargs
        """
        import ast

        if isinstance(ast_node, ast.Constant):
            return ast_node.value
        if isinstance(ast_node, ast.List):
            return [cls._convert_ast_to_py(node) for node in ast_node.elts]
        if isinstance(ast_node, ast.Dict):
            return {
                cls._convert_ast_to_py(k): cls._convert_ast_to_py(v)
                for k, v in zip(ast_node.keys, ast_node.values)
            }
        raise ValueError(f"Unsupported type {type(ast_node)}")

    def async_decorator(self):
        """
        Decorator to apply the async_impl decorator to the wrapped function

        Default implementation is a no-op
        """
        def decorator(f):
            return f

        return decorator

    def sync_ast_transform(self, decorator, wrapped_node, transformers):
        """
        When this decorator is encountered in the ast during sync generation, 
        apply this behavior

        Defaults to no-op
        """
        return wrapped_node

    @classmethod
    def get_for_node(cls, node):
        import ast
        if "CrossSync" in ast.dump(node):
            decorator_name = node.func.attr if hasattr(node, "func") else node.attr
            got_kwargs = (
                {kw.arg: cls._convert_ast_to_py(kw.value) for kw in node.keywords}
                if hasattr(node, "keywords")
                else {}
            )
            for subclass in cls.__subclasses__():
                if subclass.name == decorator_name:
                    return subclass(**got_kwargs)
        raise ValueError(f"Unknown decorator encountered")


class ExportSync(AstDecorator):

    name = "export_sync"

    def __init__(
        self,
        path:str,  # path to output the generated sync class
        *,
        replace_symbols:dict|None = None,  # replace symbols in the generated sync class
        mypy_ignore:Sequence[str] = (),  # set of mypy errors to ignore
        include_file_imports:bool = True,  # include imports from the file in the generated sync class
        add_mapping_for_name:str|None = None,  # add a new attribute to CrossSync with the given name
    ):
        self.path = path
        self.replace_symbols = replace_symbols
        self.mypy_ignore = mypy_ignore
        self.include_file_imports = include_file_imports
        self.add_mapping_for_name = add_mapping_for_name

    def async_decorator(self):
        from .cross_sync import CrossSync
        new_mapping = self.add_mapping_for_name
        def decorator(cls):
            if new_mapping:
                CrossSync.add_mapping(new_mapping, cls)
            return cls
        return decorator

    def sync_ast_transform(self, decorator, wrapped_node, transformers):
        """
        Transform async class into sync copy
        """
        import ast
        import copy
        if not self.path:
            raise ValueError(f"{wrapped_node.name} has no path specified in export_sync decorator")
        # copy wrapped node
        wrapped_node = copy.deepcopy(wrapped_node)
        # update name
        sync_cls_name = self.path.rsplit(".", 1)[-1]
        orig_name = wrapped_node.name
        wrapped_node.name = sync_cls_name
        # strip CrossSync decorators
        if hasattr(wrapped_node, "decorator_list"):
            wrapped_node.decorator_list = [
                d for d in wrapped_node.decorator_list if "CrossSync" not in ast.dump(d)
            ]
        # add mapping decorator if needed
        if self.add_mapping_for_name:
            wrapped_node.decorator_list.append(
                ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id="CrossSync", ctx=ast.Load()),
                        attr="add_mapping",
                        ctx=ast.Load(),
                    ),
                    args=[
                        ast.Constant(value=self.add_mapping_for_name),
                    ],
                    keywords=[],
                )
            )
        # convert class contents
        replace_dict = self.replace_symbols or {}
        replace_dict.update({"CrossSync": f"CrossSync._SyncImpl"})
        wrapped_node = transformers["SymbolReplacer"](replace_dict).visit(wrapped_node)
        # visit CrossSync method decorators
        wrapped_node = transformers["CrossSyncMethodDecoratorHandler"]().visit(wrapped_node)
        return wrapped_node


class Convert(AstDecorator):

    name = "convert"

    def __init__(
        self,
        *,
        sync_name:str|None = None,  # use a new name for the sync method
        replace_symbols:dict = {}  # replace symbols in the generated sync method
    ):
        self.sync_name = sync_name
        self.replace_symbols = replace_symbols

    def sync_ast_transform(self, decorator, wrapped_node, transformers):
        if self.sync_name:
            wrapped_node.name = self.sync_name
        if self.replace_symbols:
            replacer = transformers["SymbolReplacer"]
            wrapped_node = replacer(self.replace_symbols).visit(wrapped_node)
        return wrapped_node


class DropMethod(AstDecorator):

    name = "drop_method"

    def sync_ast_transform(self, decorator, wrapped_node, transformers):
        return None

class Pytest(AstDecorator):

    name = "pytest"

    def async_decorator(self):
        import pytest
        return pytest.mark.asyncio

class PytestFixture(AstDecorator):

    name = "pytest_fixture"

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def async_decorator(self):
        import pytest_asyncio
        return lambda f: pytest_asyncio.fixture(*self._args, **self._kwargs)(f)

    def sync_ast_transform(self, decorator, wrapped_node, transformers):
        import ast
        import copy
        new_decorator = copy.deepcopy(decorator)
        new_node = copy.deepcopy(wrapped_node)
        new_decorator.func.value = ast.Name(id="pytest", ctx=ast.Load())
        new_decorator.func.attr = "fixture"
        new_node.decorator_list.append(new_decorator)
        return new_node
