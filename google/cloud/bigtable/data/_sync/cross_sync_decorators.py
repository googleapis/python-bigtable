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


class AstDecorator:
    """
    Helper class for CrossSync decorators used for guiding ast transformations.

    These decorators provide arguments that are used during the code generation process,
    but act as no-ops when encountered in live code

    Args:
        attr_name: name of the attribute to attach to the CrossSync class
            e.g. pytest for CrossSync.pytest
        required_keywords: list of required keyword arguments for the decorator.
            If the decorator is used without these arguments, a ValueError is
            raised during code generation
        async_impl: If given, the async code will apply this decorator to its
            wrapped function at runtime. If not given, the decorator will be a no-op
        **default_kwargs: any kwargs passed define the valid arguments when using the decorator.
            The value of each kwarg is the default value for the argument.
    """

    name = None
    required_kwargs = ()
    default_kwargs = {}

    @classmethod
    def all_valid_keys(cls):
        return [*cls.required_kwargs, *cls.default_kwargs.keys()]

    def __call__(self, *args, **kwargs):
        """
        Called when the decorator is used in code.

        Returns a no-op decorator function, or applies the async_impl decorator
        """
        # raise error if invalid kwargs are passed
        for kwarg in kwargs:
            if kwarg not in self.all_valid_keys():
                raise ValueError(f"Invalid keyword argument: {kwarg}")
        return self.async_decorator(*args, **kwargs)

    @classmethod
    def parse_ast_keywords(cls, node):
        """
        When this decorator is encountered in the ast during sync generation, parse the
        keyword arguments back from ast nodes to python primitives

        Return a full set of kwargs, using default values for missing arguments
        """
        got_kwargs = (
            {kw.arg: cls._convert_ast_to_py(kw.value) for kw in node.keywords}
            if hasattr(node, "keywords")
            else {}
        )
        for key in got_kwargs.keys():
            if key not in cls.all_valid_keys():
                raise ValueError(f"Invalid keyword argument: {key}")
        for key in cls.required_kwargs:
            if key not in got_kwargs:
                raise ValueError(f"Missing required keyword argument: {key}")
        return {**cls.default_kwargs, **got_kwargs}

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

    @classmethod
    def async_decorator(cls, *args, **kwargs):
        """
        Decorator to apply the async_impl decorator to the wrapped function

        Default implementation is a no-op
        """
        # if no arguments, args[0] will hold the function to be decorated
        # return the function as is
        if len(args) == 1 and callable(args[0]):
            return args[0]

        # if arguments are provided, return a no-op decorator function
        def decorator(func):
            return func

        return decorator

    @classmethod
    def sync_ast_transform(cls, decorator, wrapped_node, transformers):
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
            for subclass in cls.__subclasses__():
                if subclass.name == decorator_name:
                    return subclass
        raise ValueError(f"Unknown decorator encountered")


class ExportSyncDecorator(AstDecorator):

    name = "export_sync"

    required_kwargs = ("path",)
    default_kwargs = {
        "replace_symbols": {},  # replace symbols in the generated sync class
        "mypy_ignore": (),  # set of mypy errors to ignore
        "include_file_imports": True,  # include imports from the file in the generated sync class
        "add_mapping_for_name": None,  # add a new attribute to CrossSync with the given name
    }

    @classmethod
    def async_decorator(cls, *args, **kwargs):
        from .cross_sync import CrossSync
        new_mapping = kwargs.get("add_mapping_for_name")
        def decorator(cls):
            if new_mapping:
                CrossSync.add_mapping(new_mapping, cls)
            return cls
        return decorator

class ConvertDecorator(AstDecorator):

    name = "convert"

    default_kwargs = {
        "sync_name": None,  # use a new name for the sync method
        "replace_symbols": {},  # replace symbols in the generated sync method
    }

    @classmethod
    def sync_ast_transform(cls, decorator, wrapped_node, transformers):
        kwargs = cls.parse_ast_keywords(decorator)
        if kwargs["sync_name"]:
            wrapped_node.name = kwargs["sync_name"]
        if kwargs["replace_symbols"]:
            replacer = transformers["SymbolReplacer"]
            wrapped_node = replacer(kwargs["replace_symbols"]).visit(wrapped_node)
        return wrapped_node


class DropMethodDecorator(AstDecorator):

    name = "drop_method"

    @classmethod
    def sync_ast_transform(cls, decorator, wrapped_node, transformers):
        return None

class PytestDecorator(AstDecorator):

    name = "pytest"

    @classmethod
    def async_decorator(cls, *args, **kwargs):
        import pytest
        return pytest.mark.asyncio

class PytestFixtureDecorator(AstDecorator):

    name = "pytest_fixture"

    # arguments passed down to pytest(_asyncio).fixture decorator
    default_kwargs = {
        "scope": "function",
        "params": None,
        "autouse": False,
        "ids": None,
        "name": None,
    }

    @classmethod
    def async_decorator(cls, *args, **kwargs):
        import pytest_asyncio
        def decorator(func):
            return pytest_asyncio.fixture(**kwargs)(func)
        return decorator

    @classmethod
    def sync_ast_transform(cls, decorator, wrapped_node, transformers):
        import ast
        decorator.func.value = ast.Name(id="pytest", ctx=ast.Load())
        decorator.func.attr = "fixture"
        wrapped_node.decorator_list.append(decorator)
        return wrapped_node
