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

import pytest
import ast
from unittest import mock
from google.cloud.bigtable.data._sync.cross_sync.cross_sync import CrossSync
from google.cloud.bigtable.data._sync.cross_sync._decorators import ExportSync, Convert, DropMethod, Pytest, PytestFixture


class TestExportSyncDecorator:

    def _get_class(self):
        return ExportSync

    @pytest.fixture
    def globals_mock(self):
        mock_transform = mock.Mock()
        mock_transform().visit = lambda x: x
        global_dict = {k: mock_transform for k in ["RmAioFunctions", "SymbolReplacer", "CrossSyncMethodDecoratorHandler"]}
        return global_dict

    def test_ctor_defaults(self):
        """
        Should set default values for path, add_mapping_for_name, and docstring_format_vars
        """
        with pytest.raises(TypeError) as exc:
            self._get_class()()
            assert "missing 1 required positional argument" in str(exc.value)
        path = object()
        instance = self._get_class()(path)
        assert instance.path is path
        assert instance.replace_symbols is None
        assert instance.mypy_ignore is ()
        assert instance.include_file_imports is True
        assert instance.add_mapping_for_name is None
        assert instance.async_docstring_format_vars == {}
        assert instance.sync_docstring_format_vars == {}

    def test_ctor(self):
        path = object()
        replace_symbols = {"a": "b"}
        docstring_format_vars = {"A": (1, 2)}
        mypy_ignore = ("a", "b")
        include_file_imports = False
        add_mapping_for_name = "test_name"

        instance = self._get_class()(
            path=path,
            replace_symbols=replace_symbols,
            docstring_format_vars=docstring_format_vars,
            mypy_ignore=mypy_ignore,
            include_file_imports=include_file_imports,
            add_mapping_for_name=add_mapping_for_name
        )
        assert instance.path is path
        assert instance.replace_symbols is replace_symbols
        assert instance.mypy_ignore is mypy_ignore
        assert instance.include_file_imports is include_file_imports
        assert instance.add_mapping_for_name is add_mapping_for_name
        assert instance.async_docstring_format_vars == {"A": 1}
        assert instance.sync_docstring_format_vars == {"A": 2}

    def test_class_decorator(self):
        """
        Should return class being decorated
        """
        unwrapped_class = mock.Mock
        wrapped_class = self._get_class().decorator(unwrapped_class, path=1)
        assert unwrapped_class == wrapped_class

    def test_class_decorator_adds_mapping(self):
        """
        If add_mapping_for_name is set, should call CrossSync.add_mapping with the class being decorated
        """
        with mock.patch.object(CrossSync, "add_mapping") as add_mapping:
            mock_cls = mock.Mock
            # check decoration with no add_mapping
            self._get_class().decorator(path=1)(mock_cls)
            assert add_mapping.call_count == 0
            # check decoration with add_mapping
            name = "test_name"
            self._get_class().decorator(path=1, add_mapping_for_name=name)(mock_cls)
            assert add_mapping.call_count == 1
            add_mapping.assert_called_once_with(name, mock_cls)

    @pytest.mark.parametrize("docstring,format_vars,expected", [
        ["test docstring", {}, "test docstring"],
        ["{}", {}, "{}"],
        ["test_docstring", {"A": (1, 2)}, "test_docstring"],
        ["{A}", {"A": (1, 2)}, "1"],
        ["{A} {B}", {"A": (1, 2), "B": (3, 4)}, "1 3"],
        ["hello {world_var}", {"world_var": ("world", "moon")}, "hello world"],
    ])
    def test_class_decorator_docstring_update(self, docstring, format_vars, expected):
        """
        If docstring_format_vars is set, should update the docstring
        of the class being decorated
        """
        @self._get_class().decorator(path=1, docstring_format_vars=format_vars)
        class Class:
            __doc__ = docstring
        assert Class.__doc__ == expected
        # check internal state
        instance = self._get_class()(path=1, docstring_format_vars=format_vars)
        async_replacements = {k: v[0] for k, v in format_vars.items()}
        sync_replacements = {k: v[1] for k, v in format_vars.items()}
        assert instance.async_docstring_format_vars == async_replacements
        assert instance.sync_docstring_format_vars == sync_replacements

    def test_sync_ast_transform_replaces_name(self, globals_mock):
        """
        Should update the name of the new class
        """
        decorator = self._get_class()("path.to.SyncClass")
        mock_node = ast.ClassDef(name="AsyncClass", bases=[], keywords=[], body=[])

        result = decorator.sync_ast_transform(mock_node, globals_mock)

        assert isinstance(result, ast.ClassDef)
        assert result.name == "SyncClass"

    def test_sync_ast_transform_strips_cross_sync_decorators(self, globals_mock):
        """
        should remove all CrossSync decorators from the class
        """
        decorator = self._get_class()("path")
        cross_sync_decorator = ast.Call(func=ast.Attribute(value=ast.Name(id='CrossSync', ctx=ast.Load()), attr='some_decorator', ctx=ast.Load()), args=[], keywords=[])
        other_decorator = ast.Name(id='other_decorator', ctx=ast.Load())
        mock_node = ast.ClassDef(name="AsyncClass", bases=[], keywords=[], body=[], decorator_list=[cross_sync_decorator, other_decorator])

        result = decorator.sync_ast_transform(mock_node, globals_mock)

        assert isinstance(result, ast.ClassDef)
        assert len(result.decorator_list) == 1
        assert isinstance(result.decorator_list[0], ast.Name)
        assert result.decorator_list[0].id == 'other_decorator'

    def test_sync_ast_transform_add_mapping(self, globals_mock):
        """
        If add_mapping_for_name is set, should add CrossSync.add_mapping_decorator to new class
        """
        decorator = self._get_class()("path", add_mapping_for_name="sync_class")
        mock_node = ast.ClassDef(name="AsyncClass", bases=[], keywords=[], body=[])

        result = decorator.sync_ast_transform(mock_node, globals_mock)

        assert isinstance(result, ast.ClassDef)
        assert len(result.decorator_list) == 1
        assert isinstance(result.decorator_list[0], ast.Call)
        assert isinstance(result.decorator_list[0].func, ast.Attribute)
        assert result.decorator_list[0].func.attr == 'add_mapping_decorator'
        assert result.decorator_list[0].args[0].value == 'sync_class'

    @pytest.mark.parametrize("docstring,format_vars,expected", [
        ["test docstring", {}, "test docstring"],
        ["{}", {}, "{}"],
        ["test_docstring", {"A": (1, 2)}, "test_docstring"],
        ["{A}", {"A": (1, 2)}, "2"],
        ["{A} {B}", {"A": (1, 2), "B": (3, 4)}, "2 4"],
        ["hello {world_var}", {"world_var": ("world", "moon")}, "hello moon"],
    ])
    def test_sync_ast_transform_add_docstring_format(self, docstring, format_vars, expected, globals_mock):
        """
        If docstring_format_vars is set, should format the docstring of the new class
        """
        decorator = self._get_class()("path.to.SyncClass", docstring_format_vars=format_vars)
        mock_node = ast.ClassDef(
            name="AsyncClass",
            bases=[],
            keywords=[],
            body=[ast.Expr(value=ast.Constant(value=docstring))]
        )
        result = decorator.sync_ast_transform(mock_node, globals_mock)

        assert isinstance(result, ast.ClassDef)
        assert isinstance(result.body[0], ast.Expr)
        assert isinstance(result.body[0].value, ast.Constant)
        assert result.body[0].value.value == expected

    def test_sync_ast_transform_call_cross_sync_transforms(self):
        """
        Should use transformers_globals to call some extra transforms on class:
        - RmAioFunctions
        - SymbolReplacer
        - CrossSyncMethodDecoratorHandler
        """
        decorator = self._get_class()("path.to.SyncClass")
        mock_node = ast.ClassDef(name="AsyncClass", bases=[], keywords=[], body=[])

        transformers_globals = {
            "RmAioFunctions": mock.Mock(),
            "SymbolReplacer": mock.Mock(),
            "CrossSyncMethodDecoratorHandler": mock.Mock(),
        }
        decorator.sync_ast_transform(mock_node, transformers_globals)
        # ensure each transformer was called
        for transformer in transformers_globals.values():
            assert transformer.call_count == 1

    def test_sync_ast_transform_replace_symbols(self, globals_mock):
        """
        SymbolReplacer should be called with replace_symbols
        """
        replace_symbols = {"a": "b", "c": "d"}
        decorator = self._get_class()("path.to.SyncClass", replace_symbols=replace_symbols)
        mock_node = ast.ClassDef(name="AsyncClass", bases=[], keywords=[], body=[])
        symbol_transform_mock = mock.Mock()
        globals_mock = {**globals_mock, "SymbolReplacer": symbol_transform_mock}
        decorator.sync_ast_transform(mock_node, globals_mock)
        # make sure SymbolReplacer was called with replace_symbols
        assert symbol_transform_mock.call_count == 1
        found_dict = symbol_transform_mock.call_args[0][0]
        assert "a" in found_dict
        for k, v in replace_symbols.items():
            assert found_dict[k] == v
        # should also add CrossSync replacement
        assert found_dict["CrossSync"] == "CrossSync._Sync_Impl"


class TestConvertDecorator:

    def _get_class(self):
        return Convert

    def test_decorator_functionality(self):
        pass

    def test_sync_ast_transform(self):
        pass

class TestDropMethodDecorator:

    def _get_class(self):
        return DropMethod

    def test_decorator_functionality(self):
        """
        applying the decorator should be a no-op
        """
        unwrapped = lambda x: x
        wrapped = self._get_class().decorator(unwrapped)
        assert unwrapped == wrapped
        assert unwrapped(1) == wrapped(1)
        assert wrapped(1) == 1

    def test_sync_ast_transform(self):
        pass

class TestPytestDecorator:

    def _get_class(self):
        return Pytest

    def test_decorator_functionality(self):
        pass

    def test_sync_ast_transform(self):
        pass

class TestPytestFixtureDecorator:

    def _get_class(self):
        return PytestFixture

    def test_decorator_functionality(self):
        pass

    def test_sync_ast_transform(self):
        pass
