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
from unittest import mock
from google.cloud.bigtable.data._sync.cross_sync.cross_sync import CrossSync
from google.cloud.bigtable.data._sync.cross_sync._decorators import ExportSync, Convert, DropMethod, Pytest, PytestFixture


class TestExportSyncDecorator:

    def _get_class(self):
        return ExportSync

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

    def test_sync_ast_transform(self):
        pass

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
