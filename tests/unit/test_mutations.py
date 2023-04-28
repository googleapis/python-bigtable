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

import unittest
import pytest
import sys

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore

class TestBaseMutation:

    def _target_class(self):
        from google.cloud.bigtable.mutations import Mutation
        return Mutation

    def test__to_dict(self):
        """Should be unimplemented in the base class"""
        with pytest.raises(NotImplementedError):
            self._target_class()._to_dict(mock.Mock())

    def test_is_idempotent(self):
        """is_idempotent should assume True"""
        assert self._target_class().is_idempotent(mock.Mock())

    def test___str__(self):
        """Str representation of mutations should be to_dict"""
        self_mock = mock.Mock()
        str_value = self._target_class().__str__(self_mock)
        assert self_mock._to_dict.called
        assert str_value == str(self_mock._to_dict.return_value)

class TestSetCell:

    def _target_class(self):
        from google.cloud.bigtable.mutations import SetCell
        return SetCell

    def _make_one(self, *args, **kwargs):
        return self._target_class()(*args, **kwargs)

    def test__to_dict(self):
        """Should be unimplemented in the base class"""
        expected_family = "test-family"
        expected_qualifier = b"test-qualifier"
        expected_value = b"test-value"
        expected_timestamp = 1234567890
        instance = self._make_one(expected_family, expected_qualifier, expected_value, expected_timestamp)
        got_dict = instance._to_dict()
        assert list(got_dict.keys()) == ["set_cell"]
        got_inner_dict = got_dict["set_cell"]
        assert got_inner_dict["family_name"] == expected_family
        assert got_inner_dict["column_qualifier"] == expected_qualifier
        assert got_inner_dict["timestamp_micros"] == expected_timestamp
        assert got_inner_dict["value"] == expected_value
        assert len(got_inner_dict.keys()) == 4

    def test__to_dict_server_timestamp(self):
        """Should be unimplemented in the base class"""
        expected_family = "test-family"
        expected_qualifier = b"test-qualifier"
        expected_value = b"test-value"
        expected_timestamp = -1
        instance = self._make_one(expected_family, expected_qualifier, expected_value)
        got_dict = instance._to_dict()
        assert list(got_dict.keys()) == ["set_cell"]
        got_inner_dict = got_dict["set_cell"]
        assert got_inner_dict["family_name"] == expected_family
        assert got_inner_dict["column_qualifier"] == expected_qualifier
        assert got_inner_dict["timestamp_micros"] == expected_timestamp
        assert got_inner_dict["value"] == expected_value
        assert len(got_inner_dict.keys()) == 4

    @pytest.mark.parametrize("timestamp,expected_value", [
        (1234567890, True),
        (1, True),
        (0, True),
        (-1, False),
        (None, False),
    ])
    def test_is_idempotent(self, timestamp, expected_value):
        """is_idempotent is based on whether an explicit timestamp is set"""
        instance = self._make_one("test-family", b"test-qualifier", b'test-value', timestamp)
        assert instance.is_idempotent() is expected_value

    def test___str__(self):
        """Str representation of mutations should be to_dict"""
        instance = self._make_one("test-family", b"test-qualifier", b'test-value', 1234567890)
        str_value = instance.__str__()
        dict_value = instance._to_dict()
        assert str_value == str(dict_value)

class TestDeleteRangeFromColumn:

    def _target_class(self):
        from google.cloud.bigtable.mutations import DeleteRangeFromColumn
        return DeleteRangeFromColumn

    def _make_one(self, *args, **kwargs):
        return self._target_class()(*args, **kwargs)

    def test_ctor(self):
        expected_family = "test-family"
        expected_qualifier = b"test-qualifier"
        expected_start = 1234567890
        expected_end = 1234567891
        instance = self._make_one(expected_family, expected_qualifier, expected_start, expected_end)
        assert instance.family == expected_family
        assert instance.qualifier == expected_qualifier
        assert instance.start_timestamp_micros == expected_start
        assert instance.end_timestamp_micros == expected_end

    def test_ctor_no_timestamps(self):
        expected_family = "test-family"
        expected_qualifier = b"test-qualifier"
        instance = self._make_one(expected_family, expected_qualifier)
        assert instance.family == expected_family
        assert instance.qualifier == expected_qualifier
        assert instance.start_timestamp_micros is None
        assert instance.end_timestamp_micros is None

    def test_ctor_timestamps_out_of_order(self):
        expected_family = "test-family"
        expected_qualifier = b"test-qualifier"
        expected_start = 10
        expected_end = 1
        with pytest.raises(ValueError) as excinfo:
            self._make_one(expected_family, expected_qualifier, expected_start, expected_end)
        assert "start_timestamp_micros must be <= end_timestamp_micros" in str(excinfo.value)


    @pytest.mark.parametrize("start,end", [
        (0, 1),
        (None, 1),
        (0, None),
    ])
    def test__to_dict(self, start, end):
        """Should be unimplemented in the base class"""
        expected_family = "test-family"
        expected_qualifier = b"test-qualifier"

        instance = self._make_one(expected_family, expected_qualifier, start, end)
        got_dict = instance._to_dict()
        assert list(got_dict.keys()) == ["delete_from_column"]
        got_inner_dict = got_dict["delete_from_column"]
        assert len(got_inner_dict.keys()) == 3
        assert got_inner_dict["family_name"] == expected_family
        assert got_inner_dict["column_qualifier"] == expected_qualifier
        time_range_dict = got_inner_dict["time_range"]
        expected_len = int(isinstance(start, int)) + int(isinstance(end, int))
        assert len(time_range_dict.keys()) == expected_len
        if start is not None:
            assert time_range_dict["start_timestamp_micros"] == start
        if end is not None:
            assert time_range_dict["end_timestamp_micros"] == end

    def test_is_idempotent(self):
        """is_idempotent is always true"""
        instance = self._make_one("test-family", b"test-qualifier", 1234567890, 1234567891)
        assert instance.is_idempotent() is True

    def test___str__(self):
        """Str representation of mutations should be to_dict"""
        instance = self._make_one("test-family", b"test-qualifier")
        str_value = instance.__str__()
        dict_value = instance._to_dict()
        assert str_value == str(dict_value)



class TestDeleteAllFromFamily:

    def _target_class(self):
        from google.cloud.bigtable.mutations import DeleteAllFromFamily
        return DeleteAllFromFamily

    def _make_one(self, *args, **kwargs):
        return self._target_class()(*args, **kwargs)

    def test_ctor(self):
        expected_family = "test-family"
        instance = self._make_one(expected_family)
        assert instance.family_to_delete == expected_family

    def test__to_dict(self):
        """Should be unimplemented in the base class"""
        expected_family = "test-family"
        instance = self._make_one(expected_family)
        got_dict = instance._to_dict()
        assert list(got_dict.keys()) == ["delete_from_family"]
        got_inner_dict = got_dict["delete_from_family"]
        assert len(got_inner_dict.keys()) == 1
        assert got_inner_dict["family_name"] == expected_family


    def test_is_idempotent(self):
        """is_idempotent is always true"""
        instance = self._make_one("test-family")
        assert instance.is_idempotent() is True

    def test___str__(self):
        """Str representation of mutations should be to_dict"""
        instance = self._make_one("test-family")
        str_value = instance.__str__()
        dict_value = instance._to_dict()
        assert str_value == str(dict_value)



class TestDeleteFromRow:

    def _target_class(self):
        from google.cloud.bigtable.mutations import DeleteAllFromRow
        return DeleteAllFromRow

    def _make_one(self, *args, **kwargs):
        return self._target_class()(*args, **kwargs)

    def test_ctor(self):
        instance = self._make_one()

    def test__to_dict(self):
        """Should be unimplemented in the base class"""
        instance = self._make_one()
        got_dict = instance._to_dict()
        assert list(got_dict.keys()) == ["delete_from_row"]
        assert len(got_dict["delete_from_row"].keys()) == 0

    def test_is_idempotent(self):
        """is_idempotent is always true"""
        instance = self._make_one()
        assert instance.is_idempotent() is True

    def test___str__(self):
        """Str representation of mutations should be to_dict"""
        instance = self._make_one()
        assert instance.__str__() == "{'delete_from_row': {}}"


