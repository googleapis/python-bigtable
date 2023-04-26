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

import google.cloud.bigtable.exceptions as bigtable_exceptions


# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore

class TestBigtableExceptionGroup():
    """
    Subclass for MutationsExceptionGroup and RetryExceptionGroup
    """

    def _get_class(self):
        from google.cloud.bigtable.exceptions import BigtableExceptionGroup
        return BigtableExceptionGroup

    def _make_one(self, message="test_message", excs=None):
        if excs is None:
            excs = [RuntimeError('mock')]

        return self._get_class()(message, excs=excs)

    def test_raise(self):
        """
        Create exception in raise statement, which calls __new__ and __init__
        """
        test_msg = "test message"
        test_excs = [Exception(test_msg)]
        with pytest.raises(self._get_class()) as e:
            raise self._get_class()(test_msg, test_excs)
        assert str(e.value) == test_msg
        assert list(e.value.exceptions) == test_excs

    def test_raise_empty_list(self):
        """
        Empty exception lists are not supported
        """
        with pytest.raises(ValueError) as e:
            raise self._make_one(excs=[])
        assert "non-empty sequence" in str(e.value)

    @pytest.mark.skipif(sys.version_info < (3, 11), reason="requires python3.11 or higher")
    def test_311_exception_group(self):
        """
        Python 3.11+ should handle exepctions as native exception groups
        """
        exceptions = [RuntimeError("mock"), ValueError("mock")]
        instance = self._make_one(excs=exceptions)
        assert isinstance(instance, ExceptionGroup)
        # ensure split works as expected
        runtime_error, others = instance.split(lambda e: isinstance(e, RuntimeError))
        assert isinstance(runtime_error, ExceptionGroup)
        assert runtime_error.exceptions[0] == exceptions[0]
        assert isinstance(others, ExceptionGroup)
        assert others.exceptions[0] == exceptions[1]

    def test_exception_handling(self):
        """
        All versions should inherit from exception
        and support tranditional exception handling
        """
        instance = self._make_one()
        assert isinstance(instance, Exception)
        try:
            raise instance
        except Exception as e:
            assert isinstance(e, Exception)
            assert e == instance
            was_raised = True
        assert was_raised



class TestMutationsExceptionGroup(TestBigtableExceptionGroup):
    def _get_class(self):
        from google.cloud.bigtable.exceptions import MutationsExceptionGroup
        return MutationsExceptionGroup

    def _make_one(self, excs=None, num_entries=3):
        if excs is None:
            excs = [RuntimeError('mock')]

        return self._get_class()(excs, num_entries)

    @pytest.mark.parametrize("exception_list,total_entries,expected_message", [
        ([Exception()], 1, "1 sub-exception (from 1 entry attempted)"),
        ([Exception()], 2, "1 sub-exception (from 2 entries attempted)"),
        ([Exception(), RuntimeError()], 2, "2 sub-exceptions (from 2 entries attempted)"),
    ])
    def test_raise(self, exception_list, total_entries, expected_message):
        """
        Create exception in raise statement, which calls __new__ and __init__
        """
        with pytest.raises(self._get_class()) as e:
            raise self._get_class()(exception_list, total_entries)
        assert str(e.value) == expected_message
        assert list(e.value.exceptions) == exception_list

class TestRetryExceptionGroup(TestBigtableExceptionGroup):
    def _get_class(self):
        from google.cloud.bigtable.exceptions import RetryExceptionGroup
        return RetryExceptionGroup

    def _make_one(self, excs=None):
        if excs is None:
            excs = [RuntimeError('mock')]

        return self._get_class()(excs=excs)

    @pytest.mark.parametrize("exception_list,expected_message", [
        ([Exception()], "1 failed attempt: Exception"),
        ([Exception(), RuntimeError()], "2 failed attempts. Latest: RuntimeError"),
        ([Exception(), ValueError("test")], "2 failed attempts. Latest: ValueError"),
        ([bigtable_exceptions.RetryExceptionGroup([Exception(), ValueError("test")])], "1 failed attempt: RetryExceptionGroup"),
    ])
    def test_raise(self, exception_list, expected_message):
        """
        Create exception in raise statement, which calls __new__ and __init__
        """
        with pytest.raises(self._get_class()) as e:
            raise self._get_class()(exception_list)
        assert str(e.value) == expected_message
        assert list(e.value.exceptions) == exception_list


class TestFailedMutationEntryError():
    def _get_class(self):
        from google.cloud.bigtable.exceptions import FailedMutationEntryError
        return FailedMutationEntryError

    def _make_one(self, idx=9, entry=unittest.mock.Mock(), cause=RuntimeError('mock')):

        return self._get_class()(idx, entry, cause)

    def test_raise(self):
        """
        Create exception in raise statement, which calls __new__ and __init__
        """
        test_idx = 2
        test_entry = unittest.mock.Mock()
        test_exc = ValueError("test")
        with pytest.raises(self._get_class()) as e:
            raise self._get_class()(test_idx, test_entry, test_exc)
        assert str(e.value) == "Failed idempotent mutation entry at index 2 with cause: ValueError('test')"
        assert e.value.index == test_idx
        assert e.value.entry == test_entry
        assert e.value.__cause__ == test_exc
        assert isinstance(e.value, Exception)
        assert test_entry.is_idempotent.call_count == 1

    def test_raise_idempotent(self):
        """
        Test raise with non idempotent entry
        """
        test_idx = 2
        test_entry = unittest.mock.Mock()
        test_entry.is_idempotent.return_value = False
        test_exc = ValueError("test")
        with pytest.raises(self._get_class()) as e:
            raise self._get_class()(test_idx, test_entry, test_exc)
        assert str(e.value) == "Failed non-idempotent mutation entry at index 2 with cause: ValueError('test')"
        assert e.value.index == test_idx
        assert e.value.entry == test_entry
        assert e.value.__cause__ == test_exc
        assert test_entry.is_idempotent.call_count == 1
