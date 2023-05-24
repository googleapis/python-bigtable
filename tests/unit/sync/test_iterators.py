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
from unittest import mock
import pytest

import google.cloud.bigtable
from google.cloud.bigtable._sync._autogen import _ReadRowsOperation_Sync

try:
    from unittest import mock
except ImportError:
    import mock


class MockStream(_ReadRowsOperation_Sync):
    """
    Mock a _ReadRowsOperation stream for testing
    """

    def __init__(self, items=None, errors=None, operation_timeout=None):
        self.transient_errors = errors
        self.operation_timeout = operation_timeout
        self.next_idx = 0
        if items is None:
            items = list(range(10))
        self.items = items

    def __iter__(self):
        return self

    def __next__(self):
        if self.next_idx >= len(self.items):
            raise StopIteration
        item = self.items[self.next_idx]
        self.next_idx += 1
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        pass


class TestReadRowsIterator:
    def mock_stream(self, size=10):
        for i in range(size):
            yield i

    def _make_one(self, *args, **kwargs):
        stream = MockStream(*args, **kwargs)
        return google.cloud.bigtable._sync._concrete.ReadRowsIterator_Sync_Concrete(
            stream
        )

    def test_ctor(self):
        with mock.patch("time.time", return_value=0):
            iterator = self._make_one()
            assert iterator.last_interaction_time == 0
            assert iterator._idle_timeout_task is None
            assert iterator.active is True

    def test___aiter__(self):
        iterator = self._make_one()
        assert iterator.__iter__() is iterator


    def test___anext__(self):
        num_rows = 10
        iterator = self._make_one(items=list(range(num_rows)))
        for i in range(num_rows):
            assert iterator.__next__() == i
        with pytest.raises(StopIteration):
            iterator.__next__()

    def test___anext__with_deadline_error(self):
        """
        RetryErrors mean a deadline has been hit.
        Should be wrapped in a DeadlineExceeded exception
        """
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        iterator = self._make_one(items=items, operation_timeout=expected_timeout)
        assert iterator.__next__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            iterator.__next__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        assert exc.value.__cause__ is None

    def test___anext__with_deadline_error_with_cause(self):
        """Transient errors should be exposed as an error group"""
        from google.api_core import exceptions as core_exceptions
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        errors = [RuntimeError("error1"), ValueError("error2")]
        iterator = self._make_one(
            items=items, operation_timeout=expected_timeout, errors=errors
        )
        assert iterator.__next__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            iterator.__next__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        error_group = exc.value.__cause__
        assert isinstance(error_group, RetryExceptionGroup)
        assert len(error_group.exceptions) == 2
        assert error_group.exceptions[0] is errors[0]
        assert error_group.exceptions[1] is errors[1]
        assert "2 failed attempts" in str(error_group)

    def test___anext__with_error(self):
        """Other errors should be raised as-is"""
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.InternalServerError("mock error")]
        iterator = self._make_one(items=items)
        assert iterator.__next__() == 1
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            iterator.__next__()
        assert exc.value is items[1]
        assert iterator.active is False
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            iterator.__next__()

    def test__finish_with_error(self):
        iterator = self._make_one()
        iterator._start_idle_timer(10)
        assert iterator.__next__() == 0
        assert iterator.active is True
        err = ZeroDivisionError("mock error")
        iterator._finish_with_error(err)
        assert iterator.active is False
        assert iterator._error is err
        assert iterator._idle_timeout_task is None
        with pytest.raises(ZeroDivisionError) as exc:
            iterator.__next__()
            assert exc.value is err

    def test_aclose(self):
        iterator = self._make_one()
        iterator._start_idle_timer(10)
        assert iterator.__next__() == 0
        assert iterator.active is True
        iterator.close()
        assert iterator.active is False
        assert isinstance(iterator._error, StopIteration)
        assert iterator._idle_timeout_task is None
        with pytest.raises(StopIteration) as e:
            iterator.__next__()
            assert "closed" in str(e.value)
