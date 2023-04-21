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
from __future__ import annotations

import asyncio

import pytest

from google.cloud.bigtable._read_rows import _ReadRowsOperation

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore


class MockStream(_ReadRowsOperation):
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

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.next_idx >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.next_idx]
        self.next_idx += 1
        if isinstance(item, Exception):
            raise item
        return item

    async def aclose(self):
        pass


class TestReadRowsIterator:
    async def mock_stream(self, size=10):
        for i in range(size):
            yield i

    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable.iterators import ReadRowsIterator

        stream = MockStream(*args, **kwargs)
        return ReadRowsIterator(stream)

    def test_ctor(self):
        with mock.patch("time.time", return_value=0):
            iterator = self._make_one()
            assert iterator.last_interaction_time == 0
            assert iterator._idle_timeout_task is None
            assert iterator.request_stats is None
            assert iterator.active is True

    def test___aiter__(self):
        iterator = self._make_one()
        assert iterator.__aiter__() is iterator

    @pytest.mark.asyncio
    async def test__start_idle_timer(self):
        """Should start timer coroutine"""
        iterator = self._make_one()
        expected_timeout = 10
        with mock.patch("time.time", return_value=1):
            with mock.patch.object(iterator, "_idle_timeout_coroutine") as mock_coro:
                await iterator._start_idle_timer(expected_timeout)
                assert mock_coro.call_count == 1
                assert mock_coro.call_args[0] == (expected_timeout,)
        assert iterator.last_interaction_time == 1
        assert iterator._idle_timeout_task is not None

    @pytest.mark.asyncio
    async def test__start_idle_timer_duplicate(self):
        """Multiple calls should replace task"""
        iterator = self._make_one()
        with mock.patch.object(iterator, "_idle_timeout_coroutine") as mock_coro:
            await iterator._start_idle_timer(1)
            first_task = iterator._idle_timeout_task
            await iterator._start_idle_timer(2)
            second_task = iterator._idle_timeout_task
            assert mock_coro.call_count == 2

            assert first_task is not None
            assert first_task != second_task
            # old tasks hould be cancelled
            with pytest.raises(asyncio.CancelledError):
                await first_task
            # new task should not be cancelled
            await second_task

    @pytest.mark.asyncio
    async def test__idle_timeout_coroutine(self):
        from google.cloud.bigtable.exceptions import IdleTimeout

        iterator = self._make_one()
        await iterator._idle_timeout_coroutine(0.05)
        await asyncio.sleep(0.1)
        assert iterator.active is False
        with pytest.raises(IdleTimeout):
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test__idle_timeout_coroutine_extensions(self):
        """touching the generator should reset the idle timer"""
        iterator = self._make_one(items=list(range(100)))
        await iterator._start_idle_timer(0.05)
        for i in range(10):
            # will not expire as long as it is in use
            assert iterator.active is True
            await iterator.__anext__()
            await asyncio.sleep(0.03)
        # now let it expire
        await asyncio.sleep(0.5)
        assert iterator.active is False

    @pytest.mark.asyncio
    async def test___anext__(self):
        num_rows = 10
        iterator = self._make_one(items=list(range(num_rows)))
        for i in range(num_rows):
            assert await iterator.__anext__() == i
        with pytest.raises(StopAsyncIteration):
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test___anext__with_request_stats(self):
        """
        Request stats should not be yielded, but should be set on the iterator object
        """
        from google.cloud.bigtable_v2.types import RequestStats

        stats = RequestStats()
        items = [1, 2, stats, 3]
        iterator = self._make_one(items=items)
        assert await iterator.__anext__() == 1
        assert await iterator.__anext__() == 2
        assert iterator.request_stats is None
        assert await iterator.__anext__() == 3
        with pytest.raises(StopAsyncIteration):
            await iterator.__anext__()
        assert iterator.request_stats == stats

    @pytest.mark.asyncio
    async def test___anext__with_deadline_error(self):
        """
        RetryErrors mean a deadline has been hit.
        Should be wrapped in a DeadlineExceeded exception
        """
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        iterator = self._make_one(items=items, operation_timeout=expected_timeout)
        assert await iterator.__anext__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            await iterator.__anext__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        assert exc.value.__cause__ is None

    @pytest.mark.asyncio
    async def test___anext__with_deadline_error_with_cause(self):
        """
        Transient errors should be exposed as an error group
        """
        from google.api_core import exceptions as core_exceptions
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        errors = [RuntimeError("error1"), ValueError("error2")]
        iterator = self._make_one(
            items=items, operation_timeout=expected_timeout, errors=errors
        )
        assert await iterator.__anext__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            await iterator.__anext__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        error_group = exc.value.__cause__
        assert isinstance(error_group, RetryExceptionGroup)
        assert len(error_group.exceptions) == 2
        assert error_group.exceptions[0] is errors[0]
        assert error_group.exceptions[1] is errors[1]
        assert "2 failed attempts" in str(error_group)

    @pytest.mark.asyncio
    async def test___anext__with_error(self):
        """
        Other errors should be raised as-is
        """
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.InternalServerError("mock error")]
        iterator = self._make_one(items=items)
        assert await iterator.__anext__() == 1
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            await iterator.__anext__()
        assert exc.value is items[1]
        assert iterator.active is False
        # next call should raise same error
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test__finish_with_error(self):
        iterator = self._make_one()
        await iterator._start_idle_timer(10)
        timeout_task = iterator._idle_timeout_task
        assert await iterator.__anext__() == 0
        assert iterator.active is True
        err = ZeroDivisionError("mock error")
        await iterator._finish_with_error(err)
        assert iterator.active is False
        assert iterator._merger_or_error is err
        assert iterator._idle_timeout_task is None
        with pytest.raises(ZeroDivisionError) as exc:
            await iterator.__anext__()
            assert exc.value is err
        # timeout task should be cancelled
        with pytest.raises(asyncio.CancelledError):
            await timeout_task

    @pytest.mark.asyncio
    async def test_aclose(self):
        iterator = self._make_one()
        await iterator._start_idle_timer(10)
        timeout_task = iterator._idle_timeout_task
        assert await iterator.__anext__() == 0
        assert iterator.active is True
        await iterator.aclose()
        assert iterator.active is False
        assert isinstance(iterator._merger_or_error, StopAsyncIteration)
        assert iterator._idle_timeout_task is None
        with pytest.raises(StopAsyncIteration) as e:
            await iterator.__anext__()
            assert "closed" in str(e.value)
        # timeout task should be cancelled
        with pytest.raises(asyncio.CancelledError):
            await timeout_task
