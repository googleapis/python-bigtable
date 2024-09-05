import typing
import asyncio
import pytest
import pytest_asyncio
import threading
import concurrent.futures
import time
import queue
import functools
from google import api_core
from google.cloud.bigtable.data._sync.cross_sync.cross_sync import CrossSync, T
from unittest import mock

class TestCrossSync:

    async def async_iter(self, in_list):
        for i in in_list:
            yield i

    @pytest.fixture
    def cs_sync(self):
        return CrossSync._Sync_Impl

    @pytest_asyncio.fixture
    def cs_async(self):
        return CrossSync


    @pytest.mark.parametrize(
        "attr, async_version, sync_version", [
            ("is_async", True, False),
            ("sleep", asyncio.sleep, time.sleep),
            ("wait", asyncio.wait, concurrent.futures.wait),
            ("retry_target", api_core.retry.retry_target_async, api_core.retry.retry_target),
            ("retry_target_stream", api_core.retry.retry_target_stream_async, api_core.retry.retry_target_stream),
            ("Retry", api_core.retry.AsyncRetry, api_core.retry.Retry),
            ("Queue", asyncio.Queue, queue.Queue),
            ("Condition", asyncio.Condition, threading.Condition),
            ("Future", asyncio.Future, concurrent.futures.Future),
            ("Task", asyncio.Task, concurrent.futures.Future),
            ("Event", asyncio.Event, threading.Event),
            ("Semaphore", asyncio.Semaphore, threading.Semaphore),
            ("StopIteration", StopAsyncIteration, StopIteration),
            # types
            ("Awaitable", typing.Awaitable, typing.Union[T]),
            ("Iterable", typing.AsyncIterable, typing.Iterable),
            ("Iterator", typing.AsyncIterator, typing.Iterator),
            ("Generator", typing.AsyncGenerator, typing.Generator),
        ]
    )
    def test_alias_attributes(self, attr, async_version, sync_version, cs_sync, cs_async):
        """
        Test basic alias attributes, to ensure they point to the right place
        in both sync and async versions.
        """
        assert getattr(cs_async, attr) == async_version, f"Failed async version for {attr}"
        assert getattr(cs_sync, attr) == sync_version, f"Failed sync version for {attr}"

    @pytest.mark.asyncio
    async def test_Mock(self, cs_sync, cs_async):
        """
        Test Mock class in both sync and async versions
        """
        assert isinstance(cs_async.Mock(), mock.AsyncMock)
        assert isinstance(cs_sync.Mock(), mock.Mock)
        # test with return value
        assert await cs_async.Mock(return_value=1)() == 1
        assert cs_sync.Mock(return_value=1)() == 1

    def test_next(self, cs_sync):
        """
        Test sync version of CrossSync.next()
        """
        it = iter([1, 2, 3])
        assert cs_sync.next(it) == 1
        assert cs_sync.next(it) == 2
        assert cs_sync.next(it) == 3
        with pytest.raises(StopIteration):
            cs_sync.next(it)
        with pytest.raises(cs_sync.StopIteration):
            cs_sync.next(it)

    @pytest.mark.asyncio
    async def test_next_async(self, cs_async):
        """
        test async version of CrossSync.next()
        """
        async_it = self.async_iter([1, 2, 3])
        assert await cs_async.next(async_it) == 1
        assert await cs_async.next(async_it) == 2
        assert await cs_async.next(async_it) == 3
        with pytest.raises(StopAsyncIteration):
            await cs_async.next(async_it)
        with pytest.raises(cs_async.StopIteration):
            await cs_async.next(async_it)

    def test_gather_partials(self, cs_sync):
        """
        Test sync version of CrossSync.gather_partials()
        """
        with concurrent.futures.ThreadPoolExecutor() as e:
            partials = [lambda i=i: i + 1 for i in range(5)]
            results = cs_sync.gather_partials(partials, sync_executor=e)
            assert results == [1, 2, 3, 4, 5]

    def test_gather_partials_with_excepptions(self, cs_sync):
        """
        Test sync version of CrossSync.gather_partials() with exceptions
        """
        with concurrent.futures.ThreadPoolExecutor() as e:
            partials = [lambda i=i: i + 1 if i != 3 else 1/0 for i in range(5)]
            with pytest.raises(ZeroDivisionError):
                cs_sync.gather_partials(partials, sync_executor=e)

    def test_gather_partials_return_exceptions(self, cs_sync):
        """
        Test sync version of CrossSync.gather_partials() with return_exceptions=True
        """
        with concurrent.futures.ThreadPoolExecutor() as e:
            partials = [lambda i=i: i + 1 if i != 3 else 1/0 for i in range(5)]
            results = cs_sync.gather_partials(partials, return_exceptions=True, sync_executor=e)
            assert len(results) == 5
            assert results[0] == 1
            assert results[1] == 2
            assert results[2] == 3
            assert isinstance(results[3], ZeroDivisionError)
            assert results[4] == 5

    def test_gather_partials_no_executor(self, cs_sync):
        """
        Test sync version of CrossSync.gather_partials() without an executor
        """
        partials = [lambda i=i: i + 1 for i in range(5)]
        with pytest.raises(ValueError) as e:
            results = cs_sync.gather_partials(partials)
        assert "sync_executor is required" in str(e.value)

    @pytest.mark.asyncio
    async def test_gather_partials_async(self, cs_async):
        """
        Test async version of CrossSync.gather_partials()
        """
        async def coro(i):
            return i + 1

        partials = [functools.partial(coro, i) for i in range(5)]
        results = await cs_async.gather_partials(partials)
        assert results == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_gather_partials_async_with_exceptions(self, cs_async):
        """
        Test async version of CrossSync.gather_partials() with exceptions
        """
        async def coro(i):
            return i + 1 if i != 3 else 1/0

        partials = [functools.partial(coro, i) for i in range(5)]
        with pytest.raises(ZeroDivisionError):
            await cs_async.gather_partials(partials)

    @pytest.mark.asyncio
    async def test_gather_partials_async_return_exceptions(self, cs_async):
        """
        Test async version of CrossSync.gather_partials() with return_exceptions=True
        """
        async def coro(i):
            return i + 1 if i != 3 else 1/0

        partials = [functools.partial(coro, i) for i in range(5)]
        results = await cs_async.gather_partials(partials, return_exceptions=True)
        assert len(results) == 5
        assert results[0] == 1
        assert results[1] == 2
        assert results[2] == 3
        assert isinstance(results[3], ZeroDivisionError)
        assert results[4] == 5

    @pytest.mark.asyncio
    async def test_gather_partials_async_uses_asyncio_gather(self, cs_async):
        """
        CrossSync.gather_partials() should use asyncio.gather() internally
        """
        async def coro(i):
            return i + 1

        return_exceptions=object()
        partials = [functools.partial(coro, i) for i in range(5)]
        with mock.patch.object(asyncio, "gather", mock.AsyncMock()) as gather:
            await cs_async.gather_partials(partials, return_exceptions=return_exceptions)
            gather.assert_called_once()
            found_args, found_kwargs = gather.call_args
            assert found_kwargs["return_exceptions"] == return_exceptions
            for coro in found_args:
                await coro

    def test_event_wait_passthrough(self, cs_sync):
        """
        Test sync version of CrossSync.event_wait()
        should pass through timeout directly to the event.wait() call
        """
        event = mock.Mock()
        timeout = object()
        cs_sync.event_wait(event, timeout)
        event.wait.assert_called_once_with(timeout=timeout)

    @pytest.mark.parametrize("timeout", [0, 0.01, 0.05])
    def test_event_wait_timeout_exceeded(self, cs_sync, timeout):
        """
        Test sync version of CrossSync.event_wait()
        """
        event = threading.Event()
        start_time = time.monotonic()
        cs_sync.event_wait(event, timeout=timeout)
        end_time = time.monotonic()
        assert abs((end_time - start_time) - timeout) < 0.01

    def test_event_wait_already_set(self, cs_sync):
        """
        if event is already set, do not block
        """
        event = threading.Event()
        event.set()
        start_time = time.monotonic()
        cs_sync.event_wait(event, timeout=10)
        end_time = time.monotonic()
        assert end_time - start_time < 0.01

    @pytest.mark.parametrize("break_early", [True, False])
    @pytest.mark.asyncio
    async def test_event_wait_async(self, cs_async, break_early):
        """
        With no timeout, call event.wait() with no arguments
        """
        event = mock.AsyncMock()
        await cs_async.event_wait(event, async_break_early=break_early)
        event.wait.assert_called_once_with()


    @pytest.mark.asyncio
    async def test_event_wait_async_with_timeout(self, cs_async):
        """
        In with timeout set, should call event.wait(), wrapped in wait_for()
        for the timeout
        """
        event = mock.Mock()
        event.wait.return_value = object()
        timeout = object()
        with mock.patch.object(asyncio, "wait_for", mock.AsyncMock()) as wait_for:
            await cs_async.event_wait(event, timeout=timeout)
            assert wait_for.await_count == 1
            assert wait_for.call_count == 1
            wait_for.assert_called_once_with(event.wait(), timeout=timeout)

    @pytest.mark.asyncio
    async def test_event_wait_async_timeout_exceeded(self, cs_async):
        """
        If tiemout exceeded, break without throwing exception
        """
        event = asyncio.Event()
        timeout = 0.5
        start_time = time.monotonic()
        await cs_async.event_wait(event, timeout=timeout)
        end_time = time.monotonic()
        assert abs((end_time - start_time) - timeout) < 0.01

    @pytest.mark.parametrize("break_early", [True, False])
    @pytest.mark.asyncio
    async def test_event_wait_async_already_set(self, cs_async, break_early):
        """
        if event is already set, return immediately
        """
        event = mock.AsyncMock()
        event.is_set = lambda: True
        start_time = time.monotonic()
        await cs_async.event_wait(event, async_break_early=break_early)
        end_time = time.monotonic()
        assert abs(end_time - start_time) < 0.01

    @pytest.mark.asyncio
    async def test_event_wait_no_break_early(self, cs_async):
        """
        if async_break_early is False, and the event is not set,
        simply sleep for the timeout
        """
        event = mock.Mock()
        event.is_set.return_value = False
        timeout = object()
        with mock.patch.object(asyncio, "sleep", mock.AsyncMock()) as sleep:
            await cs_async.event_wait(event, timeout=timeout, async_break_early=False)
            sleep.assert_called_once_with(timeout)
