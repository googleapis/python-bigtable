import typing
import asyncio
import pytest
import threading
import concurrent.futures
import time
import queue
from google import api_core
from google.cloud.bigtable.data._sync.cross_sync.cross_sync import CrossSync, T


class TestCrossSync:

    @pytest.mark.parametrize(
        "attr, async_version, sync_version", [
            ("is_async", True, False),
            ("sleep", asyncio.sleep, time.sleep),
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
    def test_alias_attributes(self, attr, async_version, sync_version):
        """
        Test basic alias attributes, to ensure they point to the right place
        in both sync and async versions.
        """
        assert getattr(CrossSync, attr) == async_version, f"Failed async version for {attr}"
        assert getattr(CrossSync._Sync_Impl, attr) == sync_version, f"Failed sync version for {attr}"

