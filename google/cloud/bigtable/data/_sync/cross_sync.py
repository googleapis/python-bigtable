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
#
from __future__ import annotations

from typing import (
    TypeVar,
    Any,
    Callable,
    Coroutine,
    Sequence,
    Union,
    AsyncIterable,
    AsyncIterator,
    AsyncGenerator,
    TYPE_CHECKING,
)
import typing

import asyncio
import sys
import concurrent.futures
import google.api_core.retry as retries
import queue
import threading
import time
from .cross_sync_decorators import (
    AstDecorator,
    ExportSync,
    Convert,
    DropMethod,
    Pytest,
    PytestFixture,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

T = TypeVar("T")


class CrossSync:
    # support CrossSync.is_async to check if the current environment is async
    is_async = True

    # provide aliases for common async functions and types
    sleep = asyncio.sleep
    retry_target = retries.retry_target_async
    retry_target_stream = retries.retry_target_stream_async
    Retry = retries.AsyncRetry
    Queue: TypeAlias = asyncio.Queue
    Condition: TypeAlias = asyncio.Condition
    Future: TypeAlias = asyncio.Future
    Task: TypeAlias = asyncio.Task
    Event: TypeAlias = asyncio.Event
    Semaphore: TypeAlias = asyncio.Semaphore
    StopIteration: TypeAlias = StopAsyncIteration
    # provide aliases for common async type annotations
    Awaitable: TypeAlias = typing.Awaitable
    Iterable: TypeAlias = AsyncIterable
    Iterator: TypeAlias = AsyncIterator
    Generator: TypeAlias = AsyncGenerator

    # decorators
    export_sync = ExportSync.decorator  # decorate classes to convert
    convert = Convert.decorator  # decorate methods to convert from async to sync
    drop_method = DropMethod.decorator  # decorate methods to remove from sync version
    pytest = Pytest.decorator  # decorate test methods to run with pytest-asyncio
    pytest_fixture = (
        PytestFixture.decorator
    )  # decorate test methods to run with pytest fixture

    # list of attributes that can be added to the CrossSync class at runtime
    _runtime_replacements: set[Any] = set()

    @classmethod
    def add_mapping(cls, name, value):
        """
        Add a new attribute to the CrossSync class, for replacing library-level symbols

        Raises:
            - AttributeError if the attribute already exists with a different value
        """
        if not hasattr(cls, name):
            cls._runtime_replacements.add(name)
        elif value != getattr(cls, name):
            raise AttributeError(f"Conflicting assignments for CrossSync.{name}")
        setattr(cls, name, value)

    @classmethod
    def Mock(cls, *args, **kwargs):
        """
        Alias for AsyncMock, importing at runtime to avoid hard dependency on mock
        """
        try:
            from unittest.mock import AsyncMock  # type: ignore
        except ImportError:  # pragma: NO COVER
            from mock import AsyncMock  # type: ignore
        return AsyncMock(*args, **kwargs)

    @staticmethod
    async def gather_partials(
        partial_list: Sequence[Callable[[], Awaitable[T]]],
        return_exceptions: bool = False,
        sync_executor: concurrent.futures.ThreadPoolExecutor | None = None,
    ) -> list[T | BaseException]:
        """
        abstraction over asyncio.gather, but with a set of partial functions instead
        of coroutines, to work with sync functions.
        To use gather with a set of futures instead of partials, use CrpssSync.wait

        In the async version, the partials are expected to return an awaitable object. Patials
        are unpacked and awaited in the gather call.

        Sync version implemented with threadpool executor

        Returns:
          - a list of results (or exceptions, if return_exceptions=True) in the same order as partial_list
        """
        if not partial_list:
            return []
        awaitable_list = [partial() for partial in partial_list]
        return await asyncio.gather(
            *awaitable_list, return_exceptions=return_exceptions
        )

    @staticmethod
    async def wait(
        futures: Sequence[CrossSync.Future[T]], timeout: float | None = None
    ) -> tuple[set[CrossSync.Future[T]], set[CrossSync.Future[T]]]:
        """
        abstraction over asyncio.wait

        Return:
            - a tuple of (done, pending) sets of futures
        """
        if not futures:
            return set(), set()
        return await asyncio.wait(futures, timeout=timeout)

    @staticmethod
    async def condition_wait(
        condition: CrossSync.Condition, timeout: float | None = None
    ) -> bool:
        """
        abstraction over asyncio.Condition.wait

        returns False if the timeout is reached before the condition is set, otherwise True
        """
        try:
            await asyncio.wait_for(condition.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @staticmethod
    async def event_wait(
        event: CrossSync.Event,
        timeout: float | None = None,
        async_break_early: bool = True,
    ) -> None:
        """
        abstraction over asyncio.Event.wait

        Args:
            - event: event to wait for
            - timeout: if set, will break out early after `timeout` seconds
            - async_break_early: if False, the async version will wait for
                the full timeout even if the event is set before the timeout.
                This avoids creating a new background task
        """
        if timeout is None:
            await event.wait()
        elif not async_break_early:
            await asyncio.sleep(timeout)
        else:
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass

    @staticmethod
    def create_task(
        fn: Callable[..., Coroutine[Any, Any, T]],
        *fn_args,
        sync_executor: concurrent.futures.ThreadPoolExecutor | None = None,
        task_name: str | None = None,
        **fn_kwargs,
    ) -> CrossSync.Task[T]:
        """
        abstraction over asyncio.create_task. Sync version implemented with threadpool executor

        sync_executor: ThreadPoolExecutor to use for sync operations. Ignored in async version
        """
        task: CrossSync.Task[T] = asyncio.create_task(fn(*fn_args, **fn_kwargs))
        if task_name and sys.version_info >= (3, 8):
            task.set_name(task_name)
        return task

    @staticmethod
    async def yield_to_event_loop() -> None:
        """
        Call asyncio.sleep(0) to yield to allow other tasks to run
        """
        await asyncio.sleep(0)

    @staticmethod
    def verify_async_event_loop() -> None:
        """
        Raises RuntimeError if the event loop is not running
        """
        asyncio.get_running_loop()
