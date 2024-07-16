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
from .cross_sync_decorators import AstDecorator, ExportSync, Convert, DropMethod, Pytest, PytestFixture

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

T = TypeVar("T")


def pytest_mark_asyncio(func):
    """
    Applies pytest.mark.asyncio to a function if pytest is installed, otherwise
    returns the function as is

    Used to support CrossSync.pytest decorator, without requiring pytest to be installed
    """
    try:
        import pytest

        return pytest.mark.asyncio(func)
    except ImportError:
        return func


def pytest_asyncio_fixture(*args, **kwargs):
    """
    Applies pytest.fixture to a function if pytest is installed, otherwise
    returns the function as is

    Used to support CrossSync.pytest_fixture decorator, without requiring pytest to be installed
    """
    import pytest_asyncio  # type: ignore

    def decorator(func):
        return pytest_asyncio.fixture(*args, **kwargs)(func)

    return decorator


def export_sync_impl(*args, **kwargs):
    """
    Decorator implementation for CrossSync.export_sync

    When a called with add_mapping_for_name, CrossSync.add_mapping is called to
    register the name as a CrossSync attribute
    """
    new_mapping = kwargs.pop("add_mapping_for_name", None)

    def decorator(cls):
        if new_mapping:
            # add class to mappings if requested
            CrossSync.add_mapping(new_mapping, cls)
        return cls

    return decorator


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
    export_sync = ExportSync.decorator # decorate classes to convert
    convert = Convert.decorator  # decorate methods to convert from async to sync
    drop_method = DropMethod.decorator  # decorate methods to remove from sync version
    pytest = Pytest.decorator  # decorate test methods to run with pytest-asyncio
    pytest_fixture = PytestFixture.decorator  # decorate test methods to run with pytest fixture

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

    class _Sync_Impl:
        """
        Provide sync versions of the async functions and types in CrossSync
        """
        is_async = False

        sleep = time.sleep
        retry_target = retries.retry_target
        retry_target_stream = retries.retry_target_stream
        Retry = retries.Retry
        Queue: TypeAlias = queue.Queue
        Condition: TypeAlias = threading.Condition
        Future: TypeAlias = concurrent.futures.Future
        Task: TypeAlias = concurrent.futures.Future
        Event: TypeAlias = threading.Event
        Semaphore: TypeAlias = threading.Semaphore
        StopIteration: TypeAlias = StopIteration
        # type annotations
        Awaitable: TypeAlias = Union[T]
        Iterable: TypeAlias = typing.Iterable
        Iterator: TypeAlias = typing.Iterator
        Generator: TypeAlias = typing.Generator

        _runtime_replacements: set[Any] = set()

        @classmethod
        def add_mapping_decorator(cls, name):
            def decorator(wrapped_cls):
                cls.add_mapping(name, wrapped_cls)
                return wrapped_cls
            return decorator

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
            # try/except added for compatibility with python < 3.8
            try:
                from unittest.mock import Mock
            except ImportError:  # pragma: NO COVER
                from mock import Mock  # type: ignore
            return Mock(*args, **kwargs)

        @staticmethod
        def wait(
            futures: Sequence[CrossSync._Sync_Impl.Future[T]],
            timeout: float | None = None,
        ) -> tuple[
            set[CrossSync._Sync_Impl.Future[T]], set[CrossSync._Sync_Impl.Future[T]]
        ]:
            """
            abstraction over asyncio.wait
            """
            if not futures:
                return set(), set()
            return concurrent.futures.wait(futures, timeout=timeout)

        @staticmethod
        def condition_wait(
            condition: CrossSync._Sync_Impl.Condition, timeout: float | None = None
        ) -> bool:
            """
            returns False if the timeout is reached before the condition is set, otherwise True
            """
            return condition.wait(timeout=timeout)

        @staticmethod
        def event_wait(
            event: CrossSync._Sync_Impl.Event,
            timeout: float | None = None,
            async_break_early: bool = True,
        ) -> None:
            event.wait(timeout=timeout)

        @staticmethod
        def gather_partials(
            partial_list: Sequence[Callable[[], T]],
            return_exceptions: bool = False,
            sync_executor: concurrent.futures.ThreadPoolExecutor | None = None,
        ) -> list[T | BaseException]:
            if not partial_list:
                return []
            if not sync_executor:
                raise ValueError("sync_executor is required for sync version")
            futures_list = [sync_executor.submit(partial) for partial in partial_list]
            results_list: list[T | BaseException] = []
            for future in futures_list:
                found_exc = future.exception()
                if found_exc is not None:
                    if return_exceptions:
                        results_list.append(found_exc)
                    else:
                        raise found_exc
                else:
                    results_list.append(future.result())
            return results_list

        @staticmethod
        def create_task(
            fn: Callable[..., T],
            *fn_args,
            sync_executor: concurrent.futures.ThreadPoolExecutor | None = None,
            task_name: str | None = None,
            **fn_kwargs,
        ) -> CrossSync._Sync_Impl.Task[T]:
            """
            abstraction over asyncio.create_task. Sync version implemented with threadpool executor

            sync_executor: ThreadPoolExecutor to use for sync operations. Ignored in async version
            """
            if not sync_executor:
                raise ValueError("sync_executor is required for sync version")
            return sync_executor.submit(fn, *fn_args, **fn_kwargs)

        @staticmethod
        def yield_to_event_loop() -> None:
            """
            No-op for sync version
            """
            pass

        @staticmethod
        def verify_async_event_loop() -> None:
            """
            No-op for sync version
            """
            pass
