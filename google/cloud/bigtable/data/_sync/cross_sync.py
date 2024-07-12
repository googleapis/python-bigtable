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
import time
import threading
import queue

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


class AstDecorator:
    """
    Helper class for CrossSync decorators used for guiding ast transformations.

    These decorators provide arguments that are used during the code generation process,
    but act as no-ops when encountered in live code

    Args:
        attr_name: name of the attribute to attach to the CrossSync class 
            e.g. pytest for CrossSync.pytest
        required_keywords: list of required keyword arguments for the decorator.
            If the decorator is used without these arguments, a ValueError is
            raised during code generation
        async_impl: If given, the async code will apply this decorator to its
            wrapped function at runtime. If not given, the decorator will be a no-op
        **default_kwargs: any kwargs passed define the valid arguments when using the decorator.
            The value of each kwarg is the default value for the argument.
    """

    def __init__(
        self,
        attr_name,
        required_keywords=(),
        async_impl=None,
        **default_kwargs,
    ):
        self.name = attr_name
        self.required_kwargs = required_keywords
        self.default_kwargs = default_kwargs
        self.all_valid_keys = [*required_keywords, *default_kwargs.keys()]
        self.async_impl = async_impl

    def __call__(self, *args, **kwargs):
        """
        Called when the decorator is used in code.

        Returns a no-op decorator function, or applies the async_impl decorator
        """
        # raise error if invalid kwargs are passed
        for kwarg in kwargs:
            if kwarg not in self.all_valid_keys:
                raise ValueError(f"Invalid keyword argument: {kwarg}")
        # if async_impl is provided, use the given decorator function
        if self.async_impl:
            return self.async_impl(**{**self.default_kwargs, **kwargs})
        # if no arguments, args[0] will hold the function to be decorated
        # return the function as is
        if len(args) == 1 and callable(args[0]):
            return args[0]

        # if arguments are provided, return a no-op decorator function
        def decorator(func):
            return func

        return decorator

    def parse_ast_keywords(self, node):
        """
        When this decorator is encountered in the ast during sync generation, parse the
        keyword arguments back from ast nodes to python primitives

        Return a full set of kwargs, using default values for missing arguments
        """
        got_kwargs = (
            {kw.arg: self._convert_ast_to_py(kw.value) for kw in node.keywords}
            if hasattr(node, "keywords")
            else {}
        )
        for key in got_kwargs.keys():
            if key not in self.all_valid_keys:
                raise ValueError(f"Invalid keyword argument: {key}")
        for key in self.required_kwargs:
            if key not in got_kwargs:
                raise ValueError(f"Missing required keyword argument: {key}")
        return {**self.default_kwargs, **got_kwargs}

    def _convert_ast_to_py(self, ast_node):
        """
        Helper to convert ast primitives to python primitives. Used when unwrapping kwargs
        """
        import ast

        if isinstance(ast_node, ast.Constant):
            return ast_node.value
        if isinstance(ast_node, ast.List):
            return [self._convert_ast_to_py(node) for node in ast_node.elts]
        if isinstance(ast_node, ast.Dict):
            return {
                self._convert_ast_to_py(k): self._convert_ast_to_py(v)
                for k, v in zip(ast_node.keys, ast_node.values)
            }
        raise ValueError(f"Unsupported type {type(ast_node)}")

    def _node_eq(self, node):
        """
        Check if the given ast node is a call to this decorator
        """
        import ast

        if "CrossSync" in ast.dump(node):
            decorator_type = node.func.attr if hasattr(node, "func") else node.attr
            if decorator_type == self.name:
                return True
        return False

    def __eq__(self, other):
        """
        Helper to support == comparison with ast nodes
        """
        return self._node_eq(other)


class _DecoratorMeta(type):
    """
    Metaclass to attach AstDecorator objects in internal self._decorators
    as attributes
    """

    def __getattr__(self, name):
        for decorator in self._decorators:
            if name == decorator.name:
                return decorator
        raise AttributeError(f"CrossSync has no attribute {name}")


class CrossSync(metaclass=_DecoratorMeta):
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

    _decorators: list[AstDecorator] = [
        AstDecorator("export_sync",  # decorate classes to convert
            required_keywords=["path"],  # otput path for generated sync class
            replace_symbols={},  # replace specific symbols across entire class
            mypy_ignore=(),  # set of mypy error codes to ignore in output file
            include_file_imports=True  # when True, import statements from top of file will be included in output file
        ),
        AstDecorator("convert",  # decorate methods to convert from async to sync
            sync_name=None,  # use a new name for the sync class
            replace_symbols={},  # replace specific symbols within the function
        ),
        AstDecorator("drop_method"),  # decorate methods to drop in sync version of class
        AstDecorator(
            "pytest", async_impl=pytest_mark_asyncio
        ),  # decorate test methods to run with pytest-asyncio
        AstDecorator(
            "pytest_fixture",  # decorate test methods to run with pytest fixture
            async_impl=pytest_asyncio_fixture,
            scope="function",
            params=None,
            autouse=False,
            ids=None,
            name=None,
        ),
    ]

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
