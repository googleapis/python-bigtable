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

from typing_extensions import TypeAlias

import asyncio
import sys
import concurrent.futures
import google.api_core.retry as retries
import time
import threading
import queue


class _AsyncGetAttr(type):
    def __getitem__(cls, item):
        return item


class _SyncGetAttr(type):
    def __getitem__(cls, item):
        breakpoint()
        return CrossSync.generated_replacements[item]


class CrossSync(metaclass=_AsyncGetAttr):
    SyncImports = False
    is_async = True

    sleep = asyncio.sleep
    retry_target = retries.retry_target_async
    retry_target_stream = retries.retry_target_stream_async
    Queue: TypeAlias = asyncio.Queue
    Condition: TypeAlias = asyncio.Condition
    Future: TypeAlias = asyncio.Future
    Task: TypeAlias = asyncio.Task
    Event: TypeAlias = asyncio.Event

    generated_replacements: dict[type, str] = {}

    @staticmethod
    def rename_sync(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    @classmethod
    def sync_output(cls, sync_path, replace_symbols=None):
        replace_symbols = replace_symbols or {}

        # return the async class unchanged
        def decorator(async_cls):
            cls.generated_replacements[async_cls] = sync_path
            async_cls.cross_sync_enabled = True
            async_cls.cross_sync_import_path = sync_path
            async_cls.cross_sync_class_name = sync_path.rsplit(".", 1)[-1]
            async_cls.cross_sync_file_path = "/".join(sync_path.split(".")[:-1]) + ".py"
            async_cls.cross_sync_replace_symbols = replace_symbols
            return async_cls

        return decorator

    @staticmethod
    async def gather_partials(
        partial_list, return_exceptions=False, sync_executor=None
    ):
        """
        abstraction over asyncio.gather

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
    async def wait(futures, timeout=None):
        """
        abstraction over asyncio.wait
        """
        if not futures:
            return set(), set()
        return await asyncio.wait(futures, timeout=timeout)

    @staticmethod
    async def condition_wait(condition, timeout=None):
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
    def create_task(fn, *fn_args, sync_executor=None, task_name=None, **fn_kwargs):
        """
        abstraction over asyncio.create_task. Sync version implemented with threadpool executor

        sync_executor: ThreadPoolExecutor to use for sync operations. Ignored in async version
        """
        task = asyncio.create_task(fn(*fn_args, **fn_kwargs))
        if task_name and sys.version_info >= (3, 8):
            task.set_name(task_name)
        return task

    @staticmethod
    async def yield_to_event_loop():
        """
        Call asyncio.sleep(0) to yield to allow other tasks to run
        """
        await asyncio.sleep(0)

    class _Sync_Impl(metaclass=_SyncGetAttr):
        is_async = False

        sleep = time.sleep
        retry_target = retries.retry_target
        retry_target_stream = retries.retry_target_stream
        Queue: TypeAlias = queue.Queue
        Condition: TypeAlias = threading.Condition
        Future: TypeAlias = concurrent.futures.Future
        Task: TypeAlias = concurrent.futures.Future
        Event: TypeAlias = threading.Event

        generated_replacements: dict[type, str] = {}

        @staticmethod
        def wait(futures, timeout=None):
            """
            abstraction over asyncio.wait
            """
            if not futures:
                return set(), set()
            return concurrent.futures.wait(futures, timeout=timeout)

        @staticmethod
        def condition_wait(condition, timeout=None):
            """
            returns False if the timeout is reached before the condition is set, otherwise True
            """
            return condition.wait(timeout=timeout)

        @staticmethod
        def gather_partials(partial_list, return_exceptions=False, sync_executor=None):
            if not partial_list:
                return []
            if not sync_executor:
                raise ValueError("sync_executor is required for sync version")
            futures_list = [sync_executor.submit(partial) for partial in partial_list]
            results_list = []
            for future in futures_list:
                if future.exception():
                    if return_exceptions:
                        results_list.append(future.exception())
                    else:
                        raise future.exception()
                else:
                    results_list.append(future.result())
            return results_list

        @staticmethod
        def create_task(fn, *fn_args, sync_executor=None, task_name=None, **fn_kwargs):
            """
            abstraction over asyncio.create_task. Sync version implemented with threadpool executor

            sync_executor: ThreadPoolExecutor to use for sync operations. Ignored in async version
            """
            if not sync_executor:
                raise ValueError("sync_executor is required for sync version")
            return sync_executor.submit(fn, *fn_args, **fn_kwargs)

        @staticmethod
        def yield_to_event_loop():
            pass
