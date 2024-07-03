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

from typing import TypeVar, Any, Awaitable, Callable, Coroutine, Sequence, Union, TYPE_CHECKING

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


class CrossSync:
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
    Semaphore: TypeAlias = asyncio.Semaphore
    Awaitable: TypeAlias = Awaitable

    generated_replacements: dict[type, str] = {}

    @staticmethod
    def convert(*, sync_name: str|None=None, replace_symbols: dict[str, str]|None=None):
        def decorator(func):
            return func

        return decorator

    @staticmethod
    def drop_method(func):
        return func

    @classmethod
    def sync_output(
        cls,
        sync_path: str,
        replace_symbols: dict["str", "str" | None ] | None = None,
        mypy_ignore: list[str] | None = None,
        include_file_imports: bool = True,
    ):
        replace_symbols = replace_symbols or {}
        mypy_ignore = mypy_ignore or []
        include_file_imports = include_file_imports

        # return the async class unchanged
        def decorator(async_cls):
            cls.generated_replacements[async_cls] = sync_path
            async_cls.cross_sync_enabled = True
            async_cls.cross_sync_import_path = sync_path
            async_cls.cross_sync_class_name = sync_path.rsplit(".", 1)[-1]
            async_cls.cross_sync_file_path = "/".join(sync_path.split(".")[:-1]) + ".py"
            async_cls.cross_sync_replace_symbols = replace_symbols
            async_cls.cross_sync_mypy_ignore = mypy_ignore
            async_cls.include_file_imports = include_file_imports
            return async_cls

        return decorator

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

    class _Sync_Impl:
        is_async = False

        sleep = time.sleep
        retry_target = retries.retry_target
        retry_target_stream = retries.retry_target_stream
        Queue: TypeAlias = queue.Queue
        Condition: TypeAlias = threading.Condition
        Future: TypeAlias = concurrent.futures.Future
        Task: TypeAlias = concurrent.futures.Future
        Event: TypeAlias = threading.Event
        Semaphore: TypeAlias = threading.Semaphore
        Awaitable: TypeAlias = Union[T]

        generated_replacements: dict[type, str] = {}

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
            pass

from dataclasses import dataclass, field
from typing import ClassVar
import ast

from google.cloud.bigtable.data._sync import transformers

@dataclass
class CrossSyncArtifact:
    file_path: str
    imports: list[ast.Import | ast.ImportFrom] = field(default_factory=list)
    converted_classes: dict[type, ast.ClassDef] = field(default_factory=dict)
    mypy_ignores: list[str] = field(default_factory=list)
    _instances: ClassVar[dict[str, CrossSyncArtifact]] = {}

    def __hash__(self):
        return hash(self.file_path)

    def render(self, with_black=True) -> str:
        full_str = (
            "# Copyright 2024 Google LLC\n"
            "#\n"
            '# Licensed under the Apache License, Version 2.0 (the "License");\n'
            '# you may not use this file except in compliance with the License.\n'
            '# You may obtain a copy of the License at\n'
            '#\n'
            '#     http://www.apache.org/licenses/LICENSE-2.0\n'
            '#\n'
            '# Unless required by applicable law or agreed to in writing, software\n'
            '# distributed under the License is distributed on an "AS IS" BASIS,\n'
            '# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n'
            '# See the License for the specific language governing permissions and\n'
            '# limitations under the License.\n'
            '#\n'
            '# This file is automatically generated by CrossSync. Do not edit manually.\n'
        )
        if self.mypy_ignores:
            full_str += f'\n# mypy: disable-error-code="{",".join(self.mypy_ignores)}"\n\n'
        full_str += "\n".join([ast.unparse(node) for node in self.imports])
        full_str += "\n\n"
        full_str += "\n".join([ast.unparse(node) for node in self.converted_classes.values()])
        if with_black:
            cleaned = black.format_str(autoflake.fix_code(full_str, remove_all_unused_imports=True), mode=black.FileMode())
            return cleaned
        else:
            return full_str

    @classmethod
    def get_for_path(cls, path: str) -> CrossSyncArtifact:
        if path not in cls._instances:
            cls._instances[path] = CrossSyncArtifact(path)
        return cls._instances[path]

    def add_class(self, cls):
        if cls in self.converted_classes:
            return
        crosssync_converter = transformers.SymbolReplacer({"CrossSync": "CrossSync._Sync_Impl"})
        # convert class
        cls_node = ast.parse(inspect.getsource(cls)).body[0]
        # update name
        cls_node.name = cls.cross_sync_class_name
        # remove cross_sync decorator
        if hasattr(cls_node, "decorator_list"):
            cls_node.decorator_list = [d for d in cls_node.decorator_list if not isinstance(d, ast.Call) or not isinstance(d.func, ast.Attribute) or not isinstance(d.func.value, ast.Name) or d.func.value.id != "CrossSync"]
        # do ast transformations
        converted = transformers.AsyncToSync().visit(cls_node)
        if cls.cross_sync_replace_symbols:
            converted = transformers.SymbolReplacer(cls.cross_sync_replace_symbols).visit(converted)
        converted = crosssync_converter.visit(converted)
        converted = transformers.HandleCrossSyncDecorators().visit(converted)
        self.converted_classes[cls] = converted
        # add imports for added class if required
        if cls.include_file_imports and not self.imports:
            with open(inspect.getfile(cls)) as f:
                full_ast = ast.parse(f.read())
                for node in full_ast.body:
                    if isinstance(node, (ast.Import, ast.ImportFrom, ast.If)):
                        self.imports.append(crosssync_converter.visit(node))
        # add mypy ignore if required
        if cls.cross_sync_mypy_ignore:
            self.mypy_ignores.extend(cls.cross_sync_mypy_ignore)

if __name__ == "__main__":
    import os
    import glob
    import importlib
    import inspect
    import itertools
    import black
    import autoflake
    # find all cross_sync decorated classes
    search_root = sys.argv[1]
    found_files = [path.replace("/", ".")[:-3] for path in glob.glob(search_root + "/**/*.py", recursive=True)]
    found_classes = list(itertools.chain.from_iterable([inspect.getmembers(importlib.import_module(path), inspect.isclass) for path in found_files]))
    file_obj_set = set()
    for name, cls in found_classes:
        if hasattr(cls, "cross_sync_enabled"):
            file_obj = CrossSyncArtifact.get_for_path(cls.cross_sync_file_path)
            file_obj.add_class(cls)
            file_obj_set.add(file_obj)
    for file_obj in file_obj_set:
        with open(file_obj.file_path, "w") as f:
            f.write(file_obj.render())


