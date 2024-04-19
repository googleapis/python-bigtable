import asyncio
import sys


class CrossSync:

    SyncImports = False
    is_async = True

    sleep = asyncio.sleep
    Queue = asyncio.Queue
    Condition = asyncio.Condition
    Future = asyncio.Future
    Task = asyncio.Task
    Event = asyncio.Event

    @classmethod
    def sync_output(cls, sync_path):
        # return the async class unchanged
        def decorator(async_cls):
            async_cls.cross_sync_enabled = True
            async_cls.cross_sync_import_path = sync_path
            async_cls.cross_sync_class_name = sync_path.rsplit('.', 1)[-1]
            async_cls.cross_sync_file_path = "/".join(sync_path.split(".")[:-1]) + ".py"
            return async_cls
        return decorator

    @staticmethod
    async def gather_partials(partial_list, return_exceptions=False, sync_executor=None):
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
        return await asyncio.gather(*awaitable_list, return_exceptions=return_exceptions)

    @staticmethod
    async def wait(futures, timeout=None):
        """
        abstraction over asyncio.wait
        """
        if not futures:
            return set(), set()
        return await asyncio.wait(futures, timeout=timeout)

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
