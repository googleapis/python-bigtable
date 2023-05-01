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

import asyncio
from typing import TYPE_CHECKING

from google.cloud.bigtable.mutations import Mutation
from google.cloud.bigtable.mutations import BulkMutationsEntry
from google.cloud.bigtable.exceptions import MutationsExceptionGroup

if TYPE_CHECKING:
    from google.cloud.bigtable.client import Table  # pragma: no cover


class _BatchMutationsQueue(asyncio.Queue[BulkMutationsEntry]):
    """
    asyncio.Queue subclass that tracks the size and number of mutations
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mutation_count = 0
        self._mutation_bytes_size = 0

    @property
    def mutation_count(self):
        return self._mutation_count

    @mutation_count.setter
    def mutation_count(self, value):
        if value < 0:
            raise ValueError("Mutation count cannot be negative")
        self._mutation_count = value

    @property
    def mutation_bytes_size(self):
        return self._mutation_bytes_size

    @mutation_bytes_size.setter
    def mutation_bytes_size(self, value):
        if value < 0:
            raise ValueError("Mutation bytes size cannot be negative")
        self._mutation_bytes_size = value

    def put_nowait(self, item:BulkMutationsEntry):
        super().put_nowait(item)
        self.mutation_count += len(item.mutations)
        self.mutation_bytes_size += item.size()

    def get_nowait(self):
        item = super().get_nowait()
        self.mutation_count -= len(item.mutations)
        self.mutation_bytes_size -= item.size()
        return item

    async def put(self, item:BulkMutationsEntry):
        await super().put(item)
        self.mutation_count += len(item.mutations)
        self.mutation_bytes_size += item.size()

    async def get(self):
        item = await super().get()
        self.mutation_count -= len(item.mutations)
        self.mutation_bytes_size -= item.size()
        return item


class MutationsBatcher:
    """
    Allows users to send batches using context manager API:

    Runs mutate_row,  mutate_rows, and check_and_mutate_row internally, combining
    to use as few network requests as required

    Flushes:
      - manually
      - every flush_interval seconds
      - after queue reaches flush_count in quantity
      - after queue reaches flush_size_bytes in storage size
      - when batcher is closed or destroyed

    async with table.mutations_batcher() as batcher:
       for i in range(10):
         batcher.add(row, mut)
    """

    MB_SIZE = 1024 * 1024

    def __init__(
        self,
        table: "Table",
        flush_limit_count: int = 100,
        flush_limit_bytes: int = 100 * MB_SIZE,
        max_mutation_bytes: int = 20 * MB_SIZE,
        flush_interval: float = 5,
    ):
        self.closed : bool = False
        self._queue : _BatchMutationsQueue = _BatchMutationsQueue()
        self._table : "Table" = table
        self._max_mutation_bytes = max_mutation_bytes
        self._flush_limit_bytes = flush_limit_bytes
        self._flush_limit_count = flush_limit_count
        self.exceptions = []
        self._flush_timer_task : asyncio.Task[None] = asyncio.create_task(self._flush_timer(flush_interval))
        self._flush_tasks : list[asyncio.Task[None]] = []

    async def _flush_timer(self, interval:float):
        """
        Flush queue on a timer
        """
        while not self.closed:
            await asyncio.sleep(interval)
            # add new flush task to list
            if not self.closed:
                new_task = asyncio.create_task(self.flush(timeout=None, raise_exceptions=False))
                self._flush_tasks.append(new_task)

    async def append(self, mutations:BulkMutationsEntry):
        """
        Add a new mutation to the internal queue
        """
        if self.closed:
            raise RuntimeError("Cannot append to closed MutationsBatcher")
        size = mutations.size()
        if size > self._max_mutation_bytes:
            raise ValueError(f"Mutation size exceeds max_mutation_bytes: {size} > {self._max_mutation_bytes}")
        await self._queue.put(mutations)
        if self._queue.mutation_bytes_size > self._flush_limit_bytes or self.mutation_count > self._flush_limit_count:
            # start a new flush task
            self._flush_tasks.append(asyncio.create_task(self.flush(timeout=None, raise_exceptions=False)))

    async def flush(self, *, timeout: float | None = 5.0, raise_exceptions=True):
        """
        Send queue over network in as few calls as possible

        Args:
          - timeout: operation_timeout for underlying rpc, in seconds
          - raise_exceptions: if True, raise MutationsExceptionGroup if any mutations fail. If False,
              exceptions are saved in self.exceptions and raised on close()
        Raises:
        - MutationsExceptionGroup if raise_exceptions is True and any mutations fail
        """
        entries : list[BulkMutationsEntry] = []
        # reset queue
        while not self._queue.empty():
            entries.append(await self._queue.get())
        if entries:
            try:
                await self._table.bulk_mutate_rows(entries, operation_timeout=timeout, per_request_timeout=timeout)
            except MutationsExceptionGroup as e:
                if raise_exceptions:
                    raise e
                else:
                    self.exceptions.extend(e.exceptions)

    async def __aenter__(self):
        """For context manager API"""
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """For context manager API"""
        await self.close()

    async def close(self, timeout: float = 5.0):
        """
        Flush queue and clean up resources
        """
        self.closed = True
        final_flush = self.flush(timeout=timeout, raise_exceptions=False)
        finalize_tasks = asyncio.wait_for(asyncio.gather(*self._flush_tasks), timeout=timeout)
        self._flush_timer_task.cancel()
        # wait for all to finish
        await asyncio.gather([final_flush, self._flush_timer_task, finalize_tasks])
        self._flush_tasks = []
        if self.exceptions:
            # TODO: deal with indices
            raise MutationsExceptionGroup(self.exceptions)

