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
from google.cloud.bigtable.row import row_key
from google.cloud.bigtable.row_filters import RowFilter

if TYPE_CHECKING:
    from google.cloud.bigtable.client import Table  # pragma: no cover


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
        self._queue : asyncio.Queue[BulkMutationsEntry] = asyncio.Queue()
        self._table : "Table" = table
        self._max_mutation_bytes = max_mutation_bytes
        self._flush_limit_bytes = flush_limit_bytes
        self._flush_limit_count = flush_limit_count
        self._queued_size = 0
        self._queued_count = 0
        self._flush_timer_task : asyncio.Task[None] = asyncio.create_task(self._flush_timer(flush_interval))
        self._flush_tasks : list[asyncio.Task[None]] = []

    async def _flush_timer(self, interval:float):
        """
        Flush queue on a timer
        """
        while True:
            await asyncio.sleep(interval)
            await self.flush()

    async def append(self, row_key: str | bytes, mutations: Mutation | list[Mutation]):
        """
        Add a new mutation to the internal queue
        """
        if isinstance(mutations, Mutation):
            mutations = [mutations]
        if isinstance(row_key, str):
            row_key = row_key.encode("utf-8")
        total_size = 0
        for idx, m in enumerate(mutations):
            size = m.size()
            if size > self._max_mutation_bytes:
                raise ValueError(f"Mutation {idx} exceeds max mutation size: {m.size()} > {self._max_mutation_bytes}")
            total_size += size
        new_batch = BulkMutationsEntry(row_key, mutations)
        await self._queue.put(new_batch)
        self._queued_size += total_size
        self._queued_count += len(mutations)
        if self._queued_size > self._flush_limit_bytes or self._queued_count > self._flush_limit_count:
            # start a new flush task
            self._flush_tasks.append(asyncio.create_task(self.flush()))

    async def flush(self):
        """
        Send queue over network in as few calls as possible

        Raises:
        - MutationsExceptionGroup if any mutation in the batch fails
        """
        entries : list[BulkMutationsEntry] = []
        # reset queue
        while not self._queue.empty():
            entries.append(await self._queue.get())
        self._queued_size = 0
        self._queued_count = 0
        if entries:
            await self._table.bulk_mutate_rows(entries)

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
        await self.flush()
        self._flush_timer_task.cancel()
        for task in self._flush_tasks:
            task.cancel()
        group = asyncio.gather([*self._flush_tasks, self._flush_timer_task], return_exceptions=True)
        self._flush_tasks = []
        await asyncio.wait_for(group, timeout=timeout)


