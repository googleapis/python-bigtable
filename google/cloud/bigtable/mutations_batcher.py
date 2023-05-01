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
        flush_count: int = 100,
        flush_size_bytes: int = 100 * MB_SIZE,
        max_mutation_bytes: int = 20 * MB_SIZE,
        flush_interval: float = 5,
        metadata: list[tuple[str, str]] | None = None,
    ):
        self._queue_map : dict[row_key, list[Mutation]] = {}
        self._table = table

    async def append(self, row_key: str | bytes, mutations: Mutation | list[Mutation]):
        """
        Add a new mutation to the internal queue
        """
        if isinstance(mutations, Mutation):
            mutations = [mutations]
        if isinstance(row_key, str):
            row_key = row_key.encode("utf-8")
        existing_mutations = self._queue_map.setdefault(row_key, [])
        existing_mutations.extend(mutations)

    async def flush(self):
        """
        Send queue over network in as few calls as possible

        Raises:
        - MutationsExceptionGroup if any mutation in the batch fails
        """
        entries : list[BulkMutationsEntry] = []
        for key, mutations in self._queue_map.items():
            entries.append(BulkMutationsEntry(key, mutations))
        await self._table.bulk_mutate_rows(entries)

    async def __aenter__(self):
        """For context manager API"""
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """For context manager API"""
        await self.close()

    async def close(self):
        """
        Flush queue and clean up resources
        """
        await self.flush()
