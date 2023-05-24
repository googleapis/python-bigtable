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
from google.cloud.bigtable.row_filters import RowFilter

if TYPE_CHECKING:
    from google.cloud.bigtable.client import Table  # pragma: no cover

# Type alias used internally for readability.
_row_key_type = bytes


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

    queue: asyncio.Queue[tuple[_row_key_type, list[Mutation]]]
    conditional_queues: dict[RowFilter, tuple[list[Mutation], list[Mutation]]]

    MB_SIZE = 1024 * 1024

    def __init__(
        self,
        table: "Table",
        flush_count: int = 100,
        flush_size_bytes: int = 100 * MB_SIZE,
        max_mutation_bytes: int = 20 * MB_SIZE,
        flush_interval: int = 5,
        metadata: list[tuple[str, str]] | None = None,
    ):
        raise NotImplementedError

    async def append(self, row_key: str | bytes, mutation: Mutation | list[Mutation]):
        """
        Add a new mutation to the internal queue
        """
        raise NotImplementedError

    async def append_conditional(
        self,
        predicate_filter: RowFilter,
        row_key: str | bytes,
        if_true_mutations: Mutation | list[Mutation] | None = None,
        if_false_mutations: Mutation | list[Mutation] | None = None,
    ):
        """
        Apply a different set of mutations based on the outcome of predicate_filter

        Calls check_and_mutate_row internally on flush
        """
        raise NotImplementedError

    async def flush(self):
        """
        Send queue over network in as few calls as possible

        Raises:
        - MutationsExceptionGroup if any mutation in the batch fails
        """
        raise NotImplementedError

    async def __aenter__(self):
        """For context manager API"""
        raise NotImplementedError

    async def __aexit__(self, exc_type, exc, tb):
        """For context manager API"""
        raise NotImplementedError

    async def close(self):
        """
        Flush queue and clean up resources
        """
        raise NotImplementedError
