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

from google.cloud.bigtable.mutations import BulkMutationsEntry
from google.cloud.bigtable.exceptions import MutationsExceptionGroup
from google.cloud.bigtable.exceptions import FailedMutationEntryError

from google.cloud.bigtable._mutate_rows import _mutate_rows_operation

if TYPE_CHECKING:
    from google.cloud.bigtable.client import Table  # pragma: no cover


class _FlowControl:
    """
    Manages underlying rpcs for MutationsBatcher. Ensures that in-flight requests
    stay within the configured limits (max_mutation_count, max_mutation_bytes).
    """

    def __init__(self, table, max_mutation_count:float|None, max_mutation_bytes:float|None):
        """
        Args:
          - table: Table object that performs rpc calls
          - max_mutation_count: maximum number of mutations to send in a single rpc.
             This corresponds to individual mutations in a single BulkMutationsEntry.
             If None, no limit is enforced.
          - max_mutation_bytes: maximum number of bytes to send in a single rpc.
             If None, no limit is enforced.
        """
        self.table = table
        if max_mutation_count is None:
            self.max_mutation_count = float("inf")
        if max_mutation_bytes is None:
            self.max_mutation_bytes = float("inf")
        self.max_mutation_count = max_mutation_count
        self.max_mutation_bytes = max_mutation_bytes
        self.available_mutation_count : asyncio.Semaphore = asyncio.Semaphore(max_mutation_count)
        self.available_mutation_bytes : asyncio.Semaphore = asyncio.Semaphore(max_mutation_bytes)

    def is_locked(self) -> bool:
        """
        Check if either flow control semaphore is locked
        """
        return (
            self.available_mutation_count.locked()
            or self.available_mutation_bytes.locked()
        )

    def _on_mutation_entry_complete(self, mutation_entry:BulkMutationsEntry, exception:Exception|None):
        """
        Every time an in-flight mutation is complete, release the flow control semaphore
        """
        self.available_mutation_count.release(len(mutation_entry.mutations))
        self.available_mutation_bytes.release(mutation_entry.size())

    def _execute_mutate_rows(self, batch:list[BulkMutationsEntry], timeout:float | None) -> list[FailedMutationEntryError]:
        """
        Helper to execute mutation operation on a batch

        Args:
          - batch: list of BulkMutationsEntry objects to send to server
          - timeout: timeout in seconds. Used as operation_timeout and per_request_timeout.
              If not given, will use table defaults
        Returns:
          - list of FailedMutationEntryError objects for mutations that failed.
              FailedMutationEntryError objects will not contain index information
        """
        request = {"table_name": self.table.table_name}
        if self.table.app_profile_id:
            request["app_profile_id"] = self.table.app_profile_id
        operation_timeout = timeout or self.table.default_operation_timeout
        request_timeout = timeout or self.table.default_per_request_timeout
        try:
            await _mutate_rows_operation(self.table.client._gapic_client, request, batch, operation_timeout, request_timeout, self._on_mutation_entry_complete)
        except MutationsExceptionGroup as e:
            for subexc in e.exceptions:
                subexc.index = None
            return e.exceptions
        return []

    async def process_mutations(self, mutations:list[BulkMutationsEntry], timeout:float | None) -> list[FailedMutationEntryError]:
        """
        Ascynronously send the set of mutations to the server. This method will block
        when the flow control limits are reached.

        Returns:
          - list of FailedMutationEntryError objects for mutations that failed
        """
        errors : list[FailedMutationEntryError] = []
        while mutations:
            batch : list[BulkMutationsEntry] = []
            # fill up batch until we hit a lock. Grab at least one entry
            while mutations and (not self.is_locked() or not batch):
                next_mutation = mutations.pop()
                next_mutation_size = next_mutation.size()
                # do extra sanity check to avoid deadlocks
                if len(next_mutation.mutations) > self.max_mutation_count:
                    raise ValueError(
                        f"Mutation count {len(next_mutation.mutations)} exceeds max mutation count {self.max_mutation_count}"
                    )
                if next_mutation_size > self.max_mutation_bytes:
                    raise ValueError(
                        f"Mutation size {next_mutation_size} exceeds max mutation size {self.max_mutation_bytes}"
                    )
                self.available_mutation_count.acquire(len(next_mutation.mutations))
                self.available_mutation_bytes.acquire(next_mutation_size)
                batch.append(next_mutation)
            # start mutate_rows rpc
            batch_errors = self._execute_mutate_rows(batch, timeout)
            errors.extend(batch_errors)
        # raise set of failed mutations on completion
        return errors


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
        flush_interval: float | None = 5,
        flush_limit_count: int | None = 100,
        flush_limit_bytes: int | None = 20 * MB_SIZE,
        flow_control_max_count: int | None = 100000,
        flow_control_max_bytes: int | None = 100 * MB_SIZE,
    ):
        """
        Args:
          - table: Table to preform rpc calls
          - flush_interval: Automatically flush every flush_interval seconds
          - flush_limit_count: Flush immediately after flush_limit_count mutations are added.
              If None, this limit is ignored.
          - flush_limit_bytes: Flush immediately after flush_limit_bytes bytes are added.
              If None, this limit is ignored.
          - flow_control_max_count: Maximum number of inflight mutations.
              If None, this limit is ignored.
          - flow_control_max_bytes: Maximum number of inflight bytes.
              If None, this limit is ignored.
        """
        self.closed : bool = False
        self._staged_mutations : list[BulkMutationsEntry] = []
        self._staged_count, self._staged_size = 0, 0
        self._flow_control = _FlowControl(table, flow_control_max_count, flow_control_max_bytes)
        self._flush_limit_bytes = flush_limit_bytes if flush_limit_bytes is not None else float("inf")
        self._flush_limit_count = flush_limit_count if flush_limit_count is not None else float("inf")
        self.exceptions = []
        self._flush_timer_task : asyncio.Task[None] = asyncio.create_task(self._flush_timer(flush_interval))
        self._flush_tasks : list[asyncio.Task[None]] = []

    async def _flush_timer(self, interval:float | None):
        """
        Triggers new flush tasks every `interval` seconds
        """
        if interval is None:
            return
        while not self.closed:
            await asyncio.sleep(interval)
            # add new flush task to list
            if not self.closed:
                new_task = asyncio.create_task(self.flush(timeout=None, raise_exceptions=False))
                self._flush_tasks.append(new_task)

    def append(self, mutations:BulkMutationsEntry):
        """
        Add a new set of mutations to the internal queue
        """
        if self.closed:
            raise RuntimeError("Cannot append to closed MutationsBatcher")
        size = mutations.size()
        if size > self._flow_control.max_mutation_bytes:
            raise ValueError(f"Mutation size {size} exceeds flow_control_max_bytes: {self._flow_control.max_mutation_bytes}")
        if len(mutations.mutations) > self._flow_control.max_mutation_count:
            raise ValueError(f"Mutation count {len(mutations.mutations)} exceeds flow_control_max_count: {self._flow_control.max_mutation_count}")
        self._staged_mutations.append(mutations)
        # start a new flush task if limits exceeded
        self._staged_count += len(mutations.mutations)
        self._staged_size += size
        if self._staged_count >= self._flush_limit_count or self._staged_size >= self._flush_limit_bytes:
            self._flush_tasks.append(asyncio.create_task(self.flush(timeout=None, raise_exceptions=False)))

    async def flush(self, *, timeout: float | None = 5.0, raise_exceptions=True):
        """
        Send queue over network in as few calls as possible

        Args:
          - timeout: operation_timeout for underlying rpc, in seconds
          - raise_exceptions: if True, will raise any unreported exceptions from this or previous flushes.
              If False, exceptions will be stored in self.exceptions and raised on a future flush
              or when the batcher is closed.
        Raises:
          - MutationsExceptionGroup if raise_exceptions is True and any mutations fail
        """
        # reset queue
        entries, self._staged_mutations = self._staged_mutations, []
        self._staged_count, self._staged_size = 0, 0
        # perform flush
        if entries:
            flush_errors = await self._flow_control.mutate_rows(entries, timeout=timeout)
            self.exceptions.extend(flush_errors)
            if raise_exceptions and self.exceptions:
                # raise any exceptions from this or previous flushes
                exc_list, self.exceptions = self.exceptions, []
                raise MutationsExceptionGroup(exc_list)

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
        if self.exceptions:
            raise MutationsExceptionGroup(self.exceptions)

