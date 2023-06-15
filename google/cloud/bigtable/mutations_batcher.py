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
import atexit
import warnings
from typing import Awaitable, TYPE_CHECKING

from google.cloud.bigtable.mutations import RowMutationEntry
from google.cloud.bigtable.exceptions import MutationsExceptionGroup
from google.cloud.bigtable.exceptions import FailedMutationEntryError

from google.cloud.bigtable._mutate_rows import _MutateRowsOperation
from google.cloud.bigtable._mutate_rows import MAX_MUTATE_ROWS_ENTRY_COUNT
from google.cloud.bigtable.mutations import Mutation

if TYPE_CHECKING:
    from google.cloud.bigtable.client import Table  # pragma: no cover

# used to make more readable default values
MB_SIZE = 1024 * 1024


class _FlowControl:
    """
    Manages flow control for batched mutations. Mutations are registered against
    the FlowControl object before being sent, which will block if size or count
    limits have reached capacity. As mutations completed, they are removed from
    the FlowControl object, which will notify any blocked requests that there
    is additional capacity.

    Flow limits are not hard limits. If a single mutation exceeds the configured
    limits, it will be allowed as a single batch when the capacity is available.
    """

    def __init__(
        self,
        max_mutation_count: int | None,
        max_mutation_bytes: int | None,
        max_entry_count: int = MAX_MUTATE_ROWS_ENTRY_COUNT,
    ):
        """
        Args:
          - max_mutation_count: maximum number of mutations to send in a single rpc.
             This corresponds to individual mutations in a single RowMutationEntry.
             If None, no limit is enforced.
          - max_mutation_bytes: maximum number of bytes to send in a single rpc.
             If None, no limit is enforced.
          - max_entry_count: maximum number of entries to send in a single rpc.
             Limited to 100,000 by the MutateRows API.
        """
        self.max_mutation_count = (
            max_mutation_count if max_mutation_count is not None else float("inf")
        )
        self.max_mutation_bytes = (
            max_mutation_bytes if max_mutation_bytes is not None else float("inf")
        )
        self.max_entry_count = max_entry_count
        if (
            self.max_entry_count > MAX_MUTATE_ROWS_ENTRY_COUNT
            or self.max_entry_count < 1
        ):
            raise ValueError(
                f"max_entry_count must be between 1 and {MAX_MUTATE_ROWS_ENTRY_COUNT}"
            )
        if self.max_mutation_count < 1:
            raise ValueError("max_mutation_count must be greater than 0")
        if self.max_mutation_bytes < 1:
            raise ValueError("max_mutation_bytes must be greater than 0")
        self.capacity_condition = asyncio.Condition()
        self.in_flight_mutation_count = 0
        self.in_flight_mutation_bytes = 0

    def _has_capacity(self, additional_count: int, additional_size: int) -> bool:
        """
        Checks if there is capacity to send a new mutation with the given size and count

        FlowControl limits are not hard limits. If a single mutation exceeds
        the configured limits, it can be sent in a single batch.
        """
        # adjust limits to allow overly large mutations
        acceptable_size = max(self.max_mutation_bytes, additional_size)
        acceptable_count = max(self.max_mutation_count, additional_count)
        # check if we have capacity for new mutation
        new_size = self.in_flight_mutation_bytes + additional_size
        new_count = self.in_flight_mutation_count + additional_count
        return new_size <= acceptable_size and new_count <= acceptable_count

    async def remove_from_flow(
        self, mutations: RowMutationEntry | list[RowMutationEntry]
    ) -> None:
        """
        Every time an in-flight mutation is complete, release the flow control semaphore
        """
        if not isinstance(mutations, list):
            mutations = [mutations]
        total_count = sum(len(entry.mutations) for entry in mutations)
        total_size = sum(entry.size() for entry in mutations)
        self.in_flight_mutation_count -= total_count
        self.in_flight_mutation_bytes -= total_size
        # notify any blocked requests that there is additional capacity
        async with self.capacity_condition:
            self.capacity_condition.notify_all()

    async def add_to_flow(self, mutations: RowMutationEntry | list[RowMutationEntry]):
        """
        Breaks up list of mutations into batches that were registered to fit within
        flow control limits. This method will block when the flow control limits are
        reached.

        Args:
          - mutations: list mutations to break up into batches
        Yields:
          - list of mutations that have reserved space in the flow control.
            Each batch contains at least one mutation.
        Raises:
          - ValueError if any mutation entry is larger than the flow control limits
        """
        if not isinstance(mutations, list):
            mutations = [mutations]
        start_idx = 0
        end_idx = 0
        while end_idx < len(mutations):
            start_idx = end_idx
            # fill up batch until we hit capacity
            async with self.capacity_condition:
                while end_idx < len(mutations):
                    next_entry = mutations[end_idx]
                    next_size = next_entry.size()
                    next_count = len(next_entry.mutations)
                    num_in_batch = end_idx - start_idx
                    if (
                        self._has_capacity(next_count, next_size)
                        and num_in_batch < self.max_entry_count
                    ):
                        # room for new mutation; add to batch
                        end_idx += 1
                        self.in_flight_mutation_bytes += next_size
                        self.in_flight_mutation_count += next_count
                    elif start_idx != end_idx:
                        # we have at least one mutation in the batch, so send it
                        break
                    else:
                        # batch is empty. Block until we have capacity
                        await self.capacity_condition.wait_for(
                            lambda: self._has_capacity(next_count, next_size)
                        )
            yield mutations[start_idx:end_idx]


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

    def __init__(
        self,
        table: "Table",
        *,
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
        atexit.register(self._on_exit)
        self.closed: bool = False
        self._table = table
        self._staged_mutations: list[RowMutationEntry] = []
        self._staged_count, self._staged_bytes = 0, 0
        self._flow_control = _FlowControl(
            flow_control_max_count, flow_control_max_bytes
        )
        self._flush_limit_bytes = (
            flush_limit_bytes if flush_limit_bytes is not None else float("inf")
        )
        self._flush_limit_count = (
            flush_limit_count if flush_limit_count is not None else float("inf")
        )
        self.exceptions: list[FailedMutationEntryError] = []
        self._flush_timer_task: asyncio.Task[None] = asyncio.create_task(
            self._flush_timer(flush_interval)
        )
        # create noop previous flush task to avoid None checks
        self._prev_flush: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(0))
        # MutationExceptionGroup reports number of successful entries along with failures
        self._entries_processed_since_last_raise: int = 0

    async def _flush_timer(self, interval: float | None):
        """
        Triggers new flush tasks every `interval` seconds
        """
        if interval is None:
            return
        while not self.closed:
            await asyncio.sleep(interval)
            # add new flush task to list
            if not self.closed and self._staged_mutations:
                self._schedule_flush()

    def append(self, mutation_entry: RowMutationEntry):
        """
        Add a new set of mutations to the internal queue

        Args:
          - mutation_entry: new entry to add to flush queue
        Raises:
          - RuntimeError if batcher is closed
          - ValueError if an invalid mutation type is added
        """
        if self.closed:
            raise RuntimeError("Cannot append to closed MutationsBatcher")
        if isinstance(mutation_entry, Mutation):  # type: ignore
            raise ValueError(
                f"invalid mutation type: {type(mutation_entry).__name__}. Only RowMutationEntry objects are supported by batcher"
            )
        self._staged_mutations.append(mutation_entry)
        # start a new flush task if limits exceeded
        self._staged_count += len(mutation_entry.mutations)
        self._staged_bytes += mutation_entry.size()
        if (
            self._staged_count >= self._flush_limit_count
            or self._staged_bytes >= self._flush_limit_bytes
        ):
            self._schedule_flush()

    async def flush(self, *, raise_exceptions: bool = True, timeout: float | None = 60):
        """
        Flush all staged mutations

        Args:
          - raise_exceptions: if True, will raise any unreported exceptions from this or previous flushes.
              If False, exceptions will be stored in self.exceptions and raised on a future flush
              or when the batcher is closed.
          - timeout: maximum time to wait for flush to complete, in seconds.
              If exceeded, flush will continue in the background and exceptions
              will be surfaced on the next flush
        Raises:
          - MutationsExceptionGroup if raise_exceptions is True and any mutations fail
          - asyncio.TimeoutError if timeout is reached before flush task completes.
        """
        # add recent staged mutations to flush task, and wait for flush to complete
        flush_job: Awaitable[None] = self._schedule_flush()
        if timeout is not None:
            # wait `timeout seconds for flush to complete
            # if timeout is exceeded, flush task will still be running in the background
            flush_job = asyncio.wait_for(asyncio.shield(flush_job), timeout=timeout)
        await flush_job
        # raise any unreported exceptions from this or previous flushes
        if raise_exceptions:
            self._raise_exceptions()

    def _schedule_flush(self) -> asyncio.Task[None]:
        """Update the flush task to include the latest staged mutations"""
        if self._staged_mutations:
            entries, self._staged_mutations = self._staged_mutations, []
            self._staged_count, self._staged_bytes = 0, 0
            self._prev_flush = asyncio.create_task(
                self._flush_internal(entries, self._prev_flush)
            )
        return self._prev_flush

    async def _flush_internal(
        self,
        new_entries: list[RowMutationEntry],
        prev_flush: asyncio.Task[None],
    ):
        """
        Flushes a set of mutations to the server, and updates internal state

        Args:
          - new_entries: list of mutations to flush
          - prev_flush: the previous flush task, which will be awaited before
              a new flush is initiated
        """
        # flush new entries
        in_process_requests: list[asyncio.Task[None]] = [prev_flush]
        async for batch in self._flow_control.add_to_flow(new_entries):
            batch_task = asyncio.create_task(
                self._execute_mutate_rows_w_state_update(batch)
            )
            in_process_requests.append(batch_task)
        # wait for all inflight requests to complete
        await asyncio.gather(*in_process_requests)

    async def _execute_mutate_rows_w_state_update(
        self, batch: list[RowMutationEntry]
    ) -> None:
        """
        Calls _execute_mutate_rows, and then updates internal flush state based on results
        """
        results = await self._execute_mutate_rows(batch)
        self.exceptions.extend(results)
        self._entries_processed_since_last_raise += len(batch)
        await self._flow_control.remove_from_flow(batch)

    async def _execute_mutate_rows(
        self, batch: list[RowMutationEntry]
    ) -> list[FailedMutationEntryError]:
        """
        Helper to execute mutation operation on a batch

        Args:
          - batch: list of RowMutationEntry objects to send to server
          - timeout: timeout in seconds. Used as operation_timeout and per_request_timeout.
              If not given, will use table defaults
        Returns:
          - list of FailedMutationEntryError objects for mutations that failed.
              FailedMutationEntryError objects will not contain index information
        """
        request = {"table_name": self._table.table_name}
        if self._table.app_profile_id:
            request["app_profile_id"] = self._table.app_profile_id
        try:
            operation = _MutateRowsOperation(
                self._table.client._gapic_client,
                self._table,
                batch,
                self._table.default_operation_timeout,
                self._table.default_per_request_timeout,
            )
            await operation.start()
        except MutationsExceptionGroup as e:
            # strip index information from exceptions, since it is not useful in a batch context
            for subexc in e.exceptions:
                subexc.index = None
            return list(e.exceptions)
        return []

    def _raise_exceptions(self):
        """
        Raise any unreported exceptions from background flush operations

        Raises:
          - MutationsExceptionGroup with all unreported exceptions
        """
        if self.exceptions:
            exc_list, self.exceptions = self.exceptions, []
            raise_count, self._entries_processed_since_last_raise = (
                self._entries_processed_since_last_raise,
                0,
            )
            raise MutationsExceptionGroup(exc_list, raise_count)

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
        self.closed = True
        self._flush_timer_task.cancel()
        self._schedule_flush()
        await self._prev_flush
        # raise unreported exceptions
        self._raise_exceptions()
        atexit.unregister(self._on_exit)

    def _on_exit(self):
        """
        Called when program is exited. Raises warning if unflushed mutations remain
        """
        if not self.closed and self._staged_mutations:
            warnings.warn(
                f"MutationsBatcher for table {self._table.table_name} was not closed. "
                f"{len(self._staged_mutations)} Unflushed mutations will not be sent to the server."
            )
