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

from typing import Any, Sequence, TYPE_CHECKING
import asyncio
import atexit
import warnings
from collections import deque

from google.cloud.bigtable.data.mutations import RowMutationEntry
from google.cloud.bigtable.data.exceptions import MutationsExceptionGroup
from google.cloud.bigtable.data.exceptions import FailedMutationEntryError
from google.cloud.bigtable.data._helpers import _get_retryable_errors
from google.cloud.bigtable.data._helpers import _get_timeouts
from google.cloud.bigtable.data._helpers import TABLE_DEFAULT

from google.cloud.bigtable.data._async._mutate_rows import _MutateRowsOperationAsync
from google.cloud.bigtable.data._async._mutate_rows import (
    _MUTATE_ROWS_REQUEST_MUTATION_LIMIT,
)
from google.cloud.bigtable.data.mutations import Mutation

if TYPE_CHECKING:
    from google.cloud.bigtable.data._async.client import TableAsync

# used to make more readable default values
_MB_SIZE = 1024 * 1024


class _FlowControlAsync:
    """
    Manages flow control for batched mutations. Mutations are registered against
    the FlowControl object before being sent, which will block if size or count
    limits have reached capacity. As mutations completed, they are removed from
    the FlowControl object, which will notify any blocked requests that there
    is additional capacity.

    Flow limits are not hard limits. If a single mutation exceeds the configured
    limits, it will be allowed as a single batch when the capacity is available.

    Args:
        max_mutation_count: maximum number of mutations to send in a single rpc.
            This corresponds to individual mutations in a single RowMutationEntry.
        max_mutation_bytes: maximum number of bytes to send in a single rpc.
    Raises:
        ValueError: if max_mutation_count or max_mutation_bytes is less than 0
    """

    def __init__(
        self,
        max_mutation_count: int,
        max_mutation_bytes: int,
    ):
        self._max_mutation_count = max_mutation_count
        self._max_mutation_bytes = max_mutation_bytes
        if self._max_mutation_count < 1:
            raise ValueError("max_mutation_count must be greater than 0")
        if self._max_mutation_bytes < 1:
            raise ValueError("max_mutation_bytes must be greater than 0")
        self._capacity_condition = asyncio.Condition()
        self._in_flight_mutation_count = 0
        self._in_flight_mutation_bytes = 0

    def _has_capacity(self, additional_count: int, additional_size: int) -> bool:
        """
        Checks if there is capacity to send a new entry with the given size and count

        FlowControl limits are not hard limits. If a single mutation exceeds
        the configured flow limits, it will be sent in a single batch when
        previous batches have completed.

        Args:
            additional_count: number of mutations in the pending entry
            additional_size: size of the pending entry
        Returns:
            bool: True if there is capacity to send the pending entry, False otherwise
        """
        # adjust limits to allow overly large mutations
        acceptable_size = max(self._max_mutation_bytes, additional_size)
        acceptable_count = max(self._max_mutation_count, additional_count)
        # check if we have capacity for new mutation
        new_size = self._in_flight_mutation_bytes + additional_size
        new_count = self._in_flight_mutation_count + additional_count
        return new_size <= acceptable_size and new_count <= acceptable_count

    async def remove_from_flow(
        self, mutations: RowMutationEntry | list[RowMutationEntry]
    ) -> None:
        """
        Removes mutations from flow control. This method should be called once
        for each mutation that was sent to add_to_flow, after the corresponding
        operation is complete.

        Args:
            mutations: mutation or list of mutations to remove from flow control
        """
        if not isinstance(mutations, list):
            mutations = [mutations]
        total_count = sum(len(entry.mutations) for entry in mutations)
        total_size = sum(entry.size() for entry in mutations)
        self._in_flight_mutation_count -= total_count
        self._in_flight_mutation_bytes -= total_size
        # notify any blocked requests that there is additional capacity
        async with self._capacity_condition:
            self._capacity_condition.notify_all()

    async def add_to_flow(self, mutations: RowMutationEntry | list[RowMutationEntry]):
        """
        Generator function that registers mutations with flow control. As mutations
        are accepted into the flow control, they are yielded back to the caller,
        to be sent in a batch. If the flow control is at capacity, the generator
        will block until there is capacity available.

        Args:
            mutations: list mutations to break up into batches
        Yields:
            list[RowMutationEntry]:
                list of mutations that have reserved space in the flow control.
                Each batch contains at least one mutation.
        """
        if not isinstance(mutations, list):
            mutations = [mutations]
        start_idx = 0
        end_idx = 0
        while end_idx < len(mutations):
            start_idx = end_idx
            batch_mutation_count = 0
            # fill up batch until we hit capacity
            async with self._capacity_condition:
                while end_idx < len(mutations):
                    next_entry = mutations[end_idx]
                    next_size = next_entry.size()
                    next_count = len(next_entry.mutations)
                    if (
                        self._has_capacity(next_count, next_size)
                        # make sure not to exceed per-request mutation count limits
                        and (batch_mutation_count + next_count)
                        <= _MUTATE_ROWS_REQUEST_MUTATION_LIMIT
                    ):
                        # room for new mutation; add to batch
                        end_idx += 1
                        batch_mutation_count += next_count
                        self._in_flight_mutation_bytes += next_size
                        self._in_flight_mutation_count += next_count
                    elif start_idx != end_idx:
                        # we have at least one mutation in the batch, so send it
                        break
                    else:
                        # batch is empty. Block until we have capacity
                        await self._capacity_condition.wait_for(
                            lambda: self._has_capacity(next_count, next_size)
                        )
            yield mutations[start_idx:end_idx]


class MutationsBatcherAsync:
    """
    Allows users to send batches using context manager API:

    Runs mutate_row,  mutate_rows, and check_and_mutate_row internally, combining
    to use as few network requests as required

    Will automatically flush the batcher:
    - every flush_interval seconds
    - after queue size reaches flush_limit_mutation_count
    - after queue reaches flush_limit_bytes
    - when batcher is closed or destroyed

    Args:
        table: Table to preform rpc calls
        flush_interval: Automatically flush every flush_interval seconds.
            If None, no time-based flushing is performed.
        flush_limit_mutation_count: Flush immediately after flush_limit_mutation_count
            mutations are added across all entries. If None, this limit is ignored.
        flush_limit_bytes: Flush immediately after flush_limit_bytes bytes are added.
        flow_control_max_mutation_count: Maximum number of inflight mutations.
        flow_control_max_bytes: Maximum number of inflight bytes.
        batch_operation_timeout: timeout for each mutate_rows operation, in seconds.
            If TABLE_DEFAULT, defaults to the Table's default_mutate_rows_operation_timeout.
        batch_attempt_timeout: timeout for each individual request, in seconds.
            If TABLE_DEFAULT, defaults to the Table's default_mutate_rows_attempt_timeout.
            If None, defaults to batch_operation_timeout.
        batch_retryable_errors: a list of errors that will be retried if encountered.
            Defaults to the Table's default_mutate_rows_retryable_errors.
    """

    def __init__(
        self,
        table: "TableAsync",
        *,
        flush_interval: float | None = 5,
        flush_limit_mutation_count: int | None = 1000,
        flush_limit_bytes: int = 20 * _MB_SIZE,
        flow_control_max_mutation_count: int = 100_000,
        flow_control_max_bytes: int = 100 * _MB_SIZE,
        batch_operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
        batch_attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
        batch_retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
    ):
        self._operation_timeout, self._attempt_timeout = _get_timeouts(
            batch_operation_timeout, batch_attempt_timeout, table
        )
        self._retryable_errors: list[type[Exception]] = _get_retryable_errors(
            batch_retryable_errors, table
        )

        self.closed: bool = False
        self._table = table
        self._staged_entries: list[RowMutationEntry] = []
        self._staged_count, self._staged_bytes = 0, 0
        self._flow_control = _FlowControlAsync(
            flow_control_max_mutation_count, flow_control_max_bytes
        )
        self._flush_limit_bytes = flush_limit_bytes
        self._flush_limit_count = (
            flush_limit_mutation_count
            if flush_limit_mutation_count is not None
            else float("inf")
        )
        self._flush_timer = self._start_flush_timer(flush_interval)
        self._flush_jobs: set[asyncio.Future[None]] = set()
        # MutationExceptionGroup reports number of successful entries along with failures
        self._entries_processed_since_last_raise: int = 0
        self._exceptions_since_last_raise: int = 0
        # keep track of the first and last _exception_list_limit exceptions
        self._exception_list_limit: int = 10
        self._oldest_exceptions: list[Exception] = []
        self._newest_exceptions: deque[Exception] = deque(
            maxlen=self._exception_list_limit
        )
        # clean up on program exit
        atexit.register(self._on_exit)

    def _start_flush_timer(self, interval: float | None) -> asyncio.Future[None]:
        """
        Set up a background task to flush the batcher every interval seconds

        If interval is None, an empty future is returned

        Args:
            flush_interval: Automatically flush every flush_interval seconds.
                If None, no time-based flushing is performed.
        Returns:
            asyncio.Future[None]: future representing the background task
        """
        if interval is None or self.closed:
            empty_future: asyncio.Future[None] = asyncio.Future()
            empty_future.set_result(None)
            return empty_future

        async def timer_routine(self, interval: float):
            """
            Triggers new flush tasks every `interval` seconds
            """
            while not self.closed:
                await asyncio.sleep(interval)
                # add new flush task to list
                if not self.closed and self._staged_entries:
                    self._schedule_flush()

        timer_task = asyncio.create_task(timer_routine(self, interval))
        return timer_task

    async def append(self, mutation_entry: RowMutationEntry):
        """
        Add a new set of mutations to the internal queue

        Args:
            mutation_entry: new entry to add to flush queue
        Raises:
            RuntimeError: if batcher is closed
            ValueError: if an invalid mutation type is added
        """
        # TODO: return a future to track completion of this entry
        if self.closed:
            raise RuntimeError("Cannot append to closed MutationsBatcher")
        if isinstance(mutation_entry, Mutation):  # type: ignore
            raise ValueError(
                f"invalid mutation type: {type(mutation_entry).__name__}. Only RowMutationEntry objects are supported by batcher"
            )
        self._staged_entries.append(mutation_entry)
        # start a new flush task if limits exceeded
        self._staged_count += len(mutation_entry.mutations)
        self._staged_bytes += mutation_entry.size()
        if (
            self._staged_count >= self._flush_limit_count
            or self._staged_bytes >= self._flush_limit_bytes
        ):
            self._schedule_flush()
            # yield to the event loop to allow flush to run
            await asyncio.sleep(0)

    def _schedule_flush(self) -> asyncio.Future[None] | None:
        """
        Update the flush task to include the latest staged entries

        Returns:
            asyncio.Future[None] | None:
                future representing the background task, if started
        """
        if self._staged_entries:
            entries, self._staged_entries = self._staged_entries, []
            self._staged_count, self._staged_bytes = 0, 0
            new_task = self._create_bg_task(self._flush_internal, entries)
            new_task.add_done_callback(self._flush_jobs.remove)
            self._flush_jobs.add(new_task)
            return new_task
        return None

    async def _flush_internal(self, new_entries: list[RowMutationEntry]):
        """
        Flushes a set of mutations to the server, and updates internal state

        Args:
            new_entries list of RowMutationEntry objects to flush
        """
        # flush new entries
        in_process_requests: list[asyncio.Future[list[FailedMutationEntryError]]] = []
        async for batch in self._flow_control.add_to_flow(new_entries):
            batch_task = self._create_bg_task(self._execute_mutate_rows, batch)
            in_process_requests.append(batch_task)
        # wait for all inflight requests to complete
        found_exceptions = await self._wait_for_batch_results(*in_process_requests)
        # update exception data to reflect any new errors
        self._entries_processed_since_last_raise += len(new_entries)
        self._add_exceptions(found_exceptions)

    async def _execute_mutate_rows(
        self, batch: list[RowMutationEntry]
    ) -> list[FailedMutationEntryError]:
        """
        Helper to execute mutation operation on a batch

        Args:
            batch: list of RowMutationEntry objects to send to server
            timeout: timeout in seconds. Used as operation_timeout and attempt_timeout.
                If not given, will use table defaults
        Returns:
            list[FailedMutationEntryError]:
                list of FailedMutationEntryError objects for mutations that failed.
                FailedMutationEntryError objects will not contain index information
        """
        try:
            operation = _MutateRowsOperationAsync(
                self._table.client._gapic_client,
                self._table,
                batch,
                operation_timeout=self._operation_timeout,
                attempt_timeout=self._attempt_timeout,
                retryable_exceptions=self._retryable_errors,
            )
            await operation.start()
        except MutationsExceptionGroup as e:
            # strip index information from exceptions, since it is not useful in a batch context
            for subexc in e.exceptions:
                subexc.index = None
            return list(e.exceptions)
        finally:
            # mark batch as complete in flow control
            await self._flow_control.remove_from_flow(batch)
        return []

    def _add_exceptions(self, excs: list[Exception]):
        """
        Add new list of exceptions to internal store. To avoid unbounded memory,
        the batcher will store the first and last _exception_list_limit exceptions,
        and discard any in between.

        Args:
            excs: list of exceptions to add to the internal store
        """
        self._exceptions_since_last_raise += len(excs)
        if excs and len(self._oldest_exceptions) < self._exception_list_limit:
            # populate oldest_exceptions with found_exceptions
            addition_count = self._exception_list_limit - len(self._oldest_exceptions)
            self._oldest_exceptions.extend(excs[:addition_count])
            excs = excs[addition_count:]
        if excs:
            # populate newest_exceptions with remaining found_exceptions
            self._newest_exceptions.extend(excs[-self._exception_list_limit :])

    def _raise_exceptions(self):
        """
        Raise any unreported exceptions from background flush operations

        Raises:
            MutationsExceptionGroup: exception group with all unreported exceptions
        """
        if self._oldest_exceptions or self._newest_exceptions:
            oldest, self._oldest_exceptions = self._oldest_exceptions, []
            newest = list(self._newest_exceptions)
            self._newest_exceptions.clear()
            entry_count, self._entries_processed_since_last_raise = (
                self._entries_processed_since_last_raise,
                0,
            )
            exc_count, self._exceptions_since_last_raise = (
                self._exceptions_since_last_raise,
                0,
            )
            raise MutationsExceptionGroup.from_truncated_lists(
                first_list=oldest,
                last_list=newest,
                total_excs=exc_count,
                entry_count=entry_count,
            )

    async def __aenter__(self):
        """Allow use of context manager API"""
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Allow use of context manager API.

        Flushes the batcher and cleans up resources.
        """
        await self.close()

    async def close(self):
        """
        Flush queue and clean up resources
        """
        self.closed = True
        self._flush_timer.cancel()
        self._schedule_flush()
        if self._flush_jobs:
            await asyncio.gather(*self._flush_jobs, return_exceptions=True)
        try:
            await self._flush_timer
        except asyncio.CancelledError:
            pass
        atexit.unregister(self._on_exit)
        # raise unreported exceptions
        self._raise_exceptions()

    def _on_exit(self):
        """
        Called when program is exited. Raises warning if unflushed mutations remain
        """
        if not self.closed and self._staged_entries:
            warnings.warn(
                f"MutationsBatcher for table {self._table.table_name} was not closed. "
                f"{len(self._staged_entries)} Unflushed mutations will not be sent to the server."
            )

    @staticmethod
    def _create_bg_task(func, *args, **kwargs) -> asyncio.Future[Any]:
        """
        Create a new background task, and return a future

        This method wraps asyncio to make it easier to maintain subclasses
        with different concurrency models.

        Args:
            func: function to execute in background task
            *args: positional arguments to pass to func
            **kwargs: keyword arguments to pass to func
        Returns:
            asyncio.Future: Future object representing the background task
        """
        return asyncio.create_task(func(*args, **kwargs))

    @staticmethod
    async def _wait_for_batch_results(
        *tasks: asyncio.Future[list[FailedMutationEntryError]] | asyncio.Future[None],
    ) -> list[Exception]:
        """
        Takes in a list of futures representing _execute_mutate_rows tasks,
        waits for them to complete, and returns a list of errors encountered.

        Args:
            *tasks: futures representing _execute_mutate_rows or _flush_internal tasks
        Returns:
            list[Exception]:
                list of Exceptions encountered by any of the tasks. Errors are expected
                to be FailedMutationEntryError, representing a failed mutation operation.
                If a task fails with a different exception, it will be included in the
                output list. Successful tasks will not be represented in the output list.
        """
        if not tasks:
            return []
        all_results = await asyncio.gather(*tasks, return_exceptions=True)
        found_errors = []
        for result in all_results:
            if isinstance(result, Exception):
                # will receive direct Exception objects if request task fails
                found_errors.append(result)
            elif isinstance(result, BaseException):
                # BaseException not expected from grpc calls. Raise immediately
                raise result
            elif result:
                # completed requests will return a list of FailedMutationEntryError
                for e in result:
                    # strip index information
                    e.index = None
                found_errors.extend(result)
        return found_errors
