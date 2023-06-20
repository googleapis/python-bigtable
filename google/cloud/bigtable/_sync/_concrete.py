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

from typing import Any

import google.auth.credentials
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigtable._sync._autogen import _ReadRowsOperation_Sync
from google.cloud.bigtable._sync._autogen import Table_Sync
from google.cloud.bigtable._sync._autogen import BigtableDataClient_Sync
from google.cloud.bigtable._sync._autogen import ReadRowsIterator_Sync
from google.cloud.bigtable._sync._autogen import _MutateRowsOperation_Sync
from google.cloud.bigtable._sync._autogen import _FlowControl_Sync
from google.cloud.bigtable._sync._autogen import MutationsBatcher_Sync

from concurrent.futures import Future
from concurrent.futures import wait
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from google.cloud.bigtable.exceptions import FailedMutationEntryError
import atexit
from google.cloud.bigtable.mutations_batcher import MB_SIZE



class _ReadRowsOperation_Sync_Concrete(_ReadRowsOperation_Sync):
    pass


class Table_Sync_Concrete(Table_Sync):
    # TODO: buffer_size does not apply to sync surface
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # register table with client
        self.client._register_instance(self.instance_id, self)


class BigtableDataClient_Sync_Concrete(BigtableDataClient_Sync):
    def __init__(
        self,
        *,
        project: str | None = None,
        credentials: google.auth.credentials.Credentials | None = None,
        client_options: dict[str, Any]
        | "google.api_core.client_options.ClientOptions"
        | None = None,
    ):
        # remove pool size option in sync client
        super().__init__(
            project=project, credentials=credentials, client_options=client_options
        )

    def __init__transport__(self, *args, **kwargs):
        # use grpc transport for sync client
        return "grpc"

    def close(self):
        self.transport.close()
        self._channel_refresh_tasks = []

    def _ping_and_warm_instances(self, channel=None) -> list[GoogleAPICallError | None]:
        results: list[GoogleAPICallError | None] = []
        for instance_name in self._active_instances:
            try:
                self._gapic_client.ping_and_warm(name=instance_name)
                results.append(None)
            except GoogleAPICallError as e:
                # fail warm attempts silently
                results.append(e)
        return results

    def _register_instance(self, instance_id: str, owner: Table_Sync) -> None:
        instance_name = self._gapic_client.instance_path(self.project, instance_id)
        self._instance_owners.setdefault(instance_name, set()).add(id(owner))
        if instance_name not in self._active_instances:
            self._active_instances.add(instance_name)
            # warm channel for new instance
            self._ping_and_warm_instances()


class ReadRowsIterator_Sync_Concrete(ReadRowsIterator_Sync):
    pass


class _MutateRowsOperation_Sync_Concrete(_MutateRowsOperation_Sync):
    pass


class _FlowControl_Sync_Concrete(_FlowControl_Sync):
    pass


class MutationsBatcher_Threaded(MutationsBatcher_Sync):
    # TODO: add thread-safe locks
    # TODO: share more code with async surface

    def __init__(
        self,
        table: Table_Sync,
        *,
        flush_interval: float | None = 5,
        flush_limit_mutation_count: int | None = 1000,
        flush_limit_bytes: int = 20 * MB_SIZE,
        flow_control_max_count: int | None = 100_000,
        flow_control_max_bytes: int | None = 100 * MB_SIZE,
    ):
        """
        Args:
          - table: Table to preform rpc calls
          - flush_interval: Automatically flush every flush_interval seconds.
              If None, no time-based flushing is performed.
          - flush_limit_mutation_count: Flush immediately after flush_limit_mutation_count
              mutations are added across all entries. If None, this limit is ignored.
          - flush_limit_bytes: Flush immediately after flush_limit_bytes bytes are added.
          - flow_control_max_count: Maximum number of inflight mutations.
              If None, this limit is ignored.
          - flow_control_max_bytes: Maximum number of inflight bytes.
              If None, this limit is ignored.
        """
        atexit.register(self._on_exit)
        self.closed: bool = False
        self._table = table
        self._staged_entries: list["RowMutationEntry"] = []
        self._staged_count, self._staged_bytes = 0, 0
        self._flow_control = _FlowControl_Sync_Concrete(
            flow_control_max_count, flow_control_max_bytes
        )
        self._flush_limit_bytes = flush_limit_bytes
        self._flush_limit_count = (
            flush_limit_mutation_count
            if flush_limit_mutation_count is not None
            else float("inf")
        )
        self.exceptions: list[Exception] = []
        self._flush_timer = Thread(daemon=True, target=self._flush_timer, args=(flush_interval,))
        self._flush_timer.start()
        # MutationExceptionGroup reports number of successful entries along with failures
        self._entries_processed_since_last_raise: int = 0
        # create empty previous flush to avoid None checks
        self._prev_flush: Future[None] = Future()
        self._prev_flush.set_result(None)
        self._executor = ThreadPoolExecutor()
        # TODO: remove
        self._pending_flush_entries = []

    def _schedule_flush(self) -> Future[None]:
        """Update the flush task to include the latest staged entries"""
        if self._staged_entries:
            entries, self._staged_entries = self._staged_entries, []
            self._staged_count, self._staged_bytes = 0, 0
            # flush is scheduled to run on next loop iteration
            # use _pending_flush_entries to add new extra entries before flush task starts
            self._prev_flush = self._executor.submit(self._flush_internal, entries, self._prev_flush)
        return self._prev_flush

    def _flush_internal(self, new_entries, prev_flush: Future[None]):
        """
        Flushes a set of mutations to the server, and updates internal state

        Args:
          - prev_flush: the previous flush task, which will be awaited before
              a new flush is initiated
        """
        # flush new entries
        in_process_requests: list[Future[None | list[FailedMutationEntryError]]] = [prev_flush]
        for batch in self._flow_control.add_to_flow(new_entries):
            batch_future = self._executor.submit(self._execute_mutate_rows, batch)
            in_process_requests.append(batch_future)
        # wait for all inflight requests to complete
        wait(in_process_requests)
        all_results: list[
            list[FailedMutationEntryError] | Exception | None
        ] = [f.result() or f.exception() for f in in_process_requests]
        # collect exception data for next raise, after previous flush tasks have completed
        self._entries_processed_since_last_raise += len(new_entries)
        for request_result in all_results:
            if isinstance(request_result, Exception):
                # will receive direct Exception objects if request task fails
                self.exceptions.append(request_result)
            elif request_result is not None:
                # completed requests will return a list of FailedMutationEntryError
                self.exceptions.extend(request_result)

    def flush(self, *, raise_exceptions: bool = True, timeout: float | None = 60):
        """
        Flush all staged entries

        Args:
          - raise_exceptions: if True, will raise any unreported exceptions from this or previous flushes.
              If False, exceptions will be stored in self.exceptions and raised on a future flush
              or when the batcher is closed.
          - timeout: maximum time to wait for flush to complete, in seconds.
              If exceeded, flush will continue in the background and exceptions
              will be surfaced on the next flush
        Raises:
          - MutationsExceptionGroup if raise_exceptions is True and any mutations fail
          - TimeoutError if timeout is reached before flush task completes.
        """
        # add recent staged entries to flush task, and wait for flush to complete
        flush_job: Future[None] = self._schedule_flush()
        flush_job.result(timeout=timeout)
        # raise any unreported exceptions from this or previous flushes
        if raise_exceptions:
            self._raise_exceptions()

    def flush_async(self) -> Future[None]:
        """
        Flush all staged entries asynchronously

        Returns:
          - Future[None]: resolves when flush completes
        """
        flush_job: Future[None] = self._schedule_flush()
        public_future: Future[None] = Future()
        public_future.add_done_callback(lambda _: self._raise_exceptions())
        flush_job.add_done_callback(lambda _: public_future.set_result(None))
        public_future.set_running_or_notify_cancel()
        return public_future

    def close(self):
        """
        Flush queue and clean up resources
        """
        self.closed = True
        # flush remaining entries
        # TODO: clean up timer thread
        # self._flush_timer.cancel()
        self._schedule_flush()
        self._prev_flush.result()
        self._executor.shutdown(wait=True)
        # raise unreported exceptions
        self._raise_exceptions()
        atexit.unregister(self._on_exit)
