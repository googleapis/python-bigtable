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
from threading import Timer
from google.cloud.bigtable.exceptions import FailedMutationEntryError


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

    def __init__(
        self,
        *args,
        max_workers: int = 3,
        **kwargs,
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
          - max_workers: Maximum number of threads to use for flushing.
        """
        super().__init__(
            *args,
            **kwargs,
        )
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def _start_flush_timer(self, interval: float | None) -> Timer[None]:
        """Triggers new flush tasks every `interval` seconds"""

        def timer_routine(self, interval: float | None):
            if not self.closed and interval is not None:
                self._schedule_flush()
                self._flush_timer = self._start_flush_timer(interval)

        # schedule a new timer
        self._flush_timer_task = Timer(interval or 0, timer_routine, (self, interval))
        self._flush_timer_task.start()
        return self._flush_timer_task

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

    def flush_async(self, raise_exceptions: bool = True) -> Future[None]:
        """
        Flush all staged entries asynchronously

        Returns:
          - Future[None]: resolves when flush completes
        """
        flush_job: Future[None] = self._schedule_flush()
        public_future: Future[None] = Future()
        if raise_exceptions:
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
        self._flush_timer.cancel()
        self._schedule_flush().result()
        self._prev_flush.result()
        self._executor.shutdown(wait=True)
        # make sure all other cleanup is completed
        super().close()

    def _create_bg_task(self, func, *args, **kwargs) -> Future[None]:
        return self._executor.submit(func, *args, **kwargs)

    @staticmethod
    def _wait_for_batch_results(
        *tasks: Future[list[FailedMutationEntryError]] | Future[None],
    ) -> list[list[FailedMutationEntryError] | Exception]:
        wait(tasks)
        all_results = [f.result() or f.exception() for f in tasks]
        found_errors = []
        for result in all_results:
            if isinstance(result, Exception):
                # will receive direct Exception objects if request task fails
                found_errors.append(result)
            elif result:
                # completed requests will return a list of FailedMutationEntryError
                found_errors.extend(result)
        return found_errors
