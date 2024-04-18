# Copyright 2024 Google LLC
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

from typing import TYPE_CHECKING

import google.auth.credentials
import concurrent.futures

from google.cloud.bigtable.data._sync._autogen import BigtableDataClient_SyncGen
from google.cloud.bigtable.data._sync._autogen import Table_SyncGen

# import required so Table_SyncGen can create _MutateRowsOperation and _ReadRowsOperation
import google.cloud.bigtable.data._sync._read_rows  # noqa: F401
import google.cloud.bigtable.data._sync._mutate_rows  # noqa: F401

if TYPE_CHECKING:
    from google.cloud.bigtable.data.row import Row


class BigtableDataClient(BigtableDataClient_SyncGen):
    @property
    def _executor(self) -> concurrent.futures.ThreadPoolExecutor:
        if not hasattr(self, "_executor_instance"):
            self._executor_instance = concurrent.futures.ThreadPoolExecutor()
        return self._executor_instance

    @staticmethod
    def _client_version() -> str:
        return f"{google.cloud.bigtable.__version__}-data"

    def _start_background_channel_refresh(self) -> None:
        if (
            not self._channel_refresh_tasks
            and not self._emulator_host
            and not self._is_closed.is_set()
        ):
            for channel_idx in range(self.transport.pool_size):
                self._channel_refresh_tasks.append(
                    self._executor.submit(self._manage_channel, channel_idx)
                )

    def _execute_ping_and_warms(self, *fns) -> list[BaseException | None]:
        futures_list = [self._executor.submit(f) for f in fns]
        results_list: list[BaseException | None] = []
        for future in futures_list:
            try:
                future.result()
                results_list.append(None)
            except BaseException as e:
                results_list.append(e)
        return results_list

    def close(self) -> None:
        """
        Close the client and all associated resources

        This method should be called when the client is no longer needed.
        """
        self._is_closed.set()
        with self._executor:
            self._executor.shutdown(wait=False)
        self._channel_refresh_tasks = []
        self.transport.close()


class Table(Table_SyncGen):
    def _register_with_client(self) -> concurrent.futures.Future[None]:
        return self.client._executor.submit(
            self.client._register_instance, self.instance_id, self
        )

    def _shard_batch_helper(
        self, kwargs_list: list[dict]
    ) -> list[list[Row] | BaseException]:
        futures_list = [
            self.client._executor.submit(self.read_rows, **kwargs)
            for kwargs in kwargs_list
        ]
        results_list: list[list[Row] | BaseException] = []
        for future in futures_list:
            if future.exception():
                results_list.append(future.exception())
            else:
                result = future.result()
                if result is not None:
                    results_list.append(result)
        return results_list

    def __enter__(self):
        """
        Implement  context manager protocol

        Ensure registration task has time to run, so that
        grpc channels will be warmed for the specified instance
        """
        if self._register_instance_future:
            self._register_instance_future.result()
        return self
