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

from typing import Any, TYPE_CHECKING

import time
import random
import threading

import google.auth.credentials
import concurrent.futures

from google.cloud.bigtable.data._sync._autogen import BigtableDataClient_SyncGen
from google.cloud.bigtable.data._sync._autogen import Table_SyncGen

# import required so Table_SyncGen can create _MutateRowsOperation and _ReadRowsOperation
import google.cloud.bigtable.data._sync._read_rows  # noqa: F401
import google.cloud.bigtable.data._sync._mutate_rows  # noqa: F401

from google.cloud.bigtable_v2.services.bigtable.client import BigtableClientMeta
from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc import (
    PooledBigtableGrpcTransport,
    PooledChannel
)
from google.cloud.bigtable_v2.types.bigtable import PingAndWarmRequest

if TYPE_CHECKING:
    import grpc
    from google.cloud.bigtable.data.row import Row
    from google.cloud.bigtable.data._helpers import _WarmedInstanceKey


class BigtableDataClient(BigtableDataClient_SyncGen):

    @property
    def _executor(self) -> concurrent.futures.ThreadPoolExecutor:
        if not hasattr(self, "_executor_instance"):
            self._executor_instance = concurrent.futures.ThreadPoolExecutor()
        return self._executor_instance

    @property
    def _is_closed(self) -> threading.Event:
        if not hasattr(self, "_is_closed_instance"):
            self._is_closed_instance = threading.Event()
        return self._is_closed_instance

    def _transport_init(self, pool_size: int) -> str:
        transport_str = f"pooled_grpc_{pool_size}"
        transport = PooledBigtableGrpcTransport.with_fixed_size(pool_size)
        BigtableClientMeta._transport_registry[transport_str] = transport
        return transport_str

    def _prep_emulator_channel(self, host:str, pool_size: int) -> str:
        self.transport._grpc_channel = PooledChannel(
            pool_size=pool_size,
            host=host,
            insecure=True,
        )

    @staticmethod
    def _client_version() -> str:
        return f"{google.cloud.bigtable.__version__}-data"

    def _start_background_channel_refresh(self) -> None:
        if not self._channel_refresh_tasks and not self._emulator_host:
            for channel_idx in range(self.transport.pool_size):
                self._channel_refresh_tasks.append(
                    self._executor.submit(self._manage_channel, channel_idx)
                )

    def _manage_channel(
        self,
        channel_idx: int,
        refresh_interval_min: float = 60 * 35,
        refresh_interval_max: float = 60 * 45,
        grace_period: float = 60 * 10,
    ) -> None:
        """
        Background routine that periodically refreshes and warms a grpc channel

        The backend will automatically close channels after 60 minutes, so
        `refresh_interval` + `grace_period` should be < 60 minutes

        Runs continuously until the client is closed

        Args:
            channel_idx: index of the channel in the transport's channel pool
            refresh_interval_min: minimum interval before initiating refresh
                process in seconds. Actual interval will be a random value
                between `refresh_interval_min` and `refresh_interval_max`
            refresh_interval_max: maximum interval before initiating refresh
                process in seconds. Actual interval will be a random value
                between `refresh_interval_min` and `refresh_interval_max`
            grace_period: time to allow previous channel to serve existing
                requests before closing, in seconds
        """
        first_refresh = self._channel_init_time + random.uniform(
            refresh_interval_min, refresh_interval_max
        )
        next_sleep = max(first_refresh - time.monotonic(), 0)
        if next_sleep > 0:
            # warm the current channel immediately
            channel = self.transport.channels[channel_idx]
            self._ping_and_warm_instances(channel)
        # continuously refresh the channel every `refresh_interval` seconds
        while not self._is_closed.is_set():
            # sleep until next refresh, or until client is closed
            self._is_closed.wait(next_sleep)
            if self._is_closed.is_set():
                break
            # prepare new channel for use
            new_channel = self.transport.grpc_channel._create_channel()
            self._ping_and_warm_instances(new_channel)
            # cycle channel out of use, with long grace window before closure
            start_timestamp = time.monotonic()
            self.transport.replace_channel(
                channel_idx, grace=grace_period, new_channel=new_channel, event=self._is_closed
            )
            # subtract the time spent waiting for the channel to be replaced
            next_refresh = random.uniform(refresh_interval_min, refresh_interval_max)
            next_sleep = next_refresh - (time.monotonic() - start_timestamp)

    def _ping_and_warm_instances(
        self, channel: grpc.Channel, instance_key: _WarmedInstanceKey | None = None
    ) -> list[BaseException | None]:
        """
        Prepares the backend for requests on a channel

        Pings each Bigtable instance registered in `_active_instances` on the client

        Args:
            - channel: grpc channel to warm
            - instance_key: if provided, only warm the instance associated with the key
        Returns:
            - sequence of results or exceptions from the ping requests
        """
        instance_list = (
            [instance_key] if instance_key is not None else self._active_instances
        )
        ping_rpc = channel.unary_unary(
            "/google.bigtable.v2.Bigtable/PingAndWarm",
            request_serializer=PingAndWarmRequest.serialize,
        )
        # execute pings in parallel
        futures_list = []
        for (instance_name, table_name, app_profile_id) in instance_list:
            future = self._executor.submit(
                ping_rpc,
                request={"name": instance_name, "app_profile_id": app_profile_id},
                metadata=[
                    (
                        "x-goog-request-params",
                        f"name={instance_name}&app_profile_id={app_profile_id}",
                    )
                ],
                wait_for_ready=True,
            )
            futures_list.append(future)
        results_list = []
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
        super().close()


class Table(Table_SyncGen):

    def _register_with_client(self):
        self.client._register_instance(self.instance_id, self)
        self._register_instance_task = None

    def _shard_batch_helper(self, kwargs_list: list[dict]) -> list[list[Row] | BaseException]:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures_list = [executor.submit(self.read_rows, **kwargs) for kwargs in kwargs_list]
        results_list: list[list[Row] | BaseException] = []
        for future in futures_list:
            if future.exception():
                results_list.append(future.exception())
            else:
                results_list.append(future.result())
        return results_list
