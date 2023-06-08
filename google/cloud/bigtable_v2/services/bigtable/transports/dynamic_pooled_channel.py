# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
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

import asyncio
import warnings
from functools import partialmethod
from functools import partial
from typing import (
    Awaitable,
    Callable,
    Dict,
    Optional,
    Sequence,
    Tuple,
    Union,
    List,
    Type,
)

from google.api_core import gapic_v1
from google.api_core import grpc_helpers_async
from google.auth import credentials as ga_credentials  # type: ignore
from google.auth.transport.grpc import SslCredentials  # type: ignore

import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore

from google.cloud.bigtable_v2.types import bigtable
from .base import BigtableTransport, DEFAULT_CLIENT_INFO
from .grpc_asyncio import BigtableGrpcAsyncIOTransport

from .pooled_channel import PooledChannel
from .tracked_channel import TrackedAioChannel

class DynamicPooledChannel(PooledChannel):

    def __init__(
        self,
        start_pool_size: int = 3,
        host: str = "bigtable.googleapis.com",
        credentials: Optional[ga_credentials.Credentials] = None,
        credentials_file: Optional[str] = None,
        quota_project_id: Optional[str] = None,
        default_scopes: Optional[Sequence[str]] = None,
        scopes: Optional[Sequence[str]] = None,
        default_host: Optional[str] = None,
        insecure: bool = False,
        **kwargs,
    ):
        self._pool: List[TrackedAioChannel] = []
        self._next_idx = 0
        if insecure:
            create_base_channel = partial(aio.insecure_channel, host)
        else:
            create_base_channel = partial(
                grpc_helpers_async.create_channel,
                target=host,
                credentials=credentials,
                credentials_file=credentials_file,
                quota_project_id=quota_project_id,
                default_scopes=default_scopes,
                scopes=scopes,
                default_host=default_host,
                **kwargs,
            )
        self._create_channel = lambda: TrackedAioChannel(create_base_channel())
        for i in range(pool_size):
            self._pool.append(self._create_channel())

        self.MAX_RPCS_PER_CHANNEL = 100
        self.MIN_RPCS_PER_CHANNEL = 50
        self.MAX_CHANNEL_COUNT = 20
        self.MIN_CHANNEL_COUNT = 1
        self.MAX_RESIZE_DELTA = 2 # how many to change in a single resize

    async def resize():
        """
        Called periodically to resize the number of channels based on
        the number of active RPCs
        """
        # TODO: add lock. share with replace_channel
        # estimate the peak rpcs since last resize
        # peak finds max active value for each channel since last check
        estimated_peak = sum([channel.get_and_reset_max_active_rpcs() for channel in self._pool])
        # find the minimum number of channels to serve the peak
        min_channels = estimated_peak // self.MAX_RPCS_PER_CHANNEL
        # find the maxiumum channels we'd want to serve the peak
        max_channels = estimated_peak // max(self.MIN_RPCS_PER_CHANNEL, 1)
        # clamp the number of channels to the min and max
        min_channels = max(min_channels, self.MIN_CHANNEL_COUNT)
        max_channels = min(max_channels, self.MAX_CHANNEL_COUNT)
        # Only resize the pool when thresholds are crossed
        current_size = len(self._pool)
        if current_size < min_channels or current_size > max_channels:
            # try to aim for the middle of the bound, but limit rate of change.
            tentative_target = (maxChannels + minChannels) // 2;
            delta = tentative_target - current_size;
            dampened_delta = min(max(delta, -self.MAX_RESIZE_DELTA), self.MAX_RESIZE_DELTA)
            dampened_target = current_size + dampened_delta
            if dampened_target > current_size:
                new_channels = [self._create_channel() for _ in range(dampened_delta)]
                self._pool.extend(new_channels)
            elif dampened_target < current_size:
                self._pool, remove = self._pool[:dampened_target], self._pool[dampened_target:]
                # close channels gracefully
                # TODO: magic number?
                await asyncio.gather(*[channel.close(grace=600) for channel in remove], return_exceptions=True)




