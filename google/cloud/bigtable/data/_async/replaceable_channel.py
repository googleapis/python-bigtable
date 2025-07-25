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
# limitations under the License
#
from __future__ import annotations

from typing import Callable

import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore

from google.cloud.bigtable.data._cross_sync import CrossSync

class _AsyncReplaceableChannel(aio.Channel):
    """
    A wrapper around a gRPC channel. All methods are passed
    through to the underlying channel.
    """

    def __init__(self, channel_fn: Callable[[], aio.Channel]):
        self._channel_fn = channel_fn
        self._channel = channel_fn()

    def unary_unary(self, *args, **kwargs):
        return self._channel.unary_unary(*args, **kwargs)

    def unary_stream(self, *args, **kwargs):
        return self._channel.unary_stream(*args, **kwargs)

    def stream_unary(self, *args, **kwargs):
        return self._channel.stream_unary(*args, **kwargs)

    def stream_stream(self, *args, **kwargs):
        return self._channel.stream_stream(*args, **kwargs)

    async def close(self, grace=None):
        return await self._channel.close(grace=grace)

    async def channel_ready(self):
        return await self._channel.channel_ready()

    async def __aenter__(self):
        await self._channel.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)

    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        return self._channel.get_state(try_to_connect=try_to_connect)

    async def wait_for_state_change(self, last_observed_state):
        return await self._channel.wait_for_state_change(last_observed_state)

    @property
    def wrapped_channel(self):
        return self._channel

    def create_channel(self) -> aio.Channel:
        return self._channel_fn()

    async def replace_channel(self, new_channel: aio.Channel, grace_period: float | None) -> aio.Channel:
        old_channel = self._channel
        self._channel = new_channel
        # give old_channel a chance to complete existing rpcs
        if CrossSync.is_async:
            await old_channel.close(grace_period)
        else:
            if grace_period:
                self._is_closed.wait(grace_period)  # type: ignore
            old_channel.close()  # type: ignore
        return old_channel

    @property
    def _unary_unary_interceptors(self):
        # return empty list for compatibility with gapic layer
        return []