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
#
from __future__ import annotations

from typing import Callable, Coroutine

import asyncio
import random
from time import monotonic
import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore


class RefreshableChannel(aio.Channel):
    """
    A Channel that refreshes itself periodically.
    """

    def __init__(
        self,
        create_channel_fn: Callable[[], aio.Channel],
        refresh_interval_min: float = 60 * 35,
        refresh_interval_max: float = 60 * 45,
        on_replace: Callable[[aio.Channel], Coroutine[None, None, None]] | None = None,
    ):
        self._create_channel = create_channel_fn
        self._on_replace = on_replace
        self._channel = create_channel_fn()
        self._refresh_task = asyncio.create_task(
            self._manage_channel_lifecycle(refresh_interval_min, refresh_interval_max)
        )

    async def _manage_channel_lifecycle(
        self,
        refresh_interval_min: float = 60 * 35,
        refresh_interval_max: float = 60 * 45,
    ) -> None:
        """
        Background coroutine that periodically refreshes and warms a grpc channel

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
        next_sleep = random.uniform(refresh_interval_min, refresh_interval_max)
        while True:
            # let channel run for `sleep_time` seconds, then remove it from pool
            await asyncio.sleep(next_sleep)
            # cycle channel out of use, with long grace window before closure
            start_timestamp = monotonic()
            new_channel = self._create_channel()
            await new_channel.channel_ready()
            old_channel, self._channel = self._channel, new_channel
            if self._on_replace:
                await self._on_replace(old_channel)
            # find new sleep time based on how long the refresh process took
            next_refresh = random.uniform(refresh_interval_min, refresh_interval_max)
            next_sleep = next_refresh - (monotonic() - start_timestamp)

    def unary_unary(self, *args, **kwargs) -> aio.UnaryUnaryMultiCallable:
        return self._channel.unary_unary(*args, **kwargs)

    def unary_stream(self, *args, **kwargs) -> aio.UnaryStreamMultiCallable:
        return self._channel.unary_stream(*args, **kwargs)

    def stream_unary(self, *args, **kwargs) -> aio.StreamUnaryMultiCallable:
        return self._channel.stream_unary(*args, **kwargs)

    def stream_stream(self, *args, **kwargs) -> aio.StreamStreamMultiCallable:
        return self._channel.stream_stream(*args, **kwargs)

    async def close(self, grace=None):
        self._refresh_task.cancel()
        return await self._channel.close(grace=grace)

    async def channel_ready(self):
        return await self._channel.channel_ready()

    async def __aenter__(self):
        return await self._channel.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)

    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        return self._channel.get_state(try_to_connect=try_to_connect)

    async def wait_for_state_change(self, last_observed_state):
        return await self._channel.wait_for_state_change(last_observed_state)
