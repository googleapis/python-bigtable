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

from typing import Any, Callable, Coroutine

import asyncio
import random
from time import monotonic
from functools import partial
from grpc.experimental import aio  # type: ignore

from google.cloud.bigtable._channel_pooling.wrapped_channel import (
    _WrappedChannel,
    _BackgroundTaskMixin,
)


class RefreshableChannel(_WrappedChannel, _BackgroundTaskMixin):
    """
    A Channel that refreshes itself periodically.
    """

    def __init__(
        self,
        *args,
        create_channel_fn: Callable[..., aio.Channel] | None = None,
        refresh_interval_min: float = 60 * 35,
        refresh_interval_max: float = 60 * 45,
        warm_channel_fn: Callable[[aio.Channel], Coroutine[Any, Any, Any]]
        | None = None,
        on_replace: Callable[[aio.Channel], Coroutine[Any, Any, Any]] | None = None,
        **kwargs,
    ):
        if create_channel_fn is None:
            raise ValueError("create_channel_fn is required")
        self._create_channel: Callable[[], aio.Channel] = partial(
            create_channel_fn, *args, **kwargs
        )
        self._warm_channel = warm_channel_fn
        self._on_replace = on_replace
        self._channel = create_channel_fn()
        self.refresh_interval_min = refresh_interval_min
        self.refresh_interval_max = refresh_interval_max
        self._background_task: asyncio.Task[None] | None = None
        self.start_background_task()

    def _background_coroutine(self) -> Coroutine[Any, Any, None]:
        return self._manage_channel_lifecycle(
            self.refresh_interval_min, self.refresh_interval_max
        )

    @property
    def _task_description(self) -> str:
        return "Background channel refresh"

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
        if self._warm_channel:
            await self._warm_channel(self._channel)
        next_sleep = random.uniform(refresh_interval_min, refresh_interval_max)
        while True:
            # let channel run for `sleep_time` seconds, then remove it from pool
            await asyncio.sleep(next_sleep)
            # cycle channel out of use, with long grace window before closure
            start_timestamp = monotonic()
            new_channel = self._create_channel()
            if self._warm_channel:
                await self._warm_channel(new_channel)
            await new_channel.channel_ready()
            old_channel, self._channel = self._channel, new_channel
            if self._on_replace:
                await self._on_replace(old_channel)
            # find new sleep time based on how long the refresh process took
            next_refresh = random.uniform(refresh_interval_min, refresh_interval_max)
            next_sleep = next_refresh - (monotonic() - start_timestamp)

    async def __aenter__(self):
        await _BackgroundTaskMixin.__aenter__(self)
        await _WrappedChannel.__aenter__(self)

    async def close(self, grace=None):
        await _BackgroundTaskMixin.close(self, grace)
        await _WrappedChannel.close(self, grace)
