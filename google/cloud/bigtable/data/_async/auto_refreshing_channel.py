# Copyright 2025 Google LLC
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
from __future__ import annotations

from typing import Callable
from abc import ABC, abstractmethod

import asyncio
import warnings
import grpc
import random
import time
from grpc import aio
from google.cloud.bigtable.data._cross_sync import CrossSync

async def create_warmed_channel(transport, client_options, client):
    channel = transport.create_channel(
        transport._host,
        credentials=transport._credentials,
        credentials_file=None,
        scopes=transport._scopes,
        ssl_credentials=transport._ssl_channel_credentials,
        quota_project=client_options.qupta_project_id,
        options=[
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
        ]
    )
    await client._ping_and_warm_instances(channel=channel)
    return channel

class AutoRefreshingChannel(aio.Channel):
    """
    A wrapper around a gRPC channel. All methods are passed
    through to the underlying channel.
    """

    def __init__(
        self,
        channel: aio.Channel,
        new_channel_fn: Callable[[], aio.Channel],
    ):
        self._cannel = channel
        self._channel_fn = new_channel_fn
        self._is_closed = CrossSync.Event()
        self._channel_init_time = time.monotonic()
        self._channel_refresh_task: CrossSync.Task[None] | None = None

    async def _manage_channel(
        self,
        refresh_interval_min: float = 60 * 35,
        refresh_interval_max: float = 60 * 45,
        grace_period: float = 60 * 10,
    ) -> None:
        """
        Background task that periodically refreshes and warms a grpc channel

        The backend will automatically close channels after 60 minutes, so
        `refresh_interval` + `grace_period` should be < 60 minutes

        Runs continuously until the client is closed

        Args:
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
        # continuously refresh the channel every `refresh_interval` seconds
        while not self._is_closed.is_set():
            await CrossSync.event_wait(
                self._is_closed,
                next_sleep,
                async_break_early=False,  # no need to interrupt sleep. Task will be cancelled on close
            )
            if self._is_closed.is_set():
                # don't refresh if client is closed
                break
            start_timestamp = time.monotonic()
            # prepare new channel for use
            old_channel = self._channel
            new_channel = await self._channel_fn()
            # cycle channel out of use, with long grace window before closure
            self._channel = new_channel
            # give old_channel a chance to complete existing rpcs
            if CrossSync.is_async:
                await old_channel.close(grace_period)
            else:
                if grace_period:
                    self._is_closed.wait(grace_period)  # type: ignore
                old_channel.close()  # type: ignore
            # subtract thed time spent waiting for the channel to be replaced
            next_refresh = random.uniform(refresh_interval_min, refresh_interval_max)
            next_sleep = max(next_refresh - (time.monotonic() - start_timestamp), 0)

    def unary_unary(self, *args, **kwargs):
        return self._channel.unary_unary(*args, **kwargs)

    def unary_stream(self, *args, **kwargs):
        return self._channel.unary_stream(*args, **kwargs)

    def stream_unary(self, *args, **kwargs):
        return self._channel.stream_unary(*args, **kwargs)

    def stream_stream(self, *args, **kwargs):
        return self._channel.stream_stream(*args, **kwargs)

    async def close(self, grace=None):
        self._is_closed.set()
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