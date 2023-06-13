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
from __future__ import annotations

from typing import (
    Callable,
)
import asyncio
from dataclasses import dataclass
from functools import partial

import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore
from google.cloud.bigtable._channel_pooling.wrapped_channel import WrappedUnaryUnaryMultiCallable
from google.cloud.bigtable._channel_pooling.wrapped_channel import WrappedUnaryStreamMultiCallable
from google.cloud.bigtable._channel_pooling.wrapped_channel import WrappedStreamUnaryMultiCallable
from google.cloud.bigtable._channel_pooling.wrapped_channel import WrappedStreamStreamMultiCallable


@dataclass
class StaticPoolOptions:
    pool_size: int = 3


class PooledChannel(aio.Channel):
    def __init__(
        self,
        *args,
        create_channel_fn: Callable[..., aio.Channel] = lambda: None,
        pool_options: StaticPoolOptions | None = None,
        **kwargs,
    ):
        self._pool: list[aio.Channel] = []
        self._next_idx = 0
        self._create_channel : Callable[[], aio.Channel] = partial(create_channel_fn, *args, **kwargs)
        pool_options = pool_options or StaticPoolOptions()
        for i in range(pool_options.pool_size):
            self._pool.append(self._create_channel())

    def next_channel(self) -> aio.Channel:
        next_idx = self._next_idx if self._next_idx < len(self._pool) else 0
        channel = self._pool[next_idx]
        self._next_idx = (next_idx + 1) % len(self._pool)
        return channel

    def unary_unary(self, *args, **kwargs) -> grpc.aio.UnaryUnaryMultiCallable:
        return WrappedUnaryUnaryMultiCallable(
            lambda: self.next_channel().unary_unary(*args, **kwargs)
        )

    def unary_stream(self, *args, **kwargs) -> grpc.aio.UnaryStreamMultiCallable:
        return WrappedUnaryStreamMultiCallable(
            lambda: self.next_channel().unary_stream(*args, **kwargs)
        )

    def stream_unary(self, *args, **kwargs) -> grpc.aio.StreamUnaryMultiCallable:
        return WrappedStreamUnaryMultiCallable(
            lambda: self.next_channel().stream_unary(*args, **kwargs)
        )

    def stream_stream(self, *args, **kwargs) -> grpc.aio.StreamStreamMultiCallable:
        return WrappedStreamStreamMultiCallable(
            lambda: self.next_channel().stream_stream(*args, **kwargs)
        )

    async def close(self, grace=None):
        close_fns = [channel.close(grace=grace) for channel in self._pool]
        return await asyncio.gather(*close_fns)

    async def channel_ready(self):
        ready_fns = [channel.channel_ready() for channel in self._pool]
        return asyncio.gather(*ready_fns)

    async def __aenter__(self):
        for channel in self._pool:
            await channel.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        for channel in self._pool:
            await channel.__aexit__(exc_type, exc_val, exc_tb)

    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        raise NotImplementedError()

    async def wait_for_state_change(self, last_observed_state):
        raise NotImplementedError()

    def index_of(self, channel) -> int:
        try:
            return self._pool.index(channel)
        except ValueError:
            return -1

    @property
    def channels(self) -> list[aio.Channel]:
        return self._pool

    def __getitem__(self, item: int) -> aio.Channel:
        return self._pool[item]
