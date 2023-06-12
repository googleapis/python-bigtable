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

import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore


@dataclass
class StaticPoolOptions:
    pool_size: int = 3


class PooledMultiCallable:
    def __init__(self, channel_pool: "PooledChannel", *args, **kwargs):
        self._init_args = args
        self._init_kwargs = kwargs
        self.next_channel_fn = channel_pool.next_channel


class PooledUnaryUnaryMultiCallable(PooledMultiCallable, aio.UnaryUnaryMultiCallable):
    def __call__(self, *args, **kwargs) -> aio.UnaryUnaryCall:
        return self.next_channel_fn().unary_unary(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class PooledUnaryStreamMultiCallable(PooledMultiCallable, aio.UnaryStreamMultiCallable):
    def __call__(self, *args, **kwargs) -> aio.UnaryStreamCall:
        return self.next_channel_fn().unary_stream(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class PooledStreamUnaryMultiCallable(PooledMultiCallable, aio.StreamUnaryMultiCallable):
    def __call__(self, *args, **kwargs) -> aio.StreamUnaryCall:
        return self.next_channel_fn().stream_unary(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class PooledStreamStreamMultiCallable(
    PooledMultiCallable, aio.StreamStreamMultiCallable
):
    def __call__(self, *args, **kwargs) -> aio.StreamStreamCall:
        return self.next_channel_fn().stream_stream(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class PooledChannel(aio.Channel):
    def __init__(
        self,
        create_channel_fn: Callable[[], aio.Channel],
        pool_options: StaticPoolOptions | None = None,
    ):
        self._pool: list[aio.Channel] = []
        self._next_idx = 0
        self._create_channel = create_channel_fn
        pool_options = pool_options or StaticPoolOptions()
        for i in range(pool_options.pool_size):
            self._pool.append(self._create_channel())

    def next_channel(self) -> aio.Channel:
        channel = self._pool[self._next_idx]
        self._next_idx = (self._next_idx + 1) % len(self._pool)
        return channel

    def unary_unary(self, *args, **kwargs) -> grpc.aio.UnaryUnaryMultiCallable:
        return PooledUnaryUnaryMultiCallable(self, *args, **kwargs)

    def unary_stream(self, *args, **kwargs) -> grpc.aio.UnaryStreamMultiCallable:
        return PooledUnaryStreamMultiCallable(self, *args, **kwargs)

    def stream_unary(self, *args, **kwargs) -> grpc.aio.StreamUnaryMultiCallable:
        return PooledStreamUnaryMultiCallable(self, *args, **kwargs)

    def stream_stream(self, *args, **kwargs) -> grpc.aio.StreamStreamMultiCallable:
        return PooledStreamStreamMultiCallable(self, *args, **kwargs)

    async def close(self, grace=None):
        close_fns = [channel.close(grace=grace) for channel in self._pool]
        return await asyncio.gather(*close_fns)

    async def channel_ready(self):
        ready_fns = [channel.channel_ready() for channel in self._pool]
        return asyncio.gather(*ready_fns)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

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
