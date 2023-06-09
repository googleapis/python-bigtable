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
        pool_size: int = 3,
        host: str = "bigtable.googleapis.com",
        credentials: Optional[ga_credentials.Credentials] = None,
        credentials_file: Optional[str] = None,
        quota_project_id: Optional[str] = None,
        default_scopes: Optional[Sequence[str]] = None,
        scopes: Optional[Sequence[str]] = None,
        default_host: Optional[str] = None,
        insecure: bool = False,
        channel_init_callback: Callable[[aio.Channel], Awaitable[None]] = None
        ** kwargs,
    ):
        self._pool: List[aio.Channel] = []
        self._next_idx = 0
        self._insecure_channel = insecure
        self._create_channel_kwargs = {
            "target": host,
            "credentials": credentials,
            "credentials_file": credentials_file,
            "quota_project_id": quota_project_id,
            "default_scopes": default_scopes,
            "scopes": scopes,
            "default_host": default_host,
            **kwargs,
        }
        for i in range(pool_size):
            self._pool.append(self._create_channel())
        # schedule init task on each channel
        self.channel_init_callback = channel_init_callback
        if channel_init_callback:
            self._init_task = asyncio.gather(
                [channel_init_callback(c) for c in self._pool]
            )
        else:
            self._init_task = None

    def _create_channel(self):
        if self._insecure_channel:
            return aio.insecure_channel(self._create_channel_kwargs["target"])
        else:
            return grpc_helpers_async.create_channel(**self._create_channel_kwargs)

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
        if self._init_task:
            await self._init_task
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        raise NotImplementedError()

    async def wait_for_state_change(self, last_observed_state):
        raise NotImplementedError()

    async def replace_channel(self, channel_idx) -> tuple[aio.Channel, aio.Channel]:
        """
        Replaces a channel in the pool with a fresh one.

        The `new_channel` will start processing new requests immidiately,
        but the old channel will continue serving existing clients for `grace` seconds

        Args:
          channel_idx(int): the channel index in the pool to replace
          grace(Optional[float]): The time to wait until all active RPCs are
            finished. If a grace period is not specified (by passing None for
            grace), all existing RPCs are cancelled immediately.
          swap_sleep(Optional[float]): The number of seconds to sleep in between
            replacing channels and closing the old one
          new_channel(grpc.aio.Channel): a new channel to insert into the pool
            at `channel_idx`. If `None`, a new channel will be created.
        """
        if channel_idx >= len(self._pool) or channel_idx < 0:
            raise ValueError(
                f"invalid channel_idx {channel_idx} for pool size {len(self._pool)}"
            )
        if new_channel is None:
            new_channel = self._create_channel()
            if self.channel_init_callback:
                await self.channel_init_callback(new_channel)
        old_channel = self._pool[channel_idx]
        self._pool[channel_idx] = new_channel
        return old_channel, new_channel
