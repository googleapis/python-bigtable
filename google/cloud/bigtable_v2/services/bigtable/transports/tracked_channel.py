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


class TrackedAioChannel(aio.Channel):
    """
    A Channel that tracks the number of active RPCs
    """
    def __init__(self, channel: aio.Channel):
        self._channel = channel
        self.active_rpcs = 0

    def retain(self):
        self.active_rpcs += 1

    def release(self):
        self.active_rpcs -= 1


    async def wrapped_unary(self, unary_call, *args, **kwargs):
        self.retain()
        result = await unary_call(*args, **kwargs)
        self.release()
        return result

    async def wrapped_stream(self, stream, *args, **kwargs):
        self.retain()
        try:
            async for result in stream(*args, **kwargs):
                yield result
        finally:
            self.release()

    def unary_unary(self, *args, **kwargs) -> grpc.aio.UnaryUnaryMultiCallable:
        call = self._channel.unary_unary(*args, **kwargs)
        return functools.partial(self.wrapped_unary, call)

    def unary_stream(self, *args, **kwargs) -> grpc.aio.UnaryStreamMultiCallable:
        stream = self._channel.unary_stream(*args, **kwargs)
        return functools.partial(self.wrapped_stream, stream)

    def stream_unary(self, *args, **kwargs) -> grpc.aio.StreamUnaryMultiCallable:
        call = self._channel.stream_unary(*args, **kwargs)
        return functools.partial(self.wrapped_unary, call)

    def stream_stream(self, *args, **kwargs) -> grpc.aio.StreamStreamMultiCallable:
        stream = self._channel.stream_stream(*args, **kwargs)
        return functools.partial(self.wrapped_stream, stream)

    async def close(self, grace=None):
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


