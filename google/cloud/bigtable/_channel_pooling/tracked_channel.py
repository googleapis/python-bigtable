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

from contextlib import contextmanager
from grpc.experimental import aio  # type: ignore
from google.api_core.grpc_helpers_async import _WrappedUnaryResponseMixin
from google.api_core.grpc_helpers_async import _WrappedStreamResponseMixin

from google.cloud.bigtable._channel_pooling.wrapped_channel import _WrappedChannel


class _TrackedUnaryResponseMixin(_WrappedUnaryResponseMixin):
    def __init__(self, call, channel):
        super().__init__()
        self._call: aio.UnaryUnaryCall | aio.StreamUnaryCall = call
        self._channel = channel

    def __await__(self):
        with self._channel.track_rpc():
            response = yield from self._call.__await__()
            return response

    def __getattr__(self, attr):
        return getattr(self._call, attr)


class _TrackedStreamResponseMixin(_WrappedStreamResponseMixin):
    def __init__(self, call, channel):
        super().__init__()
        self._call: aio.UnaryStreamCall | aio.StreamStreamMultiCallable = call
        self._channel = channel

    async def read(self):
        with self._channel.track_rpc():
            return await self._call.read()

    async def _wrapped_aiter(self):
        with self._channel.track_rpc():
            async for item in self._call:
                yield item

    def __getattr__(self, attr):
        return getattr(self._call, attr)


class TrackedUnaryUnaryCall(_TrackedUnaryResponseMixin, aio.UnaryUnaryCall):
    pass


class TrackedUnaryStreamCall(_TrackedStreamResponseMixin, aio.UnaryStreamCall):
    pass


class TrackedStreamUnaryCall(_TrackedUnaryResponseMixin, aio.StreamUnaryCall):
    pass


class TrackedStreamStreamCall(_TrackedStreamResponseMixin, aio.StreamStreamCall):
    pass


class TrackedChannel(_WrappedChannel):
    """
    A Channel that tracks the number of active RPCs
    """

    def __init__(self, channel: aio.Channel):
        super().__init__(channel)
        self.active_rpcs = 0
        self.max_active_rpcs = 0

    @contextmanager
    def track_rpc(self):
        self.active_rpcs += 1
        self.max_active_rpcs = max(self.max_active_rpcs, self.active_rpcs)
        try:
            yield
        finally:
            self.active_rpcs -= 1

    def get_and_reset_max_active_rpcs(self) -> int:
        current_max, self.max_active_rpcs = self.max_active_rpcs, self.active_rpcs
        return current_max

    def unary_unary(self, *args, **kwargs):
        multicallable = self._channel.unary_unary(*args, **kwargs)
        return lambda *args, **kwargs: TrackedUnaryUnaryCall(
            multicallable(*args, **kwargs), self
        )

    def unary_stream(self, *args, **kwargs):
        multicallable = self._channel.unary_stream(*args, **kwargs)
        return lambda *args, **kwargs: TrackedUnaryStreamCall(
            multicallable(*args, **kwargs), self
        )

    def stream_unary(self, *args, **kwargs):
        multicallable = self._channel.stream_unary(*args, **kwargs)
        return lambda *args, **kwargs: TrackedStreamUnaryCall(
            multicallable(*args, **kwargs), self
        )

    def stream_stream(self, *args, **kwargs):
        multicallable = self._channel.stream_stream(*args, **kwargs)
        return lambda *args, **kwargs: TrackedStreamStreamCall(
            multicallable(*args, **kwargs), self
        )
