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
# limitations under the License.
#
from __future__ import annotations

from typing import Callable

from google.cloud.bigtable.data._cross_sync import CrossSync

from grpc import ChannelConnectivity

if CrossSync.is_async:
    from grpc.aio import Channel
else:
    from grpc import Channel

__CROSS_SYNC_OUTPUT__ = "google.cloud.bigtable.data._sync_autogen._replaceable_channel"


@CrossSync.convert_class(sync_name="_WrappedChannel", rm_aio=True)
class _AsyncWrappedChannel(Channel):
    """
    A wrapper around a gRPC channel. All methods are passed
    through to the underlying channel.
    """

    def __init__(self, channel: Channel):
        self._channel = channel

    def unary_unary(self, *args, **kwargs):
        return self._channel.unary_unary(*args, **kwargs)

    def unary_stream(self, *args, **kwargs):
        return self._channel.unary_stream(*args, **kwargs)

    def stream_unary(self, *args, **kwargs):
        return self._channel.stream_unary(*args, **kwargs)

    def stream_stream(self, *args, **kwargs):
        return self._channel.stream_stream(*args, **kwargs)

    async def channel_ready(self):
        return await self._channel.channel_ready()

    @CrossSync.convert(
        sync_name="__enter__", replace_symbols={"__aenter__": "__enter__"}
    )
    async def __aenter__(self):
        await self._channel.__aenter__()
        return self

    @CrossSync.convert(sync_name="__exit__", replace_symbols={"__aexit__": "__exit__"})
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)

    def get_state(self, try_to_connect: bool = False) -> ChannelConnectivity:
        return self._channel.get_state(try_to_connect=try_to_connect)

    async def wait_for_state_change(self, last_observed_state):
        return await self._channel.wait_for_state_change(last_observed_state)

    def __getattr__(self, name):
        return getattr(self._channel, name)

    if CrossSync.is_async:
        # grace not supported by sync version
        async def close(self, grace=None):
            return await self._channel.close(grace=grace)

    else:
        # add required sync methods

        def close(self):
            return self._channel.close()

        def subscribe(self, callback, try_to_connect=False):
            return self._channel.subscribe(callback, try_to_connect)

        def unsubscribe(self, callback):
            return self._channel.unsubscribe(callback)


@CrossSync.convert_class(
    sync_name="_ReplaceableChannel",
    replace_symbols={"_AsyncWrappedChannel": "_WrappedChannel"},
)
class _AsyncReplaceableChannel(_AsyncWrappedChannel):
    def __init__(self, channel_fn: Callable[[], Channel]):
        self._channel_fn = channel_fn
        self._channel = channel_fn()

    def create_channel(self) -> Channel:
        new_channel = self._channel_fn()
        if CrossSync.is_async:
            # copy over interceptors
            # this is needed because of how gapic attaches the LoggingClientAIOInterceptor
            # sync channels add interceptors by wrapping, so this step isn't needed
            new_channel._unary_unary_interceptors = (
                self._channel._unary_unary_interceptors
            )
            new_channel._unary_stream_interceptors = (
                self._channel._unary_stream_interceptors
            )
            new_channel._stream_unary_interceptors = (
                self._channel._stream_unary_interceptors
            )
            new_channel._stream_stream_interceptors = (
                self._channel._stream_stream_interceptors
            )
        return new_channel

    def replace_wrapped_channel(self, new_channel: Channel) -> Channel:
        old_channel = self._channel
        self._channel = new_channel
        return old_channel
