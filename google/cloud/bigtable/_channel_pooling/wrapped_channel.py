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

from typing import Callable
from abc import ABC, abstractmethod

import asyncio
import warnings
import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore


class _WrappedChannel(aio.Channel):
    """
    A wrapper around a gRPC channel. All methods are passed
    through to the underlying channel.
    """

    def __init__(self, channel: aio.Channel):
        self._channel = channel

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
        return await self._channel.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)

    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        return self._channel.get_state(try_to_connect=try_to_connect)

    async def wait_for_state_change(self, last_observed_state):
        return await self._channel.wait_for_state_change(last_observed_state)

    @property
    def wrapped_channel(self):
        return self._channel


class _BackgroundTaskMixin(ABC):
    """
    A mixin that provides methods to manage a background task that
    is run throughout the lifetime of the object.
    """

    def __init__(self):
        self._background_task: asyncio.Task[None] | None = None

    def background_task_is_active(self) -> bool:
        """
        returns True if the background task is currently running
        """
        return self._background_task is not None and not self._background_task.done()

    @abstractmethod
    def _background_coroutine(self):
        """
        To be implemented by subclasses. Returns the coroutine that will
        be run in the background throughout the lifetime of the channel.
        """
        pass

    @property
    def _task_description(self) -> str:
        """
        Describe what the background task does.
        String will be displayed along with error message to describe
        the consequences when the task can not be started.

        Example: "Automatic channel pool resizing"
        """
        return "Background task"

    def start_background_task(self):
        """
        Start background task to manage channel lifecycle. If background
        task is already running, do nothing. If run outside of an asyncio
        event loop, print a warning and do nothing.
        """
        if self.background_task_is_active():
            return
        try:
            asyncio.get_running_loop()
            self._background_task = asyncio.create_task(self._background_coroutine())
        except RuntimeError:
            warnings.warn(
                f"No event loop detected. {self._task_description} is disabled "
                "and must be started manually in an asyncio event loop.",
                RuntimeWarning,
                stacklevel=2,
            )
            self._refresh_task = None

    async def __aenter__(self):
        self.start_background_task()

    async def close(self, grace=None):
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class _WrappedMultiCallable:
    """
    Wrapper class that wraps a gRPC multi callable in a custom callable.
    This allows us to prefrom custom logic when the callable is called.
    """

    def __init__(
        self,
        multicall_factory: Callable[
            [],
            aio.UnaryUnaryMultiCallable
            | aio.UnaryStreamMultiCallable
            | aio.StreamUnaryMultiCallable
            | aio.StreamStreamMultiCallable
            | Callable[
                ...,
                aio.UnaryUnaryCall
                | aio.UnaryStreamCall
                | aio.StreamUnaryCall
                | aio.StreamStreamCall,
            ],
        ],
    ):
        """
        Args:
            - call_factory (Callable): A function that returns a gRPC multi callable,
                or a generic function that returns a grpc Call (as a multi callable would).
        """
        self._multicall_factory = multicall_factory

    def __call__(self, *args, **kwargs) -> aio.Call:
        multicallable = self._multicall_factory()
        call = multicallable(*args, **kwargs)
        return call


class WrappedUnaryUnaryMultiCallable(
    _WrappedMultiCallable, aio.UnaryUnaryMultiCallable
):
    pass


class WrappedUnaryStreamMultiCallable(
    _WrappedMultiCallable, aio.UnaryStreamMultiCallable
):
    pass


class WrappedStreamUnaryMultiCallable(
    _WrappedMultiCallable, aio.StreamUnaryMultiCallable
):
    pass


class WrappedStreamStreamMultiCallable(
    _WrappedMultiCallable, aio.StreamStreamMultiCallable
):
    pass
