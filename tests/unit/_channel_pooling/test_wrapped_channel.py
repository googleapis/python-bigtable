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
from __future__ import annotations

import pytest

from grpc.experimental import aio  # type: ignore

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


class TestWrappedChannel:
    """
    WrappedChannel should delegate all methods to the wrapped channel
    """

    def _make_one_with_channel_mock(self, *args, async_mock=True, **kwargs):
        channel = AsyncMock() if async_mock else mock.Mock()
        return self._make_one(channel), channel

    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable._channel_pooling.wrapped_channel import _WrappedChannel
        return _WrappedChannel(*args, **kwargs)

    def test_ctor(self):
        channel = mock.Mock()
        instance = self._make_one(channel)
        assert instance._channel is channel

    def test_is_channel_instance(self):
        """
        should pass isinstance check for aio.Channel
        """
        instance = self._make_one(mock.Mock())
        assert isinstance(instance, aio.Channel)

    @pytest.mark.parametrize("method_name", ["unary_unary", "unary_stream", "stream_unary", "stream_stream", "get_state"])
    def test_sync_api_passthrough(self, method_name):
        """
        Wrapper should respond to full grpc Channel API, and pass through
        resonses to wrapped channel
        """
        expected_response = "response"
        instance, channel = self._make_one_with_channel_mock(async_mock=False)
        channel_method = getattr(channel, method_name)
        wrapper_method = getattr(instance, method_name)
        channel_method.return_value = expected_response
        # make function call
        arg_mock = mock.Mock()
        found_response = wrapper_method(arg_mock)
        # assert that wrapped channel method was called
        channel_method.assert_called_once()
        # combine args and kwargs
        all_args = list(channel_method.call_args.args) + list(channel_method.call_args.kwargs.values())
        assert all_args == [arg_mock]
        # assert that response was passed through
        assert found_response == expected_response

    @pytest.mark.parametrize("method_name,arg_num", [
        ("close", 1),
        ("channel_ready", 0),
        ("__aenter__", 0),
        ("__aexit__", 3),
        ("wait_for_state_change", 1),
    ])
    @pytest.mark.asyncio
    async def test_async_api_passthrough(self, method_name, arg_num):
        """
        Wrapper should respond to full grpc Channel API, and pass through
        resonses to wrapped channel
        """
        expected_response = "response"
        instance, channel = self._make_one_with_channel_mock(async_mock=True)
        channel_method = getattr(channel, method_name)
        wrapper_method = getattr(instance, method_name)
        channel_method.return_value = expected_response
        # make function call
        args = [mock.Mock() for _ in range(arg_num)]
        found_response = await wrapper_method(*args)
        # assert that wrapped channel method was called
        channel_method.assert_called_once()
        channel_method.assert_awaited_once()
        # combine args and kwargs
        all_args = list(channel_method.call_args.args) + list(channel_method.call_args.kwargs.values())
        assert all_args == args
        # assert that response was passed through
        assert found_response == expected_response

    def test_wrapped_channel_property(self):
        """
        Should be able to access wrapped channel
        """
        instance, channel = self._make_one_with_channel_mock()
        assert instance.wrapped_channel is channel

class TestBackgroundTaskMixin():

    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable._channel_pooling.wrapped_channel import _BackgroundTaskMixin
        return _BackgroundTaskMixin(*args, **kwargs)
