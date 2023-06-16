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
import asyncio

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore

from .test_wrapped_channel import TestWrappedChannel


class TestTrackedChannel(TestWrappedChannel):
    def _make_one_with_channel_mock(self, *args, async_mock=True, **kwargs):
        channel = AsyncMock() if async_mock else mock.Mock()
        return self._make_one(channel, *args, **kwargs), channel

    def _get_target(self):
        from google.cloud.bigtable._channel_pooling.tracked_channel import (
            TrackedChannel,
        )

        return TrackedChannel

    def _make_one(self, *args, **kwargs):
        return self._get_target()(*args, **kwargs)

    def test_ctor(self):
        expected_channel = mock.Mock()
        instance = self._make_one(expected_channel)
        assert instance._channel is expected_channel
        assert instance.active_rpcs == 0
        assert instance.max_active_rpcs == 0

    def test_track_rpc(self):
        """
        counts should be incremented when track_rpc is used
        """
        instance = self._make_one(mock.Mock())
        assert instance.active_rpcs == 0
        assert instance.max_active_rpcs == 0
        # start a single rpc
        with instance.track_rpc():
            assert instance.active_rpcs == 1
            assert instance.max_active_rpcs == 1
            # start a second rpc
            with instance.track_rpc():
                assert instance.active_rpcs == 2
                assert instance.max_active_rpcs == 2
            # end the second rpc
            assert instance.active_rpcs == 1
            assert instance.max_active_rpcs == 2
        # end the first rpc
        assert instance.active_rpcs == 0
        assert instance.max_active_rpcs == 2

    def test_get_and_reset_max_active_rpcs(self):
        instance = self._make_one(mock.Mock())
        expected_max = 11
        instance.max_active_rpcs = expected_max
        found_max = instance.get_and_reset_max_active_rpcs()
        assert found_max == expected_max
        assert instance.max_active_rpcs == 0

    @pytest.mark.parametrize(
        "method_name", ["unary_unary", "unary_stream", "stream_unary", "stream_stream"]
    )
    def test_calls_wrapped(self, method_name):
        from google.cloud.bigtable._channel_pooling.tracked_channel import (
            _TrackedStreamResponseMixin,
            _TrackedUnaryResponseMixin,
        )
        from google.cloud.bigtable._channel_pooling.wrapped_channel import (
            _WrappedMultiCallable,
        )

        instance, channel = self._make_one_with_channel_mock(async_mock=False)
        found_multicallable = getattr(instance, method_name)()
        assert isinstance(found_multicallable, _WrappedMultiCallable)
        found_call = found_multicallable()
        assert isinstance(
            found_call, (_TrackedUnaryResponseMixin, _TrackedStreamResponseMixin)
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method_name", ["unary_unary", "stream_unary"])
    async def test_unary_calls_tracked(self, method_name):
        """
        unary_unary calls should update track count when called
        """
        import time

        instance, channel = self._make_one_with_channel_mock(async_mock=False)
        getattr(
            channel, method_name
        ).return_value = lambda *args, **kwargs: asyncio.sleep(0.01)
        found_callable = getattr(instance, method_name)()
        assert instance.active_rpcs == 0
        assert instance.max_active_rpcs == 0
        await found_callable()
        assert instance.active_rpcs == 0
        assert instance.max_active_rpcs == 1
        # try with multiple calls at once
        start_time = time.monotonic()
        await asyncio.gather(*[found_callable(None, None) for _ in range(10)])
        assert instance.active_rpcs == 0
        assert instance.max_active_rpcs == 10
        # make sure rpcs ran in parallel
        assert time.monotonic() - start_time < 0.2

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method_name", ["unary_stream", "stream_stream"])
    async def test_stream_calls_tracked(self, method_name):
        """
        stream calls should update track count when called
        """

        async def mock_stream():
            for i in range(3):
                yield i

        instance, channel = self._make_one_with_channel_mock(async_mock=False)
        getattr(channel, method_name).return_value = lambda: mock_stream()
        found_multicallable = getattr(instance, method_name)()
        found_stream = found_multicallable()
        assert instance.active_rpcs == 0
        assert instance.max_active_rpcs == 0
        async for _ in found_stream:
            assert instance.active_rpcs == 1
            assert instance.max_active_rpcs >= 1
            new_stream = found_multicallable()
            assert instance.active_rpcs == 1
            assert instance.max_active_rpcs >= 1
            async for _ in new_stream:
                assert instance.active_rpcs == 2
                assert instance.max_active_rpcs == 2
