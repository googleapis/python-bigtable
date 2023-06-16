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

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore

from .test_pooled_channel import TestPooledChannel
from .test_wrapped_channel import TestBackgroundTaskMixin


class TestDynamicPooledChannel(TestPooledChannel, TestBackgroundTaskMixin):

    def _get_target(self):
        from google.cloud.bigtable._channel_pooling.dynamic_pooled_channel import DynamicPooledChannel

        return DynamicPooledChannel

    def _make_one(self, *args, init_background_task=False, async_mock=True, mock_track_wrapper=True, **kwargs):
        import warnings
        from google.cloud.bigtable._channel_pooling.dynamic_pooled_channel import DynamicPoolOptions
        mock_type = AsyncMock if async_mock else mock.Mock
        kwargs.setdefault("create_channel_fn", lambda *args, **kwargs: mock_type())
        pool_size = kwargs.pop("pool_size", 3)
        kwargs.setdefault("pool_options", DynamicPoolOptions(start_size=pool_size))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            if init_background_task:
                instance = self._get_target()(*args, **kwargs)
            else:
                with mock.patch.object(self._get_target(), "start_background_task"):
                    instance = self._get_target()(*args, **kwargs)
        if mock_track_wrapper:
            # replace tracked channels with mocks
            instance._create_channel = kwargs["create_channel_fn"]
            for i in range(len(instance._pool)):
                instance._pool[i] = mock_type()
        return instance

    def test_ctor(self):
        """
        test that constuctor sets starting values
        """
        from google.cloud.bigtable._channel_pooling.tracked_channel import TrackedChannel
        channel_fn = mock.Mock()
        channel_fn.side_effect = lambda *args, **kwargs: mock.Mock()
        expected_pool_options = mock.Mock()
        expected_pool_size = 12
        expected_pool_options.start_size = expected_pool_size
        extra_args = ["a", "b"]
        extra_kwargs = {"c": "d"}
        warm_fn = AsyncMock()
        on_remove = AsyncMock()
        instance = self._make_one(
            *extra_args,
            create_channel_fn=channel_fn,
            pool_options=expected_pool_options,
            warm_channel_fn=warm_fn,
            on_remove=on_remove,
            mock_track_wrapper=False,
            **extra_kwargs,
        )
        assert len(instance.channels) == expected_pool_size
        assert channel_fn.call_count == expected_pool_size
        assert channel_fn.call_args == mock.call(*extra_args, **extra_kwargs)
        # create function should create a tracked channel
        assert isinstance(instance._create_channel(), TrackedChannel)
        # no args in outer function
        assert instance._create_channel.args == tuple()
        assert instance._create_channel.keywords == {}
        # ensure each channel is unique
        assert len(set(instance.channels)) == expected_pool_size
        # callbacks are set up
        assert instance._on_remove == on_remove
        assert instance._warm_channel == warm_fn

    def test_ctor_defaults(self):
        """
        test with minimal arguments
        """
        from google.cloud.bigtable._channel_pooling.tracked_channel import TrackedChannel
        from google.cloud.bigtable._channel_pooling.dynamic_pooled_channel import DynamicPoolOptions
        channel_fn = mock.Mock()
        channel_fn.side_effect = lambda *args, **kwargs: mock.Mock()
        instance = self._make_one(
            create_channel_fn=channel_fn,
            mock_track_wrapper=False,
        )
        assert len(instance.channels) == 3
        assert channel_fn.call_count == 3
        assert channel_fn.call_args == mock.call()
        # create function should create a tracked channel
        assert isinstance(instance._create_channel(), TrackedChannel)
        # no args in outer function
        assert instance._create_channel.args == tuple()
        assert instance._create_channel.keywords == {}
        # ensure each channel is unique
        assert len(set(instance.channels)) == 3
        # callbacks are empty
        assert instance._on_remove is None
        assert instance._warm_channel is None
        # DynamicPoolOptions created automatically
        assert isinstance(instance._pool_options, DynamicPoolOptions)

    @pytest.mark.asyncio
    async def test_resize_routine(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize("start_size,max_rpcs_per_channel,min_rpcs_per_channel,max_channels,min_channels,max_delta,usages,new_size", [
        (1, 10, 0, 10, 1, 1, [0, 0, 0], 1),
    ])
    async def test_attempt_resize(
        self,
        start_size,
        max_rpcs_per_channel,
        min_rpcs_per_channel,
        max_channels,
        min_channels,
        max_delta,
        usages,
        new_size,
    ):
        """
        test different resize scenarios
        """
        pass

    @pytest.mark.asyncio
    async def test_resize_reset_next_idx(self):
        """
        if the pools shrinks below next_idx, next_idx should be set to 0
        """
        pass
