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
from .test_wrapped_channel import TestBackgroundTaskMixin


class TestRefreshableChannel(TestWrappedChannel, TestBackgroundTaskMixin):
    def _make_one_with_channel_mock(self, *args, async_mock=True, **kwargs):
        channel = AsyncMock() if async_mock else mock.Mock()
        return (
            self._make_one(*args, create_channel_fn=lambda: channel, **kwargs),
            channel,
        )

    def _get_target(self):
        from google.cloud.bigtable._channel_pooling.refreshable_channel import (
            RefreshableChannel,
        )

        return RefreshableChannel

    def _make_one(self, *args, init_background_task=False, **kwargs):
        import warnings

        kwargs.setdefault("create_channel_fn", lambda: AsyncMock())
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            if init_background_task:
                return self._get_target()(*args, **kwargs)
            else:
                with mock.patch.object(self._get_target(), "start_background_task"):
                    return self._get_target()(*args, **kwargs)

    def test_ctor(self):
        """
        test that constuctor sets starting values
        """
        expected_channel = mock.Mock()
        channel_fn = lambda: expected_channel  # noqa: E731
        warm_fn = lambda: AsyncMock()  # noqa: E731
        replace_fn = lambda: AsyncMock()  # noqa: E731
        min_refresh = 4
        max_refresh = 5
        extra_args = ["a", "b"]
        extra_kwargs = {"c": "d"}
        with mock.patch.object(
            self._get_target(), "start_background_task"
        ) as start_background_task_mock:
            instance = self._make_one(
                *extra_args,
                init_background_task=True,
                create_channel_fn=channel_fn,
                refresh_interval_min=min_refresh,
                refresh_interval_max=max_refresh,
                warm_channel_fn=warm_fn,
                on_replace=replace_fn,
                **extra_kwargs,
            )
            assert instance._create_channel.func == channel_fn
            assert instance._create_channel.args == tuple(extra_args)
            assert instance._create_channel.keywords == extra_kwargs
            assert instance._refresh_interval_min == min_refresh
            assert instance._refresh_interval_max == max_refresh
            assert instance._warm_channel == warm_fn
            assert instance._on_replace == replace_fn
            assert instance._background_task is None
            assert instance._channel == expected_channel
            assert start_background_task_mock.call_count == 1

    def test_ctor_defaults(self):
        """
        test with minimal arguments
        """
        expected_channel = mock.Mock()
        channel_fn = lambda: expected_channel  # noqa: E731
        with mock.patch.object(
            self._get_target(), "start_background_task"
        ) as start_background_task_mock:
            instance = self._make_one(
                create_channel_fn=channel_fn, init_background_task=True
            )
            assert instance._create_channel.func == channel_fn
            assert instance._create_channel.args == tuple()
            assert instance._create_channel.keywords == {}
            assert instance._refresh_interval_min == 60 * 35
            assert instance._refresh_interval_max == 60 * 45
            assert instance._warm_channel is None
            assert instance._on_replace is None
            assert instance._background_task is None
            assert instance._channel == expected_channel
            assert start_background_task_mock.call_count == 1

    def test_ctor_no_create_fn(self):
        """
        test that constuctor raises error if no create_channel_fn is provided
        """
        with pytest.raises(ValueError) as exc:
            self._get_target()()
        assert "create_channel_fn" in str(exc.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "refresh_interval, num_cycles, expected_sleep",
        [
            (None, 1, 60 * 35),
            (10, 10, 100),
            (10, 1, 10),
            (1, 10, 10),
            (30, 10, 300),
        ],
    )
    async def test__manage_channel_sleeps(
        self, refresh_interval, num_cycles, expected_sleep
    ):
        """
        Ensure manage_channel_lifecycle sleeps for the correct amount of time in between refreshes
        """
        import time
        import random

        with mock.patch.object(random, "uniform") as uniform:
            uniform.side_effect = lambda min_, max_: min_
            with mock.patch.object(time, "time") as time:
                time.return_value = 0
                with mock.patch.object(asyncio, "sleep") as sleep:
                    sleep.side_effect = [None for i in range(num_cycles - 1)] + [
                        asyncio.CancelledError
                    ]
                    try:
                        instance, _ = self._make_one_with_channel_mock()
                        args = (
                            (refresh_interval, refresh_interval)
                            if refresh_interval
                            else tuple()
                        )
                        await instance._manage_channel_lifecycle(*args)
                    except asyncio.CancelledError:
                        pass
                    assert sleep.call_count == num_cycles
                    total_sleep = sum([call[0][0] for call in sleep.call_args_list])
                    assert (
                        abs(total_sleep - expected_sleep) < 0.1
                    ), f"refresh_interval={refresh_interval}, num_cycles={num_cycles}, expected_sleep={expected_sleep}"

    @pytest.mark.asyncio
    async def test__manage_channel_random(self):
        """
        Should use random to add noise to sleep times
        """
        import random

        with mock.patch.object(asyncio, "sleep") as sleep:
            with mock.patch.object(random, "uniform") as uniform:
                uniform.return_value = 0
                try:
                    uniform.side_effect = asyncio.CancelledError
                    instance = self._make_one_with_channel_mock()[0]
                except asyncio.CancelledError:
                    uniform.side_effect = None
                    uniform.reset_mock()
                    sleep.reset_mock()
                min_val = 200
                max_val = 205
                uniform.side_effect = lambda min_, max_: min_
                sleep.side_effect = [None, None, asyncio.CancelledError]
                try:
                    await instance._manage_channel_lifecycle(min_val, max_val)
                except asyncio.CancelledError:
                    pass
                assert uniform.call_count == 3
                uniform_args = [call[0] for call in uniform.call_args_list]
                for found_min, found_max in uniform_args:
                    assert found_min == min_val
                    assert found_max == max_val

    @pytest.mark.asyncio
    async def test__manage_channel_callbacks(self):
        """
        Should call warm_channel_fn when creating a new channel,
        and on_replace when replacing a channel
        """
        instance, orig_channel = self._make_one_with_channel_mock()
        new_channel = AsyncMock()
        instance._warm_channel = AsyncMock()
        instance._on_replace = AsyncMock()
        instance._create_channel = lambda: new_channel
        new_channel = AsyncMock()
        with mock.patch.object(asyncio, "sleep", AsyncMock()) as sleep:
            # break out after second sleep
            sleep.side_effect = [None, asyncio.CancelledError]
            try:
                await instance._manage_channel_lifecycle()
            except asyncio.CancelledError:
                pass
        assert instance._channel == new_channel
        # should call warm_channel_fn on old channel at start, then new channel after replacement
        assert instance._warm_channel.call_count == 2
        assert instance._warm_channel.call_args_list[0][0][0] == orig_channel
        assert instance._warm_channel.call_args_list[1][0][0] == new_channel
        # should only call on_replace on old channel after replacement
        assert instance._on_replace.call_count == 1
        assert instance._on_replace.call_args_list[0][0][0] == orig_channel
