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

from .test_wrapped_channel import TestWrappedChannel


class TestRefreshableChannel(TestWrappedChannel):

    def _make_one_with_channel_mock(self, *args, async_mock=True, **kwargs):
        channel = AsyncMock() if async_mock else mock.Mock()
        create_channel_fn = lambda: channel
        return self._make_one(*args, create_channel_fn=create_channel_fn, **kwargs), channel

    def _get_target(self):
        from google.cloud.bigtable._channel_pooling.refreshable_channel import RefreshableChannel
        return RefreshableChannel

    def _make_one(self, *args, **kwargs):
        import warnings
        kwargs.setdefault('create_channel_fn', lambda: mock.Mock())
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            return self._get_target()(*args, **kwargs)

    def test_ctor(self):
        """
        test that constuctor sets starting values
        """
        expected_channel = mock.Mock()
        channel_fn = lambda: expected_channel
        warm_fn = lambda: AsyncMock()
        replace_fn = lambda: AsyncMock()
        min_refresh = 4
        max_refresh = 5
        extra_args = ['a', 'b']
        extra_kwargs = {'c': 'd'}
        with mock.patch.object(self._get_target(), "start_background_task") as start_background_task_mock:
            instance = self._make_one(*extra_args, create_channel_fn=channel_fn, refresh_interval_min=min_refresh, refresh_interval_max=max_refresh, warm_channel_fn=warm_fn, on_replace=replace_fn, **extra_kwargs)
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
        channel_fn = lambda: expected_channel
        with mock.patch.object(self._get_target(), "start_background_task") as start_background_task_mock:
            instance = self._make_one(create_channel_fn=channel_fn)
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

"""
   @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "refresh_interval, num_cycles, expected_sleep",
        [
            (None, 1, 60 * 35),
            (10, 10, 100),
            (10, 1, 10),
        ],
    )
    async def test__manage_channel_sleeps(
        self, refresh_interval, num_cycles, expected_sleep
    ):
        # make sure that sleeps work as expected
        import time
        import random

        channel_idx = 1
        with mock.patch.object(random, "uniform") as uniform:
            uniform.side_effect = lambda min_, max_: min_
            with mock.patch.object(time, "time") as time:
                time.return_value = 0
                with mock.patch.object(asyncio, "sleep") as sleep:
                    sleep.side_effect = [None for i in range(num_cycles - 1)] + [
                        asyncio.CancelledError
                    ]
                    try:
                        client = self._make_one(project="project-id")
                        if refresh_interval is not None:
                            await client._manage_channel(
                                channel_idx, refresh_interval, refresh_interval
                            )
                        else:
                            await client._manage_channel(channel_idx)
                    except asyncio.CancelledError:
                        pass
                    assert sleep.call_count == num_cycles
                    total_sleep = sum([call[0][0] for call in sleep.call_args_list])
                    assert (
                        abs(total_sleep - expected_sleep) < 0.1
                    ), f"refresh_interval={refresh_interval}, num_cycles={num_cycles}, expected_sleep={expected_sleep}"
        await client.close()



    @pytest.mark.asyncio
    async def test__manage_channel_ping_and_warm(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledBigtableGrpcAsyncIOTransport,
        )

        # should ping an warm all new channels, and old channels if sleeping
        client = self._make_one(project="project-id")
        new_channel = grpc.aio.insecure_channel("localhost:8080")
        with mock.patch.object(asyncio, "sleep"):
            create_channel = mock.Mock()
            create_channel.return_value = new_channel
            client.transport.grpc_channel._create_channel = create_channel
            with mock.patch.object(
                PooledBigtableGrpcAsyncIOTransport, "replace_channel"
            ) as replace_channel:
                replace_channel.side_effect = asyncio.CancelledError
                # should ping and warm old channel then new if sleep > 0
                with mock.patch.object(
                    type(self._make_one()), "_ping_and_warm_instances"
                ) as ping_and_warm:
                    try:
                        channel_idx = 2
                        old_channel = client.transport._grpc_channel._pool[channel_idx]
                        await client._manage_channel(channel_idx, 10)
                    except asyncio.CancelledError:
                        pass
                    assert ping_and_warm.call_count == 2
                    assert old_channel != new_channel
                    called_with = [call[0][0] for call in ping_and_warm.call_args_list]
                    assert old_channel in called_with
                    assert new_channel in called_with
                # should ping and warm instantly new channel only if not sleeping
                with mock.patch.object(
                    type(self._make_one()), "_ping_and_warm_instances"
                ) as ping_and_warm:
                    try:
                        await client._manage_channel(0, 0, 0)
                    except asyncio.CancelledError:
                        pass
                    ping_and_warm.assert_called_once_with(new_channel)
        await client.close()



    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "refresh_interval, wait_time, expected_sleep",
        [
            (0, 0, 0),
            (0, 1, 0),
            (10, 0, 10),
            (10, 5, 5),
            (10, 10, 0),
            (10, 15, 0),
        ],
    )
    async def test__manage_channel_first_sleep(
        self, refresh_interval, wait_time, expected_sleep
    ):
        # first sleep time should be `refresh_interval` seconds after client init
        import time

        with mock.patch.object(time, "time") as time:
            time.return_value = 0
            with mock.patch.object(asyncio, "sleep") as sleep:
                sleep.side_effect = asyncio.CancelledError
                try:
                    client = self._make_one(project="project-id")
                    client._channel_init_time = -wait_time
                    await client._manage_channel(0, refresh_interval, refresh_interval)
                except asyncio.CancelledError:
                    pass
                sleep.assert_called_once()
                call_time = sleep.call_args[0][0]
                assert (
                    abs(call_time - expected_sleep) < 0.1
                ), f"refresh_interval: {refresh_interval}, wait_time: {wait_time}, expected_sleep: {expected_sleep}"
                await client.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_cycles", [0, 1, 10, 100])
    async def test__manage_channel_refresh(self, num_cycles):
        # make sure that channels are properly refreshed
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledBigtableGrpcAsyncIOTransport,
        )
        from google.api_core import grpc_helpers_async

        expected_grace = 9
        expected_refresh = 0.5
        channel_idx = 1
        new_channel = grpc.aio.insecure_channel("localhost:8080")

        with mock.patch.object(
            PooledBigtableGrpcAsyncIOTransport, "replace_channel"
        ) as replace_channel:
            with mock.patch.object(asyncio, "sleep") as sleep:
                sleep.side_effect = [None for i in range(num_cycles)] + [
                    asyncio.CancelledError
                ]
                with mock.patch.object(
                    grpc_helpers_async, "create_channel"
                ) as create_channel:
                    create_channel.return_value = new_channel
                    client = self._make_one(project="project-id")
                    create_channel.reset_mock()
                    try:
                        await client._manage_channel(
                            channel_idx,
                            refresh_interval_min=expected_refresh,
                            refresh_interval_max=expected_refresh,
                            grace_period=expected_grace,
                        )
                    except asyncio.CancelledError:
                        pass
                    assert sleep.call_count == num_cycles + 1
                    assert create_channel.call_count == num_cycles
                    assert replace_channel.call_count == num_cycles
                    for call in replace_channel.call_args_list:
                        args, kwargs = call
                        assert args[0] == channel_idx
                        assert kwargs["grace"] == expected_grace
                        assert kwargs["new_channel"] == new_channel
                await client.close()

    @pytest.mark.asyncio
    async def test__manage_channel_random(self):
        import random

        with mock.patch.object(asyncio, "sleep") as sleep:
            with mock.patch.object(random, "uniform") as uniform:
                uniform.return_value = 0
                try:
                    uniform.side_effect = asyncio.CancelledError
                    client = self._make_one(project="project-id", pool_size=1)
                except asyncio.CancelledError:
                    uniform.side_effect = None
                    uniform.reset_mock()
                    sleep.reset_mock()
                min_val = 200
                max_val = 205
                uniform.side_effect = lambda min_, max_: min_
                sleep.side_effect = [None, None, asyncio.CancelledError]
                try:
                    await client._manage_channel(0, min_val, max_val)
                except asyncio.CancelledError:
                    pass
                assert uniform.call_count == 2
                uniform_args = [call[0] for call in uniform.call_args_list]
                for found_min, found_max in uniform_args:
                    assert found_min == min_val
                    assert found_max == max_val

"""
