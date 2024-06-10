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

import grpc
import asyncio
import re
import sys

import pytest

from google.cloud.bigtable.data import mutations
from google.auth.credentials import AnonymousCredentials
from google.cloud.bigtable_v2.types import ReadRowsResponse
from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data.exceptions import _MutateRowsIncomplete
from google.cloud.bigtable.data import TABLE_DEFAULT

from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule
from google.cloud.bigtable.data.read_modify_write_rules import AppendValueRule

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore

VENEER_HEADER_REGEX = re.compile(
    r"gapic\/[0-9]+\.[\w.-]+ gax\/[0-9]+\.[\w.-]+ gccl\/[0-9]+\.[\w.-]+-data-async gl-python\/[0-9]+\.[\w.-]+ grpc\/[0-9]+\.[\w.-]+"
)


def _make_client(*args, use_emulator=True, **kwargs):
    import os
    from google.cloud.bigtable.data._async.client import BigtableDataClientAsync

    env_mask = {}
    # by default, use emulator mode to avoid auth issues in CI
    # emulator mode must be disabled by tests that check channel pooling/refresh background tasks
    if use_emulator:
        env_mask["BIGTABLE_EMULATOR_HOST"] = "localhost"
    else:
        # set some default values
        kwargs["credentials"] = kwargs.get("credentials", AnonymousCredentials())
        kwargs["project"] = kwargs.get("project", "project-id")
    with mock.patch.dict(os.environ, env_mask):
        return BigtableDataClientAsync(*args, **kwargs)


class TestBigtableDataClientAsync:
    def _get_target_class(self):
        from google.cloud.bigtable.data._async.client import BigtableDataClientAsync

        return BigtableDataClientAsync

    def _make_one(self, *args, **kwargs):
        return _make_client(*args, **kwargs)

    @pytest.mark.asyncio
    async def test_ctor(self):
        expected_project = "project-id"
        expected_pool_size = 11
        expected_credentials = AnonymousCredentials()
        client = self._make_one(
            project="project-id",
            pool_size=expected_pool_size,
            credentials=expected_credentials,
            use_emulator=False,
        )
        await asyncio.sleep(0)
        assert client.project == expected_project
        assert len(client.transport._grpc_channel._pool) == expected_pool_size
        assert not client._active_instances
        assert len(client._channel_refresh_tasks) == expected_pool_size
        assert client.transport._credentials == expected_credentials
        await client.close()

    @pytest.mark.asyncio
    async def test_ctor_super_inits(self):
        from google.cloud.bigtable_v2.services.bigtable.async_client import (
            BigtableAsyncClient,
        )
        from google.cloud.client import ClientWithProject
        from google.api_core import client_options as client_options_lib

        project = "project-id"
        pool_size = 11
        credentials = AnonymousCredentials()
        client_options = {"api_endpoint": "foo.bar:1234"}
        options_parsed = client_options_lib.from_dict(client_options)
        transport_str = f"pooled_grpc_asyncio_{pool_size}"
        with mock.patch.object(BigtableAsyncClient, "__init__") as bigtable_client_init:
            bigtable_client_init.return_value = None
            with mock.patch.object(
                ClientWithProject, "__init__"
            ) as client_project_init:
                client_project_init.return_value = None
                try:
                    self._make_one(
                        project=project,
                        pool_size=pool_size,
                        credentials=credentials,
                        client_options=options_parsed,
                        use_emulator=False,
                    )
                except AttributeError:
                    pass
                # test gapic superclass init was called
                assert bigtable_client_init.call_count == 1
                kwargs = bigtable_client_init.call_args[1]
                assert kwargs["transport"] == transport_str
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed
                # test mixin superclass init was called
                assert client_project_init.call_count == 1
                kwargs = client_project_init.call_args[1]
                assert kwargs["project"] == project
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed

    @pytest.mark.asyncio
    async def test_ctor_dict_options(self):
        from google.cloud.bigtable_v2.services.bigtable.async_client import (
            BigtableAsyncClient,
        )
        from google.api_core.client_options import ClientOptions

        client_options = {"api_endpoint": "foo.bar:1234"}
        with mock.patch.object(BigtableAsyncClient, "__init__") as bigtable_client_init:
            try:
                self._make_one(client_options=client_options)
            except TypeError:
                pass
            bigtable_client_init.assert_called_once()
            kwargs = bigtable_client_init.call_args[1]
            called_options = kwargs["client_options"]
            assert called_options.api_endpoint == "foo.bar:1234"
            assert isinstance(called_options, ClientOptions)
        with mock.patch.object(
            self._get_target_class(), "_start_background_channel_refresh"
        ) as start_background_refresh:
            client = self._make_one(client_options=client_options, use_emulator=False)
            start_background_refresh.assert_called_once()
            await client.close()

    @pytest.mark.asyncio
    async def test_veneer_grpc_headers(self):
        # client_info should be populated with headers to
        # detect as a veneer client
        patch = mock.patch("google.api_core.gapic_v1.method_async.wrap_method")
        with patch as gapic_mock:
            client = self._make_one(project="project-id")
            wrapped_call_list = gapic_mock.call_args_list
            assert len(wrapped_call_list) > 0
            # each wrapped call should have veneer headers
            for call in wrapped_call_list:
                client_info = call.kwargs["client_info"]
                assert client_info is not None, f"{call} has no client_info"
                wrapped_user_agent_sorted = " ".join(
                    sorted(client_info.to_user_agent().split(" "))
                )
                assert VENEER_HEADER_REGEX.match(
                    wrapped_user_agent_sorted
                ), f"'{wrapped_user_agent_sorted}' does not match {VENEER_HEADER_REGEX}"
            await client.close()

    @pytest.mark.asyncio
    async def test_channel_pool_creation(self):
        pool_size = 14
        with mock.patch(
            "google.api_core.grpc_helpers_async.create_channel"
        ) as create_channel:
            create_channel.return_value = AsyncMock()
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert create_channel.call_count == pool_size
            await client.close()
        # channels should be unique
        client = self._make_one(project="project-id", pool_size=pool_size)
        pool_list = list(client.transport._grpc_channel._pool)
        pool_set = set(client.transport._grpc_channel._pool)
        assert len(pool_list) == len(pool_set)
        await client.close()

    @pytest.mark.asyncio
    async def test_channel_pool_rotation(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledChannel,
        )

        pool_size = 7

        with mock.patch.object(PooledChannel, "next_channel") as next_channel:
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert len(client.transport._grpc_channel._pool) == pool_size
            next_channel.reset_mock()
            with mock.patch.object(
                type(client.transport._grpc_channel._pool[0]), "unary_unary"
            ) as unary_unary:
                # calling an rpc `pool_size` times should use a different channel each time
                channel_next = None
                for i in range(pool_size):
                    channel_last = channel_next
                    channel_next = client.transport.grpc_channel._pool[i]
                    assert channel_last != channel_next
                    next_channel.return_value = channel_next
                    client.transport.ping_and_warm()
                    assert next_channel.call_count == i + 1
                    unary_unary.assert_called_once()
                    unary_unary.reset_mock()
        await client.close()

    @pytest.mark.asyncio
    async def test_channel_pool_replace(self):
        with mock.patch.object(asyncio, "sleep"):
            pool_size = 7
            client = self._make_one(project="project-id", pool_size=pool_size)
            for replace_idx in range(pool_size):
                start_pool = [
                    channel for channel in client.transport._grpc_channel._pool
                ]
                grace_period = 9
                with mock.patch.object(
                    type(client.transport._grpc_channel._pool[0]), "close"
                ) as close:
                    new_channel = grpc.aio.insecure_channel("localhost:8080")
                    await client.transport.replace_channel(
                        replace_idx, grace=grace_period, new_channel=new_channel
                    )
                    close.assert_called_once_with(grace=grace_period)
                    close.assert_awaited_once()
                assert client.transport._grpc_channel._pool[replace_idx] == new_channel
                for i in range(pool_size):
                    if i != replace_idx:
                        assert client.transport._grpc_channel._pool[i] == start_pool[i]
                    else:
                        assert client.transport._grpc_channel._pool[i] != start_pool[i]
            await client.close()

    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    def test__start_background_channel_refresh_sync(self):
        # should raise RuntimeError if called in a sync context
        client = self._make_one(project="project-id", use_emulator=False)
        with pytest.raises(RuntimeError):
            client._start_background_channel_refresh()

    @pytest.mark.asyncio
    async def test__start_background_channel_refresh_tasks_exist(self):
        # if tasks exist, should do nothing
        client = self._make_one(project="project-id", use_emulator=False)
        assert len(client._channel_refresh_tasks) > 0
        with mock.patch.object(asyncio, "create_task") as create_task:
            client._start_background_channel_refresh()
            create_task.assert_not_called()
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pool_size", [1, 3, 7])
    async def test__start_background_channel_refresh(self, pool_size):
        # should create background tasks for each channel
        client = self._make_one(
            project="project-id", pool_size=pool_size, use_emulator=False
        )
        ping_and_warm = AsyncMock()
        client._ping_and_warm_instances = ping_and_warm
        client._start_background_channel_refresh()
        assert len(client._channel_refresh_tasks) == pool_size
        for task in client._channel_refresh_tasks:
            assert isinstance(task, asyncio.Task)
        await asyncio.sleep(0.1)
        assert ping_and_warm.call_count == pool_size
        for channel in client.transport._grpc_channel._pool:
            ping_and_warm.assert_any_call(channel)
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.version_info < (3, 8), reason="Task.name requires python3.8 or higher"
    )
    async def test__start_background_channel_refresh_tasks_names(self):
        # if tasks exist, should do nothing
        pool_size = 3
        client = self._make_one(
            project="project-id", pool_size=pool_size, use_emulator=False
        )
        for i in range(pool_size):
            name = client._channel_refresh_tasks[i].get_name()
            assert str(i) in name
            assert "BigtableDataClientAsync channel refresh " in name
        await client.close()

    @pytest.mark.asyncio
    async def test__ping_and_warm_instances(self):
        """
        test ping and warm with mocked asyncio.gather
        """
        client_mock = mock.Mock()
        with mock.patch.object(asyncio, "gather", AsyncMock()) as gather:
            # simulate gather by returning the same number of items as passed in
            gather.side_effect = lambda *args, **kwargs: [None for _ in args]
            channel = mock.Mock()
            # test with no instances
            client_mock._active_instances = []
            result = await self._get_target_class()._ping_and_warm_instances(
                client_mock, channel
            )
            assert len(result) == 0
            gather.assert_called_once()
            gather.assert_awaited_once()
            assert not gather.call_args.args
            assert gather.call_args.kwargs == {"return_exceptions": True}
            # test with instances
            client_mock._active_instances = [
                (mock.Mock(), mock.Mock(), mock.Mock())
            ] * 4
            gather.reset_mock()
            channel.reset_mock()
            result = await self._get_target_class()._ping_and_warm_instances(
                client_mock, channel
            )
            assert len(result) == 4
            gather.assert_called_once()
            gather.assert_awaited_once()
            assert len(gather.call_args.args) == 4
            # check grpc call arguments
            grpc_call_args = channel.unary_unary().call_args_list
            for idx, (_, kwargs) in enumerate(grpc_call_args):
                (
                    expected_instance,
                    expected_table,
                    expected_app_profile,
                ) = client_mock._active_instances[idx]
                request = kwargs["request"]
                assert request["name"] == expected_instance
                assert request["app_profile_id"] == expected_app_profile
                metadata = kwargs["metadata"]
                assert len(metadata) == 1
                assert metadata[0][0] == "x-goog-request-params"
                assert (
                    metadata[0][1]
                    == f"name={expected_instance}&app_profile_id={expected_app_profile}"
                )

    @pytest.mark.asyncio
    async def test__ping_and_warm_single_instance(self):
        """
        should be able to call ping and warm with single instance
        """
        client_mock = mock.Mock()
        with mock.patch.object(asyncio, "gather", AsyncMock()) as gather:
            # simulate gather by returning the same number of items as passed in
            gather.side_effect = lambda *args, **kwargs: [None for _ in args]
            channel = mock.Mock()
            # test with large set of instances
            client_mock._active_instances = [mock.Mock()] * 100
            test_key = ("test-instance", "test-table", "test-app-profile")
            result = await self._get_target_class()._ping_and_warm_instances(
                client_mock, channel, test_key
            )
            # should only have been called with test instance
            assert len(result) == 1
            # check grpc call arguments
            grpc_call_args = channel.unary_unary().call_args_list
            assert len(grpc_call_args) == 1
            kwargs = grpc_call_args[0][1]
            request = kwargs["request"]
            assert request["name"] == "test-instance"
            assert request["app_profile_id"] == "test-app-profile"
            metadata = kwargs["metadata"]
            assert len(metadata) == 1
            assert metadata[0][0] == "x-goog-request-params"
            assert (
                metadata[0][1] == "name=test-instance&app_profile_id=test-app-profile"
            )

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

        with mock.patch.object(time, "monotonic") as time:
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
    async def test__manage_channel_ping_and_warm(self):
        """
        _manage channel should call ping and warm internally
        """
        import time

        client_mock = mock.Mock()
        client_mock._channel_init_time = time.monotonic()
        channel_list = [mock.Mock(), mock.Mock()]
        client_mock.transport.channels = channel_list
        new_channel = mock.Mock()
        client_mock.transport.grpc_channel._create_channel.return_value = new_channel
        # should ping an warm all new channels, and old channels if sleeping
        with mock.patch.object(asyncio, "sleep"):
            # stop process after replace_channel is called
            client_mock.transport.replace_channel.side_effect = asyncio.CancelledError
            ping_and_warm = client_mock._ping_and_warm_instances = AsyncMock()
            # should ping and warm old channel then new if sleep > 0
            try:
                channel_idx = 1
                await self._get_target_class()._manage_channel(
                    client_mock, channel_idx, 10
                )
            except asyncio.CancelledError:
                pass
            # should have called at loop start, and after replacement
            assert ping_and_warm.call_count == 2
            # should have replaced channel once
            assert client_mock.transport.replace_channel.call_count == 1
            # make sure new and old channels were warmed
            old_channel = channel_list[channel_idx]
            assert old_channel != new_channel
            called_with = [call[0][0] for call in ping_and_warm.call_args_list]
            assert old_channel in called_with
            assert new_channel in called_with
            # should ping and warm instantly new channel only if not sleeping
            ping_and_warm.reset_mock()
            try:
                await self._get_target_class()._manage_channel(client_mock, 0, 0, 0)
            except asyncio.CancelledError:
                pass
            ping_and_warm.assert_called_once_with(new_channel)

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
                    client = self._make_one(project="project-id", use_emulator=False)
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
    async def test__register_instance(self):
        """
        test instance registration
        """
        # set up mock client
        client_mock = mock.Mock()
        client_mock._gapic_client.instance_path.side_effect = lambda a, b: f"prefix/{b}"
        active_instances = set()
        instance_owners = {}
        client_mock._active_instances = active_instances
        client_mock._instance_owners = instance_owners
        client_mock._channel_refresh_tasks = []
        client_mock._start_background_channel_refresh.side_effect = (
            lambda: client_mock._channel_refresh_tasks.append(mock.Mock)
        )
        mock_channels = [mock.Mock() for i in range(5)]
        client_mock.transport.channels = mock_channels
        client_mock._ping_and_warm_instances = AsyncMock()
        table_mock = mock.Mock()
        await self._get_target_class()._register_instance(
            client_mock, "instance-1", table_mock
        )
        # first call should start background refresh
        assert client_mock._start_background_channel_refresh.call_count == 1
        # ensure active_instances and instance_owners were updated properly
        expected_key = (
            "prefix/instance-1",
            table_mock.table_name,
            table_mock.app_profile_id,
        )
        assert len(active_instances) == 1
        assert expected_key == tuple(list(active_instances)[0])
        assert len(instance_owners) == 1
        assert expected_key == tuple(list(instance_owners)[0])
        # should be a new task set
        assert client_mock._channel_refresh_tasks
        # next call should not call _start_background_channel_refresh again
        table_mock2 = mock.Mock()
        await self._get_target_class()._register_instance(
            client_mock, "instance-2", table_mock2
        )
        assert client_mock._start_background_channel_refresh.call_count == 1
        # but it should call ping and warm with new instance key
        assert client_mock._ping_and_warm_instances.call_count == len(mock_channels)
        for channel in mock_channels:
            assert channel in [
                call[0][0]
                for call in client_mock._ping_and_warm_instances.call_args_list
            ]
        # check for updated lists
        assert len(active_instances) == 2
        assert len(instance_owners) == 2
        expected_key2 = (
            "prefix/instance-2",
            table_mock2.table_name,
            table_mock2.app_profile_id,
        )
        assert any(
            [
                expected_key2 == tuple(list(active_instances)[i])
                for i in range(len(active_instances))
            ]
        )
        assert any(
            [
                expected_key2 == tuple(list(instance_owners)[i])
                for i in range(len(instance_owners))
            ]
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "insert_instances,expected_active,expected_owner_keys",
        [
            ([("i", "t", None)], [("i", "t", None)], [("i", "t", None)]),
            ([("i", "t", "p")], [("i", "t", "p")], [("i", "t", "p")]),
            ([("1", "t", "p"), ("1", "t", "p")], [("1", "t", "p")], [("1", "t", "p")]),
            (
                [("1", "t", "p"), ("2", "t", "p")],
                [("1", "t", "p"), ("2", "t", "p")],
                [("1", "t", "p"), ("2", "t", "p")],
            ),
        ],
    )
    async def test__register_instance_state(
        self, insert_instances, expected_active, expected_owner_keys
    ):
        """
        test that active_instances and instance_owners are updated as expected
        """
        # set up mock client
        client_mock = mock.Mock()
        client_mock._gapic_client.instance_path.side_effect = lambda a, b: b
        active_instances = set()
        instance_owners = {}
        client_mock._active_instances = active_instances
        client_mock._instance_owners = instance_owners
        client_mock._channel_refresh_tasks = []
        client_mock._start_background_channel_refresh.side_effect = (
            lambda: client_mock._channel_refresh_tasks.append(mock.Mock)
        )
        mock_channels = [mock.Mock() for i in range(5)]
        client_mock.transport.channels = mock_channels
        client_mock._ping_and_warm_instances = AsyncMock()
        table_mock = mock.Mock()
        # register instances
        for instance, table, profile in insert_instances:
            table_mock.table_name = table
            table_mock.app_profile_id = profile
            await self._get_target_class()._register_instance(
                client_mock, instance, table_mock
            )
        assert len(active_instances) == len(expected_active)
        assert len(instance_owners) == len(expected_owner_keys)
        for expected in expected_active:
            assert any(
                [
                    expected == tuple(list(active_instances)[i])
                    for i in range(len(active_instances))
                ]
            )
        for expected in expected_owner_keys:
            assert any(
                [
                    expected == tuple(list(instance_owners)[i])
                    for i in range(len(instance_owners))
                ]
            )

    @pytest.mark.asyncio
    async def test__remove_instance_registration(self):
        client = self._make_one(project="project-id")
        table = mock.Mock()
        await client._register_instance("instance-1", table)
        await client._register_instance("instance-2", table)
        assert len(client._active_instances) == 2
        assert len(client._instance_owners.keys()) == 2
        instance_1_path = client._gapic_client.instance_path(
            client.project, "instance-1"
        )
        instance_1_key = (instance_1_path, table.table_name, table.app_profile_id)
        instance_2_path = client._gapic_client.instance_path(
            client.project, "instance-2"
        )
        instance_2_key = (instance_2_path, table.table_name, table.app_profile_id)
        assert len(client._instance_owners[instance_1_key]) == 1
        assert list(client._instance_owners[instance_1_key])[0] == id(table)
        assert len(client._instance_owners[instance_2_key]) == 1
        assert list(client._instance_owners[instance_2_key])[0] == id(table)
        success = await client._remove_instance_registration("instance-1", table)
        assert success
        assert len(client._active_instances) == 1
        assert len(client._instance_owners[instance_1_key]) == 0
        assert len(client._instance_owners[instance_2_key]) == 1
        assert client._active_instances == {instance_2_key}
        success = await client._remove_instance_registration("fake-key", table)
        assert not success
        assert len(client._active_instances) == 1
        await client.close()

    @pytest.mark.asyncio
    async def test__multiple_table_registration(self):
        """
        registering with multiple tables with the same key should
        add multiple owners to instance_owners, but only keep one copy
        of shared key in active_instances
        """
        from google.cloud.bigtable.data._async.client import _WarmedInstanceKey

        async with self._make_one(project="project-id") as client:
            async with client.get_table("instance_1", "table_1") as table_1:
                instance_1_path = client._gapic_client.instance_path(
                    client.project, "instance_1"
                )
                instance_1_key = _WarmedInstanceKey(
                    instance_1_path, table_1.table_name, table_1.app_profile_id
                )
                assert len(client._instance_owners[instance_1_key]) == 1
                assert len(client._active_instances) == 1
                assert id(table_1) in client._instance_owners[instance_1_key]
                # duplicate table should register in instance_owners under same key
                async with client.get_table("instance_1", "table_1") as table_2:
                    assert len(client._instance_owners[instance_1_key]) == 2
                    assert len(client._active_instances) == 1
                    assert id(table_1) in client._instance_owners[instance_1_key]
                    assert id(table_2) in client._instance_owners[instance_1_key]
                    # unique table should register in instance_owners and active_instances
                    async with client.get_table("instance_1", "table_3") as table_3:
                        instance_3_path = client._gapic_client.instance_path(
                            client.project, "instance_1"
                        )
                        instance_3_key = _WarmedInstanceKey(
                            instance_3_path, table_3.table_name, table_3.app_profile_id
                        )
                        assert len(client._instance_owners[instance_1_key]) == 2
                        assert len(client._instance_owners[instance_3_key]) == 1
                        assert len(client._active_instances) == 2
                        assert id(table_1) in client._instance_owners[instance_1_key]
                        assert id(table_2) in client._instance_owners[instance_1_key]
                        assert id(table_3) in client._instance_owners[instance_3_key]
                # sub-tables should be unregistered, but instance should still be active
                assert len(client._active_instances) == 1
                assert instance_1_key in client._active_instances
                assert id(table_2) not in client._instance_owners[instance_1_key]
            # both tables are gone. instance should be unregistered
            assert len(client._active_instances) == 0
            assert instance_1_key not in client._active_instances
            assert len(client._instance_owners[instance_1_key]) == 0

    @pytest.mark.asyncio
    async def test__multiple_instance_registration(self):
        """
        registering with multiple instance keys should update the key
        in instance_owners and active_instances
        """
        from google.cloud.bigtable.data._async.client import _WarmedInstanceKey

        async with self._make_one(project="project-id") as client:
            async with client.get_table("instance_1", "table_1") as table_1:
                async with client.get_table("instance_2", "table_2") as table_2:
                    instance_1_path = client._gapic_client.instance_path(
                        client.project, "instance_1"
                    )
                    instance_1_key = _WarmedInstanceKey(
                        instance_1_path, table_1.table_name, table_1.app_profile_id
                    )
                    instance_2_path = client._gapic_client.instance_path(
                        client.project, "instance_2"
                    )
                    instance_2_key = _WarmedInstanceKey(
                        instance_2_path, table_2.table_name, table_2.app_profile_id
                    )
                    assert len(client._instance_owners[instance_1_key]) == 1
                    assert len(client._instance_owners[instance_2_key]) == 1
                    assert len(client._active_instances) == 2
                    assert id(table_1) in client._instance_owners[instance_1_key]
                    assert id(table_2) in client._instance_owners[instance_2_key]
                # instance2 should be unregistered, but instance1 should still be active
                assert len(client._active_instances) == 1
                assert instance_1_key in client._active_instances
                assert len(client._instance_owners[instance_2_key]) == 0
                assert len(client._instance_owners[instance_1_key]) == 1
                assert id(table_1) in client._instance_owners[instance_1_key]
            # both tables are gone. instances should both be unregistered
            assert len(client._active_instances) == 0
            assert len(client._instance_owners[instance_1_key]) == 0
            assert len(client._instance_owners[instance_2_key]) == 0

    @pytest.mark.asyncio
    async def test_get_table(self):
        from google.cloud.bigtable.data._async.client import TableAsync
        from google.cloud.bigtable.data._async.client import _WarmedInstanceKey

        client = self._make_one(project="project-id")
        assert not client._active_instances
        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        table = client.get_table(
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
        )
        await asyncio.sleep(0)
        assert isinstance(table, TableAsync)
        assert table.table_id == expected_table_id
        assert (
            table.table_name
            == f"projects/{client.project}/instances/{expected_instance_id}/tables/{expected_table_id}"
        )
        assert table.instance_id == expected_instance_id
        assert (
            table.instance_name
            == f"projects/{client.project}/instances/{expected_instance_id}"
        )
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        instance_key = _WarmedInstanceKey(
            table.instance_name, table.table_name, table.app_profile_id
        )
        assert instance_key in client._active_instances
        assert client._instance_owners[instance_key] == {id(table)}
        await client.close()

    @pytest.mark.asyncio
    async def test_get_table_arg_passthrough(self):
        """
        All arguments passed in get_table should be sent to constructor
        """
        async with self._make_one(project="project-id") as client:
            with mock.patch(
                "google.cloud.bigtable.data._async.client.TableAsync.__init__",
            ) as mock_constructor:
                mock_constructor.return_value = None
                assert not client._active_instances
                expected_table_id = "table-id"
                expected_instance_id = "instance-id"
                expected_app_profile_id = "app-profile-id"
                expected_args = (1, "test", {"test": 2})
                expected_kwargs = {"hello": "world", "test": 2}

                client.get_table(
                    expected_instance_id,
                    expected_table_id,
                    expected_app_profile_id,
                    *expected_args,
                    **expected_kwargs,
                )
                mock_constructor.assert_called_once_with(
                    client,
                    expected_instance_id,
                    expected_table_id,
                    expected_app_profile_id,
                    *expected_args,
                    **expected_kwargs,
                )

    @pytest.mark.asyncio
    async def test_get_table_context_manager(self):
        from google.cloud.bigtable.data._async.client import TableAsync
        from google.cloud.bigtable.data._async.client import _WarmedInstanceKey

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_project_id = "project-id"

        with mock.patch.object(TableAsync, "close") as close_mock:
            async with self._make_one(project=expected_project_id) as client:
                async with client.get_table(
                    expected_instance_id,
                    expected_table_id,
                    expected_app_profile_id,
                ) as table:
                    await asyncio.sleep(0)
                    assert isinstance(table, TableAsync)
                    assert table.table_id == expected_table_id
                    assert (
                        table.table_name
                        == f"projects/{expected_project_id}/instances/{expected_instance_id}/tables/{expected_table_id}"
                    )
                    assert table.instance_id == expected_instance_id
                    assert (
                        table.instance_name
                        == f"projects/{expected_project_id}/instances/{expected_instance_id}"
                    )
                    assert table.app_profile_id == expected_app_profile_id
                    assert table.client is client
                    instance_key = _WarmedInstanceKey(
                        table.instance_name, table.table_name, table.app_profile_id
                    )
                    assert instance_key in client._active_instances
                    assert client._instance_owners[instance_key] == {id(table)}
            assert close_mock.call_count == 1

    @pytest.mark.asyncio
    async def test_multiple_pool_sizes(self):
        # should be able to create multiple clients with different pool sizes without issue
        pool_sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256]
        for pool_size in pool_sizes:
            client = self._make_one(
                project="project-id", pool_size=pool_size, use_emulator=False
            )
            assert len(client._channel_refresh_tasks) == pool_size
            client_duplicate = self._make_one(
                project="project-id", pool_size=pool_size, use_emulator=False
            )
            assert len(client_duplicate._channel_refresh_tasks) == pool_size
            assert str(pool_size) in str(client.transport)
            await client.close()
            await client_duplicate.close()

    @pytest.mark.asyncio
    async def test_close(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledBigtableGrpcAsyncIOTransport,
        )

        pool_size = 7
        client = self._make_one(
            project="project-id", pool_size=pool_size, use_emulator=False
        )
        assert len(client._channel_refresh_tasks) == pool_size
        tasks_list = list(client._channel_refresh_tasks)
        for task in client._channel_refresh_tasks:
            assert not task.done()
        with mock.patch.object(
            PooledBigtableGrpcAsyncIOTransport, "close", AsyncMock()
        ) as close_mock:
            await client.close()
            close_mock.assert_called_once()
            close_mock.assert_awaited()
        for task in tasks_list:
            assert task.done()
            assert task.cancelled()
        assert client._channel_refresh_tasks == []

    @pytest.mark.asyncio
    async def test_close_with_timeout(self):
        pool_size = 7
        expected_timeout = 19
        client = self._make_one(project="project-id", pool_size=pool_size)
        tasks = list(client._channel_refresh_tasks)
        with mock.patch.object(asyncio, "wait_for", AsyncMock()) as wait_for_mock:
            await client.close(timeout=expected_timeout)
            wait_for_mock.assert_called_once()
            wait_for_mock.assert_awaited()
            assert wait_for_mock.call_args[1]["timeout"] == expected_timeout
        client._channel_refresh_tasks = tasks
        await client.close()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        # context manager should close the client cleanly
        close_mock = AsyncMock()
        true_close = None
        async with self._make_one(project="project-id") as client:
            true_close = client.close()
            client.close = close_mock
            for task in client._channel_refresh_tasks:
                assert not task.done()
            assert client.project == "project-id"
            assert client._active_instances == set()
            close_mock.assert_not_called()
        close_mock.assert_called_once()
        close_mock.assert_awaited()
        # actually close the client
        await true_close

    def test_client_ctor_sync(self):
        # initializing client in a sync context should raise RuntimeError

        with pytest.warns(RuntimeWarning) as warnings:
            client = _make_client(project="project-id", use_emulator=False)
        expected_warning = [w for w in warnings if "client.py" in w.filename]
        assert len(expected_warning) == 1
        assert (
            "BigtableDataClientAsync should be started in an asyncio event loop."
            in str(expected_warning[0].message)
        )
        assert client.project == "project-id"
        assert client._channel_refresh_tasks == []


class TestTableAsync:
    @pytest.mark.asyncio
    async def test_table_ctor(self):
        from google.cloud.bigtable.data._async.client import TableAsync
        from google.cloud.bigtable.data._async.client import _WarmedInstanceKey

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_operation_timeout = 123
        expected_attempt_timeout = 12
        expected_read_rows_operation_timeout = 1.5
        expected_read_rows_attempt_timeout = 0.5
        expected_mutate_rows_operation_timeout = 2.5
        expected_mutate_rows_attempt_timeout = 0.75
        client = _make_client()
        assert not client._active_instances

        table = TableAsync(
            client,
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
            default_operation_timeout=expected_operation_timeout,
            default_attempt_timeout=expected_attempt_timeout,
            default_read_rows_operation_timeout=expected_read_rows_operation_timeout,
            default_read_rows_attempt_timeout=expected_read_rows_attempt_timeout,
            default_mutate_rows_operation_timeout=expected_mutate_rows_operation_timeout,
            default_mutate_rows_attempt_timeout=expected_mutate_rows_attempt_timeout,
        )
        await asyncio.sleep(0)
        assert table.table_id == expected_table_id
        assert table.instance_id == expected_instance_id
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        instance_key = _WarmedInstanceKey(
            table.instance_name, table.table_name, table.app_profile_id
        )
        assert instance_key in client._active_instances
        assert client._instance_owners[instance_key] == {id(table)}
        assert table.default_operation_timeout == expected_operation_timeout
        assert table.default_attempt_timeout == expected_attempt_timeout
        assert (
            table.default_read_rows_operation_timeout
            == expected_read_rows_operation_timeout
        )
        assert (
            table.default_read_rows_attempt_timeout
            == expected_read_rows_attempt_timeout
        )
        assert (
            table.default_mutate_rows_operation_timeout
            == expected_mutate_rows_operation_timeout
        )
        assert (
            table.default_mutate_rows_attempt_timeout
            == expected_mutate_rows_attempt_timeout
        )
        # ensure task reaches completion
        await table._register_instance_task
        assert table._register_instance_task.done()
        assert not table._register_instance_task.cancelled()
        assert table._register_instance_task.exception() is None
        await client.close()

    @pytest.mark.asyncio
    async def test_table_ctor_defaults(self):
        """
        should provide default timeout values and app_profile_id
        """
        from google.cloud.bigtable.data._async.client import TableAsync

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        client = _make_client()
        assert not client._active_instances

        table = TableAsync(
            client,
            expected_instance_id,
            expected_table_id,
        )
        await asyncio.sleep(0)
        assert table.table_id == expected_table_id
        assert table.instance_id == expected_instance_id
        assert table.app_profile_id is None
        assert table.client is client
        assert table.default_operation_timeout == 60
        assert table.default_read_rows_operation_timeout == 600
        assert table.default_mutate_rows_operation_timeout == 600
        assert table.default_attempt_timeout == 20
        assert table.default_read_rows_attempt_timeout == 20
        assert table.default_mutate_rows_attempt_timeout == 60
        await client.close()

    @pytest.mark.asyncio
    async def test_table_ctor_invalid_timeout_values(self):
        """
        bad timeout values should raise ValueError
        """
        from google.cloud.bigtable.data._async.client import TableAsync

        client = _make_client()

        timeout_pairs = [
            ("default_operation_timeout", "default_attempt_timeout"),
            (
                "default_read_rows_operation_timeout",
                "default_read_rows_attempt_timeout",
            ),
            (
                "default_mutate_rows_operation_timeout",
                "default_mutate_rows_attempt_timeout",
            ),
        ]
        for operation_timeout, attempt_timeout in timeout_pairs:
            with pytest.raises(ValueError) as e:
                TableAsync(client, "", "", **{attempt_timeout: -1})
            assert "attempt_timeout must be greater than 0" in str(e.value)
            with pytest.raises(ValueError) as e:
                TableAsync(client, "", "", **{operation_timeout: -1})
            assert "operation_timeout must be greater than 0" in str(e.value)
        await client.close()

    def test_table_ctor_sync(self):
        # initializing client in a sync context should raise RuntimeError
        from google.cloud.bigtable.data._async.client import TableAsync

        client = mock.Mock()
        with pytest.raises(RuntimeError) as e:
            TableAsync(client, "instance-id", "table-id")
        assert e.match("TableAsync must be created within an async event loop context.")

    @pytest.mark.asyncio
    # iterate over all retryable rpcs
    @pytest.mark.parametrize(
        "fn_name,fn_args,retry_fn_path,extra_retryables",
        [
            (
                "read_rows_stream",
                (ReadRowsQuery(),),
                "google.api_core.retry.retry_target_stream_async",
                (),
            ),
            (
                "read_rows",
                (ReadRowsQuery(),),
                "google.api_core.retry.retry_target_stream_async",
                (),
            ),
            (
                "read_row",
                (b"row_key",),
                "google.api_core.retry.retry_target_stream_async",
                (),
            ),
            (
                "read_rows_sharded",
                ([ReadRowsQuery()],),
                "google.api_core.retry.retry_target_stream_async",
                (),
            ),
            (
                "row_exists",
                (b"row_key",),
                "google.api_core.retry.retry_target_stream_async",
                (),
            ),
            ("sample_row_keys", (), "google.api_core.retry.retry_target_async", ()),
            (
                "mutate_row",
                (b"row_key", [mock.Mock()]),
                "google.api_core.retry.retry_target_async",
                (),
            ),
            (
                "bulk_mutate_rows",
                ([mutations.RowMutationEntry(b"key", [mock.Mock()])],),
                "google.api_core.retry.retry_target_async",
                (_MutateRowsIncomplete,),
            ),
        ],
    )
    # test different inputs for retryable exceptions
    @pytest.mark.parametrize(
        "input_retryables,expected_retryables",
        [
            (
                TABLE_DEFAULT.READ_ROWS,
                [
                    core_exceptions.DeadlineExceeded,
                    core_exceptions.ServiceUnavailable,
                    core_exceptions.Aborted,
                ],
            ),
            (
                TABLE_DEFAULT.DEFAULT,
                [core_exceptions.DeadlineExceeded, core_exceptions.ServiceUnavailable],
            ),
            (
                TABLE_DEFAULT.MUTATE_ROWS,
                [core_exceptions.DeadlineExceeded, core_exceptions.ServiceUnavailable],
            ),
            ([], []),
            ([4], [core_exceptions.DeadlineExceeded]),
        ],
    )
    async def test_customizable_retryable_errors(
        self,
        input_retryables,
        expected_retryables,
        fn_name,
        fn_args,
        retry_fn_path,
        extra_retryables,
    ):
        """
        Test that retryable functions support user-configurable arguments, and that the configured retryables are passed
        down to the gapic layer.
        """
        with mock.patch(retry_fn_path) as retry_fn_mock:
            async with _make_client() as client:
                table = client.get_table("instance-id", "table-id")
                expected_predicate = lambda a: a in expected_retryables  # noqa
                retry_fn_mock.side_effect = RuntimeError("stop early")
                with mock.patch(
                    "google.api_core.retry.if_exception_type"
                ) as predicate_builder_mock:
                    predicate_builder_mock.return_value = expected_predicate
                    with pytest.raises(Exception):
                        # we expect an exception from attempting to call the mock
                        test_fn = table.__getattribute__(fn_name)
                        await test_fn(*fn_args, retryable_errors=input_retryables)
                    # passed in errors should be used to build the predicate
                    predicate_builder_mock.assert_called_once_with(
                        *expected_retryables, *extra_retryables
                    )
                    retry_call_args = retry_fn_mock.call_args_list[0].args
                    # output of if_exception_type should be sent in to retry constructor
                    assert retry_call_args[1] is expected_predicate

    @pytest.mark.parametrize(
        "fn_name,fn_args,gapic_fn",
        [
            ("read_rows_stream", (ReadRowsQuery(),), "read_rows"),
            ("read_rows", (ReadRowsQuery(),), "read_rows"),
            ("read_row", (b"row_key",), "read_rows"),
            ("read_rows_sharded", ([ReadRowsQuery()],), "read_rows"),
            ("row_exists", (b"row_key",), "read_rows"),
            ("sample_row_keys", (), "sample_row_keys"),
            ("mutate_row", (b"row_key", [mock.Mock()]), "mutate_row"),
            (
                "bulk_mutate_rows",
                ([mutations.RowMutationEntry(b"key", [mutations.DeleteAllFromRow()])],),
                "mutate_rows",
            ),
            ("check_and_mutate_row", (b"row_key", None), "check_and_mutate_row"),
            (
                "read_modify_write_row",
                (b"row_key", mock.Mock()),
                "read_modify_write_row",
            ),
        ],
    )
    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_call_metadata(self, include_app_profile, fn_name, fn_args, gapic_fn):
        """check that all requests attach proper metadata headers"""
        from google.cloud.bigtable.data import TableAsync

        profile = "profile" if include_app_profile else None
        with mock.patch(
            f"google.cloud.bigtable_v2.BigtableAsyncClient.{gapic_fn}", mock.AsyncMock()
        ) as gapic_mock:
            gapic_mock.side_effect = RuntimeError("stop early")
            async with _make_client() as client:
                table = TableAsync(client, "instance-id", "table-id", profile)
                try:
                    test_fn = table.__getattribute__(fn_name)
                    maybe_stream = await test_fn(*fn_args)
                    [i async for i in maybe_stream]
                except Exception:
                    # we expect an exception from attempting to call the mock
                    pass
                kwargs = gapic_mock.call_args_list[0].kwargs
                metadata = kwargs["metadata"]
                goog_metadata = None
                for key, value in metadata:
                    if key == "x-goog-request-params":
                        goog_metadata = value
                assert goog_metadata is not None, "x-goog-request-params not found"
                assert "table_name=" + table.table_name in goog_metadata
                if include_app_profile:
                    assert "app_profile_id=profile" in goog_metadata
                else:
                    assert "app_profile_id=" not in goog_metadata


class TestReadRows:
    """
    Tests for table.read_rows and related methods.
    """

    def _make_table(self, *args, **kwargs):
        from google.cloud.bigtable.data._async.client import TableAsync

        client_mock = mock.Mock()
        client_mock._register_instance.side_effect = (
            lambda *args, **kwargs: asyncio.sleep(0)
        )
        client_mock._remove_instance_registration.side_effect = (
            lambda *args, **kwargs: asyncio.sleep(0)
        )
        kwargs["instance_id"] = kwargs.get(
            "instance_id", args[0] if args else "instance"
        )
        kwargs["table_id"] = kwargs.get(
            "table_id", args[1] if len(args) > 1 else "table"
        )
        client_mock._gapic_client.table_path.return_value = kwargs["table_id"]
        client_mock._gapic_client.instance_path.return_value = kwargs["instance_id"]
        return TableAsync(client_mock, *args, **kwargs)

    def _make_stats(self):
        from google.cloud.bigtable_v2.types import RequestStats
        from google.cloud.bigtable_v2.types import FullReadStatsView
        from google.cloud.bigtable_v2.types import ReadIterationStats

        return RequestStats(
            full_read_stats_view=FullReadStatsView(
                read_iteration_stats=ReadIterationStats(
                    rows_seen_count=1,
                    rows_returned_count=2,
                    cells_seen_count=3,
                    cells_returned_count=4,
                )
            )
        )

    @staticmethod
    def _make_chunk(*args, **kwargs):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        kwargs["row_key"] = kwargs.get("row_key", b"row_key")
        kwargs["family_name"] = kwargs.get("family_name", "family_name")
        kwargs["qualifier"] = kwargs.get("qualifier", b"qualifier")
        kwargs["value"] = kwargs.get("value", b"value")
        kwargs["commit_row"] = kwargs.get("commit_row", True)

        return ReadRowsResponse.CellChunk(*args, **kwargs)

    @staticmethod
    async def _make_gapic_stream(
        chunk_list: list[ReadRowsResponse.CellChunk | Exception],
        sleep_time=0,
    ):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        class mock_stream:
            def __init__(self, chunk_list, sleep_time):
                self.chunk_list = chunk_list
                self.idx = -1
                self.sleep_time = sleep_time

            def __aiter__(self):
                return self

            async def __anext__(self):
                self.idx += 1
                if len(self.chunk_list) > self.idx:
                    if sleep_time:
                        await asyncio.sleep(self.sleep_time)
                    chunk = self.chunk_list[self.idx]
                    if isinstance(chunk, Exception):
                        raise chunk
                    else:
                        return ReadRowsResponse(chunks=[chunk])
                raise StopAsyncIteration

            def cancel(self):
                pass

        return mock_stream(chunk_list, sleep_time)

    async def execute_fn(self, table, *args, **kwargs):
        return await table.read_rows(*args, **kwargs)

    @pytest.mark.asyncio
    async def test_read_rows(self):
        query = ReadRowsQuery()
        chunks = [
            self._make_chunk(row_key=b"test_1"),
            self._make_chunk(row_key=b"test_2"),
        ]
        async with self._make_table() as table:
            read_rows = table.client._gapic_client.read_rows
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                chunks
            )
            results = await self.execute_fn(table, query, operation_timeout=3)
            assert len(results) == 2
            assert results[0].row_key == b"test_1"
            assert results[1].row_key == b"test_2"

    @pytest.mark.asyncio
    async def test_read_rows_stream(self):
        query = ReadRowsQuery()
        chunks = [
            self._make_chunk(row_key=b"test_1"),
            self._make_chunk(row_key=b"test_2"),
        ]
        async with self._make_table() as table:
            read_rows = table.client._gapic_client.read_rows
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                chunks
            )
            gen = await table.read_rows_stream(query, operation_timeout=3)
            results = [row async for row in gen]
            assert len(results) == 2
            assert results[0].row_key == b"test_1"
            assert results[1].row_key == b"test_2"

    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_read_rows_query_matches_request(self, include_app_profile):
        from google.cloud.bigtable.data import RowRange
        from google.cloud.bigtable.data.row_filters import PassAllFilter

        app_profile_id = "app_profile_id" if include_app_profile else None
        async with self._make_table(app_profile_id=app_profile_id) as table:
            read_rows = table.client._gapic_client.read_rows
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream([])
            row_keys = [b"test_1", "test_2"]
            row_ranges = RowRange("1start", "2end")
            filter_ = PassAllFilter(True)
            limit = 99
            query = ReadRowsQuery(
                row_keys=row_keys,
                row_ranges=row_ranges,
                row_filter=filter_,
                limit=limit,
            )

            results = await table.read_rows(query, operation_timeout=3)
            assert len(results) == 0
            call_request = read_rows.call_args_list[0][0][0]
            query_pb = query._to_pb(table)
            assert call_request == query_pb

    @pytest.mark.parametrize("operation_timeout", [0.001, 0.023, 0.1])
    @pytest.mark.asyncio
    async def test_read_rows_timeout(self, operation_timeout):
        async with self._make_table() as table:
            read_rows = table.client._gapic_client.read_rows
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                chunks, sleep_time=1
            )
            try:
                await table.read_rows(query, operation_timeout=operation_timeout)
            except core_exceptions.DeadlineExceeded as e:
                assert (
                    e.message
                    == f"operation_timeout of {operation_timeout:0.1f}s exceeded"
                )

    @pytest.mark.parametrize(
        "per_request_t, operation_t, expected_num",
        [
            (0.05, 0.08, 2),
            (0.05, 0.14, 3),
            (0.05, 0.24, 5),
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_attempt_timeout(
        self, per_request_t, operation_t, expected_num
    ):
        """
        Ensures that the attempt_timeout is respected and that the number of
        requests is as expected.

        operation_timeout does not cancel the request, so we expect the number of
        requests to be the ceiling of operation_timeout / attempt_timeout.
        """
        from google.cloud.bigtable.data.exceptions import RetryExceptionGroup

        expected_last_timeout = operation_t - (expected_num - 1) * per_request_t

        # mocking uniform ensures there are no sleeps between retries
        with mock.patch("random.uniform", side_effect=lambda a, b: 0):
            async with self._make_table() as table:
                read_rows = table.client._gapic_client.read_rows
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks, sleep_time=per_request_t
                )
                query = ReadRowsQuery()
                chunks = [core_exceptions.DeadlineExceeded("mock deadline")]

                try:
                    await table.read_rows(
                        query,
                        operation_timeout=operation_t,
                        attempt_timeout=per_request_t,
                    )
                except core_exceptions.DeadlineExceeded as e:
                    retry_exc = e.__cause__
                    if expected_num == 0:
                        assert retry_exc is None
                    else:
                        assert type(retry_exc) is RetryExceptionGroup
                        assert f"{expected_num} failed attempts" in str(retry_exc)
                        assert len(retry_exc.exceptions) == expected_num
                        for sub_exc in retry_exc.exceptions:
                            assert sub_exc.message == "mock deadline"
                assert read_rows.call_count == expected_num
                # check timeouts
                for _, call_kwargs in read_rows.call_args_list[:-1]:
                    assert call_kwargs["timeout"] == per_request_t
                    assert call_kwargs["retry"] is None
                # last timeout should be adjusted to account for the time spent
                assert (
                    abs(
                        read_rows.call_args_list[-1][1]["timeout"]
                        - expected_last_timeout
                    )
                    < 0.05
                )

    @pytest.mark.parametrize(
        "exc_type",
        [
            core_exceptions.Aborted,
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_retryable_error(self, exc_type):
        async with self._make_table() as table:
            read_rows = table.client._gapic_client.read_rows
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                [expected_error]
            )
            query = ReadRowsQuery()
            expected_error = exc_type("mock error")
            try:
                await table.read_rows(query, operation_timeout=0.1)
            except core_exceptions.DeadlineExceeded as e:
                retry_exc = e.__cause__
                root_cause = retry_exc.exceptions[0]
                assert type(root_cause) is exc_type
                assert root_cause == expected_error

    @pytest.mark.parametrize(
        "exc_type",
        [
            core_exceptions.Cancelled,
            core_exceptions.PreconditionFailed,
            core_exceptions.NotFound,
            core_exceptions.PermissionDenied,
            core_exceptions.Conflict,
            core_exceptions.InternalServerError,
            core_exceptions.TooManyRequests,
            core_exceptions.ResourceExhausted,
            InvalidChunk,
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_non_retryable_error(self, exc_type):
        async with self._make_table() as table:
            read_rows = table.client._gapic_client.read_rows
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                [expected_error]
            )
            query = ReadRowsQuery()
            expected_error = exc_type("mock error")
            try:
                await table.read_rows(query, operation_timeout=0.1)
            except exc_type as e:
                assert e == expected_error

    @pytest.mark.asyncio
    async def test_read_rows_revise_request(self):
        """
        Ensure that _revise_request is called between retries
        """
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync
        from google.cloud.bigtable.data.exceptions import InvalidChunk
        from google.cloud.bigtable_v2.types import RowSet

        return_val = RowSet()
        with mock.patch.object(
            _ReadRowsOperationAsync, "_revise_request_rowset"
        ) as revise_rowset:
            revise_rowset.return_value = return_val
            async with self._make_table() as table:
                read_rows = table.client._gapic_client.read_rows
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks
                )
                row_keys = [b"test_1", b"test_2", b"test_3"]
                query = ReadRowsQuery(row_keys=row_keys)
                chunks = [
                    self._make_chunk(row_key=b"test_1"),
                    core_exceptions.Aborted("mock retryable error"),
                ]
                try:
                    await table.read_rows(query)
                except InvalidChunk:
                    revise_rowset.assert_called()
                    first_call_kwargs = revise_rowset.call_args_list[0].kwargs
                    assert first_call_kwargs["row_set"] == query._to_pb(table).rows
                    assert first_call_kwargs["last_seen_row_key"] == b"test_1"
                    revised_call = read_rows.call_args_list[1].args[0]
                    assert revised_call.rows == return_val

    @pytest.mark.asyncio
    async def test_read_rows_default_timeouts(self):
        """
        Ensure that the default timeouts are set on the read rows operation when not overridden
        """
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync

        operation_timeout = 8
        attempt_timeout = 4
        with mock.patch.object(_ReadRowsOperationAsync, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            async with self._make_table(
                default_read_rows_operation_timeout=operation_timeout,
                default_read_rows_attempt_timeout=attempt_timeout,
            ) as table:
                try:
                    await table.read_rows(ReadRowsQuery())
                except RuntimeError:
                    pass
                kwargs = mock_op.call_args_list[0].kwargs
                assert kwargs["operation_timeout"] == operation_timeout
                assert kwargs["attempt_timeout"] == attempt_timeout

    @pytest.mark.asyncio
    async def test_read_rows_default_timeout_override(self):
        """
        When timeouts are passed, they overwrite default values
        """
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync

        operation_timeout = 8
        attempt_timeout = 4
        with mock.patch.object(_ReadRowsOperationAsync, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            async with self._make_table(
                default_operation_timeout=99, default_attempt_timeout=97
            ) as table:
                try:
                    await table.read_rows(
                        ReadRowsQuery(),
                        operation_timeout=operation_timeout,
                        attempt_timeout=attempt_timeout,
                    )
                except RuntimeError:
                    pass
                kwargs = mock_op.call_args_list[0].kwargs
                assert kwargs["operation_timeout"] == operation_timeout
                assert kwargs["attempt_timeout"] == attempt_timeout

    @pytest.mark.asyncio
    async def test_read_row(self):
        """Test reading a single row"""
        async with _make_client() as client:
            table = client.get_table("instance", "table")
            row_key = b"test_1"
            with mock.patch.object(table, "read_rows") as read_rows:
                expected_result = object()
                read_rows.side_effect = lambda *args, **kwargs: [expected_result]
                expected_op_timeout = 8
                expected_req_timeout = 4
                row = await table.read_row(
                    row_key,
                    operation_timeout=expected_op_timeout,
                    attempt_timeout=expected_req_timeout,
                )
                assert row == expected_result
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["attempt_timeout"] == expected_req_timeout
                assert len(args) == 1
                assert isinstance(args[0], ReadRowsQuery)
                query = args[0]
                assert query.row_keys == [row_key]
                assert query.row_ranges == []
                assert query.limit == 1

    @pytest.mark.asyncio
    async def test_read_row_w_filter(self):
        """Test reading a single row with an added filter"""
        async with _make_client() as client:
            table = client.get_table("instance", "table")
            row_key = b"test_1"
            with mock.patch.object(table, "read_rows") as read_rows:
                expected_result = object()
                read_rows.side_effect = lambda *args, **kwargs: [expected_result]
                expected_op_timeout = 8
                expected_req_timeout = 4
                mock_filter = mock.Mock()
                expected_filter = {"filter": "mock filter"}
                mock_filter._to_dict.return_value = expected_filter
                row = await table.read_row(
                    row_key,
                    operation_timeout=expected_op_timeout,
                    attempt_timeout=expected_req_timeout,
                    row_filter=expected_filter,
                )
                assert row == expected_result
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["attempt_timeout"] == expected_req_timeout
                assert len(args) == 1
                assert isinstance(args[0], ReadRowsQuery)
                query = args[0]
                assert query.row_keys == [row_key]
                assert query.row_ranges == []
                assert query.limit == 1
                assert query.filter == expected_filter

    @pytest.mark.asyncio
    async def test_read_row_no_response(self):
        """should return None if row does not exist"""
        async with _make_client() as client:
            table = client.get_table("instance", "table")
            row_key = b"test_1"
            with mock.patch.object(table, "read_rows") as read_rows:
                # return no rows
                read_rows.side_effect = lambda *args, **kwargs: []
                expected_op_timeout = 8
                expected_req_timeout = 4
                result = await table.read_row(
                    row_key,
                    operation_timeout=expected_op_timeout,
                    attempt_timeout=expected_req_timeout,
                )
                assert result is None
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["attempt_timeout"] == expected_req_timeout
                assert isinstance(args[0], ReadRowsQuery)
                query = args[0]
                assert query.row_keys == [row_key]
                assert query.row_ranges == []
                assert query.limit == 1

    @pytest.mark.parametrize(
        "return_value,expected_result",
        [
            ([], False),
            ([object()], True),
            ([object(), object()], True),
        ],
    )
    @pytest.mark.asyncio
    async def test_row_exists(self, return_value, expected_result):
        """Test checking for row existence"""
        async with _make_client() as client:
            table = client.get_table("instance", "table")
            row_key = b"test_1"
            with mock.patch.object(table, "read_rows") as read_rows:
                # return no rows
                read_rows.side_effect = lambda *args, **kwargs: return_value
                expected_op_timeout = 1
                expected_req_timeout = 2
                result = await table.row_exists(
                    row_key,
                    operation_timeout=expected_op_timeout,
                    attempt_timeout=expected_req_timeout,
                )
                assert expected_result == result
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["attempt_timeout"] == expected_req_timeout
                assert isinstance(args[0], ReadRowsQuery)
                expected_filter = {
                    "chain": {
                        "filters": [
                            {"cells_per_row_limit_filter": 1},
                            {"strip_value_transformer": True},
                        ]
                    }
                }
                query = args[0]
                assert query.row_keys == [row_key]
                assert query.row_ranges == []
                assert query.limit == 1
                assert query.filter._to_dict() == expected_filter


class TestReadRowsSharded:
    @pytest.mark.asyncio
    async def test_read_rows_sharded_empty_query(self):
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with pytest.raises(ValueError) as exc:
                    await table.read_rows_sharded([])
                assert "empty sharded_query" in str(exc.value)

    @pytest.mark.asyncio
    async def test_read_rows_sharded_multiple_queries(self):
        """
        Test with multiple queries. Should return results from both
        """
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    table.client._gapic_client, "read_rows"
                ) as read_rows:
                    read_rows.side_effect = (
                        lambda *args, **kwargs: TestReadRows._make_gapic_stream(
                            [
                                TestReadRows._make_chunk(row_key=k)
                                for k in args[0].rows.row_keys
                            ]
                        )
                    )
                    query_1 = ReadRowsQuery(b"test_1")
                    query_2 = ReadRowsQuery(b"test_2")
                    result = await table.read_rows_sharded([query_1, query_2])
                    assert len(result) == 2
                    assert result[0].row_key == b"test_1"
                    assert result[1].row_key == b"test_2"

    @pytest.mark.parametrize("n_queries", [1, 2, 5, 11, 24])
    @pytest.mark.asyncio
    async def test_read_rows_sharded_multiple_queries_calls(self, n_queries):
        """
        Each query should trigger a separate read_rows call
        """
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(table, "read_rows") as read_rows:
                    query_list = [ReadRowsQuery() for _ in range(n_queries)]
                    await table.read_rows_sharded(query_list)
                    assert read_rows.call_count == n_queries

    @pytest.mark.asyncio
    async def test_read_rows_sharded_errors(self):
        """
        Errors should be exposed as ShardedReadRowsExceptionGroups
        """
        from google.cloud.bigtable.data.exceptions import ShardedReadRowsExceptionGroup
        from google.cloud.bigtable.data.exceptions import FailedQueryShardError

        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(table, "read_rows") as read_rows:
                    read_rows.side_effect = RuntimeError("mock error")
                    query_1 = ReadRowsQuery(b"test_1")
                    query_2 = ReadRowsQuery(b"test_2")
                    with pytest.raises(ShardedReadRowsExceptionGroup) as exc:
                        await table.read_rows_sharded([query_1, query_2])
                    exc_group = exc.value
                    assert isinstance(exc_group, ShardedReadRowsExceptionGroup)
                    assert len(exc.value.exceptions) == 2
                    assert isinstance(exc.value.exceptions[0], FailedQueryShardError)
                    assert isinstance(exc.value.exceptions[0].__cause__, RuntimeError)
                    assert exc.value.exceptions[0].index == 0
                    assert exc.value.exceptions[0].query == query_1
                    assert isinstance(exc.value.exceptions[1], FailedQueryShardError)
                    assert isinstance(exc.value.exceptions[1].__cause__, RuntimeError)
                    assert exc.value.exceptions[1].index == 1
                    assert exc.value.exceptions[1].query == query_2

    @pytest.mark.asyncio
    async def test_read_rows_sharded_concurrent(self):
        """
        Ensure sharded requests are concurrent
        """
        import time

        async def mock_call(*args, **kwargs):
            await asyncio.sleep(0.1)
            return [mock.Mock()]

        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(table, "read_rows") as read_rows:
                    read_rows.side_effect = mock_call
                    queries = [ReadRowsQuery() for _ in range(10)]
                    start_time = time.monotonic()
                    result = await table.read_rows_sharded(queries)
                    call_time = time.monotonic() - start_time
                    assert read_rows.call_count == 10
                    assert len(result) == 10
                    # if run in sequence, we would expect this to take 1 second
                    assert call_time < 0.2

    @pytest.mark.asyncio
    async def test_read_rows_sharded_concurrency_limit(self):
        """
        Only 10 queries should be processed concurrently. Others should be queued

        Should start a new query as soon as previous finishes
        """
        from google.cloud.bigtable.data._async.client import _CONCURRENCY_LIMIT

        assert _CONCURRENCY_LIMIT == 10  # change this test if this changes
        num_queries = 15

        # each of the first 10 queries take longer than the last
        # later rpcs will have to wait on first 10
        increment_time = 0.05
        max_time = increment_time * (_CONCURRENCY_LIMIT - 1)
        rpc_times = [min(i * increment_time, max_time) for i in range(num_queries)]

        async def mock_call(*args, **kwargs):
            next_sleep = rpc_times.pop(0)
            await asyncio.sleep(next_sleep)
            return [mock.Mock()]

        starting_timeout = 10

        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(table, "read_rows") as read_rows:
                    read_rows.side_effect = mock_call
                    queries = [ReadRowsQuery() for _ in range(num_queries)]
                    await table.read_rows_sharded(
                        queries, operation_timeout=starting_timeout
                    )
                    assert read_rows.call_count == num_queries
                    # check operation timeouts to see how far into the operation each rpc started
                    rpc_start_list = [
                        starting_timeout - kwargs["operation_timeout"]
                        for _, kwargs in read_rows.call_args_list
                    ]
                    eps = 0.01
                    # first 10 should start immediately
                    assert all(
                        rpc_start_list[i] < eps for i in range(_CONCURRENCY_LIMIT)
                    )
                    # next rpcs should start as first ones finish
                    for i in range(num_queries - _CONCURRENCY_LIMIT):
                        idx = i + _CONCURRENCY_LIMIT
                        assert rpc_start_list[idx] - (i * increment_time) < eps

    @pytest.mark.asyncio
    async def test_read_rows_sharded_expirary(self):
        """
        If the operation times out before all shards complete, should raise
        a ShardedReadRowsExceptionGroup
        """
        from google.cloud.bigtable.data._async.client import _CONCURRENCY_LIMIT
        from google.cloud.bigtable.data.exceptions import ShardedReadRowsExceptionGroup
        from google.api_core.exceptions import DeadlineExceeded

        operation_timeout = 0.1

        # let the first batch complete, but the next batch times out
        num_queries = 15
        sleeps = [0] * _CONCURRENCY_LIMIT + [DeadlineExceeded("times up")] * (
            num_queries - _CONCURRENCY_LIMIT
        )

        async def mock_call(*args, **kwargs):
            next_item = sleeps.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            else:
                await asyncio.sleep(next_item)
            return [mock.Mock()]

        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(table, "read_rows") as read_rows:
                    read_rows.side_effect = mock_call
                    queries = [ReadRowsQuery() for _ in range(num_queries)]
                    with pytest.raises(ShardedReadRowsExceptionGroup) as exc:
                        await table.read_rows_sharded(
                            queries, operation_timeout=operation_timeout
                        )
                    assert isinstance(exc.value, ShardedReadRowsExceptionGroup)
                    assert len(exc.value.exceptions) == num_queries - _CONCURRENCY_LIMIT
                    # should keep successful queries
                    assert len(exc.value.successful_rows) == _CONCURRENCY_LIMIT

    @pytest.mark.asyncio
    async def test_read_rows_sharded_negative_batch_timeout(self):
        """
        try to run with batch that starts after operation timeout

        They should raise DeadlineExceeded errors
        """
        from google.cloud.bigtable.data.exceptions import ShardedReadRowsExceptionGroup
        from google.api_core.exceptions import DeadlineExceeded

        async def mock_call(*args, **kwargs):
            await asyncio.sleep(0.05)
            return [mock.Mock()]

        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(table, "read_rows") as read_rows:
                    read_rows.side_effect = mock_call
                    queries = [ReadRowsQuery() for _ in range(15)]
                    with pytest.raises(ShardedReadRowsExceptionGroup) as exc:
                        await table.read_rows_sharded(queries, operation_timeout=0.01)
                    assert isinstance(exc.value, ShardedReadRowsExceptionGroup)
                    assert len(exc.value.exceptions) == 5
                    assert all(
                        isinstance(e.__cause__, DeadlineExceeded)
                        for e in exc.value.exceptions
                    )


class TestSampleRowKeys:
    async def _make_gapic_stream(self, sample_list: list[tuple[bytes, int]]):
        from google.cloud.bigtable_v2.types import SampleRowKeysResponse

        for value in sample_list:
            yield SampleRowKeysResponse(row_key=value[0], offset_bytes=value[1])

    @pytest.mark.asyncio
    async def test_sample_row_keys(self):
        """
        Test that method returns the expected key samples
        """
        samples = [
            (b"test_1", 0),
            (b"test_2", 100),
            (b"test_3", 200),
        ]
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    table.client._gapic_client, "sample_row_keys", AsyncMock()
                ) as sample_row_keys:
                    sample_row_keys.return_value = self._make_gapic_stream(samples)
                    result = await table.sample_row_keys()
                    assert len(result) == 3
                    assert all(isinstance(r, tuple) for r in result)
                    assert all(isinstance(r[0], bytes) for r in result)
                    assert all(isinstance(r[1], int) for r in result)
                    assert result[0] == samples[0]
                    assert result[1] == samples[1]
                    assert result[2] == samples[2]

    @pytest.mark.asyncio
    async def test_sample_row_keys_bad_timeout(self):
        """
        should raise error if timeout is negative
        """
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with pytest.raises(ValueError) as e:
                    await table.sample_row_keys(operation_timeout=-1)
                    assert "operation_timeout must be greater than 0" in str(e.value)
                with pytest.raises(ValueError) as e:
                    await table.sample_row_keys(attempt_timeout=-1)
                    assert "attempt_timeout must be greater than 0" in str(e.value)

    @pytest.mark.asyncio
    async def test_sample_row_keys_default_timeout(self):
        """Should fallback to using table default operation_timeout"""
        expected_timeout = 99
        async with _make_client() as client:
            async with client.get_table(
                "i",
                "t",
                default_operation_timeout=expected_timeout,
                default_attempt_timeout=expected_timeout,
            ) as table:
                with mock.patch.object(
                    table.client._gapic_client, "sample_row_keys", AsyncMock()
                ) as sample_row_keys:
                    sample_row_keys.return_value = self._make_gapic_stream([])
                    result = await table.sample_row_keys()
                    _, kwargs = sample_row_keys.call_args
                    assert abs(kwargs["timeout"] - expected_timeout) < 0.1
                    assert result == []
                    assert kwargs["retry"] is None

    @pytest.mark.asyncio
    async def test_sample_row_keys_gapic_params(self):
        """
        make sure arguments are propagated to gapic call as expected
        """
        expected_timeout = 10
        expected_profile = "test1"
        instance = "instance_name"
        table_id = "my_table"
        async with _make_client() as client:
            async with client.get_table(
                instance, table_id, app_profile_id=expected_profile
            ) as table:
                with mock.patch.object(
                    table.client._gapic_client, "sample_row_keys", AsyncMock()
                ) as sample_row_keys:
                    sample_row_keys.return_value = self._make_gapic_stream([])
                    await table.sample_row_keys(attempt_timeout=expected_timeout)
                    args, kwargs = sample_row_keys.call_args
                    assert len(args) == 0
                    assert len(kwargs) == 5
                    assert kwargs["timeout"] == expected_timeout
                    assert kwargs["app_profile_id"] == expected_profile
                    assert kwargs["table_name"] == table.table_name
                    assert kwargs["metadata"] is not None
                    assert kwargs["retry"] is None

    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_sample_row_keys_retryable_errors(self, retryable_exception):
        """
        retryable errors should be retried until timeout
        """
        from google.api_core.exceptions import DeadlineExceeded
        from google.cloud.bigtable.data.exceptions import RetryExceptionGroup

        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    table.client._gapic_client, "sample_row_keys", AsyncMock()
                ) as sample_row_keys:
                    sample_row_keys.side_effect = retryable_exception("mock")
                    with pytest.raises(DeadlineExceeded) as e:
                        await table.sample_row_keys(operation_timeout=0.05)
                    cause = e.value.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert len(cause.exceptions) > 0
                    assert isinstance(cause.exceptions[0], retryable_exception)

    @pytest.mark.parametrize(
        "non_retryable_exception",
        [
            core_exceptions.OutOfRange,
            core_exceptions.NotFound,
            core_exceptions.FailedPrecondition,
            RuntimeError,
            ValueError,
            core_exceptions.Aborted,
        ],
    )
    @pytest.mark.asyncio
    async def test_sample_row_keys_non_retryable_errors(self, non_retryable_exception):
        """
        non-retryable errors should cause a raise
        """
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    table.client._gapic_client, "sample_row_keys", AsyncMock()
                ) as sample_row_keys:
                    sample_row_keys.side_effect = non_retryable_exception("mock")
                    with pytest.raises(non_retryable_exception):
                        await table.sample_row_keys()


class TestMutateRow:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mutation_arg",
        [
            mutations.SetCell("family", b"qualifier", b"value"),
            mutations.SetCell(
                "family", b"qualifier", b"value", timestamp_micros=1234567890
            ),
            mutations.DeleteRangeFromColumn("family", b"qualifier"),
            mutations.DeleteAllFromFamily("family"),
            mutations.DeleteAllFromRow(),
            [mutations.SetCell("family", b"qualifier", b"value")],
            [
                mutations.DeleteRangeFromColumn("family", b"qualifier"),
                mutations.DeleteAllFromRow(),
            ],
        ],
    )
    async def test_mutate_row(self, mutation_arg):
        """Test mutations with no errors"""
        expected_attempt_timeout = 19
        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.return_value = None
                    await table.mutate_row(
                        "row_key",
                        mutation_arg,
                        attempt_timeout=expected_attempt_timeout,
                    )
                    assert mock_gapic.call_count == 1
                    kwargs = mock_gapic.call_args_list[0].kwargs
                    assert (
                        kwargs["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert kwargs["row_key"] == b"row_key"
                    formatted_mutations = (
                        [mutation._to_pb() for mutation in mutation_arg]
                        if isinstance(mutation_arg, list)
                        else [mutation_arg._to_pb()]
                    )
                    assert kwargs["mutations"] == formatted_mutations
                    assert kwargs["timeout"] == expected_attempt_timeout
                    # make sure gapic layer is not retrying
                    assert kwargs["retry"] is None

    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_mutate_row_retryable_errors(self, retryable_exception):
        from google.api_core.exceptions import DeadlineExceeded
        from google.cloud.bigtable.data.exceptions import RetryExceptionGroup

        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.side_effect = retryable_exception("mock")
                    with pytest.raises(DeadlineExceeded) as e:
                        mutation = mutations.DeleteAllFromRow()
                        assert mutation.is_idempotent() is True
                        await table.mutate_row(
                            "row_key", mutation, operation_timeout=0.01
                        )
                    cause = e.value.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert isinstance(cause.exceptions[0], retryable_exception)

    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_mutate_row_non_idempotent_retryable_errors(
        self, retryable_exception
    ):
        """
        Non-idempotent mutations should not be retried
        """
        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.side_effect = retryable_exception("mock")
                    with pytest.raises(retryable_exception):
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", -1
                        )
                        assert mutation.is_idempotent() is False
                        await table.mutate_row(
                            "row_key", mutation, operation_timeout=0.2
                        )

    @pytest.mark.parametrize(
        "non_retryable_exception",
        [
            core_exceptions.OutOfRange,
            core_exceptions.NotFound,
            core_exceptions.FailedPrecondition,
            RuntimeError,
            ValueError,
            core_exceptions.Aborted,
        ],
    )
    @pytest.mark.asyncio
    async def test_mutate_row_non_retryable_errors(self, non_retryable_exception):
        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.side_effect = non_retryable_exception("mock")
                    with pytest.raises(non_retryable_exception):
                        mutation = mutations.SetCell(
                            "family",
                            b"qualifier",
                            b"value",
                            timestamp_micros=1234567890,
                        )
                        assert mutation.is_idempotent() is True
                        await table.mutate_row(
                            "row_key", mutation, operation_timeout=0.2
                        )

    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_mutate_row_metadata(self, include_app_profile):
        """request should attach metadata headers"""
        profile = "profile" if include_app_profile else None
        async with _make_client() as client:
            async with client.get_table("i", "t", app_profile_id=profile) as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row", AsyncMock()
                ) as read_rows:
                    await table.mutate_row("rk", mock.Mock())
                kwargs = read_rows.call_args_list[0].kwargs
                metadata = kwargs["metadata"]
                goog_metadata = None
                for key, value in metadata:
                    if key == "x-goog-request-params":
                        goog_metadata = value
                assert goog_metadata is not None, "x-goog-request-params not found"
                assert "table_name=" + table.table_name in goog_metadata
                if include_app_profile:
                    assert "app_profile_id=profile" in goog_metadata
                else:
                    assert "app_profile_id=" not in goog_metadata

    @pytest.mark.parametrize("mutations", [[], None])
    @pytest.mark.asyncio
    async def test_mutate_row_no_mutations(self, mutations):
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with pytest.raises(ValueError) as e:
                    await table.mutate_row("key", mutations=mutations)
                    assert e.value.args[0] == "No mutations provided"


class TestBulkMutateRows:
    async def _mock_response(self, response_list):
        from google.cloud.bigtable_v2.types import MutateRowsResponse
        from google.rpc import status_pb2

        statuses = []
        for response in response_list:
            if isinstance(response, core_exceptions.GoogleAPICallError):
                statuses.append(
                    status_pb2.Status(
                        message=str(response), code=response.grpc_status_code.value[0]
                    )
                )
            else:
                statuses.append(status_pb2.Status(code=0))
        entries = [
            MutateRowsResponse.Entry(index=i, status=statuses[i])
            for i in range(len(response_list))
        ]

        async def generator():
            yield MutateRowsResponse(entries=entries)

        return generator()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mutation_arg",
        [
            [mutations.SetCell("family", b"qualifier", b"value")],
            [
                mutations.SetCell(
                    "family", b"qualifier", b"value", timestamp_micros=1234567890
                )
            ],
            [mutations.DeleteRangeFromColumn("family", b"qualifier")],
            [mutations.DeleteAllFromFamily("family")],
            [mutations.DeleteAllFromRow()],
            [mutations.SetCell("family", b"qualifier", b"value")],
            [
                mutations.DeleteRangeFromColumn("family", b"qualifier"),
                mutations.DeleteAllFromRow(),
            ],
        ],
    )
    async def test_bulk_mutate_rows(self, mutation_arg):
        """Test mutations with no errors"""
        expected_attempt_timeout = 19
        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.return_value = self._mock_response([None])
                    bulk_mutation = mutations.RowMutationEntry(b"row_key", mutation_arg)
                    await table.bulk_mutate_rows(
                        [bulk_mutation],
                        attempt_timeout=expected_attempt_timeout,
                    )
                    assert mock_gapic.call_count == 1
                    kwargs = mock_gapic.call_args[1]
                    assert (
                        kwargs["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert kwargs["entries"] == [bulk_mutation._to_pb()]
                    assert kwargs["timeout"] == expected_attempt_timeout
                    assert kwargs["retry"] is None

    @pytest.mark.asyncio
    async def test_bulk_mutate_rows_multiple_entries(self):
        """Test mutations with no errors"""
        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.return_value = self._mock_response([None, None])
                    mutation_list = [mutations.DeleteAllFromRow()]
                    entry_1 = mutations.RowMutationEntry(b"row_key_1", mutation_list)
                    entry_2 = mutations.RowMutationEntry(b"row_key_2", mutation_list)
                    await table.bulk_mutate_rows(
                        [entry_1, entry_2],
                    )
                    assert mock_gapic.call_count == 1
                    kwargs = mock_gapic.call_args[1]
                    assert (
                        kwargs["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert kwargs["entries"][0] == entry_1._to_pb()
                    assert kwargs["entries"][1] == entry_2._to_pb()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    async def test_bulk_mutate_rows_idempotent_mutation_error_retryable(
        self, exception
    ):
        """
        Individual idempotent mutations should be retried if they fail with a retryable error
        """
        from google.cloud.bigtable.data.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = lambda *a, **k: self._mock_response(
                        [exception("mock")]
                    )
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.DeleteAllFromRow()
                        entry = mutations.RowMutationEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.05)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert "non-idempotent" not in str(failed_exception)
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert isinstance(cause.exceptions[0], exception)
                    # last exception should be due to retry timeout
                    assert isinstance(
                        cause.exceptions[-1], core_exceptions.DeadlineExceeded
                    )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception",
        [
            core_exceptions.OutOfRange,
            core_exceptions.NotFound,
            core_exceptions.FailedPrecondition,
            core_exceptions.Aborted,
        ],
    )
    async def test_bulk_mutate_rows_idempotent_mutation_error_non_retryable(
        self, exception
    ):
        """
        Individual idempotent mutations should not be retried if they fail with a non-retryable error
        """
        from google.cloud.bigtable.data.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = lambda *a, **k: self._mock_response(
                        [exception("mock")]
                    )
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.DeleteAllFromRow()
                        entry = mutations.RowMutationEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.05)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert "non-idempotent" not in str(failed_exception)
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, exception)

    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_bulk_mutate_idempotent_retryable_request_errors(
        self, retryable_exception
    ):
        """
        Individual idempotent mutations should be retried if the request fails with a retryable error
        """
        from google.cloud.bigtable.data.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = retryable_exception("mock")
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", timestamp_micros=123
                        )
                        entry = mutations.RowMutationEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.05)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    assert "non-idempotent" not in str(failed_exception)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert isinstance(cause.exceptions[0], retryable_exception)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    async def test_bulk_mutate_rows_non_idempotent_retryable_errors(
        self, retryable_exception
    ):
        """Non-Idempotent mutations should never be retried"""
        from google.cloud.bigtable.data.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = lambda *a, **k: self._mock_response(
                        [retryable_exception("mock")]
                    )
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", -1
                        )
                        entry = mutations.RowMutationEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is False
                        await table.bulk_mutate_rows([entry], operation_timeout=0.2)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    assert "non-idempotent" in str(failed_exception)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, retryable_exception)

    @pytest.mark.parametrize(
        "non_retryable_exception",
        [
            core_exceptions.OutOfRange,
            core_exceptions.NotFound,
            core_exceptions.FailedPrecondition,
            RuntimeError,
            ValueError,
        ],
    )
    @pytest.mark.asyncio
    async def test_bulk_mutate_rows_non_retryable_errors(self, non_retryable_exception):
        """
        If the request fails with a non-retryable error, mutations should not be retried
        """
        from google.cloud.bigtable.data.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = non_retryable_exception("mock")
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", timestamp_micros=123
                        )
                        entry = mutations.RowMutationEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.2)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    assert "non-idempotent" not in str(failed_exception)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, non_retryable_exception)

    @pytest.mark.asyncio
    async def test_bulk_mutate_error_index(self):
        """
        Test partial failure, partial success. Errors should be associated with the correct index
        """
        from google.api_core.exceptions import (
            DeadlineExceeded,
            ServiceUnavailable,
            FailedPrecondition,
        )
        from google.cloud.bigtable.data.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with _make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    # fail with retryable errors, then a non-retryable one
                    mock_gapic.side_effect = [
                        self._mock_response([None, ServiceUnavailable("mock"), None]),
                        self._mock_response([DeadlineExceeded("mock")]),
                        self._mock_response([FailedPrecondition("final")]),
                    ]
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", timestamp_micros=123
                        )
                        entries = [
                            mutations.RowMutationEntry(
                                (f"row_key_{i}").encode(), [mutation]
                            )
                            for i in range(3)
                        ]
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows(entries, operation_timeout=1000)
                    assert len(e.value.exceptions) == 1
                    failed = e.value.exceptions[0]
                    assert isinstance(failed, FailedMutationEntryError)
                    assert failed.index == 1
                    assert failed.entry == entries[1]
                    cause = failed.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert len(cause.exceptions) == 3
                    assert isinstance(cause.exceptions[0], ServiceUnavailable)
                    assert isinstance(cause.exceptions[1], DeadlineExceeded)
                    assert isinstance(cause.exceptions[2], FailedPrecondition)

    @pytest.mark.asyncio
    async def test_bulk_mutate_error_recovery(self):
        """
        If an error occurs, then resolves, no exception should be raised
        """
        from google.api_core.exceptions import DeadlineExceeded

        async with _make_client(project="project") as client:
            table = client.get_table("instance", "table")
            with mock.patch.object(client._gapic_client, "mutate_rows") as mock_gapic:
                # fail with a retryable error, then a non-retryable one
                mock_gapic.side_effect = [
                    self._mock_response([DeadlineExceeded("mock")]),
                    self._mock_response([None]),
                ]
                mutation = mutations.SetCell(
                    "family", b"qualifier", b"value", timestamp_micros=123
                )
                entries = [
                    mutations.RowMutationEntry((f"row_key_{i}").encode(), [mutation])
                    for i in range(3)
                ]
                await table.bulk_mutate_rows(entries, operation_timeout=1000)


class TestCheckAndMutateRow:
    @pytest.mark.parametrize("gapic_result", [True, False])
    @pytest.mark.asyncio
    async def test_check_and_mutate(self, gapic_result):
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse

        app_profile = "app_profile_id"
        async with _make_client() as client:
            async with client.get_table(
                "instance", "table", app_profile_id=app_profile
            ) as table:
                with mock.patch.object(
                    client._gapic_client, "check_and_mutate_row"
                ) as mock_gapic:
                    mock_gapic.return_value = CheckAndMutateRowResponse(
                        predicate_matched=gapic_result
                    )
                    row_key = b"row_key"
                    predicate = None
                    true_mutations = [mock.Mock()]
                    false_mutations = [mock.Mock(), mock.Mock()]
                    operation_timeout = 0.2
                    found = await table.check_and_mutate_row(
                        row_key,
                        predicate,
                        true_case_mutations=true_mutations,
                        false_case_mutations=false_mutations,
                        operation_timeout=operation_timeout,
                    )
                    assert found == gapic_result
                    kwargs = mock_gapic.call_args[1]
                    assert kwargs["table_name"] == table.table_name
                    assert kwargs["row_key"] == row_key
                    assert kwargs["predicate_filter"] == predicate
                    assert kwargs["true_mutations"] == [
                        m._to_pb() for m in true_mutations
                    ]
                    assert kwargs["false_mutations"] == [
                        m._to_pb() for m in false_mutations
                    ]
                    assert kwargs["app_profile_id"] == app_profile
                    assert kwargs["timeout"] == operation_timeout
                    assert kwargs["retry"] is None

    @pytest.mark.asyncio
    async def test_check_and_mutate_bad_timeout(self):
        """Should raise error if operation_timeout < 0"""
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with pytest.raises(ValueError) as e:
                    await table.check_and_mutate_row(
                        b"row_key",
                        None,
                        true_case_mutations=[mock.Mock()],
                        false_case_mutations=[],
                        operation_timeout=-1,
                    )
                assert str(e.value) == "operation_timeout must be greater than 0"

    @pytest.mark.asyncio
    async def test_check_and_mutate_single_mutations(self):
        """if single mutations are passed, they should be internally wrapped in a list"""
        from google.cloud.bigtable.data.mutations import SetCell
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse

        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "check_and_mutate_row"
                ) as mock_gapic:
                    mock_gapic.return_value = CheckAndMutateRowResponse(
                        predicate_matched=True
                    )
                    true_mutation = SetCell("family", b"qualifier", b"value")
                    false_mutation = SetCell("family", b"qualifier", b"value")
                    await table.check_and_mutate_row(
                        b"row_key",
                        None,
                        true_case_mutations=true_mutation,
                        false_case_mutations=false_mutation,
                    )
                    kwargs = mock_gapic.call_args[1]
                    assert kwargs["true_mutations"] == [true_mutation._to_pb()]
                    assert kwargs["false_mutations"] == [false_mutation._to_pb()]

    @pytest.mark.asyncio
    async def test_check_and_mutate_predicate_object(self):
        """predicate filter should be passed to gapic request"""
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse

        mock_predicate = mock.Mock()
        predicate_pb = {"predicate": "dict"}
        mock_predicate._to_pb.return_value = predicate_pb
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "check_and_mutate_row"
                ) as mock_gapic:
                    mock_gapic.return_value = CheckAndMutateRowResponse(
                        predicate_matched=True
                    )
                    await table.check_and_mutate_row(
                        b"row_key",
                        mock_predicate,
                        false_case_mutations=[mock.Mock()],
                    )
                    kwargs = mock_gapic.call_args[1]
                    assert kwargs["predicate_filter"] == predicate_pb
                    assert mock_predicate._to_pb.call_count == 1
                    assert kwargs["retry"] is None

    @pytest.mark.asyncio
    async def test_check_and_mutate_mutations_parsing(self):
        """mutations objects should be converted to protos"""
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse
        from google.cloud.bigtable.data.mutations import DeleteAllFromRow

        mutations = [mock.Mock() for _ in range(5)]
        for idx, mutation in enumerate(mutations):
            mutation._to_pb.return_value = f"fake {idx}"
        mutations.append(DeleteAllFromRow())
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "check_and_mutate_row"
                ) as mock_gapic:
                    mock_gapic.return_value = CheckAndMutateRowResponse(
                        predicate_matched=True
                    )
                    await table.check_and_mutate_row(
                        b"row_key",
                        None,
                        true_case_mutations=mutations[0:2],
                        false_case_mutations=mutations[2:],
                    )
                    kwargs = mock_gapic.call_args[1]
                    assert kwargs["true_mutations"] == ["fake 0", "fake 1"]
                    assert kwargs["false_mutations"] == [
                        "fake 2",
                        "fake 3",
                        "fake 4",
                        DeleteAllFromRow()._to_pb(),
                    ]
                    assert all(
                        mutation._to_pb.call_count == 1 for mutation in mutations[:5]
                    )


class TestReadModifyWriteRow:
    @pytest.mark.parametrize(
        "call_rules,expected_rules",
        [
            (
                AppendValueRule("f", "c", b"1"),
                [AppendValueRule("f", "c", b"1")._to_pb()],
            ),
            (
                [AppendValueRule("f", "c", b"1")],
                [AppendValueRule("f", "c", b"1")._to_pb()],
            ),
            (IncrementRule("f", "c", 1), [IncrementRule("f", "c", 1)._to_pb()]),
            (
                [AppendValueRule("f", "c", b"1"), IncrementRule("f", "c", 1)],
                [
                    AppendValueRule("f", "c", b"1")._to_pb(),
                    IncrementRule("f", "c", 1)._to_pb(),
                ],
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_read_modify_write_call_rule_args(self, call_rules, expected_rules):
        """
        Test that the gapic call is called with given rules
        """
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    await table.read_modify_write_row("key", call_rules)
                assert mock_gapic.call_count == 1
                found_kwargs = mock_gapic.call_args_list[0][1]
                assert found_kwargs["rules"] == expected_rules
                assert found_kwargs["retry"] is None

    @pytest.mark.parametrize("rules", [[], None])
    @pytest.mark.asyncio
    async def test_read_modify_write_no_rules(self, rules):
        async with _make_client() as client:
            async with client.get_table("instance", "table") as table:
                with pytest.raises(ValueError) as e:
                    await table.read_modify_write_row("key", rules=rules)
                    assert e.value.args[0] == "rules must contain at least one item"

    @pytest.mark.asyncio
    async def test_read_modify_write_call_defaults(self):
        instance = "instance1"
        table_id = "table1"
        project = "project1"
        row_key = "row_key1"
        async with _make_client(project=project) as client:
            async with client.get_table(instance, table_id) as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    await table.read_modify_write_row(row_key, mock.Mock())
                    assert mock_gapic.call_count == 1
                    kwargs = mock_gapic.call_args_list[0][1]
                    assert (
                        kwargs["table_name"]
                        == f"projects/{project}/instances/{instance}/tables/{table_id}"
                    )
                    assert kwargs["app_profile_id"] is None
                    assert kwargs["row_key"] == row_key.encode()
                    assert kwargs["timeout"] > 1

    @pytest.mark.asyncio
    async def test_read_modify_write_call_overrides(self):
        row_key = b"row_key1"
        expected_timeout = 12345
        profile_id = "profile1"
        async with _make_client() as client:
            async with client.get_table(
                "instance", "table_id", app_profile_id=profile_id
            ) as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    await table.read_modify_write_row(
                        row_key,
                        mock.Mock(),
                        operation_timeout=expected_timeout,
                    )
                    assert mock_gapic.call_count == 1
                    kwargs = mock_gapic.call_args_list[0][1]
                    assert kwargs["app_profile_id"] is profile_id
                    assert kwargs["row_key"] == row_key
                    assert kwargs["timeout"] == expected_timeout

    @pytest.mark.asyncio
    async def test_read_modify_write_string_key(self):
        row_key = "string_row_key1"
        async with _make_client() as client:
            async with client.get_table("instance", "table_id") as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    await table.read_modify_write_row(row_key, mock.Mock())
                    assert mock_gapic.call_count == 1
                    kwargs = mock_gapic.call_args_list[0][1]
                    assert kwargs["row_key"] == row_key.encode()

    @pytest.mark.asyncio
    async def test_read_modify_write_row_building(self):
        """
        results from gapic call should be used to construct row
        """
        from google.cloud.bigtable.data.row import Row
        from google.cloud.bigtable_v2.types import ReadModifyWriteRowResponse
        from google.cloud.bigtable_v2.types import Row as RowPB

        mock_response = ReadModifyWriteRowResponse(row=RowPB())
        async with _make_client() as client:
            async with client.get_table("instance", "table_id") as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    with mock.patch.object(Row, "_from_pb") as constructor_mock:
                        mock_gapic.return_value = mock_response
                        await table.read_modify_write_row("key", mock.Mock())
                        assert constructor_mock.call_count == 1
                        constructor_mock.assert_called_once_with(mock_response.row)
