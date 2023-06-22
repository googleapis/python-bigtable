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

from google.cloud.bigtable import mutations
from google.auth.credentials import AnonymousCredentials
from google.cloud.bigtable_v2.types import ReadRowsResponse
from google.cloud.bigtable.read_rows_query import ReadRowsQuery
from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.exceptions import InvalidChunk

from google.cloud.bigtable.read_modify_write_rules import IncrementRule
from google.cloud.bigtable.read_modify_write_rules import AppendValueRule

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore

VENEER_HEADER_REGEX = re.compile(
    r"gapic\/[0-9]+\.[\w.-]+ gax\/[0-9]+\.[\w.-]+ gccl\/[0-9]+\.[\w.-]+ gl-python\/[0-9]+\.[\w.-]+ grpc\/[0-9]+\.[\w.-]+"
)


class TestBigtableDataClient:
    def _get_target_class(self):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @pytest.mark.asyncio
    async def test_ctor(self):
        expected_project = "project-id"
        expected_pool_size = 11
        expected_credentials = AnonymousCredentials()
        client = self._make_one(
            project="project-id",
            pool_size=expected_pool_size,
            credentials=expected_credentials,
        )
        await asyncio.sleep(0.1)
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
        from google.cloud.bigtable.client import BigtableDataClient

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
            BigtableDataClient, "start_background_channel_refresh"
        ) as start_background_refresh:
            client = self._make_one(client_options=client_options)
            start_background_refresh.assert_called_once()
            await client.close()

    @pytest.mark.asyncio
    async def test_veneer_grpc_headers(self):
        # client_info should be populated with headers to
        # detect as a veneer client
        patch = mock.patch("google.api_core.gapic_v1.method.wrap_method")
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
    def test_start_background_channel_refresh_sync(self):
        # should raise RuntimeError if called in a sync context
        client = self._make_one(project="project-id")
        with pytest.raises(RuntimeError):
            client.start_background_channel_refresh()

    @pytest.mark.asyncio
    async def test_start_background_channel_refresh_tasks_exist(self):
        # if tasks exist, should do nothing
        client = self._make_one(project="project-id")
        with mock.patch.object(asyncio, "create_task") as create_task:
            client.start_background_channel_refresh()
            create_task.assert_not_called()
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pool_size", [1, 3, 7])
    async def test_start_background_channel_refresh(self, pool_size):
        # should create background tasks for each channel
        client = self._make_one(project="project-id", pool_size=pool_size)
        ping_and_warm = AsyncMock()
        client._ping_and_warm_instances = ping_and_warm
        client.start_background_channel_refresh()
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
    async def test_start_background_channel_refresh_tasks_names(self):
        # if tasks exist, should do nothing
        pool_size = 3
        client = self._make_one(project="project-id", pool_size=pool_size)
        for i in range(pool_size):
            name = client._channel_refresh_tasks[i].get_name()
            assert str(i) in name
            assert "BigtableDataClient channel refresh " in name
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
            result = await self._get_target_class()._ping_and_warm_instances(client_mock, channel)
            assert len(result) == 0
            gather.assert_called_once()
            gather.assert_awaited_once()
            assert not gather.call_args.args
            assert gather.call_args.kwargs == {"return_exceptions": True}
            # test with instances
            client_mock._active_instances = [
                "instance-1",
                "instance-2",
                "instance-3",
                "instance-4",
            ]
            gather.reset_mock()
            result = await self._get_target_class()._ping_and_warm_instances(client_mock, channel)
            assert len(result) == 4
            gather.assert_called_once()
            gather.assert_awaited_once()
            assert len(gather.call_args.args) == 4
            assert gather.call_args.kwargs == {"return_exceptions": True}
            for idx, call in enumerate(gather.call_args.args):
                assert call == channel.unary_unary()()

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
    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    async def test__register_instance(self):
        # create the client without calling start_background_channel_refresh
        with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
            get_event_loop.side_effect = RuntimeError("no event loop")
            client = self._make_one(project="project-id")
        assert not client._channel_refresh_tasks
        # first call should start background refresh
        assert client._active_instances == set()
        await client._register_instance("instance-1", mock.Mock())
        assert len(client._active_instances) == 1
        assert client._active_instances == {"projects/project-id/instances/instance-1"}
        assert client._channel_refresh_tasks
        # next call should not
        with mock.patch.object(
            type(self._make_one()), "start_background_channel_refresh"
        ) as refresh_mock:
            await client._register_instance("instance-2", mock.Mock())
            assert len(client._active_instances) == 2
            assert client._active_instances == {
                "projects/project-id/instances/instance-1",
                "projects/project-id/instances/instance-2",
            }
            refresh_mock.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    async def test__register_instance_ping_and_warm(self):
        # should ping and warm each new instance
        pool_size = 7
        with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
            get_event_loop.side_effect = RuntimeError("no event loop")
            client = self._make_one(project="project-id", pool_size=pool_size)
        # first call should start background refresh
        assert not client._channel_refresh_tasks
        await client._register_instance("instance-1", mock.Mock())
        client = self._make_one(project="project-id", pool_size=pool_size)
        assert len(client._channel_refresh_tasks) == pool_size
        assert not client._active_instances
        # next calls should trigger ping and warm
        with mock.patch.object(
            type(self._make_one()), "_ping_and_warm_instances"
        ) as ping_mock:
            # new instance should trigger ping and warm
            await client._register_instance("instance-2", mock.Mock())
            assert ping_mock.call_count == pool_size
            await client._register_instance("instance-3", mock.Mock())
            assert ping_mock.call_count == pool_size * 2
            # duplcate instances should not trigger ping and warm
            await client._register_instance("instance-3", mock.Mock())
            assert ping_mock.call_count == pool_size * 2
        await client.close()

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
        instance_2_path = client._gapic_client.instance_path(
            client.project, "instance-2"
        )
        assert len(client._instance_owners[instance_1_path]) == 1
        assert list(client._instance_owners[instance_1_path])[0] == id(table)
        assert len(client._instance_owners[instance_2_path]) == 1
        assert list(client._instance_owners[instance_2_path])[0] == id(table)
        success = await client._remove_instance_registration("instance-1", table)
        assert success
        assert len(client._active_instances) == 1
        assert len(client._instance_owners[instance_1_path]) == 0
        assert len(client._instance_owners[instance_2_path]) == 1
        assert client._active_instances == {"projects/project-id/instances/instance-2"}
        success = await client._remove_instance_registration("nonexistant", table)
        assert not success
        assert len(client._active_instances) == 1
        await client.close()

    @pytest.mark.asyncio
    async def test__multiple_table_registration(self):
        async with self._make_one(project="project-id") as client:
            async with client.get_table("instance_1", "table_1") as table_1:
                instance_1_path = client._gapic_client.instance_path(
                    client.project, "instance_1"
                )
                assert len(client._instance_owners[instance_1_path]) == 1
                assert len(client._active_instances) == 1
                assert id(table_1) in client._instance_owners[instance_1_path]
                async with client.get_table("instance_1", "table_2") as table_2:
                    assert len(client._instance_owners[instance_1_path]) == 2
                    assert len(client._active_instances) == 1
                    assert id(table_1) in client._instance_owners[instance_1_path]
                    assert id(table_2) in client._instance_owners[instance_1_path]
                # table_2 should be unregistered, but instance should still be active
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert id(table_2) not in client._instance_owners[instance_1_path]
            # both tables are gone. instance should be unregistered
            assert len(client._active_instances) == 0
            assert instance_1_path not in client._active_instances
            assert len(client._instance_owners[instance_1_path]) == 0

    @pytest.mark.asyncio
    async def test__multiple_instance_registration(self):
        async with self._make_one(project="project-id") as client:
            async with client.get_table("instance_1", "table_1") as table_1:
                async with client.get_table("instance_2", "table_2") as table_2:
                    instance_1_path = client._gapic_client.instance_path(
                        client.project, "instance_1"
                    )
                    instance_2_path = client._gapic_client.instance_path(
                        client.project, "instance_2"
                    )
                    assert len(client._instance_owners[instance_1_path]) == 1
                    assert len(client._instance_owners[instance_2_path]) == 1
                    assert len(client._active_instances) == 2
                    assert id(table_1) in client._instance_owners[instance_1_path]
                    assert id(table_2) in client._instance_owners[instance_2_path]
                # instance2 should be unregistered, but instance1 should still be active
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert len(client._instance_owners[instance_2_path]) == 0
                assert len(client._instance_owners[instance_1_path]) == 1
                assert id(table_1) in client._instance_owners[instance_1_path]
            # both tables are gone. instances should both be unregistered
            assert len(client._active_instances) == 0
            assert len(client._instance_owners[instance_1_path]) == 0
            assert len(client._instance_owners[instance_2_path]) == 0

    @pytest.mark.asyncio
    async def test_get_table(self):
        from google.cloud.bigtable.client import Table

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
        assert isinstance(table, Table)
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
        assert table.instance_name in client._active_instances
        await client.close()

    @pytest.mark.asyncio
    async def test_get_table_context_manager(self):
        from google.cloud.bigtable.client import Table

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_project_id = "project-id"

        with mock.patch.object(Table, "close") as close_mock:
            async with self._make_one(project=expected_project_id) as client:
                async with client.get_table(
                    expected_instance_id,
                    expected_table_id,
                    expected_app_profile_id,
                ) as table:
                    await asyncio.sleep(0)
                    assert isinstance(table, Table)
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
                    assert table.instance_name in client._active_instances
            assert close_mock.call_count == 1

    @pytest.mark.asyncio
    async def test_multiple_pool_sizes(self):
        # should be able to create multiple clients with different pool sizes without issue
        pool_sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256]
        for pool_size in pool_sizes:
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert len(client._channel_refresh_tasks) == pool_size
            client_duplicate = self._make_one(project="project-id", pool_size=pool_size)
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
        client = self._make_one(project="project-id", pool_size=pool_size)
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
        from google.cloud.bigtable.client import BigtableDataClient

        with pytest.warns(RuntimeWarning) as warnings:
            client = BigtableDataClient(project="project-id")
        expected_warning = [w for w in warnings if "client.py" in w.filename]
        assert len(expected_warning) == 1
        assert "BigtableDataClient should be started in an asyncio event loop." in str(
            expected_warning[0].message
        )
        assert client.project == "project-id"
        assert client._channel_refresh_tasks == []


class TestTable:
    @pytest.mark.asyncio
    async def test_table_ctor(self):
        from google.cloud.bigtable.client import BigtableDataClient
        from google.cloud.bigtable.client import Table

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_operation_timeout = 123
        expected_per_request_timeout = 12
        client = BigtableDataClient()
        assert not client._active_instances

        table = Table(
            client,
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
            default_operation_timeout=expected_operation_timeout,
            default_per_request_timeout=expected_per_request_timeout,
        )
        await asyncio.sleep(0)
        assert table.table_id == expected_table_id
        assert table.instance_id == expected_instance_id
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        assert table.instance_name in client._active_instances
        assert table.default_operation_timeout == expected_operation_timeout
        assert table.default_per_request_timeout == expected_per_request_timeout
        # ensure task reaches completion
        await table._register_instance_task
        assert table._register_instance_task.done()
        assert not table._register_instance_task.cancelled()
        assert table._register_instance_task.exception() is None
        await client.close()

    @pytest.mark.asyncio
    async def test_table_ctor_bad_timeout_values(self):
        from google.cloud.bigtable.client import BigtableDataClient
        from google.cloud.bigtable.client import Table

        client = BigtableDataClient()

        with pytest.raises(ValueError) as e:
            Table(client, "", "", default_per_request_timeout=-1)
        assert "default_per_request_timeout must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            Table(client, "", "", default_operation_timeout=-1)
        assert "default_operation_timeout must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            Table(
                client,
                "",
                "",
                default_operation_timeout=1,
                default_per_request_timeout=2,
            )
        assert (
            "default_per_request_timeout must be less than default_operation_timeout"
            in str(e.value)
        )
        await client.close()

    def test_table_ctor_sync(self):
        # initializing client in a sync context should raise RuntimeError
        from google.cloud.bigtable.client import Table

        client = mock.Mock()
        with pytest.raises(RuntimeError) as e:
            Table(client, "instance-id", "table-id")
        assert e.match("Table must be created within an async event loop context.")


class TestReadRows:
    """
    Tests for table.read_rows and related methods.
    """

    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

    def _make_table(self, *args, **kwargs):
        from google.cloud.bigtable.client import Table
        client_mock = mock.Mock()
        client_mock._register_instance.side_effect = lambda *args, **kwargs: asyncio.sleep(0)
        client_mock._remove_instance_registration.side_effect = lambda *args, **kwargs: asyncio.sleep(0)
        kwargs["instance_id"] = kwargs.get("instance_id", args[0] if args else "instance")
        kwargs["table_id"] = kwargs.get("table_id", args[1] if len(args) > 1 else "table")
        client_mock._gapic_client.table_path.return_value = kwargs["table_id"]
        client_mock._gapic_client.instance_path.return_value = kwargs["instance_id"]
        return Table(client_mock, *args, **kwargs)

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

    def _make_chunk(self, *args, **kwargs):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        kwargs["row_key"] = kwargs.get("row_key", b"row_key")
        kwargs["family_name"] = kwargs.get("family_name", "family_name")
        kwargs["qualifier"] = kwargs.get("qualifier", b"qualifier")
        kwargs["value"] = kwargs.get("value", b"value")
        kwargs["commit_row"] = kwargs.get("commit_row", True)

        return ReadRowsResponse.CellChunk(*args, **kwargs)

    async def _make_gapic_stream(
        self,
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
            results = await table.read_rows(query, operation_timeout=3)
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
        from google.cloud.bigtable import RowRange

        app_profile_id = "app_profile_id" if include_app_profile else None
        async with self._make_table(app_profile_id=app_profile_id) as table:
            read_rows = table.client._gapic_client.read_rows
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                []
            )
            row_keys = [b"test_1", "test_2"]
            row_ranges = RowRange("start", "end")
            filter_ = {"test": "filter"}
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
            query_dict = query._to_dict()
            if include_app_profile:
                assert set(call_request.keys()) == set(query_dict.keys()) | {
                    "table_name",
                    "app_profile_id",
                }
            else:
                assert set(call_request.keys()) == set(query_dict.keys()) | {
                    "table_name"
                }
            assert call_request["rows"] == query_dict["rows"]
            assert call_request["filter"] == filter_
            assert call_request["rows_limit"] == limit
            assert call_request["table_name"] == table.table_name
            if include_app_profile:
                assert call_request["app_profile_id"] == app_profile_id

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
            (0.05, 0.54, 11),
            (0.05, 0.14, 3),
            (0.05, 0.24, 5),
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_per_request_timeout(
        self, per_request_t, operation_t, expected_num
    ):
        """
        Ensures that the per_request_timeout is respected and that the number of
        requests is as expected.

        operation_timeout does not cancel the request, so we expect the number of
        requests to be the ceiling of operation_timeout / per_request_timeout.
        """
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        expected_last_timeout = operation_t - (expected_num - 1) * per_request_t

        # mocking uniform ensures there are no sleeps between retries
        with mock.patch("random.uniform", side_effect=lambda a, b: 0):
            async with self._make_table() as table:
                read_rows = table.client._gapic_client.read_rows
                read_rows.side_effect = (
                    lambda *args, **kwargs: self._make_gapic_stream(
                        chunks, sleep_time=per_request_t
                    )
                )
                query = ReadRowsQuery()
                chunks = [core_exceptions.DeadlineExceeded("mock deadline")]

                try:
                    await table.read_rows(
                        query,
                        operation_timeout=operation_t,
                        per_request_timeout=per_request_t,
                    )
                except core_exceptions.DeadlineExceeded as e:
                    retry_exc = e.__cause__
                    if expected_num == 0:
                        assert retry_exc is None
                    else:
                        assert type(retry_exc) == RetryExceptionGroup
                        assert f"{expected_num} failed attempts" in str(retry_exc)
                        assert len(retry_exc.exceptions) == expected_num
                        for sub_exc in retry_exc.exceptions:
                            assert sub_exc.message == "mock deadline"
                assert read_rows.call_count == expected_num
                # check timeouts
                for _, call_kwargs in read_rows.call_args_list[:-1]:
                    assert call_kwargs["timeout"] == per_request_t
                # last timeout should be adjusted to account for the time spent
                assert (
                    abs(
                        read_rows.call_args_list[-1][1]["timeout"]
                        - expected_last_timeout
                    )
                    < 0.05
                )

    @pytest.mark.asyncio
    async def test_read_rows_idle_timeout(self):
        from google.cloud.bigtable.client import ReadRowsIterator
        from google.cloud.bigtable_v2.services.bigtable.async_client import (
            BigtableAsyncClient,
        )
        from google.cloud.bigtable.exceptions import IdleTimeout
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        chunks = [
            self._make_chunk(row_key=b"test_1"),
            self._make_chunk(row_key=b"test_2"),
        ]
        with mock.patch.object(BigtableAsyncClient, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                chunks
            )
            with mock.patch.object(
                ReadRowsIterator, "_start_idle_timer"
            ) as start_idle_timer:
                client = self._make_client()
                table = client.get_table("instance", "table")
                query = ReadRowsQuery()
                gen = await table.read_rows_stream(query)
            # should start idle timer on creation
            start_idle_timer.assert_called_once()
        with mock.patch.object(_ReadRowsOperation, "aclose", AsyncMock()) as aclose:
            # start idle timer with our own value
            await gen._start_idle_timer(0.1)
            # should timeout after being abandoned
            await gen.__anext__()
            await asyncio.sleep(0.2)
            # generator should be expired
            assert not gen.active
            assert type(gen._error) == IdleTimeout
            assert gen._idle_timeout_task is None
            await client.close()
            with pytest.raises(IdleTimeout) as e:
                await gen.__anext__()

            expected_msg = (
                "Timed out waiting for next Row to be consumed. (idle_timeout=0.1s)"
            )
            assert e.value.message == expected_msg
            aclose.assert_called_once()
            aclose.assert_awaited()

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
                assert type(root_cause) == exc_type
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
        from google.cloud.bigtable._read_rows import _ReadRowsOperation
        from google.cloud.bigtable.exceptions import InvalidChunk

        with mock.patch.object(
            _ReadRowsOperation, "_revise_request_rowset"
        ) as revise_rowset:
            with mock.patch.object(_ReadRowsOperation, "aclose"):
                revise_rowset.return_value = "modified"
                async with self._make_table() as table:
                    read_rows = table.client._gapic_client.read_rows
                    read_rows.side_effect = (
                        lambda *args, **kwargs: self._make_gapic_stream(chunks)
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
                        revise_call_kwargs = revise_rowset.call_args_list[0].kwargs
                        assert (
                            revise_call_kwargs["row_set"]
                            == query._to_dict()["rows"]
                        )
                        assert revise_call_kwargs["last_seen_row_key"] == b"test_1"
                        read_rows_request = read_rows.call_args_list[1].args[0]
                        assert read_rows_request["rows"] == "modified"

    @pytest.mark.asyncio
    async def test_read_rows_default_timeouts(self):
        """
        Ensure that the default timeouts are set on the read rows operation when not overridden
        """
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        operation_timeout = 8
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            async with self._make_table(default_operation_timeout=operation_timeout,default_per_request_timeout=per_request_timeout) as table:
                try:
                    await table.read_rows(ReadRowsQuery())
                except RuntimeError:
                    pass
                kwargs = mock_op.call_args_list[0].kwargs
                assert kwargs["operation_timeout"] == operation_timeout
                assert kwargs["per_request_timeout"] == per_request_timeout

    @pytest.mark.asyncio
    async def test_read_rows_default_timeout_override(self):
        """
        When timeouts are passed, they overwrite default values
        """
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        operation_timeout = 8
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            async with self._make_table(default_operation_timeout=99, default_per_request_timeout=97) as table:
                try:
                    await table.read_rows(
                        ReadRowsQuery(),
                        operation_timeout=operation_timeout,
                        per_request_timeout=per_request_timeout,
                    )
                except RuntimeError:
                    pass
                kwargs = mock_op.call_args_list[0].kwargs
                assert kwargs["operation_timeout"] == operation_timeout
                assert kwargs["per_request_timeout"] == per_request_timeout

    @pytest.mark.asyncio
    async def test_read_row(self):
        """Test reading a single row"""
        async with self._make_client() as client:
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
                    per_request_timeout=expected_req_timeout,
                )
                assert row == expected_result
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["per_request_timeout"] == expected_req_timeout
                assert len(args) == 1
                assert isinstance(args[0], ReadRowsQuery)
                assert args[0]._to_dict() == {
                    "rows": {"row_keys": [row_key]},
                    "rows_limit": 1,
                }

    @pytest.mark.asyncio
    async def test_read_row_w_filter(self):
        """Test reading a single row with an added filter"""
        async with self._make_client() as client:
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
                    per_request_timeout=expected_req_timeout,
                    row_filter=expected_filter,
                )
                assert row == expected_result
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["per_request_timeout"] == expected_req_timeout
                assert len(args) == 1
                assert isinstance(args[0], ReadRowsQuery)
                assert args[0]._to_dict() == {
                    "rows": {"row_keys": [row_key]},
                    "rows_limit": 1,
                    "filter": expected_filter,
                }

    @pytest.mark.asyncio
    async def test_read_row_no_response(self):
        """should return None if row does not exist"""
        async with self._make_client() as client:
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
                    per_request_timeout=expected_req_timeout,
                )
                assert result is None
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["per_request_timeout"] == expected_req_timeout
                assert isinstance(args[0], ReadRowsQuery)
                assert args[0]._to_dict() == {
                    "rows": {"row_keys": [row_key]},
                    "rows_limit": 1,
                }

    @pytest.mark.parametrize("input_row", [None, 5, object()])
    @pytest.mark.asyncio
    async def test_read_row_w_invalid_input(self, input_row):
        """Should raise error when passed None"""
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            with pytest.raises(ValueError) as e:
                await table.read_row(input_row)
                assert "must be string or bytes" in e

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
        async with self._make_client() as client:
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
                    per_request_timeout=expected_req_timeout,
                )
                assert expected_result == result
                assert read_rows.call_count == 1
                args, kwargs = read_rows.call_args_list[0]
                assert kwargs["operation_timeout"] == expected_op_timeout
                assert kwargs["per_request_timeout"] == expected_req_timeout
                assert isinstance(args[0], ReadRowsQuery)
                expected_filter = {
                    "chain": {
                        "filters": [
                            {"cells_per_row_limit_filter": 1},
                            {"strip_value_transformer": True},
                        ]
                    }
                }
                assert args[0]._to_dict() == {
                    "rows": {"row_keys": [row_key]},
                    "rows_limit": 1,
                    "filter": expected_filter,
                }

    @pytest.mark.parametrize("input_row", [None, 5, object()])
    @pytest.mark.asyncio
    async def test_row_exists_w_invalid_input(self, input_row):
        """Should raise error when passed None"""
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            with pytest.raises(ValueError) as e:
                await table.row_exists(input_row)
                assert "must be string or bytes" in e

    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_read_rows_metadata(self, include_app_profile):
        """request should attach metadata headers"""
        profile = "profile" if include_app_profile else None
        async with self._make_table(app_profile_id=profile) as table:
            read_rows = table.client._gapic_client.read_rows
            read_rows.return_value = self._make_gapic_stream([])
            await table.read_rows(ReadRowsQuery())
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


class TestMutateRow:
    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

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
        expected_per_request_timeout = 19
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.return_value = None
                    await table.mutate_row(
                        "row_key",
                        mutation_arg,
                        per_request_timeout=expected_per_request_timeout,
                    )
                    assert mock_gapic.call_count == 1
                    request = mock_gapic.call_args[0][0]
                    assert (
                        request["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert request["row_key"] == b"row_key"
                    formatted_mutations = (
                        [mutation._to_dict() for mutation in mutation_arg]
                        if isinstance(mutation_arg, list)
                        else [mutation_arg._to_dict()]
                    )
                    assert request["mutations"] == formatted_mutations
                    found_per_request_timeout = mock_gapic.call_args[1]["timeout"]
                    assert found_per_request_timeout == expected_per_request_timeout

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
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.side_effect = retryable_exception("mock")
                    with pytest.raises(DeadlineExceeded) as e:
                        mutation = mutations.DeleteAllFromRow()
                        assert mutation.is_idempotent() is True
                        await table.mutate_row(
                            "row_key", mutation, operation_timeout=0.05
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
        async with self._make_client(project="project") as client:
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
        async with self._make_client(project="project") as client:
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
        async with self._make_client() as client:
            async with client.get_table("i", "t", app_profile_id=profile) as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row", AsyncMock()
                ) as read_rows:
                    await table.mutate_row("rk", {})
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


class TestBulkMutateRows:
    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

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
        expected_per_request_timeout = 19
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.return_value = self._mock_response([None])
                    bulk_mutation = mutations.RowMutationEntry(b"row_key", mutation_arg)
                    await table.bulk_mutate_rows(
                        [bulk_mutation],
                        per_request_timeout=expected_per_request_timeout,
                    )
                    assert mock_gapic.call_count == 1
                    kwargs = mock_gapic.call_args[1]
                    assert (
                        kwargs["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert kwargs["entries"] == [bulk_mutation._to_dict()]
                    assert kwargs["timeout"] == expected_per_request_timeout

    @pytest.mark.asyncio
    async def test_bulk_mutate_rows_multiple_entries(self):
        """Test mutations with no errors"""
        async with self._make_client(project="project") as client:
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
                    assert kwargs["entries"][0] == entry_1._to_dict()
                    assert kwargs["entries"][1] == entry_2._to_dict()

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
        from google.cloud.bigtable.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
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
        from google.cloud.bigtable.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
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
        from google.cloud.bigtable.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
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
        from google.cloud.bigtable.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
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
        from google.cloud.bigtable.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
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
        from google.cloud.bigtable.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
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

    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_bulk_mutate_row_metadata(self, include_app_profile):
        """request should attach metadata headers"""
        profile = "profile" if include_app_profile else None
        async with self._make_client() as client:
            async with client.get_table("i", "t", app_profile_id=profile) as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows", AsyncMock()
                ) as read_rows:
                    read_rows.side_effect = core_exceptions.Aborted("mock")
                    try:
                        await table.bulk_mutate_rows([mock.Mock()])
                    except Exception:
                        # exception used to end early
                        pass
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


class TestCheckAndMutateRow:
    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

    @pytest.mark.parametrize("gapic_result", [True, False])
    @pytest.mark.asyncio
    async def test_check_and_mutate(self, gapic_result):
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse

        app_profile = "app_profile_id"
        async with self._make_client() as client:
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
                    request = kwargs["request"]
                    assert request["table_name"] == table.table_name
                    assert request["row_key"] == row_key
                    assert request["predicate_filter"] == predicate
                    assert request["true_mutations"] == [
                        m._to_dict() for m in true_mutations
                    ]
                    assert request["false_mutations"] == [
                        m._to_dict() for m in false_mutations
                    ]
                    assert request["app_profile_id"] == app_profile
                    assert kwargs["timeout"] == operation_timeout

    @pytest.mark.asyncio
    async def test_check_and_mutate_bad_timeout(self):
        """Should raise error if operation_timeout < 0"""
        async with self._make_client() as client:
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
    async def test_check_and_mutate_no_mutations(self):
        """Requests require either true_case_mutations or false_case_mutations"""
        from google.api_core.exceptions import InvalidArgument

        async with self._make_client() as client:
            async with client.get_table("instance", "table") as table:
                with pytest.raises(InvalidArgument) as e:
                    await table.check_and_mutate_row(
                        b"row_key",
                        None,
                        true_case_mutations=None,
                        false_case_mutations=None,
                    )
                assert "No mutations provided" in str(e.value)

    @pytest.mark.asyncio
    async def test_check_and_mutate_single_mutations(self):
        """if single mutations are passed, they should be internally wrapped in a list"""
        from google.cloud.bigtable.mutations import SetCell
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse

        async with self._make_client() as client:
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
                    request = kwargs["request"]
                    assert request["true_mutations"] == [true_mutation._to_dict()]
                    assert request["false_mutations"] == [false_mutation._to_dict()]

    @pytest.mark.asyncio
    async def test_check_and_mutate_predicate_object(self):
        """predicate object should be converted to dict"""
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse

        mock_predicate = mock.Mock()
        fake_dict = {"fake": "dict"}
        mock_predicate.to_dict.return_value = fake_dict
        async with self._make_client() as client:
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
                    assert kwargs["request"]["predicate_filter"] == fake_dict
                    assert mock_predicate.to_dict.call_count == 1

    @pytest.mark.asyncio
    async def test_check_and_mutate_mutations_parsing(self):
        """mutations objects should be converted to dicts"""
        from google.cloud.bigtable_v2.types import CheckAndMutateRowResponse
        from google.cloud.bigtable.mutations import DeleteAllFromRow

        mutations = [mock.Mock() for _ in range(5)]
        for idx, mutation in enumerate(mutations):
            mutation._to_dict.return_value = {"fake": idx}
        mutations.append(DeleteAllFromRow())
        async with self._make_client() as client:
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
                    kwargs = mock_gapic.call_args[1]["request"]
                    assert kwargs["true_mutations"] == [{"fake": 0}, {"fake": 1}]
                    assert kwargs["false_mutations"] == [
                        {"fake": 2},
                        {"fake": 3},
                        {"fake": 4},
                        {"delete_from_row": {}},
                    ]
                    assert all(
                        mutation._to_dict.call_count == 1 for mutation in mutations[:5]
                    )

    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_check_and_mutate_metadata(self, include_app_profile):
        """request should attach metadata headers"""
        profile = "profile" if include_app_profile else None
        async with self._make_client() as client:
            async with client.get_table("i", "t", app_profile_id=profile) as table:
                with mock.patch.object(
                    client._gapic_client, "check_and_mutate_row", AsyncMock()
                ) as mock_gapic:
                    await table.check_and_mutate_row(b"key", mock.Mock())
                kwargs = mock_gapic.call_args_list[0].kwargs
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


class TestReadModifyWriteRow:
    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

    @pytest.mark.parametrize(
        "call_rules,expected_rules",
        [
            (
                AppendValueRule("f", "c", b"1"),
                [AppendValueRule("f", "c", b"1")._to_dict()],
            ),
            (
                [AppendValueRule("f", "c", b"1")],
                [AppendValueRule("f", "c", b"1")._to_dict()],
            ),
            (IncrementRule("f", "c", 1), [IncrementRule("f", "c", 1)._to_dict()]),
            (
                [AppendValueRule("f", "c", b"1"), IncrementRule("f", "c", 1)],
                [
                    AppendValueRule("f", "c", b"1")._to_dict(),
                    IncrementRule("f", "c", 1)._to_dict(),
                ],
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_read_modify_write_call_rule_args(self, call_rules, expected_rules):
        """
        Test that the gapic call is called with given rules
        """
        async with self._make_client() as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    await table.read_modify_write_row("key", call_rules)
                assert mock_gapic.call_count == 1
                found_kwargs = mock_gapic.call_args_list[0][1]
                assert found_kwargs["request"]["rules"] == expected_rules

    @pytest.mark.parametrize("rules", [[], None])
    @pytest.mark.asyncio
    async def test_read_modify_write_no_rules(self, rules):
        async with self._make_client() as client:
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
        async with self._make_client(project=project) as client:
            async with client.get_table(instance, table_id) as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    await table.read_modify_write_row(row_key, mock.Mock())
                    assert mock_gapic.call_count == 1
                    found_kwargs = mock_gapic.call_args_list[0][1]
                    request = found_kwargs["request"]
                    assert (
                        request["table_name"]
                        == f"projects/{project}/instances/{instance}/tables/{table_id}"
                    )
                    assert request["app_profile_id"] is None
                    assert request["row_key"] == row_key.encode()
                    assert found_kwargs["timeout"] > 1

    @pytest.mark.asyncio
    async def test_read_modify_write_call_overrides(self):
        row_key = b"row_key1"
        expected_timeout = 12345
        profile_id = "profile1"
        async with self._make_client() as client:
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
                    found_kwargs = mock_gapic.call_args_list[0][1]
                    request = found_kwargs["request"]
                    assert request["app_profile_id"] is profile_id
                    assert request["row_key"] == row_key
                    assert found_kwargs["timeout"] == expected_timeout

    @pytest.mark.asyncio
    async def test_read_modify_write_string_key(self):
        row_key = "string_row_key1"
        async with self._make_client() as client:
            async with client.get_table("instance", "table_id") as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    await table.read_modify_write_row(row_key, mock.Mock())
                    assert mock_gapic.call_count == 1
                    found_kwargs = mock_gapic.call_args_list[0][1]
                    assert found_kwargs["request"]["row_key"] == row_key.encode()

    @pytest.mark.asyncio
    async def test_read_modify_write_row_building(self):
        """
        results from gapic call should be used to construct row
        """
        from google.cloud.bigtable.row import Row
        from google.cloud.bigtable_v2.types import ReadModifyWriteRowResponse
        from google.cloud.bigtable_v2.types import Row as RowPB

        mock_response = ReadModifyWriteRowResponse(row=RowPB())
        async with self._make_client() as client:
            async with client.get_table("instance", "table_id") as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row"
                ) as mock_gapic:
                    with mock.patch.object(Row, "_from_pb") as constructor_mock:
                        mock_gapic.return_value = mock_response
                        await table.read_modify_write_row("key", mock.Mock())
                        assert constructor_mock.call_count == 1
                        constructor_mock.assert_called_once_with(mock_response.row)

    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_read_modify_write_metadata(self, include_app_profile):
        """request should attach metadata headers"""
        profile = "profile" if include_app_profile else None
        async with self._make_client() as client:
            async with client.get_table("i", "t", app_profile_id=profile) as table:
                with mock.patch.object(
                    client._gapic_client, "read_modify_write_row", AsyncMock()
                ) as mock_gapic:
                    await table.read_modify_write_row("key", mock.Mock())
                kwargs = mock_gapic.call_args_list[0].kwargs
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
