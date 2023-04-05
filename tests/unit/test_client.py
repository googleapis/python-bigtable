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


import grpc
import asyncio
import re
import sys

from google.auth.credentials import AnonymousCredentials
import pytest

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


def _get_target_class():
    from google.cloud.bigtable.client import BigtableDataClient

    return BigtableDataClient


def _make_one(*args, **kwargs):
    return _get_target_class()(*args, **kwargs)


@pytest.mark.asyncio
async def test_ctor():
    expected_project = "project-id"
    expected_pool_size = 11
    expected_credentials = AnonymousCredentials()
    client = _make_one(
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
async def test_ctor_super_inits():
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
        with mock.patch.object(ClientWithProject, "__init__") as client_project_init:
            client_project_init.return_value = None
            try:
                _make_one(
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
async def test_ctor_dict_options():
    from google.cloud.bigtable_v2.services.bigtable.async_client import (
        BigtableAsyncClient,
    )
    from google.api_core.client_options import ClientOptions
    from google.cloud.bigtable.client import BigtableDataClient

    client_options = {"api_endpoint": "foo.bar:1234"}
    with mock.patch.object(BigtableAsyncClient, "__init__") as bigtable_client_init:
        try:
            _make_one(client_options=client_options)
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
        client = _make_one(client_options=client_options)
        start_background_refresh.assert_called_once()
        await client.close()


@pytest.mark.asyncio
async def test_veneer_grpc_headers():
    # client_info should be populated with headers to
    # detect as a veneer client
    patch = mock.patch("google.api_core.gapic_v1.method.wrap_method")
    with patch as gapic_mock:
        client = _make_one(project="project-id")
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
async def test_channel_pool_creation():
    pool_size = 14
    with mock.patch(
        "google.api_core.grpc_helpers_async.create_channel"
    ) as create_channel:
        create_channel.return_value = AsyncMock()
        client = _make_one(project="project-id", pool_size=pool_size)
        assert create_channel.call_count == pool_size
        await client.close()
    # channels should be unique
    client = _make_one(project="project-id", pool_size=pool_size)
    pool_list = list(client.transport._grpc_channel._pool)
    pool_set = set(client.transport._grpc_channel._pool)
    assert len(pool_list) == len(pool_set)
    await client.close()


@pytest.mark.asyncio
async def test_channel_pool_rotation():
    from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
        PooledChannel,
    )

    pool_size = 7

    with mock.patch.object(PooledChannel, "next_channel") as next_channel:
        client = _make_one(project="project-id", pool_size=pool_size)
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
async def test_channel_pool_replace():
    with mock.patch.object(asyncio, "sleep"):
        pool_size = 7
        client = _make_one(project="project-id", pool_size=pool_size)
        for replace_idx in range(pool_size):
            start_pool = [channel for channel in client.transport._grpc_channel._pool]
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
def test_start_background_channel_refresh_sync():
    # should raise RuntimeError if called in a sync context
    client = _make_one(project="project-id")
    with pytest.raises(RuntimeError):
        client.start_background_channel_refresh()


@pytest.mark.asyncio
async def test_start_background_channel_refresh_tasks_exist():
    # if tasks exist, should do nothing
    client = _make_one(project="project-id")
    with mock.patch.object(asyncio, "create_task") as create_task:
        client.start_background_channel_refresh()
        create_task.assert_not_called()
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("pool_size", [1, 3, 7])
async def test_start_background_channel_refresh(pool_size):
    # should create background tasks for each channel
    client = _make_one(project="project-id", pool_size=pool_size)
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
async def test_start_background_channel_refresh_tasks_names():
    # if tasks exist, should do nothing
    pool_size = 3
    client = _make_one(project="project-id", pool_size=pool_size)
    for i in range(pool_size):
        name = client._channel_refresh_tasks[i].get_name()
        assert str(i) in name
        assert "BigtableDataClient channel refresh " in name
    await client.close()


@pytest.mark.asyncio
async def test__ping_and_warm_instances():
    # test with no instances
    with mock.patch.object(asyncio, "gather", AsyncMock()) as gather:
        client = _make_one(project="project-id", pool_size=1)
        channel = client.transport._grpc_channel._pool[0]
        await client._ping_and_warm_instances(channel)
        gather.assert_called_once()
        gather.assert_awaited_once()
        assert not gather.call_args.args
        assert gather.call_args.kwargs == {"return_exceptions": True}
        # test with instances
        client._active_instances = [
            "instance-1",
            "instance-2",
            "instance-3",
            "instance-4",
        ]
    with mock.patch.object(asyncio, "gather", AsyncMock()) as gather:
        await client._ping_and_warm_instances(channel)
        gather.assert_called_once()
        gather.assert_awaited_once()
        assert len(gather.call_args.args) == 4
        assert gather.call_args.kwargs == {"return_exceptions": True}
        for idx, call in enumerate(gather.call_args.args):
            assert isinstance(call, grpc.aio.UnaryUnaryCall)
            call._request["name"] = client._active_instances[idx]
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
async def test__manage_channel_first_sleep(refresh_interval, wait_time, expected_sleep):
    # first sleep time should be `refresh_interval` seconds after client init
    import time

    with mock.patch.object(time, "time") as time:
        time.return_value = 0
        with mock.patch.object(asyncio, "sleep") as sleep:
            sleep.side_effect = asyncio.CancelledError
            try:
                client = _make_one(project="project-id")
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
async def test__manage_channel_ping_and_warm():
    from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
        PooledBigtableGrpcAsyncIOTransport,
    )

    # should ping an warm all new channels, and old channels if sleeping
    client = _make_one(project="project-id")
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
                type(_make_one()), "_ping_and_warm_instances"
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
                type(_make_one()), "_ping_and_warm_instances"
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
async def test__manage_channel_sleeps(refresh_interval, num_cycles, expected_sleep):
    # make sure that sleeps work as expected
    import time
    import random

    channel_idx = 1
    random.uniform = mock.Mock()
    random.uniform.side_effect = lambda min_, max_: min_
    with mock.patch.object(time, "time") as time:
        time.return_value = 0
        with mock.patch.object(asyncio, "sleep") as sleep:
            sleep.side_effect = [None for i in range(num_cycles - 1)] + [
                asyncio.CancelledError
            ]
            try:
                client = _make_one(project="project-id")
                if refresh_interval is not None:
                    await client._manage_channel(channel_idx, refresh_interval, refresh_interval)
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
async def test__manage_channel_random():
    import random
    with mock.patch.object(asyncio, "sleep") as sleep:
        with mock.patch.object(random, "uniform") as uniform:
            uniform.return_value = 0
            try:
                uniform.side_effect = asyncio.CancelledError
                client = _make_one(project="project-id", pool_size=1)
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
async def test__manage_channel_refresh(num_cycles):
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
                client = _make_one(project="project-id")
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
async def test_register_instance():
    # create the client without calling start_background_channel_refresh
    with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
        get_event_loop.side_effect = RuntimeError("no event loop")
        client = _make_one(project="project-id")
    assert not client._channel_refresh_tasks
    # first call should start background refresh
    assert client._active_instances == set()
    await client.register_instance("instance-1")
    assert len(client._active_instances) == 1
    assert client._active_instances == {"projects/project-id/instances/instance-1"}
    assert client._channel_refresh_tasks
    # next call should not
    with mock.patch.object(
        type(_make_one()), "start_background_channel_refresh"
    ) as refresh_mock:
        await client.register_instance("instance-2")
        assert len(client._active_instances) == 2
        assert client._active_instances == {
            "projects/project-id/instances/instance-1",
            "projects/project-id/instances/instance-2",
        }
        refresh_mock.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
async def test_register_instance_ping_and_warm():
    # should ping and warm each new instance
    pool_size = 7
    with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
        get_event_loop.side_effect = RuntimeError("no event loop")
        client = _make_one(project="project-id", pool_size=pool_size)
    # first call should start background refresh
    assert not client._channel_refresh_tasks
    await client.register_instance("instance-1")
    client = _make_one(project="project-id", pool_size=pool_size)
    assert len(client._channel_refresh_tasks) == pool_size
    assert not client._active_instances
    # next calls should trigger ping and warm
    with mock.patch.object(type(_make_one()), "_ping_and_warm_instances") as ping_mock:
        # new instance should trigger ping and warm
        await client.register_instance("instance-2")
        assert ping_mock.call_count == pool_size
        await client.register_instance("instance-3")
        assert ping_mock.call_count == pool_size * 2
        # duplcate instances should not trigger ping and warm
        await client.register_instance("instance-3")
        assert ping_mock.call_count == pool_size * 2
    await client.close()


@pytest.mark.asyncio
async def test_remove_instance_registration():
    client = _make_one(project="project-id")
    await client.register_instance("instance-1")
    await client.register_instance("instance-2")
    assert len(client._active_instances) == 2
    success = await client.remove_instance_registration("instance-1")
    assert success
    assert len(client._active_instances) == 1
    assert client._active_instances == {"projects/project-id/instances/instance-2"}
    success = await client.remove_instance_registration("nonexistant")
    assert not success
    assert len(client._active_instances) == 1
    await client.close()


@pytest.mark.asyncio
async def test_get_table():
    from google.cloud.bigtable.client import Table

    client = _make_one(project="project-id")
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
    assert table.instance == expected_instance_id
    assert table.app_profile_id == expected_app_profile_id
    assert table.client is client
    full_instance_name = client._gapic_client.instance_path(
        client.project, expected_instance_id
    )
    assert full_instance_name in client._active_instances
    await client.close()


@pytest.mark.asyncio
async def test_multiple_pool_sizes():
    # should be able to create multiple clients with different pool sizes without issue
    pool_sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256]
    for pool_size in pool_sizes:
        client = _make_one(project="project-id", pool_size=pool_size)
        assert len(client._channel_refresh_tasks) == pool_size
        client_duplicate = _make_one(project="project-id", pool_size=pool_size)
        assert len(client_duplicate._channel_refresh_tasks) == pool_size
        assert str(pool_size) in str(client.transport)
        await client.close()
        await client_duplicate.close()


@pytest.mark.asyncio
async def test_close():
    from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
        PooledBigtableGrpcAsyncIOTransport,
    )

    pool_size = 7
    client = _make_one(project="project-id", pool_size=pool_size)
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
async def test_close_with_timeout():
    pool_size = 7
    expected_timeout = 19
    client = _make_one(project="project-id", pool_size=pool_size)
    tasks = list(client._channel_refresh_tasks)
    with mock.patch.object(asyncio, "wait_for", AsyncMock()) as wait_for_mock:
        await client.close(timeout=expected_timeout)
        wait_for_mock.assert_called_once()
        wait_for_mock.assert_awaited()
        assert wait_for_mock.call_args[1]["timeout"] == expected_timeout
    client._channel_refresh_tasks = tasks
    await client.close()


@pytest.mark.asyncio
async def test_context_manager():
    # context manager should close the client cleanly
    close_mock = AsyncMock()
    true_close = None
    async with _make_one(project="project-id") as client:
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


def test_client_ctor_sync():
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


######################################################################
# Table Tests
######################################################################


@pytest.mark.asyncio
async def test_table_ctor():
    from google.cloud.bigtable.client import BigtableDataClient
    from google.cloud.bigtable.client import Table

    expected_table_id = "table-id"
    expected_instance_id = "instance-id"
    expected_app_profile_id = "app-profile-id"
    client = BigtableDataClient()
    assert not client._active_instances

    table = Table(
        client,
        expected_instance_id,
        expected_table_id,
        expected_app_profile_id,
    )
    await asyncio.sleep(0)
    assert table.table_id == expected_table_id
    assert table.instance == expected_instance_id
    assert table.app_profile_id == expected_app_profile_id
    assert table.client is client
    full_instance_name = client._gapic_client.instance_path(
        client.project, expected_instance_id
    )
    assert full_instance_name in client._active_instances
    # ensure task reaches completion
    await table._register_instance_task
    assert table._register_instance_task.done()
    assert not table._register_instance_task.cancelled()
    assert table._register_instance_task.exception() is None
    await client.close()


def test_table_ctor_sync():
    # initializing client in a sync context should raise RuntimeError
    from google.cloud.bigtable.client import Table

    client = mock.Mock()
    with pytest.warns(RuntimeWarning) as warnings:
        table = Table(client, "instance-id", "table-id")
    assert "event loop" in str(warnings[0].message)
    assert table.table_id == "table-id"
    assert table.instance == "instance-id"
    assert table.app_profile_id is None
    assert table.client is client
