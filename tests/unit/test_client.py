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


import unittest
import grpc
import asyncio
import re

from google.auth.credentials import AnonymousCredentials
import pytest

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    import mock

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
    expected_metadata = [("a", "b")]
    expected_credentials = AnonymousCredentials()
    client = _make_one(
        project="project-id",
        pool_size=expected_pool_size,
        metadata=expected_metadata,
        credentials=expected_credentials,
    )
    await asyncio.sleep(0.1)
    assert client.project == expected_project
    assert len(client.transport.channel_pool) == expected_pool_size
    assert client.metadata == expected_metadata
    assert not client._active_instances
    assert len(client._channel_refresh_tasks) == expected_pool_size
    assert client.transport._credentials == expected_credentials
    await client.close()


@pytest.mark.asyncio
async def test_ctor_super_inits():
    from google.cloud.bigtable_v2.services.bigtable.async_client import (
        BigtableAsyncClient,
    )
    from google.cloud.client import _ClientProjectMixin
    from google.api_core import client_options as client_options_lib

    project = "project-id"
    pool_size = 11
    credentials = AnonymousCredentials()
    client_options = {"api_endpoint": "foo.bar:1234"}
    options_parsed = client_options_lib.from_dict(client_options)
    metadata = [("a", "b")]
    transport_str = f"pooled_grpc_asyncio_{pool_size}"
    with mock.patch.object(BigtableAsyncClient, "__init__") as bigtable_client_init:
        with mock.patch.object(
            _ClientProjectMixin, "__init__"
        ) as client_project_mixin_init:
            try:
                _make_one(
                    project=project,
                    pool_size=pool_size,
                    credentials=credentials,
                    client_options=options_parsed,
                    metadata=metadata,
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
            assert client_project_mixin_init.call_count == 1
            kwargs = client_project_mixin_init.call_args[1]
            assert kwargs["project"] == project
            assert kwargs["credentials"] == credentials


@pytest.mark.asyncio
async def test_ctor_dict_options():
    from google.cloud.bigtable_v2.services.bigtable.async_client import (
        BigtableAsyncClient,
    )
    from google.api_core.client_options import ClientOptions

    client_options = {"api_endpoint": "foo.bar:1234"}
    with mock.patch.object(BigtableAsyncClient, "__init__") as bigtable_client_init:
        _make_one(client_options=client_options)
        bigtable_client_init.assert_called_once()
        kwargs = bigtable_client_init.call_args[1]
        called_options = kwargs["client_options"]
        assert called_options.api_endpoint == "foo.bar:1234"
        assert isinstance(called_options, ClientOptions)


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
    from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
        PooledBigtableGrpcAsyncIOTransport,
    )

    pool_size = 14
    with mock.patch.object(
        PooledBigtableGrpcAsyncIOTransport, "create_channel"
    ) as create_channel:
        client = _make_one(project="project-id", pool_size=pool_size)
        assert create_channel.call_count == pool_size
    # channels should be unique
    client = _make_one(project="project-id", pool_size=pool_size)
    pool_list = list(client.transport.channel_pool)
    pool_set = set(client.transport.channel_pool)
    assert len(pool_list) == len(pool_set)
    await client.close()


@pytest.mark.asyncio
async def test_channel_pool_rotation():
    pool_size = 7
    client = _make_one(project="project-id", pool_size=pool_size)
    assert len(client.transport.channel_pool) == pool_size

    with mock.patch.object(type(client.transport), "next_channel") as next_channel:
        with mock.patch.object(type(client.transport.channel_pool[0]), "unary_unary"):
            # calling an rpc `pool_size` times should use a different channel each time
            for i in range(pool_size):
                channel_1 = client.transport.channel_pool[client.transport._next_idx]
                next_channel.return_value = channel_1
                client.transport.ping_and_warm()
                assert next_channel.call_count == i + 1
                channel_1.unary_unary.assert_called_once()
    await client.close()


@pytest.mark.asyncio
async def test_channel_pool_replace():
    pool_size = 7
    client = _make_one(project="project-id", pool_size=pool_size)
    for replace_idx in range(pool_size):
        start_pool = [channel for channel in client.transport.channel_pool]
        grace_period = 9
        with mock.patch.object(
            type(client.transport.channel_pool[0]), "close"
        ) as close:
            new_channel = grpc.aio.insecure_channel("localhost:8080")
            await client.transport.replace_channel(
                replace_idx, grace=grace_period, new_channel=new_channel
            )
            close.assert_called_once_with(grace=grace_period)
            close.assert_awaited_once()
        assert client.transport.channel_pool[replace_idx] == new_channel
        for i in range(pool_size):
            if i != replace_idx:
                assert client.transport.channel_pool[i] == start_pool[i]
            else:
                assert client.transport.channel_pool[i] != start_pool[i]
    await client.close()


@pytest.mark.asyncio
async def test_ctor_background_channel_refresh():
    # should create background tasks for each channel
    for pool_size in [1, 3, 7]:
        client = _make_one(project="project-id", pool_size=pool_size)
        ping_and_warm = AsyncMock()
        client._ping_and_warm_instances = ping_and_warm
        assert len(client._channel_refresh_tasks) == pool_size
        for task in client._channel_refresh_tasks:
            assert isinstance(task, asyncio.Task)
        await asyncio.sleep(0.1)
        assert ping_and_warm.call_count == pool_size
        for channel in client.transport.channel_pool:
            ping_and_warm.assert_any_call(channel)
        await client.close()


@pytest.mark.asyncio
async def test__ping_and_warm_instances():
    # test with no instances
    gather = AsyncMock()
    asyncio.gather = gather
    client = _make_one(project="project-id", pool_size=1)
    channel = client.transport.channel_pool[0]
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
    gather = AsyncMock()
    asyncio.gather = gather
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
async def test__manage_channel_first_sleep():
    # first sleep time should be `refresh_interval` seconds after client init
    import time
    from collections import namedtuple

    params = namedtuple("params", ["refresh_interval", "wait_time", "expected_sleep"])
    test_params = [
        params(refresh_interval=0, wait_time=0, expected_sleep=0),
        params(refresh_interval=0, wait_time=1, expected_sleep=0),
        params(refresh_interval=10, wait_time=0, expected_sleep=10),
        params(refresh_interval=10, wait_time=5, expected_sleep=5),
        params(refresh_interval=10, wait_time=10, expected_sleep=0),
        params(refresh_interval=10, wait_time=15, expected_sleep=0),
    ]
    with mock.patch.object(time, "time") as time:
        time.return_value = 0
        for refresh_interval, wait_time, expected_sleep in test_params:
            with mock.patch.object(asyncio, "sleep") as sleep:
                sleep.side_effect = asyncio.CancelledError
                try:
                    client = _make_one(project="project-id")
                    client._channel_init_time = -wait_time
                    await client._manage_channel(0, refresh_interval)
                except asyncio.CancelledError:
                    pass
                sleep.assert_called_once()
                call_time = sleep.call_args[0][0]
                assert abs(call_time - expected_sleep) < 0.1, f"params={params}"
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
        with mock.patch.object(
            PooledBigtableGrpcAsyncIOTransport, "create_channel"
        ) as create_channel:
            create_channel.return_value = new_channel
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
                        old_channel = client.transport.channel_pool[channel_idx]
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
                        await client._manage_channel(0, 0)
                    except asyncio.CancelledError:
                        pass
                    ping_and_warm.assert_called_once_with(new_channel)
    await client.close()


@pytest.mark.asyncio
async def test__manage_channel_sleeps():
    # make sure that sleeps work as expected
    from collections import namedtuple
    import time

    params = namedtuple("params", ["refresh_interval", "num_cycles", "expected_sleep"])
    test_params = [
        params(refresh_interval=None, num_cycles=1, expected_sleep=60 * 45),
        params(refresh_interval=10, num_cycles=10, expected_sleep=100),
        params(refresh_interval=10, num_cycles=1, expected_sleep=10),
    ]
    channel_idx = 1
    with mock.patch.object(time, "time") as time:
        time.return_value = 0
        for refresh_interval, num_cycles, expected_sleep in test_params:
            with mock.patch.object(asyncio, "sleep") as sleep:
                sleep.side_effect = [None for i in range(num_cycles - 1)] + [
                    asyncio.CancelledError
                ]
                try:
                    client = _make_one(project="project-id")
                    if refresh_interval is not None:
                        await client._manage_channel(channel_idx, refresh_interval)
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
async def test__manage_channel_refresh():
    # make sure that channels are properly refreshed
    from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
        PooledBigtableGrpcAsyncIOTransport,
    )

    expected_grace = 9
    expected_refresh = 0.5
    channel_idx = 1
    new_channel = grpc.aio.insecure_channel("localhost:8080")

    for num_cycles in [0, 1, 10, 100]:
        with mock.patch.object(
            PooledBigtableGrpcAsyncIOTransport, "replace_channel"
        ) as replace_channel:
            with mock.patch.object(asyncio, "sleep") as sleep:
                sleep.side_effect = [None for i in range(num_cycles)] + [
                    asyncio.CancelledError
                ]
                client = _make_one(project="project-id")
                with mock.patch.object(
                    PooledBigtableGrpcAsyncIOTransport, "create_channel"
                ) as create_channel:
                    create_channel.return_value = new_channel
                    try:
                        await client._manage_channel(
                            channel_idx,
                            refresh_interval=expected_refresh,
                            grace_period=expected_grace,
                        )
                    except asyncio.CancelledError:
                        pass
                    assert sleep.call_count == num_cycles + 1
                    assert create_channel.call_count == num_cycles
                    assert replace_channel.call_count == num_cycles
                    for call in replace_channel.call_args_list:
                        assert call[0][0] == channel_idx
                        assert call[0][1] == expected_grace
                        assert call[0][2] == new_channel
                await client.close()


@pytest.mark.asyncio
async def test_register_instance_ping_and_warm():
    # should ping and warm each new instance
    pool_size = 7
    client = _make_one(project="project-id", pool_size=pool_size)
    assert len(client._channel_refresh_tasks) == pool_size
    assert not client._active_instances
    # next calls should trigger ping and warm
    with mock.patch.object(type(_make_one()), "_ping_and_warm_instances") as ping_mock:
        # new instance should trigger ping and warm
        await client.register_instance("instance-1")
        assert ping_mock.call_count == pool_size
        await client.register_instance("instance-2")
        assert ping_mock.call_count == pool_size * 2
        # duplcate instances should not trigger ping and warm
        await client.register_instance("instance-2")
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
    expected_metadata = [("a", "b")]
    table = client.get_table(
        expected_instance_id,
        expected_table_id,
        expected_app_profile_id,
        expected_metadata,
    )
    await asyncio.sleep(0)
    assert isinstance(table, Table)
    assert table.table_id == expected_table_id
    assert table.instance == expected_instance_id
    assert table.app_profile_id == expected_app_profile_id
    assert table.metadata == expected_metadata
    assert table.client is client
    full_instance_name = client.instance_path(client.project, expected_instance_id)
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
    with mock.patch.object(asyncio, "wait_for") as wait_for_mock:
        await client.close(timeout=expected_timeout)
        wait_for_mock.assert_called_once()
        wait_for_mock.assert_awaited()
        assert wait_for_mock.call_args[1]["timeout"] == expected_timeout


@pytest.mark.asyncio
async def test___del__():
    # no warnings on __del__ after close
    pool_size = 7
    client = _make_one(project="project-id", pool_size=pool_size)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test___del____no_close():
    import warnings

    # if client is garbage collected before being closed, it should raise a warning
    pool_size = 7
    client = _make_one(project="project-id", pool_size=pool_size)
    # replace tasks with mocks
    await client.close()
    client._channel_refresh_tasks = [mock.Mock() for i in range(pool_size)]
    assert len(client._channel_refresh_tasks) == pool_size
    with pytest.warns(UserWarning) as warnings:
        client.__del__()
        assert len(warnings) == 1
        assert "Please call the close() method" in str(warnings[0].message)
        for i in range(pool_size):
            assert client._channel_refresh_tasks[i].cancel.call_count == 1


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

    with pytest.raises(RuntimeError) as err:
        BigtableDataClient(project="project-id")
    assert "event loop" in str(err.value)


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
    expected_metadata = [("a", "b")]
    client = BigtableDataClient()
    assert not client._active_instances

    table = Table(
        client,
        expected_instance_id,
        expected_table_id,
        expected_app_profile_id,
        expected_metadata,
    )
    await asyncio.sleep(0)
    assert table.table_id == expected_table_id
    assert table.instance == expected_instance_id
    assert table.app_profile_id == expected_app_profile_id
    assert table.metadata == expected_metadata
    assert table.client is client
    full_instance_name = client.instance_path(client.project, expected_instance_id)
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
    with pytest.raises(RuntimeError) as err:
        Table(client, "instance-id", "table-id")
    assert "event loop" in str(err.value)
