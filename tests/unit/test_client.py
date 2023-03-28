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


import pytest
import unittest
import grpc
import asyncio
# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    import mock


class TestBigtableDataClient(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        pass

    def test_channel_pool_creation(self):
        pool_size = 14
        with mock.patch.object(type(self._make_one().transport), "create_channel") as create_channel:
            client = self._make_one(project="project-id", pool_size=pool_size)
            self.assertEqual(create_channel.call_count, pool_size)
        # channels should be unique
        client = self._make_one(project="project-id", pool_size=pool_size)
        pool_list = list(client.transport.channel_pool)
        pool_set = set(client.transport.channel_pool)
        self.assertEqual(len(pool_list), len(pool_set))


    def test_channel_pool_rotation(self):
        pool_size = 7
        client = self._make_one(project="project-id", pool_size=pool_size)
        self.assertEqual(len(client.transport.channel_pool), pool_size)

        with mock.patch.object(type(client.transport), "next_channel") as next_channel:
            with mock.patch.object(type(client.transport.channel_pool[0]), "unary_unary") as unary_unary:
                # calling an rpc `pool_size` times should use a different channel each time
                for i in range(pool_size):
                    channel_1 = client.transport.channel_pool[client.transport._next_idx]
                    next_channel.return_value = channel_1
                    client.transport.ping_and_warm()
                    self.assertEqual(next_channel.call_count, i + 1)
                    channel_1.unary_unary.assert_called_once()


    @pytest.mark.asyncio
    async def test_channel_pool_replace(self):
        pool_size = 7
        client = self._make_one(project="project-id", pool_size=pool_size)
        for replace_idx in range(pool_size):
            start_pool = [channel for channel in client.transport.channel_pool]
            grace_period = 9
            with mock.patch.object(type(client.transport.channel_pool[0]), "close") as close:
                new_channel = grpc.aio.insecure_channel("localhost:8080")
                await client.transport.replace_channel(replace_idx, grace=grace_period, new_channel=new_channel)
                close.assert_called_once_with(grace=grace_period)
                close.assert_awaited_once()
            self.assertEqual(client.transport.channel_pool[replace_idx], new_channel)
            for i in range(pool_size):
                if i != replace_idx:
                    self.assertEqual(client.transport.channel_pool[i], start_pool[i])
                else:
                    self.assertNotEqual(client.transport.channel_pool[i], start_pool[i])

    def test_start_background_channel_refresh_sync(self):
        # should raise RuntimeError if called in a sync context
        client = self._make_one(project="project-id")
        with self.assertRaises(RuntimeError):
            client.start_background_channel_refresh()

    @pytest.mark.asyncio
    async def test_start_background_channel_refresh_tasks_exist(self):
        # if tasks exist, should do nothing
        client = self._make_one(project="project-id")
        client._channel_refresh_tasks = [object()]
        with mock.patch.object(asyncio, "create_task") as create_task:
            client.start_background_channel_refresh()
            create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_background_channel_refresh(self):
        # should create background tasks for each channel
        for pool_size in [1, 3, 7]:
            client = self._make_one(project="project-id", pool_size=pool_size)
            ping_and_warm = AsyncMock()
            client._ping_and_warm_instances = ping_and_warm
            client.start_background_channel_refresh()
            self.assertEqual(len(client._channel_refresh_tasks), pool_size)
            for task in client._channel_refresh_tasks:
                self.assertIsInstance(task, asyncio.Task)
            await asyncio.gather(*client._channel_refresh_tasks)
            await asyncio.sleep(0.1)
            self.assertEqual(ping_and_warm.call_count, pool_size)
            for channel in client.transport.channel_pool:
                ping_and_warm.assert_any_call(channel)

    @pytest.mark.asyncio
    async def test__ping_and_warm_instances(self):
        # test with no instances
        gather = AsyncMock()
        asyncio.gather = gather
        client = self._make_one(project="project-id", pool_size=1)
        channel = client.transport.channel_pool[0]
        await client._ping_and_warm_instances(channel)
        gather.assert_called_once()
        gather.assert_awaited_once()
        self.assertFalse(gather.call_args.args)
        self.assertEqual(gather.call_args.kwargs, {"return_exceptions": True})
        # test with instances
        client._active_instances = ["instance-1", "instance-2", "instance-3", "instance-4"]
        gather = AsyncMock()
        asyncio.gather = gather
        await client._ping_and_warm_instances(channel)
        gather.assert_called_once()
        gather.assert_awaited_once()
        self.assertEqual(len(gather.call_args.args), 4)
        self.assertEqual(gather.call_args.kwargs, {"return_exceptions": True})
        for idx, call in enumerate(gather.call_args.args):
            self.assertIsInstance(call, grpc.aio.UnaryUnaryCall)
            call._request["name"] = client._active_instances[idx]

    @pytest.mark.asyncio
    async def test__manage_channel_first_sleep(self):
        # first sleep time should be `refresh_interval` seconds after client init
        import time
        from collections import namedtuple
        params = namedtuple('params', ['refresh_interval', 'wait_time', 'expected_sleep'])
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
                        client = self._make_one(project="project-id")
                        client._channel_init_time = -wait_time
                        await client._manage_channel(0, refresh_interval)
                    except asyncio.CancelledError:
                        pass
                    sleep.assert_called_once()
                    call_time = sleep.call_args[0][0]
                    self.assertAlmostEqual(call_time, expected_sleep, delta=0.1, 
                        msg=f"params={params}")

    @pytest.mark.asyncio
    async def test__manage_channel_ping_and_warm(self):
        # should ping an warm all new channels, and old channels if sleeping
        client = self._make_one(project="project-id")
        new_channel = grpc.aio.insecure_channel("localhost:8080")
        with mock.patch.object(asyncio, "sleep") as sleep:
            with mock.patch.object(type(self._make_one().transport), "create_channel") as create_channel:
                create_channel.return_value = new_channel
                with mock.patch.object(type(self._make_one().transport), "replace_channel") as replace_channel:
                    replace_channel.side_effect = asyncio.CancelledError
                    # should ping and warm old channel then new if sleep > 0
                    with mock.patch.object(type(self._make_one()), "_ping_and_warm_instances") as ping_and_warm:
                        try:
                            channel_idx = 2
                            old_channel = client.transport.channel_pool[channel_idx]
                            await client._manage_channel(channel_idx, 10)
                        except asyncio.CancelledError:
                            pass
                        self.assertEqual(ping_and_warm.call_count, 2)
                        self.assertNotEqual(old_channel, new_channel)
                        called_with = [call[0][0] for call in ping_and_warm.call_args_list]
                        self.assertIn(old_channel, called_with)
                        self.assertIn(new_channel, called_with)
                    # should ping and warm instantly new channel only if not sleeping
                    with mock.patch.object(type(self._make_one()), "_ping_and_warm_instances") as ping_and_warm:
                        try:
                            await client._manage_channel(0,  0)
                        except asyncio.CancelledError:
                            pass
                        ping_and_warm.assert_called_once_with(new_channel)

    @pytest.mark.asyncio
    async def test__manage_channel_sleeps(self):
        # make sure that sleeps work as expected
        from collections import namedtuple
        import time
        params = namedtuple('params', ['refresh_interval',  'num_cycles', 'expected_sleep'])
        test_params = [
            params(refresh_interval=None, num_cycles=1, expected_sleep=60*45),
            params(refresh_interval=10, num_cycles=10, expected_sleep=100),
            params(refresh_interval=10, num_cycles=1, expected_sleep=10),
        ]
        channel_idx = 1
        with mock.patch.object(time, "time") as time:
            time.return_value = 0
            for refresh_interval, num_cycles, expected_sleep in test_params:
                with mock.patch.object(asyncio, "sleep") as sleep:
                    sleep.side_effect = [None for i in range(num_cycles-1)] + [asyncio.CancelledError]
                    try:
                        client = self._make_one(project="project-id")
                        if refresh_interval is not None:
                            await client._manage_channel(channel_idx, refresh_interval)
                        else:
                            await client._manage_channel(channel_idx)
                    except asyncio.CancelledError:
                        pass
                    self.assertEqual(sleep.call_count, num_cycles)
                    total_sleep = sum([call[0][0] for call in sleep.call_args_list])
                    self.assertAlmostEqual(total_sleep, expected_sleep, delta=0.1, 
                        msg=f"refresh_interval={refresh_interval}, num_cycles={num_cycles}, expected_sleep={expected_sleep}")

    @pytest.mark.asyncio
    async def test__manage_channel_refresh(self):
        # make sure that channels are properly refreshed
        from collections import namedtuple
        import time
        expected_grace = 9
        expected_refresh = 0.5
        channel_idx = 1
        new_channel = grpc.aio.insecure_channel("localhost:8080")

        for num_cycles in [0, 1, 10, 100]:
            with mock.patch.object(type(self._make_one().transport), "replace_channel") as replace_channel:
                with mock.patch.object(asyncio, "sleep") as sleep:
                    sleep.side_effect = [None for i in range(num_cycles)] + [asyncio.CancelledError]
                    client = self._make_one(project="project-id")
                    with mock.patch.object(type(self._make_one().transport), "create_channel") as create_channel:
                        create_channel.return_value = new_channel
                        try:
                            await client._manage_channel(channel_idx, refresh_interval=expected_refresh, grace_period=expected_grace)
                        except asyncio.CancelledError:
                            pass
                        self.assertEqual(sleep.call_count, num_cycles+1)
                        self.assertEqual(create_channel.call_count, num_cycles)
                        self.assertEqual(replace_channel.call_count, num_cycles)
                        for call in replace_channel.call_args_list:
                            self.assertEqual(call[0][0], channel_idx)
                            self.assertEqual(call[0][1], expected_grace)
                            self.assertEqual(call[0][2], new_channel)

    @pytest.mark.asyncio
    async def test_register_instance(self):
        # create the client without calling start_background_channel_refresh
        with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
            get_event_loop.side_effect = RuntimeError("no event loop")
            client = self._make_one(project="project-id")
        self.assertFalse(client._channel_refresh_tasks)
        # first call should start background refresh
        self.assertEqual(client._active_instances, set())
        await client.register_instance("instance-1")
        self.assertEqual(len(client._active_instances), 1)
        self.assertEqual(client._active_instances, {"projects/project-id/instances/instance-1"})
        self.assertTrue(client._channel_refresh_tasks)
        # next call should not
        with mock.patch.object(type(self._make_one()), "start_background_channel_refresh") as refresh_mock:
            await client.register_instance("instance-2")
            self.assertEqual(len(client._active_instances), 2)
            self.assertEqual(client._active_instances, {"projects/project-id/instances/instance-1", "projects/project-id/instances/instance-2"})
            refresh_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_instance_ping_and_warm(self):
        # should ping and warm each new instance
        pool_size = 7
        with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
            get_event_loop.side_effect = RuntimeError("no event loop")
            client = self._make_one(project="project-id", pool_size=pool_size)
        # first call should start background refresh
        self.assertFalse(client._channel_refresh_tasks)
        await client.register_instance("instance-1")
        self.assertEqual(len(client._channel_refresh_tasks), pool_size)
        # next calls should trigger ping and warm
        with mock.patch.object(type(self._make_one()), "_ping_and_warm_instances") as ping_mock:
            await client.register_instance("instance-2")
            self.assertEqual(ping_mock.call_count, pool_size)
            await client.register_instance("instance-3")
            self.assertEqual(ping_mock.call_count, pool_size * 2)
            # duplcate instances should not trigger ping and warm
            await client.register_instance("instance-3")
            self.assertEqual(ping_mock.call_count, pool_size * 2)

    @pytest.mark.asyncio
    async def test_remove_instance_registration(self):
        client = self._make_one(project="project-id")
        await client.register_instance("instance-1")
        await client.register_instance("instance-2")
        self.assertEqual(len(client._active_instances), 2)
        success = await client.remove_instance_registration("instance-1")
        self.assertTrue(success)
        self.assertEqual(len(client._active_instances), 1)
        self.assertEqual(client._active_instances, {"projects/project-id/instances/instance-2"})
        success = await client.remove_instance_registration("nonexistant")
        self.assertFalse(success)
        self.assertEqual(len(client._active_instances), 1)

    @pytest.mark.asyncio
    async def test_get_table(self):
        from google.cloud.bigtable.client import Table
        client = self._make_one(project="project-id")
        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        with mock.patch.object(type(self._make_one()), "register_instance") as register_instance:
            table = client.get_table(expected_instance_id, expected_table_id, expected_app_profile_id)
            register_instance.assert_called_once_with(expected_instance_id)
        self.assertIsInstance(table, Table)
        self.assertEqual(table.table_id, expected_table_id)
        self.assertEqual(table.instance, expected_instance_id)
        self.assertEqual(table.app_profile_id, expected_app_profile_id)
        self.assertIs(table.client, client)

    def test_get_table_no_loop(self):
        client = self._make_one(project="project-id")
        with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
            get_event_loop.side_effect = RuntimeError("no event loop")
            client.get_table("instance-id", "table-id")
            with self.assertWarns(Warning) as cm:
                client.get_table("instance-id", "table-id")
            self.assertIn("Table should be created in an asyncio event loop", str(cm.warning))


class TestTable(unittest.TestCase):


    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient().get_table(*args, **kwargs)

    def test_ctor(self):
        pass
