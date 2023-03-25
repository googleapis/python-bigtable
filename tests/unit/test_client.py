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

    def test_start_background_channel_refresh(self):
        pass

    def test__ping_and_warm_instances(self):
        pass

    def test__manage_channel(self):
        pass

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
            table = await client.get_table(expected_instance_id, expected_table_id, expected_app_profile_id)
            register_instance.assert_called_once_with(expected_instance_id)
            register_instance.assert_awaited_once()
        self.assertIsInstance(table, Table)
        self.assertEqual(table.table_id, expected_table_id)
        self.assertEqual(table.instance, expected_instance_id)
        self.assertEqual(table.app_profile_id, expected_app_profile_id)
        self.assertIs(table.client, client)


class TestTable(unittest.TestCase):


    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient().get_table(*args, **kwargs)

    def test_ctor(self):
        pass
