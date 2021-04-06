# Copyright 2015 Google LLC
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

import mock

from ._testing import _make_credentials


class Test__create_gapic_client(unittest.TestCase):
    def _invoke_client_factory(self, client_class, **kw):
        from google.cloud.bigtable.client import _create_gapic_client

        return _create_gapic_client(client_class, **kw)

    def test_wo_emulator(self):
        client_class = mock.Mock()
        credentials = _make_credentials()
        client = _Client(credentials)
        client_info = client._client_info = mock.Mock()
        transport = mock.Mock()

        result = self._invoke_client_factory(client_class, transport=transport)(client)

        self.assertIs(result, client_class.return_value)
        client_class.assert_called_once_with(
            credentials=None,
            client_info=client_info,
            client_options=None,
            transport=transport,
        )

    def test_wo_emulator_w_client_options(self):
        client_class = mock.Mock()
        credentials = _make_credentials()
        client = _Client(credentials)
        client_info = client._client_info = mock.Mock()
        client_options = mock.Mock()
        transport = mock.Mock()

        result = self._invoke_client_factory(
            client_class, client_options=client_options, transport=transport
        )(client)

        self.assertIs(result, client_class.return_value)
        client_class.assert_called_once_with(
            credentials=None,
            client_info=client_info,
            client_options=client_options,
            transport=transport,
        )

    def test_w_emulator(self):
        client_class = mock.Mock()
        emulator_host = emulator_channel = object()
        credentials = _make_credentials()
        client_options = mock.Mock()
        transport = mock.Mock()

        client = _Client(
            credentials, emulator_host=emulator_host, emulator_channel=emulator_channel
        )
        client_info = client._client_info = mock.Mock()
        result = self._invoke_client_factory(
            client_class, client_options=client_options, transport=transport
        )(client)

        self.assertIs(result, client_class.return_value)
        client_class.assert_called_once_with(
            credentials=None,
            client_info=client_info,
            client_options=client_options,
            transport=transport,
        )


class _Client(object):
    def __init__(self, credentials, emulator_host=None, emulator_channel=None):
        self._credentials = credentials
        self._emulator_host = emulator_host
        self._emulator_channel = emulator_channel


class TestClient(unittest.TestCase):

    PROJECT = "PROJECT"
    INSTANCE_ID = "instance-id"
    DISPLAY_NAME = "display-name"
    USER_AGENT = "you-sir-age-int"

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @mock.patch("os.environ", {})
    def test_constructor_defaults(self):
        from google.cloud.bigtable.client import _CLIENT_INFO
        from google.cloud.bigtable.client import DATA_SCOPE

        credentials = _make_credentials()

        with mock.patch("google.auth.default") as mocked:
            mocked.return_value = credentials, self.PROJECT
            client = self._make_one()

        self.assertEqual(client.project, self.PROJECT)
        self.assertIs(client._credentials, credentials.with_scopes.return_value)
        self.assertFalse(client._read_only)
        self.assertFalse(client._admin)
        self.assertIs(client._client_info, _CLIENT_INFO)
        self.assertIsNone(client._channel)
        self.assertIsNone(client._emulator_host)
        self.assertEqual(client.SCOPE, (DATA_SCOPE,))

    def test_constructor_explicit(self):
        import warnings
        from google.cloud.bigtable.client import ADMIN_SCOPE
        from google.cloud.bigtable.client import DATA_SCOPE

        credentials = _make_credentials()
        client_info = mock.Mock()

        with warnings.catch_warnings(record=True) as warned:
            client = self._make_one(
                project=self.PROJECT,
                credentials=credentials,
                read_only=False,
                admin=True,
                client_info=client_info,
                channel=mock.sentinel.channel,
            )

        self.assertEqual(len(warned), 1)

        self.assertEqual(client.project, self.PROJECT)
        self.assertIs(client._credentials, credentials.with_scopes.return_value)
        self.assertFalse(client._read_only)
        self.assertTrue(client._admin)
        self.assertIs(client._client_info, client_info)
        self.assertIs(client._channel, mock.sentinel.channel)
        self.assertEqual(client.SCOPE, (DATA_SCOPE, ADMIN_SCOPE))

    def test_constructor_both_admin_and_read_only(self):
        credentials = _make_credentials()
        with self.assertRaises(ValueError):
            self._make_one(
                project=self.PROJECT,
                credentials=credentials,
                admin=True,
                read_only=True,
            )

    def test_constructor_with_emulator_host(self):
        from google.cloud.environment_vars import BIGTABLE_EMULATOR

        credentials = _make_credentials()
        emulator_host = "localhost:8081"
        with mock.patch("os.environ", {BIGTABLE_EMULATOR: emulator_host}):
            with mock.patch("grpc.secure_channel") as factory:
                client = self._make_one(project=self.PROJECT, credentials=credentials)
                # don't test local_composite_credentials
                client._local_composite_credentials = lambda: credentials
                # channels are formed when needed, so access a client
                # create a gapic channel
                client.table_data_client

        self.assertEqual(client._emulator_host, emulator_host)
        options = {
            "grpc.max_send_message_length": -1,
            "grpc.max_receive_message_length": -1,
            "grpc.keepalive_time_ms": 30000,
            "grpc.keepalive_timeout_ms": 10000,
        }.items()
        factory.assert_called_once_with(emulator_host, credentials, options=options)

    def test__get_scopes_default(self):
        from google.cloud.bigtable.client import DATA_SCOPE

        client = self._make_one(project=self.PROJECT, credentials=_make_credentials())
        self.assertEqual(client._get_scopes(), (DATA_SCOPE,))

    def test__get_scopes_admin(self):
        from google.cloud.bigtable.client import ADMIN_SCOPE
        from google.cloud.bigtable.client import DATA_SCOPE

        client = self._make_one(
            project=self.PROJECT, credentials=_make_credentials(), admin=True
        )
        expected_scopes = (DATA_SCOPE, ADMIN_SCOPE)
        self.assertEqual(client._get_scopes(), expected_scopes)

    def test__get_scopes_read_only(self):
        from google.cloud.bigtable.client import READ_ONLY_SCOPE

        client = self._make_one(
            project=self.PROJECT, credentials=_make_credentials(), read_only=True
        )
        self.assertEqual(client._get_scopes(), (READ_ONLY_SCOPE,))

    def test_project_path_property(self):
        credentials = _make_credentials()
        project = "PROJECT"
        client = self._make_one(project=project, credentials=credentials, admin=True)
        project_name = "projects/" + project
        self.assertEqual(client.project_path, project_name)

    def test_table_data_client_not_initialized(self):
        from google.cloud.bigtable.client import _CLIENT_INFO
        from google.cloud.bigtable_v2 import BigtableClient

        credentials = _make_credentials()
        client = self._make_one(project=self.PROJECT, credentials=credentials)

        table_data_client = client.table_data_client
        self.assertIsInstance(table_data_client, BigtableClient)
        self.assertIs(client._client_info, _CLIENT_INFO)
        self.assertIs(client._table_data_client, table_data_client)

    def test_table_data_client_not_initialized_w_client_info(self):
        from google.cloud.bigtable_v2 import BigtableClient

        credentials = _make_credentials()
        client_info = mock.Mock()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, client_info=client_info
        )

        table_data_client = client.table_data_client
        self.assertIsInstance(table_data_client, BigtableClient)
        self.assertIs(client._client_info, client_info)
        self.assertIs(client._table_data_client, table_data_client)

    def test_table_data_client_not_initialized_w_client_options(self):
        from google.api_core.client_options import ClientOptions

        credentials = _make_credentials()
        client_options = ClientOptions(
            quota_project_id="QUOTA-PROJECT", api_endpoint="xyz"
        )
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, client_options=client_options
        )

        patch = mock.patch("google.cloud.bigtable_v2.BigtableClient")
        with patch as mocked:
            table_data_client = client.table_data_client

        self.assertIs(table_data_client, mocked.return_value)
        self.assertIs(client._table_data_client, table_data_client)

        mocked.assert_called_once_with(
            client_info=client._client_info,
            credentials=None,
            transport=mock.ANY,
            client_options=client_options,
        )

    def test_table_data_client_initialized(self):
        credentials = _make_credentials()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        already = client._table_data_client = object()
        self.assertIs(client.table_data_client, already)

    def test_table_admin_client_not_initialized_no_admin_flag(self):
        credentials = _make_credentials()
        client = self._make_one(project=self.PROJECT, credentials=credentials)

        with self.assertRaises(ValueError):
            client.table_admin_client()

    def test_table_admin_client_not_initialized_w_admin_flag(self):
        from google.cloud.bigtable.client import _CLIENT_INFO
        from google.cloud.bigtable_admin_v2 import BigtableTableAdminClient

        credentials = _make_credentials()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        table_admin_client = client.table_admin_client
        self.assertIsInstance(table_admin_client, BigtableTableAdminClient)
        self.assertIs(client._client_info, _CLIENT_INFO)
        self.assertIs(client._table_admin_client, table_admin_client)

    def test_table_admin_client_not_initialized_w_client_info(self):
        from google.cloud.bigtable_admin_v2 import BigtableTableAdminClient

        credentials = _make_credentials()
        client_info = mock.Mock()
        client = self._make_one(
            project=self.PROJECT,
            credentials=credentials,
            admin=True,
            client_info=client_info,
        )

        table_admin_client = client.table_admin_client
        self.assertIsInstance(table_admin_client, BigtableTableAdminClient)
        self.assertIs(client._client_info, client_info)
        self.assertIs(client._table_admin_client, table_admin_client)

    def test_table_admin_client_not_initialized_w_client_options(self):
        credentials = _make_credentials()
        admin_client_options = mock.Mock()
        client = self._make_one(
            project=self.PROJECT,
            credentials=credentials,
            admin=True,
            admin_client_options=admin_client_options,
        )

        client._create_gapic_client_channel = mock.Mock()
        patch = mock.patch("google.cloud.bigtable_admin_v2.BigtableTableAdminClient")
        with patch as mocked:
            table_admin_client = client.table_admin_client

        self.assertIs(table_admin_client, mocked.return_value)
        self.assertIs(client._table_admin_client, table_admin_client)
        mocked.assert_called_once_with(
            client_info=client._client_info,
            credentials=None,
            transport=mock.ANY,
            client_options=admin_client_options,
        )

    def test_table_admin_client_initialized(self):
        credentials = _make_credentials()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        already = client._table_admin_client = object()
        self.assertIs(client.table_admin_client, already)

    def test_instance_admin_client_not_initialized_no_admin_flag(self):
        credentials = _make_credentials()
        client = self._make_one(project=self.PROJECT, credentials=credentials)

        with self.assertRaises(ValueError):
            client.instance_admin_client()

    def test_instance_admin_client_not_initialized_w_admin_flag(self):
        from google.cloud.bigtable.client import _CLIENT_INFO
        from google.cloud.bigtable_admin_v2 import BigtableInstanceAdminClient

        credentials = _make_credentials()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        instance_admin_client = client.instance_admin_client
        self.assertIsInstance(instance_admin_client, BigtableInstanceAdminClient)
        self.assertIs(client._client_info, _CLIENT_INFO)
        self.assertIs(client._instance_admin_client, instance_admin_client)

    def test_instance_admin_client_not_initialized_w_client_info(self):
        from google.cloud.bigtable_admin_v2 import BigtableInstanceAdminClient

        credentials = _make_credentials()
        client_info = mock.Mock()
        client = self._make_one(
            project=self.PROJECT,
            credentials=credentials,
            admin=True,
            client_info=client_info,
        )

        instance_admin_client = client.instance_admin_client
        self.assertIsInstance(instance_admin_client, BigtableInstanceAdminClient)
        self.assertIs(client._client_info, client_info)
        self.assertIs(client._instance_admin_client, instance_admin_client)

    def test_instance_admin_client_not_initialized_w_client_options(self):
        credentials = _make_credentials()
        admin_client_options = mock.Mock()
        client = self._make_one(
            project=self.PROJECT,
            credentials=credentials,
            admin=True,
            admin_client_options=admin_client_options,
        )

        client._create_gapic_client_channel = mock.Mock()
        patch = mock.patch("google.cloud.bigtable_admin_v2.BigtableInstanceAdminClient")
        with patch as mocked:
            instance_admin_client = client.instance_admin_client

        self.assertIs(instance_admin_client, mocked.return_value)
        self.assertIs(client._instance_admin_client, instance_admin_client)
        mocked.assert_called_once_with(
            client_info=client._client_info,
            credentials=None,
            transport=mock.ANY,
            client_options=admin_client_options,
        )

    def test_instance_admin_client_initialized(self):
        credentials = _make_credentials()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        already = client._instance_admin_client = object()
        self.assertIs(client.instance_admin_client, already)

    def test_instance_factory_defaults(self):
        from google.cloud.bigtable.instance import Instance

        PROJECT = "PROJECT"
        INSTANCE_ID = "instance-id"
        credentials = _make_credentials()
        client = self._make_one(project=PROJECT, credentials=credentials)

        instance = client.instance(INSTANCE_ID)

        self.assertIsInstance(instance, Instance)
        self.assertEqual(instance.instance_id, INSTANCE_ID)
        self.assertEqual(instance.display_name, INSTANCE_ID)
        self.assertIsNone(instance.type_)
        self.assertIsNone(instance.labels)
        self.assertIs(instance._client, client)

    def test_instance_factory_non_defaults(self):
        from google.cloud.bigtable.instance import Instance
        from google.cloud.bigtable import enums

        PROJECT = "PROJECT"
        INSTANCE_ID = "instance-id"
        DISPLAY_NAME = "display-name"
        instance_type = enums.Instance.Type.DEVELOPMENT
        labels = {"foo": "bar"}
        credentials = _make_credentials()
        client = self._make_one(project=PROJECT, credentials=credentials)

        instance = client.instance(
            INSTANCE_ID,
            display_name=DISPLAY_NAME,
            instance_type=instance_type,
            labels=labels,
        )

        self.assertIsInstance(instance, Instance)
        self.assertEqual(instance.instance_id, INSTANCE_ID)
        self.assertEqual(instance.display_name, DISPLAY_NAME)
        self.assertEqual(instance.type_, instance_type)
        self.assertEqual(instance.labels, labels)
        self.assertIs(instance._client, client)

    def test_list_instances(self):
        from google.cloud.bigtable_admin_v2.types import instance as data_v2_pb2
        from google.cloud.bigtable_admin_v2.types import (
            bigtable_instance_admin as messages_v2_pb2,
        )
        from google.cloud.bigtable_admin_v2.services.bigtable_instance_admin import (
            BigtableInstanceAdminClient,
        )
        from google.cloud.bigtable.instance import Instance

        FAILED_LOCATION = "FAILED"
        INSTANCE_ID1 = "instance-id1"
        INSTANCE_ID2 = "instance-id2"
        INSTANCE_NAME1 = "projects/" + self.PROJECT + "/instances/" + INSTANCE_ID1
        INSTANCE_NAME2 = "projects/" + self.PROJECT + "/instances/" + INSTANCE_ID2

        api = mock.create_autospec(BigtableInstanceAdminClient)
        credentials = _make_credentials()

        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        # Create response_pb
        response_pb = messages_v2_pb2.ListInstancesResponse(
            failed_locations=[FAILED_LOCATION],
            instances=[
                data_v2_pb2.Instance(name=INSTANCE_NAME1, display_name=INSTANCE_NAME1),
                data_v2_pb2.Instance(name=INSTANCE_NAME2, display_name=INSTANCE_NAME2),
            ],
        )

        # Patch the stub used by the API method.
        client._instance_admin_client = api
        instance_stub = client._instance_admin_client

        instance_stub.list_instances.side_effect = [response_pb]

        # Perform the method and check the result.
        instances, failed_locations = client.list_instances()

        instance_1, instance_2 = instances

        self.assertIsInstance(instance_1, Instance)
        self.assertEqual(instance_1.instance_id, INSTANCE_ID1)
        self.assertTrue(instance_1._client is client)

        self.assertIsInstance(instance_2, Instance)
        self.assertEqual(instance_2.instance_id, INSTANCE_ID2)
        self.assertTrue(instance_2._client is client)

        self.assertEqual(failed_locations, [FAILED_LOCATION])

    def test_list_clusters(self):
        from google.cloud.bigtable_admin_v2.services.bigtable_instance_admin import (
            BigtableInstanceAdminClient,
        )
        from google.cloud.bigtable_admin_v2.types import (
            bigtable_instance_admin as messages_v2_pb2,
        )
        from google.cloud.bigtable_admin_v2.types import instance as data_v2_pb2
        from google.cloud.bigtable.instance import Cluster

        instance_api = mock.create_autospec(BigtableInstanceAdminClient)

        credentials = _make_credentials()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        INSTANCE_ID1 = "instance-id1"
        INSTANCE_ID2 = "instance-id2"

        failed_location = "FAILED"
        cluster_id1 = "{}-cluster".format(INSTANCE_ID1)
        cluster_id2 = "{}-cluster-1".format(INSTANCE_ID2)
        cluster_id3 = "{}-cluster-2".format(INSTANCE_ID2)
        cluster_name1 = client.instance_admin_client.cluster_path(
            self.PROJECT, INSTANCE_ID1, cluster_id1
        )
        cluster_name2 = client.instance_admin_client.cluster_path(
            self.PROJECT, INSTANCE_ID2, cluster_id2
        )
        cluster_name3 = client.instance_admin_client.cluster_path(
            self.PROJECT, INSTANCE_ID2, cluster_id3
        )

        # Create response_pb
        response_pb = messages_v2_pb2.ListClustersResponse(
            failed_locations=[failed_location],
            clusters=[
                data_v2_pb2.Cluster(name=cluster_name1),
                data_v2_pb2.Cluster(name=cluster_name2),
                data_v2_pb2.Cluster(name=cluster_name3),
            ],
        )

        # Patch the stub used by the API method.
        client._instance_admin_client = instance_api
        instance_stub = client._instance_admin_client

        instance_stub.list_clusters.side_effect = [response_pb]

        # Perform the method and check the result.
        clusters, failed_locations = client.list_clusters()

        cluster_1, cluster_2, cluster_3 = clusters

        self.assertIsInstance(cluster_1, Cluster)
        self.assertEqual(cluster_1.cluster_id, cluster_id1)
        self.assertEqual(cluster_1._instance.instance_id, INSTANCE_ID1)

        self.assertIsInstance(cluster_2, Cluster)
        self.assertEqual(cluster_2.cluster_id, cluster_id2)
        self.assertEqual(cluster_2._instance.instance_id, INSTANCE_ID2)

        self.assertIsInstance(cluster_3, Cluster)
        self.assertEqual(cluster_3.cluster_id, cluster_id3)
        self.assertEqual(cluster_3._instance.instance_id, INSTANCE_ID2)

        self.assertEqual(failed_locations, [failed_location])
