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

from tests.unit.test_base_client import TestClientConstants


class TestClient(unittest.TestCase, TestClientConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_constructor_defaults(self):
        from google.cloud.bigtable.base_client import _CLIENT_INFO
        from google.cloud.bigtable.base_client import DATA_SCOPE

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
        self.assertIsNone(client._emulator_channel)
        self.assertEqual(client.SCOPE, (DATA_SCOPE,))

    def test_instance_admin_client_not_initialized_no_admin_flag(self):
        credentials = _make_credentials()
        client = self._make_one(project=self.PROJECT, credentials=credentials)

        with self.assertRaises(ValueError):
            client.instance_admin_client()

    def test_instance_admin_client_not_initialized_w_admin_flag(self):
        from google.cloud.bigtable.base_client import _CLIENT_INFO
        from google.cloud.bigtable_admin_v2 import BigtableInstanceAdminClient

        credentials = _make_credentials()
        client = self._make_one(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        instance_admin_client = client.instance_admin_client
        self.assertIsInstance(instance_admin_client, BigtableInstanceAdminClient)
        self.assertIs(instance_admin_client._client_info, _CLIENT_INFO)
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
        self.assertIs(instance_admin_client._client_info, client_info)
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

        patch = mock.patch("google.cloud.bigtable_admin_v2.BigtableInstanceAdminClient")
        with patch as mocked:
            instance_admin_client = client.instance_admin_client

        self.assertIs(instance_admin_client, mocked.return_value)
        self.assertIs(client._instance_admin_client, instance_admin_client)
        mocked.assert_called_once_with(
            client_info=client._client_info,
            credentials=mock.ANY,  # added scopes
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
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable.instance import Instance

        FAILED_LOCATION = "FAILED"
        INSTANCE_ID1 = "instance-id1"
        INSTANCE_ID2 = "instance-id2"
        INSTANCE_NAME1 = "projects/" + self.PROJECT + "/instances/" + INSTANCE_ID1
        INSTANCE_NAME2 = "projects/" + self.PROJECT + "/instances/" + INSTANCE_ID2

        credentials = _make_credentials()
        api = bigtable_instance_admin_client.BigtableInstanceAdminClient(mock.Mock())
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
        bigtable_instance_stub = client.instance_admin_client.transport
        bigtable_instance_stub.list_instances.side_effect = [response_pb]

        # Perform the method and check the result.
        instances, failed_locations = client.list_instances()

        instance_1, instance_2 = instances

        self.assertIsInstance(instance_1, Instance)
        self.assertEqual(instance_1.name, INSTANCE_NAME1)
        self.assertTrue(instance_1._client is client)

        self.assertIsInstance(instance_2, Instance)
        self.assertEqual(instance_2.name, INSTANCE_NAME2)
        self.assertTrue(instance_2._client is client)

        self.assertEqual(failed_locations, [FAILED_LOCATION])

    def test_list_clusters(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable.instance import Cluster

        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            mock.Mock()
        )
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
        instance_stub = client._instance_admin_client.transport
        instance_stub.list_clusters.side_effect = [response_pb]

        # Perform the method and check the result.
        clusters, failed_locations = client.list_clusters()

        cluster_1, cluster_2, cluster_3 = clusters

        self.assertIsInstance(cluster_1, Cluster)
        self.assertEqual(cluster_1.name, cluster_name1)
        self.assertEqual(cluster_1._instance.instance_id, INSTANCE_ID1)

        self.assertIsInstance(cluster_2, Cluster)
        self.assertEqual(cluster_2.name, cluster_name2)
        self.assertEqual(cluster_2._instance.instance_id, INSTANCE_ID2)

        self.assertIsInstance(cluster_3, Cluster)
        self.assertEqual(cluster_3.name, cluster_name3)
        self.assertEqual(cluster_3._instance.instance_id, INSTANCE_ID2)

        self.assertEqual(failed_locations, [failed_location])
