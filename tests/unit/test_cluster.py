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

from tests.unit.test_base_cluster import (
    _Instance,
    _Client,
    ChannelStub,
    TestClusterConstants,
)


class TestCluster(unittest.TestCase, TestClusterConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.cluster import Cluster

        return Cluster

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor_defaults(self):
        client = _Client(self.PROJECT)
        instance = _Instance(self.INSTANCE_ID, client)

        cluster = self._make_one(self.CLUSTER_ID, instance)
        self.assertEqual(cluster.cluster_id, self.CLUSTER_ID)
        self.assertIs(cluster._instance, instance)
        self.assertIsNone(cluster.location_id)
        self.assertIsNone(cluster.state)
        self.assertIsNone(cluster.serve_nodes)
        self.assertIsNone(cluster.default_storage_type)

    def test_reload(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable.enums import StorageType
        from google.cloud.bigtable.enums import Cluster

        api = bigtable_instance_admin_client.BigtableInstanceAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        STORAGE_TYPE_SSD = StorageType.SSD
        instance = _Instance(self.INSTANCE_ID, client)
        cluster = self._make_one(
            self.CLUSTER_ID,
            instance,
            location_id=self.LOCATION_ID,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=STORAGE_TYPE_SSD,
        )

        # Create response_pb
        LOCATION_ID_FROM_SERVER = "new-location-id"
        STATE = Cluster.State.READY
        SERVE_NODES_FROM_SERVER = 10
        STORAGE_TYPE_FROM_SERVER = StorageType.HDD

        response_pb = data_v2_pb2.Cluster(
            name=cluster.name,
            location=self.LOCATION_PATH + LOCATION_ID_FROM_SERVER,
            state=STATE,
            serve_nodes=SERVE_NODES_FROM_SERVER,
            default_storage_type=STORAGE_TYPE_FROM_SERVER,
        )

        # Patch the stub used by the API method.
        client._instance_admin_client = api
        instance_admin_client = client._instance_admin_client
        instance_stub = instance_admin_client.transport
        instance_stub.get_cluster.side_effect = [response_pb]

        # Create expected_result.
        expected_result = None  # reload() has no return value.

        # Check Cluster optional config values before.
        self.assertEqual(cluster.location_id, self.LOCATION_ID)
        self.assertIsNone(cluster.state)
        self.assertEqual(cluster.serve_nodes, self.SERVE_NODES)
        self.assertEqual(cluster.default_storage_type, STORAGE_TYPE_SSD)

        # Perform the method and check the result.
        result = cluster.reload()
        self.assertEqual(result, expected_result)
        self.assertEqual(cluster.location_id, LOCATION_ID_FROM_SERVER)
        self.assertEqual(cluster.state, STATE)
        self.assertEqual(cluster.serve_nodes, SERVE_NODES_FROM_SERVER)
        self.assertEqual(cluster.default_storage_type, STORAGE_TYPE_FROM_SERVER)

    def test_exists(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable.instance import Instance
        from google.api_core import exceptions

        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            mock.Mock()
        )
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = Instance(self.INSTANCE_ID, client)

        # Create response_pb
        cluster_name = client.instance_admin_client.cluster_path(
            self.PROJECT, self.INSTANCE_ID, self.CLUSTER_ID
        )
        response_pb = data_v2_pb2.Cluster(name=cluster_name)

        # Patch the stub used by the API method.
        client._instance_admin_client = instance_api
        instance_admin_client = client._instance_admin_client
        instance_stub = instance_admin_client.transport
        instance_stub.get_cluster.side_effect = [
            response_pb,
            exceptions.NotFound("testing"),
            exceptions.BadRequest("testing"),
        ]

        # Perform the method and check the result.
        non_existing_cluster_id = "cluster-id-2"
        alt_cluster_1 = self._make_one(self.CLUSTER_ID, instance)
        alt_cluster_2 = self._make_one(non_existing_cluster_id, instance)
        self.assertTrue(alt_cluster_1.exists())
        self.assertFalse(alt_cluster_2.exists())
        with self.assertRaises(exceptions.BadRequest):
            alt_cluster_1.exists()

    def test_create(self):
        import datetime
        from google.api_core import operation
        from google.longrunning import operations_pb2
        from google.protobuf.any_pb2 import Any
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable.instance import Instance
        from google.cloud.bigtable_admin_v2.types import instance_pb2
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as instance_v2_pb2,
        )
        from google.cloud.bigtable.enums import StorageType

        NOW = datetime.datetime.utcnow()
        NOW_PB = _datetime_to_pb_timestamp(NOW)
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        STORAGE_TYPE_SSD = StorageType.SSD
        LOCATION = self.LOCATION_PATH + self.LOCATION_ID
        instance = Instance(self.INSTANCE_ID, client)
        cluster = self._make_one(
            self.CLUSTER_ID,
            instance,
            location_id=self.LOCATION_ID,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=STORAGE_TYPE_SSD,
        )
        expected_request_cluster = instance_pb2.Cluster(
            location=LOCATION,
            serve_nodes=cluster.serve_nodes,
            default_storage_type=cluster.default_storage_type,
        )
        expected_request = instance_v2_pb2.CreateClusterRequest(
            parent=instance.name,
            cluster_id=self.CLUSTER_ID,
            cluster=expected_request_cluster,
        )

        metadata = messages_v2_pb2.CreateClusterMetadata(request_time=NOW_PB)
        type_url = "type.googleapis.com/{}".format(
            messages_v2_pb2.CreateClusterMetadata.DESCRIPTOR.full_name
        )
        response_pb = operations_pb2.Operation(
            name=self.OP_NAME,
            metadata=Any(type_url=type_url, value=metadata.SerializeToString()),
        )

        # Patch the stub used by the API method.
        channel = ChannelStub(responses=[response_pb])
        api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            channel=channel
        )
        client._instance_admin_client = api

        # Perform the method and check the result.
        result = cluster.create()
        actual_request = channel.requests[0][1]

        self.assertEqual(actual_request, expected_request)
        self.assertIsInstance(result, operation.Operation)
        self.assertEqual(result.operation.name, self.OP_NAME)
        self.assertIsInstance(result.metadata, messages_v2_pb2.CreateClusterMetadata)

    def test_update(self):
        import datetime
        from google.api_core import operation
        from google.longrunning import operations_pb2
        from google.protobuf.any_pb2 import Any
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_instance_admin_pb2 as messages_v2_pb2,
        )
        from google.cloud.bigtable_admin_v2.types import instance_pb2
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client
        from google.cloud.bigtable.enums import StorageType

        NOW = datetime.datetime.utcnow()
        NOW_PB = _datetime_to_pb_timestamp(NOW)

        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        STORAGE_TYPE_SSD = StorageType.SSD
        instance = _Instance(self.INSTANCE_ID, client)
        cluster = self._make_one(
            self.CLUSTER_ID,
            instance,
            location_id=self.LOCATION_ID,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=STORAGE_TYPE_SSD,
        )
        # Create expected_request
        expected_request = instance_pb2.Cluster(
            name=cluster.name, serve_nodes=self.SERVE_NODES
        )

        metadata = messages_v2_pb2.UpdateClusterMetadata(request_time=NOW_PB)
        type_url = "type.googleapis.com/{}".format(
            messages_v2_pb2.UpdateClusterMetadata.DESCRIPTOR.full_name
        )
        response_pb = operations_pb2.Operation(
            name=self.OP_NAME,
            metadata=Any(type_url=type_url, value=metadata.SerializeToString()),
        )

        # Patch the stub used by the API method.
        channel = ChannelStub(responses=[response_pb])
        api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            channel=channel
        )
        client._instance_admin_client = api

        # Perform the method and check the result.
        result = cluster.update()
        actual_request = channel.requests[0][1]

        self.assertEqual(actual_request, expected_request)
        self.assertIsInstance(result, operation.Operation)
        self.assertEqual(result.operation.name, self.OP_NAME)
        self.assertIsInstance(result.metadata, messages_v2_pb2.UpdateClusterMetadata)

    def test_delete(self):
        from google.protobuf import empty_pb2
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client

        api = bigtable_instance_admin_client.BigtableInstanceAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance = _Instance(self.INSTANCE_ID, client)
        cluster = self._make_one(self.CLUSTER_ID, instance, self.LOCATION_ID)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Patch the stub used by the API method.
        client._instance_admin_client = api
        instance_admin_client = client._instance_admin_client
        instance_stub = instance_admin_client.transport
        instance_stub.delete_cluster.side_effect = [response_pb]

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # Perform the method and check the result.
        result = cluster.delete()

        self.assertEqual(result, expected_result)
