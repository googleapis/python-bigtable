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


class TestCluster(unittest.TestCase):

    PROJECT = "project"
    INSTANCE_ID = "instance-id"
    LOCATION_ID = "location-id"
    CLUSTER_ID = "cluster-id"
    LOCATION_ID = "location-id"
    CLUSTER_NAME = (
        "projects/" + PROJECT + "/instances/" + INSTANCE_ID + "/clusters/" + CLUSTER_ID
    )
    LOCATION_PATH = "projects/" + PROJECT + "/locations/"
    SERVE_NODES = 5

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.cluster import Cluster

        return Cluster

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def _mock_instance(self):
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable.instance import Instance
        from google.cloud.bigtable_admin_v2.gapic import (
            bigtable_instance_admin_client as admin_client_module,
        )

        admin_api = mock.create_autospec(
            admin_client_module.BigtableInstanceAdminClient,
            instance=True,
        )
        admin_api.location_path.return_value = self.LOCATION_PATH + self.LOCATION_ID
        admin_api.cluster_path.return_value = self.CLUSTER_NAME

        client = mock.create_autospec(Client, instance=True, project=self.PROJECT)
        client.instance_admin_client = admin_api

        instance_name = "projects/{}/instances/{}".format(
            self.PROJECT,
            self.INSTANCE_ID,
        )
        instance = mock.create_autospec(
            Instance,
            instance=True,
            _client=client,
            instance_id=self.INSTANCE_ID,
        )
        instance.name = instance_name

        return admin_api, client, instance

    def test_constructor_defaults(self):
        _, _, instance = self._mock_instance()

        cluster = self._make_one(self.CLUSTER_ID, instance)

        self.assertEqual(cluster.cluster_id, self.CLUSTER_ID)
        self.assertIs(cluster._instance, instance)
        self.assertIsNone(cluster.location_id)
        self.assertIsNone(cluster.state)
        self.assertIsNone(cluster.serve_nodes)
        self.assertIsNone(cluster.default_storage_type)

    def test_constructor_non_default(self):
        from google.cloud.bigtable.enums import StorageType
        from google.cloud.bigtable.enums import Cluster

        _, _, instance = self._mock_instance()

        cluster = self._make_one(
            self.CLUSTER_ID,
            instance,
            location_id=self.LOCATION_ID,
            _state=Cluster.State.READY,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=StorageType.SSD,
        )

        self.assertEqual(cluster.cluster_id, self.CLUSTER_ID)
        self.assertIs(cluster._instance, instance)
        self.assertEqual(cluster.location_id, self.LOCATION_ID)
        self.assertEqual(cluster.state, Cluster.State.READY)
        self.assertEqual(cluster.serve_nodes, self.SERVE_NODES)
        self.assertEqual(cluster.default_storage_type, StorageType.SSD)

    def test_name_property(self):
        _, _, instance = self._mock_instance()
        cluster = self._make_one(self.CLUSTER_ID, instance)

        self.assertEqual(cluster.name, self.CLUSTER_NAME)

    def test_from_pb_success(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2
        from google.cloud.bigtable.enums import StorageType
        from google.cloud.bigtable.enums import Cluster

        location = self.LOCATION_PATH + self.LOCATION_ID
        cluster_pb = instance_pb2.Cluster(
            name=self.CLUSTER_NAME,
            location=location,
            state=Cluster.State.RESIZING,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=StorageType.SSD,
        )
        klass = self._get_target_class()
        _, _, instance = self._mock_instance()

        cluster = klass.from_pb(cluster_pb, instance)

        self.assertIsInstance(cluster, klass)
        self.assertEqual(cluster._instance, instance)
        self.assertEqual(cluster.cluster_id, self.CLUSTER_ID)
        self.assertEqual(cluster.location_id, self.LOCATION_ID)
        self.assertEqual(cluster.state, Cluster.State.RESIZING)
        self.assertEqual(cluster.serve_nodes, self.SERVE_NODES)
        self.assertEqual(cluster.default_storage_type, StorageType.SSD)

    def test_from_pb_bad_cluster_name(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        bad_cluster_name = "BAD_NAME"
        cluster_pb = instance_pb2.Cluster(name=bad_cluster_name)
        klass = self._get_target_class()
        _, _, instance = self._mock_instance()

        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, None)

    def test_from_pb_instance_id_mistmatch(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        cluster_pb = instance_pb2.Cluster(name=self.CLUSTER_NAME)
        klass = self._get_target_class()
        _, _, instance = self._mock_instance()
        instance.instance_id = "other-instance-id"

        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, instance)

    def test_from_pb_project_mistmatch(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        cluster_pb = instance_pb2.Cluster(name=self.CLUSTER_NAME)
        klass = self._get_target_class()
        _, client, instance = self._mock_instance()
        client.project = "other-project"

        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, instance)

    def test__update_from_pb(self):
        from google.cloud.bigtable_admin_v2.types import instance_pb2
        from google.cloud.bigtable.enums import StorageType
        from google.cloud.bigtable.enums import Cluster

        _, client, instance = self._mock_instance()
        cluster = self._make_one(self.CLUSTER_ID, instance)
        updated_nodes = 10
        cluster_pb = instance_pb2.Cluster(
            name=self.CLUSTER_NAME,
            location=self.LOCATION_PATH + self.LOCATION_ID,
            state=Cluster.State.READY,
            serve_nodes=updated_nodes,
            default_storage_type=StorageType.HDD,
        )

        cluster._update_from_pb(cluster_pb)

        self.assertEqual(cluster.location_id, self.LOCATION_ID)
        self.assertEqual(cluster.state, Cluster.State.READY)
        self.assertEqual(cluster.serve_nodes, updated_nodes)
        self.assertEqual(cluster.default_storage_type, StorageType.HDD)

    def test__to_pb(self):
        from google.cloud.bigtable_admin_v2.types import instance_pb2
        from google.cloud.bigtable.enums import StorageType
        from google.cloud.bigtable.enums import Cluster

        _, client, instance = self._mock_instance()
        cluster = self._make_one(
            self.CLUSTER_ID,
            instance,
            location_id=self.LOCATION_ID,
            _state=Cluster.State.READY,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=StorageType.SSD,
        )

        expected_pb = instance_pb2.Cluster(
            location=self.LOCATION_PATH + self.LOCATION_ID,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=StorageType.SSD,
        )

        self.assertEqual(cluster._to_pb(), expected_pb)

    def test___eq__(self):
        _, _, instance = self._mock_instance()
        cluster1 = self._make_one(self.CLUSTER_ID, instance, self.LOCATION_ID)
        cluster2 = self._make_one(self.CLUSTER_ID, instance, self.LOCATION_ID)

        self.assertEqual(cluster1, cluster2)

    def test___eq__type_differ(self):
        _, _, instance = self._mock_instance()
        cluster1 = self._make_one(self.CLUSTER_ID, instance, self.LOCATION_ID)
        cluster2 = object()

        self.assertNotEqual(cluster1, cluster2)

    def test___ne__same_value(self):
        _, _, instance = self._mock_instance()
        cluster1 = self._make_one(self.CLUSTER_ID, instance, self.LOCATION_ID)
        cluster2 = self._make_one(self.CLUSTER_ID, instance, self.LOCATION_ID)

        self.assertFalse(cluster1 != cluster2)

    def test___ne__(self):
        _, _, instance = self._mock_instance()
        cluster1 = self._make_one("cluster_id1", instance, self.LOCATION_ID)
        cluster2 = self._make_one("cluster_id2", instance, self.LOCATION_ID)

        self.assertTrue(cluster1 != cluster2)

    def test_reload(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2
        from google.cloud.bigtable.enums import StorageType
        from google.cloud.bigtable.enums import Cluster

        admin_api, _, instance = self._mock_instance()

        cluster = self._make_one(
            self.CLUSTER_ID,
            instance,
            location_id=self.LOCATION_ID,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=StorageType.SSD,
        )

        updated_location_id = "new-location-id"
        updated_state = Cluster.State.READY
        updated_nodes = 10

        response_pb = instance_pb2.Cluster(
            name=cluster.name,
            location=self.LOCATION_PATH + updated_location_id,
            state=updated_state,
            serve_nodes=updated_nodes,
            default_storage_type=StorageType.HDD,
        )
        admin_api.get_cluster.return_value = response_pb

        self.assertIsNone(cluster.reload())

        self.assertEqual(cluster.location_id, updated_location_id)
        self.assertEqual(cluster.state, updated_state)
        self.assertEqual(cluster.serve_nodes, updated_nodes)
        self.assertEqual(cluster.default_storage_type, StorageType.HDD)

    def test_exists_hit(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2

        admin_api, _, instance = self._mock_instance()
        cluster = self._make_one(self.CLUSTER_ID, instance)
        response_pb = instance_pb2.Cluster()
        admin_api.get_cluster.return_value = response_pb

        self.assertTrue(cluster.exists())

        admin_api.get_cluster.assert_called_once_with(self.CLUSTER_NAME)

    def test_exists_miss(self):
        from google.api_core import exceptions

        admin_api, _, instance = self._mock_instance()
        cluster = self._make_one(self.CLUSTER_ID, instance)
        admin_api.get_cluster.side_effect = [exceptions.NotFound("testing")]

        self.assertFalse(cluster.exists())

        admin_api.get_cluster.assert_called_once_with(self.CLUSTER_NAME)

    def test_exists_bad_request(self):
        from google.api_core import exceptions

        admin_api, _, instance = self._mock_instance()
        cluster = self._make_one(self.CLUSTER_ID, instance)
        admin_api.get_cluster.side_effect = [exceptions.BadRequest("testing")]

        with self.assertRaises(exceptions.BadRequest):
            cluster.exists()

        admin_api.get_cluster.assert_called_once_with(self.CLUSTER_NAME)

    def test_create(self):
        admin_api, _, instance = self._mock_instance()
        cluster = self._make_one(self.CLUSTER_ID, instance)

        response = cluster.create()

        self.assertIs(response, admin_api.create_cluster.return_value)

        admin_api.create_cluster.assert_called_once_with(
            instance.name,
            self.CLUSTER_ID,
            cluster._to_pb(),
        )

    def test_update(self):
        from google.cloud.bigtable.enums import StorageType

        admin_api, _, instance = self._mock_instance()
        cluster = self._make_one(
            self.CLUSTER_ID,
            instance,
            location_id=self.LOCATION_ID,
            serve_nodes=self.SERVE_NODES,
            default_storage_type=StorageType.SSD,
        )

        result = cluster.update()

        self.assertIs(result, admin_api.update_cluster.return_value)
        admin_api.update_cluster.assert_called_once_with(
            name=cluster.name,
            serve_nodes=self.SERVE_NODES,
            location=None,
        )

    def test_delete(self):
        from google.protobuf import empty_pb2

        admin_api, _, instance = self._mock_instance()
        admin_api.delete_cluster.return_value = empty_pb2.Empty()
        cluster = self._make_one(self.CLUSTER_ID, instance)
        self.assertIsNone(cluster.delete())

        admin_api.delete_cluster.assert_called_once_with(cluster.name)
