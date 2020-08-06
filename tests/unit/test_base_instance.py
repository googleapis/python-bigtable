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


class TestInstanceConstants:
    PROJECT = "project"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "projects/" + PROJECT + "/instances/" + INSTANCE_ID
    LOCATION_ID = "locid"
    LOCATION = "projects/" + PROJECT + "/locations/" + LOCATION_ID
    APP_PROFILE_PATH = (
        "projects/" + PROJECT + "/instances/" + INSTANCE_ID + "/appProfiles/"
    )
    DISPLAY_NAME = "display_name"
    LABELS = {"foo": "bar"}
    OP_ID = 8915
    OP_NAME = "operations/projects/{}/instances/{}operations/{}".format(
        PROJECT, INSTANCE_ID, OP_ID
    )
    TABLE_ID = "table_id"
    TABLE_NAME = INSTANCE_NAME + "/tables/" + TABLE_ID
    CLUSTER_ID = "cluster-id"
    CLUSTER_NAME = INSTANCE_NAME + "/clusters/" + CLUSTER_ID
    BACKUP_ID = "backup-id"
    BACKUP_NAME = CLUSTER_NAME + "/backups/" + BACKUP_ID


class TestBaseInstance(unittest.TestCase, TestInstanceConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.instance import Instance

        return Instance

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor_defaults(self):

        client = object()
        instance = self._make_one(self.INSTANCE_ID, client)
        self.assertEqual(instance.instance_id, self.INSTANCE_ID)
        self.assertEqual(instance.display_name, self.INSTANCE_ID)
        self.assertIsNone(instance.type_)
        self.assertIsNone(instance.labels)
        self.assertIs(instance._client, client)
        self.assertIsNone(instance.state)

    def test_constructor_non_default(self):
        from google.cloud.bigtable import enums

        instance_type = enums.Instance.Type.DEVELOPMENT
        state = enums.Instance.State.READY
        labels = {"test": "test"}
        client = object()

        instance = self._make_one(
            self.INSTANCE_ID,
            client,
            display_name=self.DISPLAY_NAME,
            instance_type=instance_type,
            labels=labels,
            _state=state,
        )
        self.assertEqual(instance.instance_id, self.INSTANCE_ID)
        self.assertEqual(instance.display_name, self.DISPLAY_NAME)
        self.assertEqual(instance.type_, instance_type)
        self.assertEqual(instance.labels, labels)
        self.assertIs(instance._client, client)
        self.assertEqual(instance.state, state)

    def test__update_from_pb_success(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable import enums

        instance_type = enums.Instance.Type.PRODUCTION
        state = enums.Instance.State.READY
        instance_pb = data_v2_pb2.Instance(
            display_name=self.DISPLAY_NAME,
            type=instance_type,
            labels=self.LABELS,
            state=state,
        )

        instance = self._make_one(None, None)
        self.assertIsNone(instance.display_name)
        self.assertIsNone(instance.type_)
        self.assertIsNone(instance.labels)
        instance._update_from_pb(instance_pb)
        self.assertEqual(instance.display_name, self.DISPLAY_NAME)
        self.assertEqual(instance.type_, instance_type)
        self.assertEqual(instance.labels, self.LABELS)
        self.assertEqual(instance._state, state)

    def test__update_from_pb_success_defaults(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable import enums

        instance_pb = data_v2_pb2.Instance(display_name=self.DISPLAY_NAME)

        instance = self._make_one(None, None)
        self.assertIsNone(instance.display_name)
        self.assertIsNone(instance.type_)
        self.assertIsNone(instance.labels)
        instance._update_from_pb(instance_pb)
        self.assertEqual(instance.display_name, self.DISPLAY_NAME)
        self.assertEqual(instance.type_, enums.Instance.Type.UNSPECIFIED)
        self.assertFalse(instance.labels)

    def test__update_from_pb_no_display_name(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2

        instance_pb = data_v2_pb2.Instance()
        instance = self._make_one(None, None)
        self.assertIsNone(instance.display_name)
        with self.assertRaises(ValueError):
            instance._update_from_pb(instance_pb)

    def test_from_pb_success(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2
        from google.cloud.bigtable import enums

        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )
        instance_type = enums.Instance.Type.PRODUCTION
        state = enums.Instance.State.READY
        instance_pb = data_v2_pb2.Instance(
            name=self.INSTANCE_NAME,
            display_name=self.INSTANCE_ID,
            type=instance_type,
            labels=self.LABELS,
            state=state,
        )

        klass = self._get_target_class()
        instance = klass.from_pb(instance_pb, client)
        self.assertIsInstance(instance, klass)
        self.assertEqual(instance._client, client)
        self.assertEqual(instance.instance_id, self.INSTANCE_ID)
        self.assertEqual(instance.display_name, self.INSTANCE_ID)
        self.assertEqual(instance.type_, instance_type)
        self.assertEqual(instance.labels, self.LABELS)
        self.assertEqual(instance._state, state)

    def test_from_pb_bad_instance_name(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2

        instance_name = "INCORRECT_FORMAT"
        instance_pb = data_v2_pb2.Instance(name=instance_name)

        klass = self._get_target_class()
        with self.assertRaises(ValueError):
            klass.from_pb(instance_pb, None)

    def test_from_pb_project_mistmatch(self):
        from google.cloud.bigtable_admin_v2.proto import instance_pb2 as data_v2_pb2

        ALT_PROJECT = "ALT_PROJECT"
        credentials = _make_credentials()
        client = self._make_client(
            project=ALT_PROJECT, credentials=credentials, admin=True
        )

        self.assertNotEqual(self.PROJECT, ALT_PROJECT)

        instance_pb = data_v2_pb2.Instance(name=self.INSTANCE_NAME)

        klass = self._get_target_class()
        with self.assertRaises(ValueError):
            klass.from_pb(instance_pb, client)

    def test_name_property(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_instance_admin_client

        api = bigtable_instance_admin_client.BigtableInstanceAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT, credentials=credentials, admin=True
        )

        # Patch the the API method.
        client._instance_admin_client = api

        instance = self._make_one(self.INSTANCE_ID, client)
        self.assertEqual(instance.name, self.INSTANCE_NAME)

    def test___eq__(self):
        client = object()
        instance1 = self._make_one(self.INSTANCE_ID, client)
        instance2 = self._make_one(self.INSTANCE_ID, client)
        self.assertEqual(instance1, instance2)

    def test___eq__type_differ(self):
        client = object()
        instance1 = self._make_one(self.INSTANCE_ID, client)
        instance2 = object()
        self.assertNotEqual(instance1, instance2)

    def test___ne__same_value(self):
        client = object()
        instance1 = self._make_one(self.INSTANCE_ID, client)
        instance2 = self._make_one(self.INSTANCE_ID, client)
        comparison_val = instance1 != instance2
        self.assertFalse(comparison_val)

    def test___ne__(self):
        instance1 = self._make_one("instance_id1", "client1")
        instance2 = self._make_one("instance_id2", "client2")
        self.assertNotEqual(instance1, instance2)
