# Copyright 2020 Google LLC All rights reserved.
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


import datetime
import mock
import unittest

from ._testing import _make_credentials
from google.cloud._helpers import UTC


class TestBackupConstants:
    PROJECT_ID = "project-id"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID
    CLUSTER_ID = "cluster-id"
    CLUSTER_NAME = INSTANCE_NAME + "/clusters/" + CLUSTER_ID
    TABLE_ID = "table-id"
    TABLE_NAME = INSTANCE_NAME + "/tables/" + TABLE_ID
    BACKUP_ID = "backup-id"
    BACKUP_NAME = CLUSTER_NAME + "/backups/" + BACKUP_ID


class TestBaseBackup(unittest.TestCase, TestBackupConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.base_backup import BaseBackup

        return BaseBackup

    @staticmethod
    def _make_table_admin_client():
        from google.cloud.bigtable_admin_v2 import BigtableTableAdminClient

        return mock.create_autospec(BigtableTableAdminClient, instance=True)

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def _make_timestamp(self):
        return datetime.datetime.utcnow().replace(tzinfo=UTC)

    def test_constructor_defaults(self):
        instance = _Instance(self.INSTANCE_NAME)
        backup = self._make_one(self.BACKUP_ID, instance)

        self.assertEqual(backup.backup_id, self.BACKUP_ID)
        self.assertIs(backup._instance, instance)
        self.assertIsNone(backup._cluster)
        self.assertIsNone(backup.table_id)
        self.assertIsNone(backup._expire_time)

        self.assertIsNone(backup._parent)
        self.assertIsNone(backup._source_table)
        self.assertIsNone(backup._start_time)
        self.assertIsNone(backup._end_time)
        self.assertIsNone(backup._size_bytes)
        self.assertIsNone(backup._state)

    def test_constructor_non_defaults(self):
        instance = _Instance(self.INSTANCE_NAME)
        expire_time = self._make_timestamp()

        backup = self._make_one(
            self.BACKUP_ID,
            instance,
            cluster_id=self.CLUSTER_ID,
            table_id=self.TABLE_ID,
            expire_time=expire_time,
        )

        self.assertEqual(backup.backup_id, self.BACKUP_ID)
        self.assertIs(backup._instance, instance)
        self.assertIs(backup._cluster, self.CLUSTER_ID)
        self.assertEqual(backup.table_id, self.TABLE_ID)
        self.assertEqual(backup._expire_time, expire_time)

        self.assertIsNone(backup._parent)
        self.assertIsNone(backup._source_table)
        self.assertIsNone(backup._start_time)
        self.assertIsNone(backup._end_time)
        self.assertIsNone(backup._size_bytes)
        self.assertIsNone(backup._state)

    def test_from_pb_project_mismatch(self):
        from google.cloud.bigtable_admin_v2.proto import table_pb2

        alt_project_id = "alt-project-id"
        client = _Client(project=alt_project_id)
        instance = _Instance(self.INSTANCE_NAME, client)
        backup_pb = table_pb2.Backup(name=self.BACKUP_NAME)
        klasse = self._get_target_class()

        with self.assertRaises(ValueError):
            klasse.from_pb(backup_pb, instance)

    def test_from_pb_instance_mismatch(self):
        from google.cloud.bigtable_admin_v2.proto import table_pb2

        alt_instance = "/projects/%s/instances/alt-instance" % self.PROJECT_ID
        client = _Client()
        instance = _Instance(alt_instance, client)
        backup_pb = table_pb2.Backup(name=self.BACKUP_NAME)
        klasse = self._get_target_class()

        with self.assertRaises(ValueError):
            klasse.from_pb(backup_pb, instance)

    def test_from_pb_bad_name(self):
        from google.cloud.bigtable_admin_v2.proto import table_pb2

        client = _Client()
        instance = _Instance(self.INSTANCE_NAME, client)
        backup_pb = table_pb2.Backup(name="invalid_name")
        klasse = self._get_target_class()

        with self.assertRaises(ValueError):
            klasse.from_pb(backup_pb, instance)

    def test_from_pb_success(self):
        from google.cloud.bigtable_admin_v2.gapic import enums
        from google.cloud.bigtable_admin_v2.proto import table_pb2
        from google.cloud._helpers import _datetime_to_pb_timestamp

        client = _Client()
        instance = _Instance(self.INSTANCE_NAME, client)
        timestamp = _datetime_to_pb_timestamp(self._make_timestamp())
        size_bytes = 1234
        state = enums.Backup.State.READY
        backup_pb = table_pb2.Backup(
            name=self.BACKUP_NAME,
            source_table=self.TABLE_NAME,
            expire_time=timestamp,
            start_time=timestamp,
            end_time=timestamp,
            size_bytes=size_bytes,
            state=state,
        )
        klasse = self._get_target_class()

        backup = klasse.from_pb(backup_pb, instance)

        self.assertTrue(isinstance(backup, klasse))
        self.assertEqual(backup._instance, instance)
        self.assertEqual(backup.backup_id, self.BACKUP_ID)
        self.assertEqual(backup.cluster, self.CLUSTER_ID)
        self.assertEqual(backup.table_id, self.TABLE_ID)
        self.assertEqual(backup._expire_time, timestamp)
        self.assertEqual(backup._start_time, timestamp)
        self.assertEqual(backup._end_time, timestamp)
        self.assertEqual(backup._size_bytes, size_bytes)
        self.assertEqual(backup._state, state)

    def test_property_name(self):
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = Client(project=self.PROJECT_ID, credentials=credentials, admin=True)
        client._table_admin_client = api
        instance = _Instance(self.INSTANCE_NAME, client)

        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)
        self.assertEqual(backup.name, self.BACKUP_NAME)

    def test_property_cluster(self):
        backup = self._make_one(
            self.BACKUP_ID, _Instance(self.INSTANCE_NAME), cluster_id=self.CLUSTER_ID
        )
        self.assertEqual(backup.cluster, self.CLUSTER_ID)

    def test_property_cluster_setter(self):
        backup = self._make_one(self.BACKUP_ID, _Instance(self.INSTANCE_NAME))
        backup.cluster = self.CLUSTER_ID
        self.assertEqual(backup.cluster, self.CLUSTER_ID)

    def test_property_parent_none(self):
        backup = self._make_one(self.BACKUP_ID, _Instance(self.INSTANCE_NAME),)
        self.assertIsNone(backup.parent)

    def test_property_parent_w_cluster(self):
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = Client(project=self.PROJECT_ID, credentials=credentials, admin=True)
        client._table_admin_client = api
        instance = _Instance(self.INSTANCE_NAME, client)

        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)
        self.assertEqual(backup._cluster, self.CLUSTER_ID)
        self.assertEqual(backup.parent, self.CLUSTER_NAME)

    def test_property_source_table_none(self):
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = Client(project=self.PROJECT_ID, credentials=credentials, admin=True)
        client._table_admin_client = api
        instance = _Instance(self.INSTANCE_NAME, client)

        backup = self._make_one(self.BACKUP_ID, instance)
        self.assertIsNone(backup.source_table)

    def test_property_source_table_valid(self):
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = Client(project=self.PROJECT_ID, credentials=credentials, admin=True)
        client._table_admin_client = api
        instance = _Instance(self.INSTANCE_NAME, client)

        backup = self._make_one(self.BACKUP_ID, instance, table_id=self.TABLE_ID)
        self.assertEqual(backup.source_table, self.TABLE_NAME)

    def test_property_expire_time(self):
        instance = _Instance(self.INSTANCE_NAME)
        expire_time = self._make_timestamp()
        backup = self._make_one(self.BACKUP_ID, instance, expire_time=expire_time)
        self.assertEqual(backup.expire_time, expire_time)

    def test_property_expire_time_setter(self):
        instance = _Instance(self.INSTANCE_NAME)
        expire_time = self._make_timestamp()
        backup = self._make_one(self.BACKUP_ID, instance)
        backup.expire_time = expire_time
        self.assertEqual(backup.expire_time, expire_time)

    def test_property_start_time(self):
        instance = _Instance(self.INSTANCE_NAME)
        backup = self._make_one(self.BACKUP_ID, instance)
        expected = backup._start_time = self._make_timestamp()
        self.assertEqual(backup.start_time, expected)

    def test_property_end_time(self):
        instance = _Instance(self.INSTANCE_NAME)
        backup = self._make_one(self.BACKUP_ID, instance)
        expected = backup._end_time = self._make_timestamp()
        self.assertEqual(backup.end_time, expected)

    def test_property_size(self):
        instance = _Instance(self.INSTANCE_NAME)
        backup = self._make_one(self.BACKUP_ID, instance)
        expected = backup._size_bytes = 10
        self.assertEqual(backup.size_bytes, expected)

    def test_property_state(self):
        from google.cloud.bigtable_admin_v2.gapic import enums

        instance = _Instance(self.INSTANCE_NAME)
        backup = self._make_one(self.BACKUP_ID, instance)
        expected = backup._state = enums.Backup.State.READY
        self.assertEqual(backup.state, expected)

    def test___eq__(self):
        instance = object()
        backup1 = self._make_one(self.BACKUP_ID, instance)
        backup2 = self._make_one(self.BACKUP_ID, instance)
        self.assertTrue(backup1 == backup2)

    def test___eq__different_types(self):
        instance = object()
        backup1 = self._make_one(self.BACKUP_ID, instance)
        backup2 = object()
        self.assertFalse(backup1 == backup2)

    def test___ne__same_value(self):
        instance = object()
        backup1 = self._make_one(self.BACKUP_ID, instance)
        backup2 = self._make_one(self.BACKUP_ID, instance)
        self.assertFalse(backup1 != backup2)

    def test___ne__(self):
        backup1 = self._make_one("backup_1", "instance1")
        backup2 = self._make_one("backup_2", "instance2")
        self.assertTrue(backup1 != backup2)


class _Client(object):
    def __init__(self, project=TestBackupConstants.PROJECT_ID):
        self.project = project
        self.project_name = "projects/" + self.project


class _Instance(object):
    def __init__(self, name, client=None):
        self.name = name
        self.instance_id = name.rsplit("/", 1)[1]
        self._client = client
