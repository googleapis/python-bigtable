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

from google.cloud._helpers import UTC

from tests.unit.test_base_backup import (
    TestBackupConstants,
    _Client,
    _Instance,
)


class TestBackup(unittest.TestCase, TestBackupConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.backup import Backup

        return Backup

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

    def test_create_grpc_error(self):
        from google.api_core.exceptions import GoogleAPICallError
        from google.api_core.exceptions import Unknown
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.types import table_pb2

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.create_backup.side_effect = Unknown("testing")

        timestamp = self._make_timestamp()
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME, client=client),
            table_id=self.TABLE_ID,
            expire_time=timestamp,
        )

        backup_pb = table_pb2.Backup(
            source_table=self.TABLE_NAME,
            expire_time=_datetime_to_pb_timestamp(timestamp),
        )

        with self.assertRaises(GoogleAPICallError):
            backup.create(self.CLUSTER_ID)

        api.create_backup.assert_called_once_with(
            parent=self.CLUSTER_NAME, backup_id=self.BACKUP_ID, backup=backup_pb,
        )

    def test_create_already_exists(self):
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.types import table_pb2
        from google.cloud.exceptions import Conflict

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.create_backup.side_effect = Conflict("testing")

        timestamp = self._make_timestamp()
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME, client=client),
            table_id=self.TABLE_ID,
            expire_time=timestamp,
        )

        backup_pb = table_pb2.Backup(
            source_table=self.TABLE_NAME,
            expire_time=_datetime_to_pb_timestamp(timestamp),
        )

        with self.assertRaises(Conflict):
            backup.create(self.CLUSTER_ID)

        api.create_backup.assert_called_once_with(
            parent=self.CLUSTER_NAME, backup_id=self.BACKUP_ID, backup=backup_pb,
        )

    def test_create_instance_not_found(self):
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.types import table_pb2
        from google.cloud.exceptions import NotFound

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.create_backup.side_effect = NotFound("testing")

        timestamp = self._make_timestamp()
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME, client=client),
            table_id=self.TABLE_ID,
            expire_time=timestamp,
        )

        backup_pb = table_pb2.Backup(
            source_table=self.TABLE_NAME,
            expire_time=_datetime_to_pb_timestamp(timestamp),
        )

        with self.assertRaises(NotFound):
            backup.create(self.CLUSTER_ID)

        api.create_backup.assert_called_once_with(
            parent=self.CLUSTER_NAME, backup_id=self.BACKUP_ID, backup=backup_pb,
        )

    def test_create_cluster_not_set(self):
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME),
            table_id=self.TABLE_ID,
            expire_time=self._make_timestamp(),
        )

        with self.assertRaises(ValueError):
            backup.create()

    def test_create_table_not_set(self):
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME),
            expire_time=self._make_timestamp(),
        )

        with self.assertRaises(ValueError):
            backup.create(self.CLUSTER_ID)

    def test_create_expire_time_not_set(self):
        backup = self._make_one(
            self.BACKUP_ID, _Instance(self.INSTANCE_NAME), table_id=self.TABLE_ID,
        )

        with self.assertRaises(ValueError):
            backup.create(self.CLUSTER_ID)

    def test_create_success(self):
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.types import table_pb2

        op_future = object()
        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.create_backup.return_value = op_future

        timestamp = self._make_timestamp()
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME, client=client),
            table_id=self.TABLE_ID,
            expire_time=timestamp,
        )

        backup_pb = table_pb2.Backup(
            source_table=self.TABLE_NAME,
            expire_time=_datetime_to_pb_timestamp(timestamp),
        )

        future = backup.create(self.CLUSTER_ID)
        self.assertEqual(backup._cluster, self.CLUSTER_ID)
        self.assertIs(future, op_future)

        api.create_backup.assert_called_once_with(
            parent=self.CLUSTER_NAME, backup_id=self.BACKUP_ID, backup=backup_pb,
        )

    def test_exists_grpc_error(self):
        from google.api_core.exceptions import Unknown

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.get_backup.side_effect = Unknown("testing")

        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        with self.assertRaises(Unknown):
            backup.exists()

        api.get_backup.assert_called_once_with(self.BACKUP_NAME)

    def test_exists_not_found(self):
        from google.api_core.exceptions import NotFound

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.get_backup.side_effect = NotFound("testing")

        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        self.assertFalse(backup.exists())

        api.get_backup.assert_called_once_with(self.BACKUP_NAME)

    def test_get(self):
        from google.cloud.bigtable_admin_v2.gapic import enums
        from google.cloud.bigtable_admin_v2.proto import table_pb2
        from google.cloud._helpers import _datetime_to_pb_timestamp

        timestamp = _datetime_to_pb_timestamp(self._make_timestamp())
        state = enums.Backup.State.READY

        client = _Client()
        backup_pb = table_pb2.Backup(
            name=self.BACKUP_NAME,
            source_table=self.TABLE_NAME,
            expire_time=timestamp,
            start_time=timestamp,
            end_time=timestamp,
            size_bytes=0,
            state=state,
        )
        api = client.table_admin_client = self._make_table_admin_client()
        api.get_backup.return_value = backup_pb

        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        self.assertEqual(backup.get(), backup_pb)

    def test_reload(self):
        from google.cloud.bigtable_admin_v2.gapic import enums
        from google.cloud.bigtable_admin_v2.proto import table_pb2
        from google.cloud._helpers import _datetime_to_pb_timestamp

        timestamp = _datetime_to_pb_timestamp(self._make_timestamp())
        state = enums.Backup.State.READY

        client = _Client()
        backup_pb = table_pb2.Backup(
            name=self.BACKUP_NAME,
            source_table=self.TABLE_NAME,
            expire_time=timestamp,
            start_time=timestamp,
            end_time=timestamp,
            size_bytes=0,
            state=state,
        )
        api = client.table_admin_client = self._make_table_admin_client()
        api.get_backup.return_value = backup_pb

        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        backup.reload()
        self.assertEqual(backup._source_table, self.TABLE_NAME)
        self.assertEqual(backup._expire_time, timestamp)
        self.assertEqual(backup._start_time, timestamp)
        self.assertEqual(backup._end_time, timestamp)
        self.assertEqual(backup._size_bytes, 0)
        self.assertEqual(backup._state, state)

    def test_exists_success(self):
        from google.cloud.bigtable_admin_v2.proto import table_pb2

        client = _Client()
        backup_pb = table_pb2.Backup(name=self.BACKUP_NAME)
        api = client.table_admin_client = self._make_table_admin_client()
        api.get_backup.return_value = backup_pb

        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        self.assertTrue(backup.exists())

        api.get_backup.assert_called_once_with(self.BACKUP_NAME)

    def test_delete_grpc_error(self):
        from google.api_core.exceptions import Unknown

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.delete_backup.side_effect = Unknown("testing")
        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        with self.assertRaises(Unknown):
            backup.delete()

        api.delete_backup.assert_called_once_with(self.BACKUP_NAME)

    def test_delete_not_found(self):
        from google.api_core.exceptions import NotFound

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.delete_backup.side_effect = NotFound("testing")
        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        with self.assertRaises(NotFound):
            backup.delete()

        api.delete_backup.assert_called_once_with(self.BACKUP_NAME)

    def test_delete_success(self):
        from google.protobuf.empty_pb2 import Empty

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.delete_backup.return_value = Empty()
        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)

        backup.delete()

        api.delete_backup.assert_called_once_with(self.BACKUP_NAME)

    def test_update_expire_time_grpc_error(self):
        from google.api_core.exceptions import Unknown
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.types import table_pb2
        from google.protobuf import field_mask_pb2

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.update_backup.side_effect = Unknown("testing")
        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)
        expire_time = self._make_timestamp()

        with self.assertRaises(Unknown):
            backup.update_expire_time(expire_time)

        backup_update = table_pb2.Backup(
            name=self.BACKUP_NAME, expire_time=_datetime_to_pb_timestamp(expire_time),
        )
        update_mask = field_mask_pb2.FieldMask(paths=["expire_time"])
        api.update_backup.assert_called_once_with(
            backup_update, update_mask,
        )

    def test_update_expire_time_not_found(self):
        from google.api_core.exceptions import NotFound
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.types import table_pb2
        from google.protobuf import field_mask_pb2

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.update_backup.side_effect = NotFound("testing")
        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)
        expire_time = self._make_timestamp()

        with self.assertRaises(NotFound):
            backup.update_expire_time(expire_time)

        backup_update = table_pb2.Backup(
            name=self.BACKUP_NAME, expire_time=_datetime_to_pb_timestamp(expire_time),
        )
        update_mask = field_mask_pb2.FieldMask(paths=["expire_time"])
        api.update_backup.assert_called_once_with(
            backup_update, update_mask,
        )

    def test_update_expire_time_success(self):
        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable_admin_v2.proto import table_pb2
        from google.protobuf import field_mask_pb2

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.update_backup.return_type = table_pb2.Backup(name=self.BACKUP_NAME)
        instance = _Instance(self.INSTANCE_NAME, client=client)
        backup = self._make_one(self.BACKUP_ID, instance, cluster_id=self.CLUSTER_ID)
        expire_time = self._make_timestamp()

        backup.update_expire_time(expire_time)

        backup_update = table_pb2.Backup(
            name=self.BACKUP_NAME, expire_time=_datetime_to_pb_timestamp(expire_time),
        )
        update_mask = field_mask_pb2.FieldMask(paths=["expire_time"])
        api.update_backup.assert_called_once_with(
            backup_update, update_mask,
        )

    def test_restore_grpc_error(self):
        from google.api_core.exceptions import GoogleAPICallError
        from google.api_core.exceptions import Unknown

        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.restore_table.side_effect = Unknown("testing")

        timestamp = self._make_timestamp()
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME, client=client),
            cluster_id=self.CLUSTER_ID,
            table_id=self.TABLE_NAME,
            expire_time=timestamp,
        )

        with self.assertRaises(GoogleAPICallError):
            backup.restore(self.TABLE_ID)

        api.restore_table.assert_called_once_with(
            parent=self.INSTANCE_NAME, table_id=self.TABLE_ID, backup=self.BACKUP_NAME,
        )

    def test_restore_cluster_not_set(self):
        client = _Client()
        client.table_admin_client = self._make_table_admin_client()
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME, client=client),
            table_id=self.TABLE_ID,
            expire_time=self._make_timestamp(),
        )

        with self.assertRaises(ValueError):
            backup.restore(self.TABLE_ID)

    def test_restore_success(self):
        op_future = object()
        client = _Client()
        api = client.table_admin_client = self._make_table_admin_client()
        api.restore_table.return_value = op_future

        timestamp = self._make_timestamp()
        backup = self._make_one(
            self.BACKUP_ID,
            _Instance(self.INSTANCE_NAME, client=client),
            cluster_id=self.CLUSTER_ID,
            table_id=self.TABLE_NAME,
            expire_time=timestamp,
        )

        future = backup.restore(self.TABLE_ID)
        self.assertEqual(backup._cluster, self.CLUSTER_ID)
        self.assertIs(future, op_future)

        api.restore_table.assert_called_once_with(
            parent=self.INSTANCE_NAME, table_id=self.TABLE_ID, backup=self.BACKUP_NAME,
        )
