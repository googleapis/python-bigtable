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


class TestTable(unittest.TestCase):

    PROJECT_ID = "project-id"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID
    CLUSTER_ID = "cluster-id"
    CLUSTER_NAME = INSTANCE_NAME + "/clusters/" + CLUSTER_ID
    TABLE_ID = "table-id"
    TABLE_NAME = INSTANCE_NAME + "/tables/" + TABLE_ID
    BACKUP_ID = "backup-id"
    BACKUP_NAME = CLUSTER_NAME + "/backups/" + BACKUP_ID
    ROW_KEY = b"row-key"
    ROW_KEY_1 = b"row-key-1"
    ROW_KEY_2 = b"row-key-2"
    ROW_KEY_3 = b"row-key-3"
    FAMILY_NAME = u"family"
    QUALIFIER = b"qualifier"
    TIMESTAMP_MICROS = 100
    VALUE = b"value"
    _json_tests = None

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.table import Table

        return Table

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def _mock_instance(self):
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable.instance import Instance

        client = mock.create_autospec(Client, instance=True, project=self.PROJECT_ID)
        instance = mock.create_autospec(
            Instance, instance=True, instance_id=self.INSTANCE_NAME
        )
        instance._client = client
        return client, instance

    def test_constructor_defaults(self):
        _, instance = self._mock_instance()

        table = self._make_one(self.TABLE_ID, instance)

        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertIs(table._instance, instance)
        self.assertIsNone(table.mutation_timeout)
        self.assertIsNone(table._app_profile_id)

    def test_constructor_explicit(self):
        _, instance = self._mock_instance()
        mutation_timeout = 123
        app_profile_id = "profile-123"

        table = self._make_one(
            self.TABLE_ID,
            instance,
            mutation_timeout=mutation_timeout,
            app_profile_id=app_profile_id,
        )

        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertIs(table._instance, instance)
        self.assertEqual(table.mutation_timeout, mutation_timeout)
        self.assertEqual(table._app_profile_id, app_profile_id)

    def test___eq__(self):
        _, instance = self._mock_instance()
        table1 = self._make_one(self.TABLE_ID, instance)
        table2 = self._make_one(self.TABLE_ID, instance)
        self.assertEqual(table1, table2)

    def test___eq__type_differ(self):
        _, instance = self._mock_instance()
        table1 = self._make_one(self.TABLE_ID, instance)
        table2 = object()
        self.assertNotEqual(table1, table2)

    def test___ne__same_value(self):
        _, instance = self._mock_instance()
        table1 = self._make_one(self.TABLE_ID, instance)
        table2 = self._make_one(self.TABLE_ID, instance)
        comparison_val = table1 != table2
        self.assertFalse(comparison_val)

    def test___ne__(self):
        _, instance = self._mock_instance()
        table1 = self._make_one("table_id1", instance)
        table2 = self._make_one("table_id2", instance)
        self.assertNotEqual(table1, table2)

    def test_name(self):
        table_data_client = mock.Mock(spec=["table_path"])
        client, instance = self._mock_instance()
        client.table_data_client = table_data_client
        table = self._make_one(self.TABLE_ID, instance)

        self.assertEqual(table.name, table_data_client.table_path.return_value)

    def _column_family_helper(self):
        client, instance = self._mock_instance()
        table = self._make_one(self.TABLE_ID, instance)
        column_family_id = "column_family"

        return table, column_family_id

    def test_column_family_default(self):
        from google.cloud.bigtable.column_family import ColumnFamily

        table, column_family_id = self._column_family_helper()
        column_family = table.column_family(column_family_id)

        self.assertIsInstance(column_family, ColumnFamily)
        self.assertEqual(column_family.column_family_id, column_family_id)
        self.assertIs(column_family._table, table)
        self.assertIsNone(column_family.gc_rule)

    def test_column_family_explicit(self):
        from google.cloud.bigtable.column_family import ColumnFamily

        table, column_family_id = self._column_family_helper()
        gc_rule = mock.Mock(spec=[])
        column_family = table.column_family(column_family_id, gc_rule=gc_rule)

        self.assertIsInstance(column_family, ColumnFamily)
        self.assertEqual(column_family.column_family_id, column_family_id)
        self.assertIs(column_family._table, table)
        self.assertIs(column_family.gc_rule, gc_rule)

    def _row_methods_helper(self):
        client, instance = self._mock_instance()
        table = self._make_one(self.TABLE_ID, instance)
        row_key = b"row_key"

        return table, row_key

    def test_row_factory_direct(self):
        import warnings
        from google.cloud.bigtable.row import DirectRow

        table, row_key = self._row_methods_helper()

        with warnings.catch_warnings(record=True) as warned:
            row = table.row(row_key)

        self.assertIsInstance(row, DirectRow)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)

        self.assertEqual(len(warned), 1)
        self.assertIs(warned[0].category, DeprecationWarning)

    def test_row_factory_conditional(self):
        import warnings
        from google.cloud.bigtable.row import ConditionalRow

        table, row_key = self._row_methods_helper()
        filter_ = object()

        with warnings.catch_warnings(record=True) as warned:
            row = table.row(row_key, filter_=filter_)

        self.assertIsInstance(row, ConditionalRow)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)

        self.assertEqual(len(warned), 1)
        self.assertIs(warned[0].category, DeprecationWarning)

    def test_row_factory_append(self):
        import warnings
        from google.cloud.bigtable.row import AppendRow

        table, row_key = self._row_methods_helper()

        with warnings.catch_warnings(record=True) as warned:
            row = table.row(row_key, append=True)

        self.assertIsInstance(row, AppendRow)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)

        self.assertEqual(len(warned), 1)
        self.assertIs(warned[0].category, DeprecationWarning)

    def test_row_factory_failure(self):
        import warnings

        table, row_key = self._row_methods_helper()

        with warnings.catch_warnings(record=True) as warned:
            with self.assertRaises(ValueError):
                table.row(row_key, filter_=object(), append=True)

        self.assertEqual(len(warned), 1)
        self.assertIs(warned[0].category, DeprecationWarning)

    def test_direct_row(self):
        from google.cloud.bigtable.row import DirectRow

        table, row_key = self._row_methods_helper()
        row = table.direct_row(row_key)

        self.assertIsInstance(row, DirectRow)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)

    def test_conditional_row(self):
        from google.cloud.bigtable.row import ConditionalRow

        table, row_key = self._row_methods_helper()
        filter_ = object()
        row = table.conditional_row(row_key, filter_=filter_)

        self.assertIsInstance(row, ConditionalRow)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)

    def test_append_row(self):
        from google.cloud.bigtable.row import AppendRow

        table, row_key = self._row_methods_helper()
        row = table.append_row(row_key)

        self.assertIsInstance(row, AppendRow)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)

    def _mock_admin_client(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable.instance import Instance

        admin_client = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        client = mock.create_autospec(Client, table_admin_client=admin_client)
        client.project = self.PROJECT_ID
        instance = mock.create_autospec(Instance, _client=client)
        instance.instance_id = self.INSTANCE_ID
        instance.name = self.INSTANCE_NAME

        return admin_client, client, instance

    def _create_test_helper(self, split_keys=[], column_families={}):
        from google.cloud.bigtable_admin_v2.proto import table_pb2
        from google.cloud.bigtable_admin_v2.proto import bigtable_table_admin_pb2
        from google.cloud.bigtable.column_family import ColumnFamily

        admin_client, client, instance = self._mock_admin_client()

        table = self._make_one(self.TABLE_ID, instance)

        # Perform the method and check the result.
        table.create(column_families=column_families, initial_split_keys=split_keys)

        families = {
            id: ColumnFamily(id, self, rule).to_pb()
            for (id, rule) in column_families.items()
        }

        split = bigtable_table_admin_pb2.CreateTableRequest.Split
        splits = [split(key=split_key) for split_key in split_keys]

        admin_client.create_table.assert_called_once_with(
            parent=self.INSTANCE_NAME,
            table=table_pb2.Table(column_families=families),
            table_id=self.TABLE_ID,
            initial_splits=splits,
        )

    def test_create(self):
        self._create_test_helper()

    def test_create_with_families(self):
        from google.cloud.bigtable.column_family import MaxVersionsGCRule

        families = {"family": MaxVersionsGCRule(5)}
        self._create_test_helper(column_families=families)

    def test_create_with_split_keys(self):
        self._create_test_helper(split_keys=[b"split1", b"split2", b"split3"])

    def test_exists_hit(self):
        from google.cloud.bigtable.table import VIEW_NAME_ONLY

        admin_client, client, instance = self._mock_admin_client()
        admin_client.get_table.return_value = mock.Mock(spec=[])
        table = self._make_one(self.TABLE_ID, instance)

        self.assertTrue(table.exists())

        admin_client.get_table.assert_called_once_with(table.name, VIEW_NAME_ONLY)

    def test_exists_miss(self):
        from google.api_core.exceptions import NotFound
        from google.cloud.bigtable.table import VIEW_NAME_ONLY

        admin_client, client, instance = self._mock_admin_client()
        admin_client.get_table.side_effect = [NotFound("testing")]
        table = self._make_one(self.TABLE_ID, instance)

        self.assertFalse(table.exists())

        admin_client.get_table.assert_called_once_with(table.name, VIEW_NAME_ONLY)

    def test_exists_bad_request(self):
        from google.api_core.exceptions import BadRequest
        from google.cloud.bigtable.table import VIEW_NAME_ONLY

        admin_client, client, instance = self._mock_admin_client()
        admin_client.get_table.side_effect = [BadRequest("testing")]
        table = self._make_one(self.TABLE_ID, instance)

        with self.assertRaises(BadRequest):
            table.exists()

        admin_client.get_table.assert_called_once_with(table.name, VIEW_NAME_ONLY)

    def test_delete_hit(self):
        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)

        self.assertIsNone(table.delete())

        admin_client.delete_table.assert_called_once_with(table.name)

    def test_delete_miss(self):
        from google.api_core.exceptions import NotFound

        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)
        admin_client.delete_table.side_effect = [NotFound("testing")]

        with self.assertRaises(NotFound):
            table.delete()

        admin_client.delete_table.assert_called_once_with(table.name)

    def test_backup_factory_defaults(self):
        from google.cloud.bigtable.backup import Backup

        instance = self._make_one(self.INSTANCE_ID, None)
        table = self._make_one(self.TABLE_ID, instance)
        backup = table.backup(self.BACKUP_ID)

        self.assertIsInstance(backup, Backup)
        self.assertEqual(backup.backup_id, self.BACKUP_ID)
        self.assertIs(backup._instance, instance)
        self.assertIsNone(backup._cluster)
        self.assertEqual(backup.table_id, self.TABLE_ID)
        self.assertIsNone(backup._expire_time)

        self.assertIsNone(backup._parent)
        self.assertIsNone(backup._source_table)
        self.assertIsNone(backup._start_time)
        self.assertIsNone(backup._end_time)
        self.assertIsNone(backup._size_bytes)
        self.assertIsNone(backup._state)

    def test_backup_factory_non_defaults(self):
        import datetime
        from google.cloud._helpers import UTC
        from google.cloud.bigtable.backup import Backup

        instance = self._make_one(self.INSTANCE_ID, None)
        table = self._make_one(self.TABLE_ID, instance)
        timestamp = datetime.datetime.utcnow().replace(tzinfo=UTC)
        backup = table.backup(
            self.BACKUP_ID,
            cluster_id=self.CLUSTER_ID,
            expire_time=timestamp,
        )

        self.assertIsInstance(backup, Backup)
        self.assertEqual(backup.backup_id, self.BACKUP_ID)
        self.assertIs(backup._instance, instance)

        self.assertEqual(backup.backup_id, self.BACKUP_ID)
        self.assertIs(backup._cluster, self.CLUSTER_ID)
        self.assertEqual(backup.table_id, self.TABLE_ID)
        self.assertEqual(backup._expire_time, timestamp)
        self.assertIsNone(backup._start_time)
        self.assertIsNone(backup._end_time)
        self.assertIsNone(backup._size_bytes)
        self.assertIsNone(backup._state)

    def _list_backups_helper(self, cluster_id=None, filter_=None, **kwargs):
        from google.cloud.bigtable_admin_v2.proto import table_pb2
        from google.cloud.bigtable.backup import Backup

        admin_client, client, instance = self._mock_admin_client()
        client.table_data_client.table_path.return_value = self.TABLE_NAME
        table = self._make_one(self.TABLE_ID, instance)

        parent = self.INSTANCE_NAME + "/clusters/cluster"
        backups = [
            table_pb2.Backup(name=parent + "/backups/op1"),
            table_pb2.Backup(name=parent + "/backups/op2"),
            table_pb2.Backup(name=parent + "/backups/op3"),
        ]
        admin_client.list_backups.return_value = iter(backups)

        backups_filter = "source_table:{}".format(self.TABLE_NAME)

        if filter_:
            backups_filter = "({}) AND ({})".format(backups_filter, filter_)

        backups = table.list_backups(cluster_id=cluster_id, filter_=filter_, **kwargs)

        for backup in backups:
            self.assertIsInstance(backup, Backup)

        if not cluster_id:
            cluster_id = "-"
        parent = "{}/clusters/{}".format(self.INSTANCE_NAME, cluster_id)

        admin_client.list_backups.assert_called_once_with(
            parent=parent,
            filter_=backups_filter,
            order_by=kwargs.get("order_by", None),
            page_size=kwargs.get("page_size", 0),
        )

    def test_list_backups_defaults(self):
        self._list_backups_helper()

    def test_list_backups_w_options(self):
        self._list_backups_helper(
            cluster_id="cluster", filter_="filter", order_by="order_by", page_size=10
        )

    def _restore_helper(self, backup_name=None):
        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)

        if backup_name:
            future = table.restore(self.TABLE_ID, backup_name=self.BACKUP_NAME)
        else:
            future = table.restore(self.TABLE_ID, self.CLUSTER_ID, self.BACKUP_ID)
        self.assertIs(future, admin_client.restore_table.return_value)

        admin_client.restore_table.assert_called_once_with(
            parent=self.INSTANCE_NAME,
            table_id=self.TABLE_ID,
            backup=self.BACKUP_NAME,
        )

    def test_restore_table_w_backup_id(self):
        self._restore_helper()

    def test_restore_table_w_backup_name(self):
        self._restore_helper(backup_name=self.BACKUP_NAME)

    def _truncate_helper(self, timeout=None):
        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)

        kwargs = {}

        if timeout is not None:
            kwargs["timeout"] = timeout

        result = table.truncate(**kwargs)

        self.assertIsNone(result)

        admin_client.drop_row_range.assert_called_once_with(
            name=table.name, delete_all_data_from_table=True, **kwargs
        )

    def test_truncate_defaults(self):
        self._truncate_helper()

    def test_truncate_w_timeout(self):
        timeout = 120
        self._truncate_helper(timeout=timeout)

    def _drop_by_prefix_helper(self, timeout=None):
        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)

        row_key_prefix = "row-key-prefix"
        kwargs = {}

        if timeout is not None:
            kwargs["timeout"] = timeout

        result = table.drop_by_prefix(row_key_prefix=row_key_prefix, **kwargs)

        self.assertIsNone(result)

        admin_client.drop_row_range.assert_called_once_with(
            name=table.name, row_key_prefix=row_key_prefix.encode("utf-8"), **kwargs
        )

    def test_drop_by_prefix_defaults(self):
        self._drop_by_prefix_helper()

    def test_drop_by_prefix_w_timeout(self):
        timeout = 120
        self._drop_by_prefix_helper(timeout=timeout)

    def test_list_column_families(self):
        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)
        cf_id = "foo"
        column_family = _ColumnFamilyPB()
        response_pb = _TablePB(column_families={cf_id: column_family})
        admin_client.get_table.side_effect = [response_pb]
        expected_result = {cf_id: table.column_family(cf_id)}

        self.assertEqual(table.list_column_families(), expected_result)

        admin_client.get_table.assert_called_once_with(table.name)

    def test_get_cluster_states(self):
        from google.cloud.bigtable.enums import Table as enum_table
        from google.cloud.bigtable.table import ClusterState

        INITIALIZING = enum_table.ReplicationState.INITIALIZING
        PLANNED_MAINTENANCE = enum_table.ReplicationState.PLANNED_MAINTENANCE
        READY = enum_table.ReplicationState.READY
        response_pb = _TablePB(
            cluster_states={
                "cluster-id1": _ClusterStatePB(INITIALIZING),
                "cluster-id2": _ClusterStatePB(PLANNED_MAINTENANCE),
                "cluster-id3": _ClusterStatePB(READY),
            }
        )
        admin_client, client, instance = self._mock_admin_client()
        admin_client.get_table.side_effect = [response_pb]
        table = self._make_one(self.TABLE_ID, instance)

        expected_result = {
            u"cluster-id1": ClusterState(INITIALIZING),
            u"cluster-id2": ClusterState(PLANNED_MAINTENANCE),
            u"cluster-id3": ClusterState(READY),
        }

        self.assertEqual(table.get_cluster_states(), expected_result)

        admin_client.get_table.assert_called_once_with(
            table.name,
            view=enum_table.View.REPLICATION_VIEW,
        )

    def test_get_iam_policy(self):
        from google.iam.v1 import policy_pb2
        from google.cloud.bigtable.policy import BIGTABLE_ADMIN_ROLE
        from google.cloud.bigtable.policy import Policy

        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)

        version = 1
        etag = b"etag_v1"
        members = ["serviceAccount:service_acc1@test.com", "user:user1@test.com"]
        bindings = [{"role": BIGTABLE_ADMIN_ROLE, "members": members}]
        policy = policy_pb2.Policy(version=version, etag=etag, bindings=bindings)
        admin_client.get_iam_policy.return_value = policy

        result = table.get_iam_policy()

        self.assertIsInstance(result, Policy)
        self.assertEqual(result.version, version)
        self.assertEqual(result.etag, etag)

        admins = result.bigtable_admins
        self.assertEqual(len(admins), len(members))

        for found, expected in zip(sorted(admins), sorted(members)):
            self.assertEqual(found, expected)

        admin_client.get_iam_policy.assert_called_once_with(resource=table.name)

    def test_set_iam_policy(self):
        from google.iam.v1 import policy_pb2
        from google.cloud.bigtable.policy import Policy
        from google.cloud.bigtable.policy import BIGTABLE_ADMIN_ROLE

        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)

        version = 1
        etag = b"etag_v1"
        members = ["serviceAccount:service_acc1@test.com", "user:user1@test.com"]
        bindings = [{"role": BIGTABLE_ADMIN_ROLE, "members": sorted(members)}]
        policy_pb = policy_pb2.Policy(version=version, etag=etag, bindings=bindings)

        admin_client.set_iam_policy.return_value = policy_pb

        policy = Policy(etag=etag, version=version)
        policy[BIGTABLE_ADMIN_ROLE] = [
            "user:user1@test.com",
            "serviceAccount:service_acc1@test.com",
        ]

        result = table.set_iam_policy(policy)

        self.assertIsInstance(result, Policy)
        self.assertEqual(result.version, version)
        self.assertEqual(result.etag, etag)

        admins = result.bigtable_admins
        self.assertEqual(len(admins), len(members))

        for found, expected in zip(sorted(admins), sorted(members)):
            self.assertEqual(found, expected)

        admin_client.set_iam_policy.assert_called_once_with(
            resource=table.name, policy=policy_pb
        )

    def test_test_iam_permissions(self):
        from google.iam.v1 import iam_policy_pb2

        admin_client, client, instance = self._mock_admin_client()
        table = self._make_one(self.TABLE_ID, instance)

        permissions = ["bigtable.tables.mutateRows", "bigtable.tables.readRows"]

        response = iam_policy_pb2.TestIamPermissionsResponse(permissions=permissions)

        admin_client.test_iam_permissions.return_value = response

        result = table.test_iam_permissions(permissions)

        self.assertEqual(result, permissions)
        admin_client.test_iam_permissions.assert_called_once_with(
            resource=table.name, permissions=permissions
        )

    def _mock_data_client(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable.instance import Instance

        transport = mock.Mock(spec=["read_rows"])
        data_client = mock.create_autospec(
            bigtable_client.BigtableClient,
            transport=transport,
            spec_set=False,
        )
        client = mock.create_autospec(Client, table_data_client=data_client)
        client.project = self.PROJECT_ID
        instance = mock.create_autospec(Instance, _client=client)
        instance.instance_id = self.INSTANCE_ID
        instance.name = self.INSTANCE_NAME

        return data_client, client, instance

    def _read_row_helper(self, responses, expected_result):

        from google.cloud.bigtable.row_set import RowSet
        from google.cloud.bigtable.row_filters import RowSampleFilter

        _, _, instance = self._mock_data_client()
        table = self._make_one(self.TABLE_ID, instance)

        table.read_rows = mock.Mock(return_value=iter(responses))
        filter_obj = RowSampleFilter(0.33)

        result = table.read_row(self.ROW_KEY, filter_=filter_obj)

        self.assertEqual(result, expected_result)

        row_set = RowSet()
        row_set.add_row_key(self.ROW_KEY)

        table.read_rows.assert_called_once_with(filter_=filter_obj, row_set=row_set)

    def test_read_row_w_no_responses(self):
        self._read_row_helper([], None)

    def test_read_row_w_response(self):
        from google.cloud.bigtable.row_data import PartialRowData

        row_data = PartialRowData(row_key=self.ROW_KEY)
        self._read_row_helper([row_data], row_data)

    def test_read_row_w_too_many_responses(self):
        from google.cloud.bigtable.row_data import PartialRowData

        row_data_1 = PartialRowData(row_key=self.ROW_KEY)
        row_data_2 = PartialRowData(row_key=b"other-row-key")
        with self.assertRaises(ValueError):
            self._read_row_helper([row_data_1, row_data_2], object())

    def _read_rows_helper(
        self,
        app_profile_id=None,
        start_key=None,
        end_key=None,
        limit=None,
        filter_=None,
        end_inclusive=None,
        row_set=None,
        retry=None,
    ):
        from google.cloud.bigtable.row_data import PartialRowsData
        from google.cloud.bigtable.row_data import DEFAULT_RETRY_READ_ROWS

        data_client, _, instance = self._mock_data_client()
        if app_profile_id is None:
            table = self._make_one(self.TABLE_ID, instance)
        else:
            table = self._make_one(
                self.TABLE_ID, instance, app_profile_id=app_profile_id
            )

        kwargs = {}

        if start_key is not None:
            kwargs["start_key"] = start_key

        if end_key is not None:
            kwargs["end_key"] = end_key

        if limit is not None:
            kwargs["limit"] = limit

        if filter_ is not None:
            kwargs["filter_"] = filter_

        if end_inclusive is not None:
            kwargs["end_inclusive"] = end_inclusive
            expected_end_inclusive = end_inclusive
        else:
            expected_end_inclusive = False

        if row_set is not None:
            kwargs["row_set"] = row_set

        if retry is not None:
            kwargs["retry"] = retry
            expected_retry = retry
        else:
            expected_retry = DEFAULT_RETRY_READ_ROWS

        patch = mock.patch("google.cloud.bigtable.table._create_row_request")
        with patch as create_row_request:
            result = table.read_rows(**kwargs)

        expected_result = PartialRowsData(
            read_method=data_client.transport.read_rows,
            request=create_row_request.return_value,
            retry=expected_retry,
        )
        self.assertEqual(result.read_method, expected_result.read_method)
        self.assertEqual(result.request, expected_result.request)
        self.assertEqual(result.retry, expected_result.retry)

        create_row_request.assert_called_once_with(
            table.name,
            start_key=start_key,
            end_key=end_key,
            limit=limit,
            filter_=filter_,
            end_inclusive=expected_end_inclusive,
            app_profile_id=app_profile_id,
            row_set=row_set,
        )

    def test_read_rows_defaults(self):
        self._read_rows_helper()

    def test_read_rows_explicit(self):
        from google.api_core.retry import Retry

        app_profile_id = "app-profile-id"
        start_key = b"start-key"
        end_key = b"end-key"
        filter_ = mock.Mock(spec=[])
        limit = 22
        end_inclusive = True
        row_set = mock.Mock(spec=[])
        retry = Retry()

        self._read_rows_helper(
            app_profile_id=app_profile_id,
            start_key=start_key,
            end_key=end_key,
            limit=limit,
            filter_=filter_,
            end_inclusive=end_inclusive,
            row_set=row_set,
            retry=retry,
        )

    def _yield_rows_helper(
        self,
        start_key=None,
        end_key=None,
        limit=None,
        filter_=None,
        end_inclusive=None,
        row_set=None,
        retry=None,
    ):
        import warnings

        data_client, _, instance = self._mock_data_client()
        table = self._make_one(self.TABLE_ID, instance)
        table.read_rows = mock.MagicMock()

        kwargs = {}

        if start_key is not None:
            kwargs["start_key"] = start_key

        if end_key is not None:
            kwargs["end_key"] = end_key

        if limit is not None:
            kwargs["limit"] = limit

        if filter_ is not None:
            kwargs["filter_"] = filter_

        if end_inclusive is not None:
            kwargs["end_inclusive"] = end_inclusive

        if row_set is not None:
            kwargs["row_set"] = row_set

        if retry is not None:
            kwargs["retry"] = retry

        with warnings.catch_warnings(record=True) as warned:
            iterable = table.yield_rows(**kwargs)

        self.assertIs(iterable, table.read_rows.return_value)

        table.read_rows.assert_called_once_with(**kwargs)

        self.assertEqual(len(warned), 1)
        self.assertIs(warned[0].category, DeprecationWarning)

    def test_yield_rows_defaults(self):
        self._yield_rows_helper()

    def test_yield_rows_explicit(self):
        from google.api_core.retry import Retry

        start_key = b"start-key"
        end_key = b"end-key"
        filter_ = mock.Mock(spec=[])
        limit = 22
        end_inclusive = True
        row_set = mock.Mock(spec=[])
        retry = Retry()

        self._yield_rows_helper(
            start_key=start_key,
            end_key=end_key,
            limit=limit,
            filter_=filter_,
            end_inclusive=end_inclusive,
            row_set=row_set,
            retry=retry,
        )

    def _mutate_rows_helper(
        self, mutation_timeout=None, app_profile_id=None, retry=None, timeout=None
    ):
        from google.rpc.status_pb2 import Status
        from google.cloud.bigtable.table import DEFAULT_RETRY

        _, client, instance = self._mock_data_client()

        ctor_kwargs = {}

        if mutation_timeout is not None:
            ctor_kwargs["mutation_timeout"] = mutation_timeout

        if app_profile_id is not None:
            ctor_kwargs["app_profile_id"] = app_profile_id

        table = self._make_one(self.TABLE_ID, instance, **ctor_kwargs)

        rows = [mock.MagicMock(), mock.MagicMock()]
        response = [Status(code=0), Status(code=1)]
        instance_mock = mock.Mock(return_value=response)
        klass_mock = mock.patch(
            "google.cloud.bigtable.table._RetryableMutateRowsWorker",
            new=mock.MagicMock(return_value=instance_mock),
        )

        call_kwargs = {}

        if retry is not None:
            call_kwargs["retry"] = retry

        if timeout is not None:
            expected_timeout = call_kwargs["timeout"] = timeout
        else:
            expected_timeout = mutation_timeout

        with klass_mock:
            statuses = table.mutate_rows(rows, **call_kwargs)

        result = [status.code for status in statuses]
        expected_result = [0, 1]
        self.assertEqual(result, expected_result)

        klass_mock.new.assert_called_once_with(
            client,
            table.name,
            rows,
            app_profile_id=app_profile_id,
            timeout=expected_timeout,
        )

        if retry is not None:
            instance_mock.assert_called_once_with(retry=retry)
        else:
            instance_mock.assert_called_once_with(retry=DEFAULT_RETRY)

    def test_mutate_rows_w_default_mutation_timeout_app_profile_id(self):
        self._mutate_rows_helper()

    def test_mutate_rows_w_mutation_timeout(self):
        mutation_timeout = 123
        self._mutate_rows_helper(mutation_timeout=mutation_timeout)

    def test_mutate_rows_w_app_profile_id(self):
        app_profile_id = "profile-123"
        self._mutate_rows_helper(app_profile_id=app_profile_id)

    def test_mutate_rows_w_retry(self):
        retry = mock.Mock()
        self._mutate_rows_helper(retry=retry)

    def test_mutate_rows_w_timeout_arg(self):
        timeout = 123
        self._mutate_rows_helper(timeout=timeout)

    def test_mutate_rows_w_mutation_timeout_and_timeout_arg(self):
        mutation_timeout = 123
        timeout = 456
        self._mutate_rows_helper(mutation_timeout=mutation_timeout, timeout=timeout)

    def _sample_row_keys_helper(self, app_profile_id=None):
        data_client, client, instance = self._mock_data_client()

        if app_profile_id is None:
            table = self._make_one(self.TABLE_ID, instance)
        else:
            table = self._make_one(
                self.TABLE_ID,
                instance,
                app_profile_id=app_profile_id,
            )

        result = table.sample_row_keys()

        self.assertEqual(result, data_client.sample_row_keys.return_value)

        data_client.sample_row_keys.assert_called_once_with(
            table.name,
            app_profile_id=app_profile_id,
        )

    def test_sample_row_keys_wo_app_profile_id(self):
        self._sample_row_keys_helper()

    def test_sample_row_keys_w_app_profile_id(self):
        app_profile_id = "app-profile-123"
        self._sample_row_keys_helper(app_profile_id=app_profile_id)

    def test_mutations_batcher_factory(self):
        flush_count = 100
        max_row_bytes = 1000
        table = self._make_one(self.TABLE_ID, None)

        mutation_batcher = table.mutations_batcher(
            flush_count=flush_count, max_row_bytes=max_row_bytes
        )

        self.assertEqual(mutation_batcher.table.table_id, self.TABLE_ID)
        self.assertEqual(mutation_batcher.flush_count, flush_count)
        self.assertEqual(mutation_batcher.max_row_bytes, max_row_bytes)


class Test__RetryableMutateRowsWorker(unittest.TestCase):
    from grpc import StatusCode

    PROJECT_ID = "project-id"
    INSTANCE_ID = "instance-id"
    INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID
    TABLE_ID = "table-id"

    # RPC Status Codes
    SUCCESS = StatusCode.OK.value[0]
    RETRYABLE_1 = StatusCode.DEADLINE_EXCEEDED.value[0]
    RETRYABLE_2 = StatusCode.ABORTED.value[0]
    NON_RETRYABLE = StatusCode.CANCELLED.value[0]

    @staticmethod
    def _get_target_class_for_worker():
        from google.cloud.bigtable.table import _RetryableMutateRowsWorker

        return _RetryableMutateRowsWorker

    def _make_worker(self, *args, **kwargs):
        return self._get_target_class_for_worker()(*args, **kwargs)

    def _mock_table(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable.client import Client
        from google.cloud.bigtable.instance import Instance
        from google.cloud.bigtable.table import Table

        transport = mock.Mock(spec=["read_rows"])
        data_client = mock.create_autospec(
            bigtable_client.BigtableClient,
            transport=transport,
            spec_set=False,
        )
        client = mock.create_autospec(Client, table_data_client=data_client)
        client.project = self.PROJECT_ID
        instance = mock.create_autospec(Instance, _client=client)
        instance.instance_id = self.INSTANCE_ID
        instance.name = self.INSTANCE_NAME
        table = mock.create_autospec(Table)
        table.table_id = self.TABLE_ID

        return data_client, client, instance, table

    def _make_responses_statuses(self, codes):
        from google.rpc.status_pb2 import Status

        response = [Status(code=code) for code in codes]
        return response

    def _make_responses(self, codes):
        import six
        from google.cloud.bigtable_v2.proto.bigtable_pb2 import MutateRowsResponse
        from google.rpc.status_pb2 import Status

        entries = [
            MutateRowsResponse.Entry(index=i, status=Status(code=codes[i]))
            for i in six.moves.xrange(len(codes))
        ]
        return MutateRowsResponse(entries=entries)

    def test___call___w_empty_rows(self):
        _, client, _, table = self._mock_table()
        worker = self._make_worker(client, table.name, [])
        mocked = worker._do_mutate_retryable_rows = mock.Mock()

        statuses = worker()

        self.assertEqual(len(statuses), 0)

        mocked.assert_called_once_with()

    def test___call___w_retry_None(self):
        from google.cloud.bigtable.row import DirectRow

        _, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        row_3 = DirectRow(row_key=b"row_key_3", table=table)
        row_3.set_cell("cf", b"col", b"value3")

        worker = self._make_worker(client, table.name, [row_1, row_2, row_3])
        mocked = worker._do_mutate_retryable_rows = mock.Mock()

        statuses = worker(retry=None)

        self.assertEqual(statuses, [None, None, None])

        mocked.assert_called_once_with()

    def test___call___w__bigtable_retry_error(self):
        from google.cloud.bigtable.row import DirectRow
        from google.cloud.bigtable.table import _BigtableRetryableError

        _, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        row_3 = DirectRow(row_key=b"row_key_3", table=table)
        row_3.set_cell("cf", b"col", b"value3")

        worker = self._make_worker(client, table.name, [row_1, row_2, row_3])
        mocked = worker._do_mutate_retryable_rows = mock.Mock(
            side_effect=[_BigtableRetryableError("testing")]
        )

        statuses = worker(retry=None)

        self.assertEqual(statuses, [None, None, None])

        mocked.assert_called_once_with()

    def test___call___w__retry_error(self):
        from google.cloud.bigtable.row import DirectRow
        from google.api_core.exceptions import RetryError

        _, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        row_3 = DirectRow(row_key=b"row_key_3", table=table)
        row_3.set_cell("cf", b"col", b"value3")

        worker = self._make_worker(client, table.name, [row_1, row_2, row_3])
        mocked = worker._do_mutate_retryable_rows = mock.Mock(
            side_effect=[RetryError("testing", "cause")]
        )

        statuses = worker(retry=None)

        self.assertEqual(statuses, [None, None, None])

        mocked.assert_called_once_with()

    def test___call___w_retry(self):
        from google.cloud.bigtable.row import DirectRow
        from google.cloud.bigtable.table import DEFAULT_RETRY

        _, client, _, table = self._mock_table()

        # Setup:
        #   - Mutate 3 rows.
        # Action:
        #   - Initial attempt will mutate all 3 rows.
        # Expectation:
        #   - First attempt will result in one retryable error.
        #   - Second attempt will result in success for the retry-ed row.
        #   - Check MutateRows is called twice.
        #   - State of responses_statuses should be
        #       [success, success, non-retryable]

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        row_3 = DirectRow(row_key=b"row_key_3", table=table)
        row_3.set_cell("cf", b"col", b"value3")
        worker = self._make_worker(client, table.name, [row_1, row_2, row_3])

        mocked = worker._do_mutate_retryable_rows = mock.Mock()
        retry = DEFAULT_RETRY.with_delay(initial=0.1)

        statuses = worker(retry=retry)

        self.assertEqual(statuses, [None, None, None])

        mocked.assert_called_once_with()

    def test__do_mutate_retryable_rows_empty_rows(self):
        data_client, client, _, table = self._mock_table()
        worker = self._make_worker(client, table.name, [])

        statuses = worker._do_mutate_retryable_rows()

        self.assertEqual(len(statuses), 0)

        data_client.mutate_rows.assert_not_called()

    def test__do_mutate_retryable_rows_wo_retry_w_timeout(self):
        from google.cloud.bigtable.row import DirectRow

        # Setup:
        #   - Mutate 2 rows.
        # Action:
        #   - Initial attempt will mutate all 2 rows.
        # Expectation:
        #   - Expect [success, non-retryable]

        data_client, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        timeout = 123
        worker = self._make_worker(
            client,
            table.name,
            [row_1, row_2],
            timeout=timeout,
        )

        response = self._make_responses([self.SUCCESS, self.NON_RETRYABLE])

        # Patch the stub used by the API method.
        data_client.mutate_rows = mock.Mock(side_effect=[[response]])

        patch = mock.patch("google.cloud.bigtable.table._compile_mutation_entries")
        with patch as _compile_mutation_entries:
            statuses = worker._do_mutate_retryable_rows()

        result = [status.code for status in statuses]
        expected_result = [self.SUCCESS, self.NON_RETRYABLE]

        self.assertEqual(result, expected_result)

        data_client.mutate_rows.assert_called_once_with(
            table.name,
            _compile_mutation_entries.return_value,
            app_profile_id=None,
            retry=None,
            timeout=mock.ANY,
        )
        self.assertEqual(
            data_client.mutate_rows.mock_calls[0].kwargs["timeout"]._deadline,
            timeout,
        )

    def test__do_mutate_retryable_rows_w_retryable_error(self):
        from google.api_core.exceptions import ServiceUnavailable
        from google.cloud.bigtable.row import DirectRow
        from google.cloud.bigtable.table import _BigtableRetryableError

        # Setup:
        #   - Mutate 2 rows.
        # Action:
        #   - Initial attempt will mutate all 2 rows.
        # Expectation:
        #   - Expect [success, non-retryable]

        data_client, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        timeout = 123
        worker = self._make_worker(
            client,
            table.name,
            [row_1, row_2],
            timeout=timeout,
        )

        # Patch the stub used by the API method.
        data_client.mutate_rows = mock.Mock(
            side_effect=[ServiceUnavailable("testing")],
        )

        patch = mock.patch("google.cloud.bigtable.table._compile_mutation_entries")
        with patch as _compile_mutation_entries:
            with self.assertRaises(_BigtableRetryableError):
                worker._do_mutate_retryable_rows()

        data_client.mutate_rows.assert_called_once_with(
            table.name,
            _compile_mutation_entries.return_value,
            app_profile_id=None,
            retry=None,
            timeout=mock.ANY,
        )
        self.assertEqual(
            data_client.mutate_rows.mock_calls[0].kwargs["timeout"]._deadline,
            timeout,
        )

    def test__do_mutate_retryable_rows_w_retry(self):
        from google.cloud.bigtable.row import DirectRow
        from google.cloud.bigtable.table import _BigtableRetryableError

        # Setup:
        #   - Mutate 3 rows.
        # Action:
        #   - Initial attempt will mutate all 3 rows.
        # Expectation:
        #   - Second row returns retryable error code, so expect a raise.
        #   - State of responses_statuses should be
        #       [success, retryable, non-retryable]

        data_client, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        row_3 = DirectRow(row_key=b"row_key_3", table=table)
        row_3.set_cell("cf", b"col", b"value3")
        worker = self._make_worker(client, table.name, [row_1, row_2, row_3])

        response = self._make_responses(
            [self.SUCCESS, self.RETRYABLE_1, self.NON_RETRYABLE]
        )
        data_client.mutate_rows = mock.Mock(side_effect=[[response]])

        patch = mock.patch("google.cloud.bigtable.table._compile_mutation_entries")
        with patch as _compile_mutation_entries:
            with self.assertRaises(_BigtableRetryableError):
                worker._do_mutate_retryable_rows()

        statuses = worker.responses_statuses
        result = [status.code for status in statuses]
        expected_result = [self.SUCCESS, self.RETRYABLE_1, self.NON_RETRYABLE]

        self.assertEqual(result, expected_result)

        data_client.mutate_rows.assert_called_once_with(
            table.name,
            _compile_mutation_entries.return_value,
            app_profile_id=None,
            retry=None,
        )

    def test__do_mutate_retryable_rows_second_retry(self):
        from google.cloud.bigtable.row import DirectRow
        from google.cloud.bigtable.table import _BigtableRetryableError

        # Setup:
        #   - Mutate 4 rows.
        #   - First try results:
        #       [success, retryable, non-retryable, retryable]
        # Action:
        #   - Second try should re-attempt the 'retryable' rows.
        # Expectation:
        #   - After second try:
        #       [success, success, non-retryable, retryable]
        #   - One of the rows tried second time returns retryable error code,
        #     so expect a raise.
        #   - Exception contains response whose index should be '3' even though
        #     only two rows were retried.

        data_client, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        row_3 = DirectRow(row_key=b"row_key_3", table=table)
        row_3.set_cell("cf", b"col", b"value3")
        row_4 = DirectRow(row_key=b"row_key_4", table=table)
        row_4.set_cell("cf", b"col", b"value4")
        worker = self._make_worker(client, table.name, [row_1, row_2, row_3, row_4])

        response = self._make_responses([self.SUCCESS, self.RETRYABLE_1])

        # Patch the stub used by the API method.
        data_client.mutate_rows = mock.Mock(side_effect=[[response]])

        worker.responses_statuses = self._make_responses_statuses(
            [self.SUCCESS, self.RETRYABLE_1, self.NON_RETRYABLE, self.RETRYABLE_2]
        )

        patch = mock.patch("google.cloud.bigtable.table._compile_mutation_entries")
        with patch as _compile_mutation_entries:
            with self.assertRaises(_BigtableRetryableError):
                worker._do_mutate_retryable_rows()

        statuses = worker.responses_statuses
        result = [status.code for status in statuses]
        expected_result = [
            self.SUCCESS,
            self.SUCCESS,
            self.NON_RETRYABLE,
            self.RETRYABLE_1,
        ]

        self.assertEqual(result, expected_result)

        data_client.mutate_rows.assert_called_once_with(
            table.name,
            _compile_mutation_entries.return_value,
            app_profile_id=None,
            retry=None,
        )

    def test__do_mutate_retryable_rows_second_try(self):
        from google.cloud.bigtable.row import DirectRow

        # Setup:
        #   - Mutate 4 rows.
        #   - First try results:
        #       [success, retryable, non-retryable, retryable]
        # Action:
        #   - Second try should re-attempt the 'retryable' rows.
        # Expectation:
        #   - After second try:
        #       [success, non-retryable, non-retryable, success]
        data_client, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        row_3 = DirectRow(row_key=b"row_key_3", table=table)
        row_3.set_cell("cf", b"col", b"value3")
        row_4 = DirectRow(row_key=b"row_key_4", table=table)
        row_4.set_cell("cf", b"col", b"value4")
        worker = self._make_worker(client, table.name, [row_1, row_2, row_3, row_4])

        response = self._make_responses([self.NON_RETRYABLE, self.SUCCESS])

        # Patch the stub used by the API method.
        data_client.mutate_rows = mock.Mock(side_effect=[[response]])

        worker.responses_statuses = self._make_responses_statuses(
            [self.SUCCESS, self.RETRYABLE_1, self.NON_RETRYABLE, self.RETRYABLE_2]
        )

        patch = mock.patch("google.cloud.bigtable.table._compile_mutation_entries")
        with patch as _compile_mutation_entries:
            statuses = worker._do_mutate_retryable_rows()

        result = [status.code for status in statuses]
        expected_result = [
            self.SUCCESS,
            self.NON_RETRYABLE,
            self.NON_RETRYABLE,
            self.SUCCESS,
        ]

        self.assertEqual(result, expected_result)

        data_client.mutate_rows.assert_called_once_with(
            table.name,
            _compile_mutation_entries.return_value,
            app_profile_id=None,
            retry=None,
        )

    def test__do_mutate_retryable_rows_second_try_no_retryable(self):
        from google.cloud.bigtable.row import DirectRow

        # Setup:
        #   - Mutate 2 rows.
        #   - First try results: [success, non-retryable]
        # Action:
        #   - Second try has no row to retry.
        # Expectation:
        #   - After second try: [success, non-retryable]
        data_client, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")
        worker = self._make_worker(client, table.name, [row_1, row_2])
        worker.responses_statuses = self._make_responses_statuses(
            [self.SUCCESS, self.NON_RETRYABLE]
        )

        statuses = worker._do_mutate_retryable_rows()

        result = [status.code for status in statuses]
        expected_result = [self.SUCCESS, self.NON_RETRYABLE]

        self.assertEqual(result, expected_result)

        data_client.mutate_rows.assert_not_called()

    def test__do_mutate_retryable_rows_mismatch_num_responses(self):
        from google.cloud.bigtable.row import DirectRow

        data_client, client, _, table = self._mock_table()

        row_1 = DirectRow(row_key=b"row_key", table=table)
        row_1.set_cell("cf", b"col", b"value1")
        row_2 = DirectRow(row_key=b"row_key_2", table=table)
        row_2.set_cell("cf", b"col", b"value2")

        response = self._make_responses([self.SUCCESS])

        data_client.mutate_rows = mock.Mock(side_effect=[[response]])

        worker = self._make_worker(client, table.name, [row_1, row_2])
        with self.assertRaises(RuntimeError):
            worker._do_mutate_retryable_rows()


class Test__create_row_request(unittest.TestCase):
    def _call_fut(
        self,
        table_name,
        start_key=None,
        end_key=None,
        filter_=None,
        limit=None,
        end_inclusive=False,
        app_profile_id=None,
        row_set=None,
    ):

        from google.cloud.bigtable.table import _create_row_request

        return _create_row_request(
            table_name,
            start_key=start_key,
            end_key=end_key,
            filter_=filter_,
            limit=limit,
            end_inclusive=end_inclusive,
            app_profile_id=app_profile_id,
            row_set=row_set,
        )

    def test_table_name_only(self):
        table_name = "table_name"
        result = self._call_fut(table_name)
        expected_result = _ReadRowsRequestPB(table_name=table_name)
        self.assertEqual(result, expected_result)

    def test_row_range_row_set_conflict(self):
        with self.assertRaises(ValueError):
            self._call_fut(None, end_key=object(), row_set=object())

    def test_row_range_start_key(self):
        table_name = "table_name"
        start_key = b"start_key"
        result = self._call_fut(table_name, start_key=start_key)
        expected_result = _ReadRowsRequestPB(table_name=table_name)
        expected_result.rows.row_ranges.add(start_key_closed=start_key)
        self.assertEqual(result, expected_result)

    def test_row_range_end_key(self):
        table_name = "table_name"
        end_key = b"end_key"
        result = self._call_fut(table_name, end_key=end_key)
        expected_result = _ReadRowsRequestPB(table_name=table_name)
        expected_result.rows.row_ranges.add(end_key_open=end_key)
        self.assertEqual(result, expected_result)

    def test_row_range_both_keys(self):
        table_name = "table_name"
        start_key = b"start_key"
        end_key = b"end_key"
        result = self._call_fut(table_name, start_key=start_key, end_key=end_key)
        expected_result = _ReadRowsRequestPB(table_name=table_name)
        expected_result.rows.row_ranges.add(
            start_key_closed=start_key, end_key_open=end_key
        )
        self.assertEqual(result, expected_result)

    def test_row_range_both_keys_inclusive(self):
        table_name = "table_name"
        start_key = b"start_key"
        end_key = b"end_key"
        result = self._call_fut(
            table_name, start_key=start_key, end_key=end_key, end_inclusive=True
        )
        expected_result = _ReadRowsRequestPB(table_name=table_name)
        expected_result.rows.row_ranges.add(
            start_key_closed=start_key, end_key_closed=end_key
        )
        self.assertEqual(result, expected_result)

    def test_with_filter(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter

        table_name = "table_name"
        row_filter = RowSampleFilter(0.33)
        result = self._call_fut(table_name, filter_=row_filter)
        expected_result = _ReadRowsRequestPB(
            table_name=table_name, filter=row_filter.to_pb()
        )
        self.assertEqual(result, expected_result)

    def test_with_limit(self):
        table_name = "table_name"
        limit = 1337
        result = self._call_fut(table_name, limit=limit)
        expected_result = _ReadRowsRequestPB(table_name=table_name, rows_limit=limit)
        self.assertEqual(result, expected_result)

    def test_with_row_set(self):
        from google.cloud.bigtable.row_set import RowSet

        table_name = "table_name"
        row_set = RowSet()
        result = self._call_fut(table_name, row_set=row_set)
        expected_result = _ReadRowsRequestPB(table_name=table_name)
        self.assertEqual(result, expected_result)

    def test_with_app_profile_id(self):
        table_name = "table_name"
        limit = 1337
        app_profile_id = "app-profile-id"
        result = self._call_fut(table_name, limit=limit, app_profile_id=app_profile_id)
        expected_result = _ReadRowsRequestPB(
            table_name=table_name, rows_limit=limit, app_profile_id=app_profile_id
        )
        self.assertEqual(result, expected_result)


class Test_ClusterState(unittest.TestCase):
    def test___eq__(self):
        from google.cloud.bigtable.enums import Table as enum_table
        from google.cloud.bigtable.table import ClusterState

        READY = enum_table.ReplicationState.READY
        state1 = ClusterState(READY)
        state2 = ClusterState(READY)
        self.assertEqual(state1, state2)

    def test___eq__type_differ(self):
        from google.cloud.bigtable.enums import Table as enum_table
        from google.cloud.bigtable.table import ClusterState

        READY = enum_table.ReplicationState.READY
        state1 = ClusterState(READY)
        state2 = object()
        self.assertNotEqual(state1, state2)

    def test___ne__same_value(self):
        from google.cloud.bigtable.enums import Table as enum_table
        from google.cloud.bigtable.table import ClusterState

        READY = enum_table.ReplicationState.READY
        state1 = ClusterState(READY)
        state2 = ClusterState(READY)
        comparison_val = state1 != state2
        self.assertFalse(comparison_val)

    def test___ne__(self):
        from google.cloud.bigtable.enums import Table as enum_table
        from google.cloud.bigtable.table import ClusterState

        READY = enum_table.ReplicationState.READY
        INITIALIZING = enum_table.ReplicationState.INITIALIZING
        state1 = ClusterState(READY)
        state2 = ClusterState(INITIALIZING)
        self.assertNotEqual(state1, state2)

    def test__repr__(self):
        from google.cloud.bigtable.enums import Table as enum_table
        from google.cloud.bigtable.table import ClusterState

        STATE_NOT_KNOWN = enum_table.ReplicationState.STATE_NOT_KNOWN
        INITIALIZING = enum_table.ReplicationState.INITIALIZING
        PLANNED_MAINTENANCE = enum_table.ReplicationState.PLANNED_MAINTENANCE
        UNPLANNED_MAINTENANCE = enum_table.ReplicationState.UNPLANNED_MAINTENANCE
        READY = enum_table.ReplicationState.READY

        replication_dict = {
            STATE_NOT_KNOWN: "STATE_NOT_KNOWN",
            INITIALIZING: "INITIALIZING",
            PLANNED_MAINTENANCE: "PLANNED_MAINTENANCE",
            UNPLANNED_MAINTENANCE: "UNPLANNED_MAINTENANCE",
            READY: "READY",
        }

        self.assertEqual(
            str(ClusterState(STATE_NOT_KNOWN)), replication_dict[STATE_NOT_KNOWN]
        )
        self.assertEqual(
            str(ClusterState(INITIALIZING)), replication_dict[INITIALIZING]
        )
        self.assertEqual(
            str(ClusterState(PLANNED_MAINTENANCE)),
            replication_dict[PLANNED_MAINTENANCE],
        )
        self.assertEqual(
            str(ClusterState(UNPLANNED_MAINTENANCE)),
            replication_dict[UNPLANNED_MAINTENANCE],
        )
        self.assertEqual(str(ClusterState(READY)), replication_dict[READY])

        self.assertEqual(
            ClusterState(STATE_NOT_KNOWN).replication_state, STATE_NOT_KNOWN
        )
        self.assertEqual(ClusterState(INITIALIZING).replication_state, INITIALIZING)
        self.assertEqual(
            ClusterState(PLANNED_MAINTENANCE).replication_state, PLANNED_MAINTENANCE
        )
        self.assertEqual(
            ClusterState(UNPLANNED_MAINTENANCE).replication_state, UNPLANNED_MAINTENANCE
        )
        self.assertEqual(ClusterState(READY).replication_state, READY)


class Test__compile_mutation_entries(unittest.TestCase):
    def _call_fut(self, table_name, rows):
        from google.cloud.bigtable.table import _compile_mutation_entries

        return _compile_mutation_entries(table_name, rows)

    @mock.patch("google.cloud.bigtable.table._MAX_BULK_MUTATIONS", new=3)
    def test_w_too_many_mutations(self):
        from google.cloud.bigtable.row import DirectRow
        from google.cloud.bigtable.table import TooManyMutationsError

        table = mock.Mock(name="table", spec=["name"])
        table.name = "table"
        rows = [
            DirectRow(row_key=b"row_key", table=table),
            DirectRow(row_key=b"row_key_2", table=table),
        ]
        rows[0].set_cell("cf1", b"c1", 1)
        rows[0].set_cell("cf1", b"c1", 2)
        rows[1].set_cell("cf1", b"c1", 3)
        rows[1].set_cell("cf1", b"c1", 4)

        with self.assertRaises(TooManyMutationsError):
            self._call_fut("table", rows)

    def test_normal(self):
        from google.cloud.bigtable.row import DirectRow
        from google.cloud.bigtable_v2.proto import bigtable_pb2

        table = mock.Mock(spec=["name"])
        table.name = "table"
        rows = [
            DirectRow(row_key=b"row_key", table=table),
            DirectRow(row_key=b"row_key_2"),
        ]
        rows[0].set_cell("cf1", b"c1", b"1")
        rows[1].set_cell("cf1", b"c1", b"2")

        result = self._call_fut("table", rows)

        Entry = bigtable_pb2.MutateRowsRequest.Entry

        entry_1 = Entry(row_key=b"row_key")
        mutations_1 = entry_1.mutations.add()
        mutations_1.set_cell.family_name = "cf1"
        mutations_1.set_cell.column_qualifier = b"c1"
        mutations_1.set_cell.timestamp_micros = -1
        mutations_1.set_cell.value = b"1"

        entry_2 = Entry(row_key=b"row_key_2")
        mutations_2 = entry_2.mutations.add()
        mutations_2.set_cell.family_name = "cf1"
        mutations_2.set_cell.column_qualifier = b"c1"
        mutations_2.set_cell.timestamp_micros = -1
        mutations_2.set_cell.value = b"2"

        self.assertEqual(result, [entry_1, entry_2])


class Test__check_row_table_name(unittest.TestCase):
    def _call_fut(self, table_name, row):
        from google.cloud.bigtable.table import _check_row_table_name

        return _check_row_table_name(table_name, row)

    def test_wrong_table_name(self):
        from google.cloud.bigtable.table import TableMismatchError
        from google.cloud.bigtable.row import DirectRow

        table = mock.Mock(name="table", spec=["name"])
        table.name = "table"
        row = DirectRow(row_key=b"row_key", table=table)
        with self.assertRaises(TableMismatchError):
            self._call_fut("other_table", row)

    def test_right_table_name(self):
        from google.cloud.bigtable.row import DirectRow

        table = mock.Mock(name="table", spec=["name"])
        table.name = "table"
        row = DirectRow(row_key=b"row_key", table=table)
        result = self._call_fut("table", row)
        self.assertFalse(result)


class Test__check_row_type(unittest.TestCase):
    def _call_fut(self, row):
        from google.cloud.bigtable.table import _check_row_type

        return _check_row_type(row)

    def test_test_wrong_row_type(self):
        from google.cloud.bigtable.row import ConditionalRow

        row = ConditionalRow(row_key=b"row_key", table="table", filter_=None)
        with self.assertRaises(TypeError):
            self._call_fut(row)

    def test_right_row_type(self):
        from google.cloud.bigtable.row import DirectRow

        row = DirectRow(row_key=b"row_key", table="table")
        result = self._call_fut(row)
        self.assertFalse(result)


def _ReadRowsRequestPB(*args, **kw):
    from google.cloud.bigtable_v2.proto import bigtable_pb2 as messages_v2_pb2

    return messages_v2_pb2.ReadRowsRequest(*args, **kw)


def _TablePB(*args, **kw):
    from google.cloud.bigtable_admin_v2.proto import table_pb2 as table_v2_pb2

    return table_v2_pb2.Table(*args, **kw)


def _ColumnFamilyPB(*args, **kw):
    from google.cloud.bigtable_admin_v2.proto import table_pb2 as table_v2_pb2

    return table_v2_pb2.ColumnFamily(*args, **kw)


def _ClusterStatePB(replication_state):
    from google.cloud.bigtable_admin_v2.proto import table_pb2 as table_v2_pb2

    return table_v2_pb2.Table.ClusterState(replication_state=replication_state)
