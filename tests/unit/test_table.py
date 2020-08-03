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

from tests.unit.test_base_table import (
    TestTableConstants,
    _MockReadRowsIterator,
    _MockFailureIterator_1,
    _MockFailureIterator_2,
    _ReadRowsResponseV2,
    _ReadRowsResponseCellChunkPB,
    _ReadRowsResponsePB,
    _TablePB,
    _ColumnFamilyPB,
    _ClusterStatePB,
    _read_rows_retry_exception,
)


class TestTable(unittest.TestCase, TestTableConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.table import Table

        return Table

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor_w_admin(self):
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT_ID, credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)
        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertIs(table._instance._client, client)
        self.assertEqual(table.name, self.TABLE_NAME)

    def test_constructor_wo_admin(self):
        credentials = _make_credentials()
        client = self._make_client(
            project=self.PROJECT_ID, credentials=credentials, admin=False
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)
        self.assertEqual(table.table_id, self.TABLE_ID)
        self.assertIs(table._instance._client, client)
        self.assertEqual(table.name, self.TABLE_NAME)

    def _row_methods_helper(self):
        client = self._make_client(
            project="project-id", credentials=_make_credentials(), admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)
        row_key = b"row_key"
        return table, row_key

    def test_row_factory_direct(self):
        from google.cloud.bigtable.row import DirectRow

        table, row_key = self._row_methods_helper()
        row = table.row(row_key)

        self.assertIsInstance(row, DirectRow)
        self.assertEqual(row._row_key, row_key)
        self.assertEqual(row._table, table)

    def test_row_factory_conditional(self):
        from google.cloud.bigtable.row import ConditionalRow

        table, row_key = self._row_methods_helper()
        filter_ = object()
        row = table.row(row_key, filter_=filter_)

        self.assertIsInstance(row, ConditionalRow)
        self.assertEqual(row._row_key, row_key)
        self.assertEqual(row._table, table)

    def test_row_factory_append(self):
        from google.cloud.bigtable.row import AppendRow

        table, row_key = self._row_methods_helper()
        row = table.row(row_key, append=True)

        self.assertIsInstance(row, AppendRow)
        self.assertEqual(row._row_key, row_key)
        self.assertEqual(row._table, table)

    def test_direct_row(self):
        from google.cloud.bigtable.row import DirectRow

        table, row_key = self._row_methods_helper()
        row = table.direct_row(row_key)

        self.assertIsInstance(row, DirectRow)
        self.assertEqual(row._row_key, row_key)
        self.assertEqual(row._table, table)

    def test_conditional_row(self):
        from google.cloud.bigtable.row import ConditionalRow

        table, row_key = self._row_methods_helper()
        filter_ = object()
        row = table.conditional_row(row_key, filter_=filter_)

        self.assertIsInstance(row, ConditionalRow)
        self.assertEqual(row._row_key, row_key)
        self.assertEqual(row._table, table)

    def test_append_row(self):
        from google.cloud.bigtable.row import AppendRow

        table, row_key = self._row_methods_helper()
        row = table.append_row(row_key)

        self.assertIsInstance(row, AppendRow)
        self.assertEqual(row._row_key, row_key)
        self.assertEqual(row._table, table)

    def test_row_factory_failure(self):
        table, row_key = self._row_methods_helper()
        with self.assertRaises(ValueError):
            table.row(row_key, filter_=object(), append=True)

    def _create_test_helper(self, split_keys=[], column_families={}):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.cloud.bigtable_admin_v2.proto import table_pb2
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_table_admin_pb2 as table_admin_messages_v2_pb2,
        )
        from google.cloud.bigtable.column_family import ColumnFamily

        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        # Patch API calls
        client._table_admin_client = table_api

        # Perform the method and check the result.
        table.create(column_families=column_families, initial_split_keys=split_keys)

        families = {
            id: ColumnFamily(id, self, rule).to_pb()
            for (id, rule) in column_families.items()
        }

        split = table_admin_messages_v2_pb2.CreateTableRequest.Split
        splits = [split(key=split_key) for split_key in split_keys]

        table_api.create_table.assert_called_once_with(
            parent=self.INSTANCE_NAME,
            table=table_pb2.Table(column_families=families),
            table_id=self.TABLE_ID,
            initial_splits=splits,
        )

    def test_create(self):
        self._create_test_helper()

    def test_create_with_families(self):
        from google.cloud.bigtable.base_column_family import MaxVersionsGCRule

        families = {"family": MaxVersionsGCRule(5)}
        self._create_test_helper(column_families=families)

    def test_create_with_split_keys(self):
        self._create_test_helper(split_keys=[b"split1", b"split2", b"split3"])

    def test_exists(self):
        from google.cloud.bigtable_admin_v2.proto import table_pb2 as table_data_v2_pb2
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_table_admin_pb2 as table_messages_v1_pb2,
        )
        from google.cloud.bigtable_admin_v2.gapic import (
            bigtable_instance_admin_client,
            bigtable_table_admin_client,
        )
        from google.api_core.exceptions import NotFound
        from google.api_core.exceptions import BadRequest

        table_api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        instance_api = bigtable_instance_admin_client.BigtableInstanceAdminClient(
            mock.Mock()
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        # Create response_pb
        response_pb = table_messages_v1_pb2.ListTablesResponse(
            tables=[table_data_v2_pb2.Table(name=self.TABLE_NAME)]
        )

        # Patch API calls
        client._table_admin_client = table_api
        client._instance_admin_client = instance_api
        bigtable_table_stub = client._table_admin_client.transport
        bigtable_table_stub.get_table.side_effect = [
            response_pb,
            NotFound("testing"),
            BadRequest("testing"),
        ]

        # Perform the method and check the result.
        table1 = instance.table(self.TABLE_ID)
        table2 = instance.table("table-id2")

        result = table1.exists()
        self.assertEqual(True, result)

        result = table2.exists()
        self.assertEqual(False, result)

        with self.assertRaises(BadRequest):
            table2.exists()

    def test_delete(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        # Patch API calls
        client._table_admin_client = table_api

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # Perform the method and check the result.
        result = table.delete()
        self.assertEqual(result, expected_result)

    def _list_column_families_helper(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        table_api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        # Create response_pb
        COLUMN_FAMILY_ID = "foo"
        column_family = _ColumnFamilyPB()
        response_pb = _TablePB(column_families={COLUMN_FAMILY_ID: column_family})

        # Patch the stub used by the API method.
        client._table_admin_client = table_api
        bigtable_table_stub = client._table_admin_client.transport
        bigtable_table_stub.get_table.side_effect = [response_pb]

        # Create expected_result.
        expected_result = {COLUMN_FAMILY_ID: table.column_family(COLUMN_FAMILY_ID)}

        # Perform the method and check the result.
        result = table.list_column_families()
        self.assertEqual(result, expected_result)

    def test_list_column_families(self):
        self._list_column_families_helper()

    def test_get_cluster_states(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.cloud.bigtable.enums import Table as enum_table
        from google.cloud.bigtable.table import ClusterState

        INITIALIZING = enum_table.ReplicationState.INITIALIZING
        PLANNED_MAINTENANCE = enum_table.ReplicationState.PLANNED_MAINTENANCE
        READY = enum_table.ReplicationState.READY

        table_api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        response_pb = _TablePB(
            cluster_states={
                "cluster-id1": _ClusterStatePB(INITIALIZING),
                "cluster-id2": _ClusterStatePB(PLANNED_MAINTENANCE),
                "cluster-id3": _ClusterStatePB(READY),
            }
        )

        # Patch the stub used by the API method.
        client._table_admin_client = table_api
        bigtable_table_stub = client._table_admin_client.transport
        bigtable_table_stub.get_table.side_effect = [response_pb]

        # build expected result
        expected_result = {
            u"cluster-id1": ClusterState(INITIALIZING),
            u"cluster-id2": ClusterState(PLANNED_MAINTENANCE),
            u"cluster-id3": ClusterState(READY),
        }

        # Perform the method and check the result.
        result = table.get_cluster_states()
        self.assertEqual(result, expected_result)

    def _read_row_helper(self, chunks, expected_result, app_profile_id=None):

        from google.cloud._testing import _Monkey
        from google.cloud.bigtable import table as MUT
        from google.cloud.bigtable.row_set import RowSet
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.cloud.bigtable.row_filters import RowSampleFilter

        data_api = bigtable_client.BigtableClient(mock.Mock())
        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance, app_profile_id=app_profile_id)

        # Create request_pb
        request_pb = object()  # Returned by our mock.
        mock_created = []

        def mock_create_row_request(table_name, **kwargs):
            mock_created.append((table_name, kwargs))
            return request_pb

        # Create response_iterator
        if chunks is None:
            response_iterator = iter(())  # no responses at all
        else:
            response_pb = _ReadRowsResponsePB(chunks=chunks)
            response_iterator = iter([response_pb])

        # Patch the stub used by the API method.
        client._table_data_client = data_api
        client._table_admin_client = table_api
        client._table_data_client.transport.read_rows = mock.Mock(
            side_effect=[response_iterator]
        )

        # Perform the method and check the result.
        filter_obj = RowSampleFilter(0.33)
        result = None
        with _Monkey(MUT, _create_row_request=mock_create_row_request):
            result = table.read_row(self.ROW_KEY, filter_=filter_obj)
        row_set = RowSet()
        row_set.add_row_key(self.ROW_KEY)
        expected_request = [
            (
                table.name,
                {
                    "end_inclusive": False,
                    "row_set": row_set,
                    "app_profile_id": app_profile_id,
                    "end_key": None,
                    "limit": None,
                    "start_key": None,
                    "filter_": filter_obj,
                },
            )
        ]
        self.assertEqual(result, expected_result)
        self.assertEqual(mock_created, expected_request)

    def test_read_row_miss_no__responses(self):
        self._read_row_helper(None, None)

    def test_read_row_miss_no_chunks_in_response(self):
        chunks = []
        self._read_row_helper(chunks, None)

    def test_read_row_complete(self):
        from google.cloud.bigtable.base_row_data import Cell
        from google.cloud.bigtable.base_row_data import PartialRowData

        app_profile_id = "app-profile-id"
        chunk = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )
        chunks = [chunk]
        expected_result = PartialRowData(row_key=self.ROW_KEY)
        family = expected_result._cells.setdefault(self.FAMILY_NAME, {})
        column = family.setdefault(self.QUALIFIER, [])
        column.append(Cell.from_pb(chunk))
        self._read_row_helper(chunks, expected_result, app_profile_id)

    def test_read_row_more_than_one_row_returned(self):
        app_profile_id = "app-profile-id"
        chunk_1 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )
        chunk_2 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_2,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        chunks = [chunk_1, chunk_2]
        with self.assertRaises(ValueError):
            self._read_row_helper(chunks, None, app_profile_id)

    def test_read_row_still_partial(self):
        chunk = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
        )
        # No "commit row".
        chunks = [chunk]
        with self.assertRaises(ValueError):
            self._read_row_helper(chunks, None)

    def test_mutate_rows(self):
        from google.rpc.status_pb2 import Status
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        client._table_admin_client = table_api
        table = self._make_one(self.TABLE_ID, instance)

        response = [Status(code=0), Status(code=1)]

        mock_worker = mock.Mock(return_value=response)
        with mock.patch(
            "google.cloud.bigtable.table._RetryableMutateRowsWorker",
            new=mock.MagicMock(return_value=mock_worker),
        ):
            statuses = table.mutate_rows([mock.MagicMock(), mock.MagicMock()])
        result = [status.code for status in statuses]
        expected_result = [0, 1]

        self.assertEqual(result, expected_result)

    def test_read_rows(self):
        from google.cloud._testing import _Monkey
        from google.cloud.bigtable.row_data import PartialRowsData
        from google.cloud.bigtable import table as MUT
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        data_api = bigtable_client.BigtableClient(mock.Mock())
        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        app_profile_id = "app-profile-id"
        table = self._make_one(self.TABLE_ID, instance, app_profile_id=app_profile_id)

        # Create request_pb
        request = retry = object()  # Returned by our mock.
        mock_created = []

        def mock_create_row_request(table_name, **kwargs):
            mock_created.append((table_name, kwargs))
            return request

        # Create expected_result.
        expected_result = PartialRowsData(
            client._table_data_client.transport.read_rows, request, retry
        )

        # Perform the method and check the result.
        start_key = b"start-key"
        end_key = b"end-key"
        filter_obj = object()
        limit = 22
        with _Monkey(MUT, _create_row_request=mock_create_row_request):
            result = table.read_rows(
                start_key=start_key,
                end_key=end_key,
                filter_=filter_obj,
                limit=limit,
                retry=retry,
            )

        self.assertEqual(result.rows, expected_result.rows)
        self.assertEqual(result.retry, expected_result.retry)
        created_kwargs = {
            "start_key": start_key,
            "end_key": end_key,
            "filter_": filter_obj,
            "limit": limit,
            "end_inclusive": False,
            "app_profile_id": app_profile_id,
            "row_set": None,
        }
        self.assertEqual(mock_created, [(table.name, created_kwargs)])

    def test_read_retry_rows(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.api_core import retry

        data_api = bigtable_client.BigtableClient(mock.Mock())
        table_api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        retry_read_rows = retry.Retry(predicate=_read_rows_retry_exception)

        # Create response_iterator
        chunk_1 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_1,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        chunk_2 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_2,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        response_1 = _ReadRowsResponseV2([chunk_1])
        response_2 = _ReadRowsResponseV2([chunk_2])
        response_failure_iterator_1 = _MockFailureIterator_1()
        response_failure_iterator_2 = _MockFailureIterator_2([response_1])
        response_iterator = _MockReadRowsIterator(response_2)

        # Patch the stub used by the API method.
        client._table_data_client.transport.read_rows = mock.Mock(
            side_effect=[
                response_failure_iterator_1,
                response_failure_iterator_2,
                response_iterator,
            ]
        )

        rows = []
        for row in table.read_rows(
            start_key=self.ROW_KEY_1, end_key=self.ROW_KEY_2, retry=retry_read_rows
        ):
            rows.append(row)

        result = rows[1]
        self.assertEqual(result.row_key, self.ROW_KEY_2)

    def test_yield_retry_rows(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        import warnings

        data_api = bigtable_client.BigtableClient(mock.Mock())
        table_api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        # Create response_iterator
        chunk_1 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_1,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        chunk_2 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_2,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        response_1 = _ReadRowsResponseV2([chunk_1])
        response_2 = _ReadRowsResponseV2([chunk_2])
        response_failure_iterator_1 = _MockFailureIterator_1()
        response_failure_iterator_2 = _MockFailureIterator_2([response_1])
        response_iterator = _MockReadRowsIterator(response_2)

        # Patch the stub used by the API method.
        client._table_data_client.transport.read_rows = mock.Mock(
            side_effect=[
                response_failure_iterator_1,
                response_failure_iterator_2,
                response_iterator,
            ]
        )

        rows = []
        with warnings.catch_warnings(record=True) as warned:
            for row in table.yield_rows(
                start_key=self.ROW_KEY_1, end_key=self.ROW_KEY_2
            ):
                rows.append(row)

        self.assertEqual(len(warned), 1)
        self.assertIs(warned[0].category, DeprecationWarning)

        result = rows[1]
        self.assertEqual(result.row_key, self.ROW_KEY_2)

    def test_yield_rows_with_row_set(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.cloud.bigtable.row_set import RowSet
        from google.cloud.bigtable.row_set import RowRange
        import warnings

        data_api = bigtable_client.BigtableClient(mock.Mock())
        table_api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        # Create response_iterator
        chunk_1 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_1,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        chunk_2 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_2,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        chunk_3 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY_3,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )

        response_1 = _ReadRowsResponseV2([chunk_1])
        response_2 = _ReadRowsResponseV2([chunk_2])
        response_3 = _ReadRowsResponseV2([chunk_3])
        response_iterator = _MockReadRowsIterator(response_1, response_2, response_3)

        # Patch the stub used by the API method.
        client._table_data_client.transport.read_rows = mock.Mock(
            side_effect=[response_iterator]
        )

        rows = []
        row_set = RowSet()
        row_set.add_row_range(
            RowRange(start_key=self.ROW_KEY_1, end_key=self.ROW_KEY_2)
        )
        row_set.add_row_key(self.ROW_KEY_3)

        with warnings.catch_warnings(record=True) as warned:
            for row in table.yield_rows(row_set=row_set):
                rows.append(row)

        self.assertEqual(len(warned), 1)
        self.assertIs(warned[0].category, DeprecationWarning)

        self.assertEqual(rows[0].row_key, self.ROW_KEY_1)
        self.assertEqual(rows[1].row_key, self.ROW_KEY_2)
        self.assertEqual(rows[2].row_key, self.ROW_KEY_3)

    def test_sample_row_keys(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        data_api = bigtable_client.BigtableClient(mock.Mock())
        table_api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        # Create response_iterator
        response_iterator = object()  # Just passed to a mock.

        # Patch the stub used by the API method.
        inner_api_calls = client._table_data_client._inner_api_calls
        inner_api_calls["sample_row_keys"] = mock.Mock(
            side_effect=[[response_iterator]]
        )

        # Create expected_result.
        expected_result = response_iterator

        # Perform the method and check the result.
        result = table.sample_row_keys()
        self.assertEqual(result[0], expected_result)

    def test_truncate(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        data_api = mock.create_autospec(bigtable_client.BigtableClient)
        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        expected_result = None  # truncate() has no return value.
        with mock.patch("google.cloud.bigtable.table.Table.name", new=self.TABLE_NAME):
            result = table.truncate()

        table_api.drop_row_range.assert_called_once_with(
            name=self.TABLE_NAME, delete_all_data_from_table=True
        )

        self.assertEqual(result, expected_result)

    def test_truncate_w_timeout(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        data_api = mock.create_autospec(bigtable_client.BigtableClient)
        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        expected_result = None  # truncate() has no return value.

        timeout = 120
        result = table.truncate(timeout=timeout)

        self.assertEqual(result, expected_result)

    def test_drop_by_prefix(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        data_api = mock.create_autospec(bigtable_client.BigtableClient)
        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        expected_result = None  # drop_by_prefix() has no return value.

        row_key_prefix = "row-key-prefix"

        result = table.drop_by_prefix(row_key_prefix=row_key_prefix)

        self.assertEqual(result, expected_result)

    def test_drop_by_prefix_w_timeout(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        data_api = mock.create_autospec(bigtable_client.BigtableClient)
        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        client._table_admin_client = table_api
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        expected_result = None  # drop_by_prefix() has no return value.

        row_key_prefix = "row-key-prefix"

        timeout = 120
        result = table.drop_by_prefix(row_key_prefix=row_key_prefix, timeout=timeout)

        self.assertEqual(result, expected_result)

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

    def test_get_iam_policy(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.iam.v1 import policy_pb2
        from google.cloud.bigtable.policy import BIGTABLE_ADMIN_ROLE

        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        version = 1
        etag = b"etag_v1"
        members = ["serviceAccount:service_acc1@test.com", "user:user1@test.com"]
        bindings = [{"role": BIGTABLE_ADMIN_ROLE, "members": members}]
        iam_policy = policy_pb2.Policy(version=version, etag=etag, bindings=bindings)

        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        client._table_admin_client = table_api
        table_api.get_iam_policy.return_value = iam_policy

        result = table.get_iam_policy()

        table_api.get_iam_policy.assert_called_once_with(resource=table.name)
        self.assertEqual(result.version, version)
        self.assertEqual(result.etag, etag)
        admins = result.bigtable_admins
        self.assertEqual(len(admins), len(members))
        for found, expected in zip(sorted(admins), sorted(members)):
            self.assertEqual(found, expected)

    def test_set_iam_policy(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.iam.v1 import policy_pb2
        from google.cloud.bigtable.policy import Policy
        from google.cloud.bigtable.policy import BIGTABLE_ADMIN_ROLE

        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        version = 1
        etag = b"etag_v1"
        members = ["serviceAccount:service_acc1@test.com", "user:user1@test.com"]
        bindings = [{"role": BIGTABLE_ADMIN_ROLE, "members": sorted(members)}]
        iam_policy_pb = policy_pb2.Policy(version=version, etag=etag, bindings=bindings)

        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        client._table_admin_client = table_api
        table_api.set_iam_policy.return_value = iam_policy_pb

        iam_policy = Policy(etag=etag, version=version)
        iam_policy[BIGTABLE_ADMIN_ROLE] = [
            Policy.user("user1@test.com"),
            Policy.service_account("service_acc1@test.com"),
        ]

        result = table.set_iam_policy(iam_policy)

        table_api.set_iam_policy.assert_called_once_with(
            resource=table.name, policy=iam_policy_pb
        )
        self.assertEqual(result.version, version)
        self.assertEqual(result.etag, etag)
        admins = result.bigtable_admins
        self.assertEqual(len(admins), len(members))
        for found, expected in zip(sorted(admins), sorted(members)):
            self.assertEqual(found, expected)

    def test_test_iam_permissions(self):
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client
        from google.iam.v1 import iam_policy_pb2

        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        instance = client.instance(instance_id=self.INSTANCE_ID)
        table = self._make_one(self.TABLE_ID, instance)

        permissions = ["bigtable.tables.mutateRows", "bigtable.tables.readRows"]

        response = iam_policy_pb2.TestIamPermissionsResponse(permissions=permissions)

        table_api = mock.create_autospec(
            bigtable_table_admin_client.BigtableTableAdminClient
        )
        table_api.test_iam_permissions.return_value = response
        client._table_admin_client = table_api

        result = table.test_iam_permissions(permissions)

        self.assertEqual(result, permissions)
        table_api.test_iam_permissions.assert_called_once_with(
            resource=table.name, permissions=permissions
        )
