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

from tests.unit.test_base_row import (
    _CheckAndMutateRowResponsePB,
    _Table,
)


class TestDirectRow(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row import DirectRow

        return DirectRow

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor(self):
        row_key = b"row_key"
        table = object()

        row = self._make_one(row_key, table)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)
        self.assertEqual(row._pb_mutations, [])

    def test_commit(self):
        project_id = "project-id"
        row_key = b"row_key"
        table_name = "projects/more-stuff"
        column_family_id = u"column_family_id"
        column = b"column"

        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(table_name, client=client)
        row = self._make_one(row_key, table)
        value = b"bytes-value"

        # Perform the method and check the result.
        row.set_cell(column_family_id, column, value)
        row.commit()
        self.assertEqual(table.mutated_rows, [row])


class TestConditionalRow(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row import ConditionalRow

        return ConditionalRow

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor(self):
        row_key = b"row_key"
        table = object()
        filter_ = object()

        row = self._make_one(row_key, table, filter_=filter_)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)
        self.assertIs(row._filter, filter_)
        self.assertEqual(row._true_pb_mutations, [])
        self.assertEqual(row._false_pb_mutations, [])

    def test_commit(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter
        from google.cloud.bigtable_v2.gapic import bigtable_client

        project_id = "project-id"
        row_key = b"row_key"
        table_name = "projects/more-stuff"
        app_profile_id = "app_profile_id"
        column_family_id1 = u"column_family_id1"
        column_family_id2 = u"column_family_id2"
        column_family_id3 = u"column_family_id3"
        column1 = b"column1"
        column2 = b"column2"

        api = bigtable_client.BigtableClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(table_name, client=client, app_profile_id=app_profile_id)
        row_filter = RowSampleFilter(0.33)
        row = self._make_one(row_key, table, filter_=row_filter)

        # Create request_pb
        value1 = b"bytes-value"

        # Create response_pb
        predicate_matched = True
        response_pb = _CheckAndMutateRowResponsePB(predicate_matched=predicate_matched)

        # Patch the stub used by the API method.
        api.transport.check_and_mutate_row.side_effect = [response_pb]
        client._table_data_client = api

        # Create expected_result.
        expected_result = predicate_matched

        # Perform the method and check the result.
        row.set_cell(column_family_id1, column1, value1, state=True)
        row.delete(state=False)
        row.delete_cell(column_family_id2, column2, state=True)
        row.delete_cells(column_family_id3, row.ALL_COLUMNS, state=True)
        result = row.commit()
        call_args = api.transport.check_and_mutate_row.call_args.args[0]
        self.assertEqual(app_profile_id, call_args.app_profile_id)
        self.assertEqual(result, expected_result)
        self.assertEqual(row._true_pb_mutations, [])
        self.assertEqual(row._false_pb_mutations, [])

    def test_commit_too_many_mutations(self):
        from google.cloud._testing import _Monkey
        from google.cloud.bigtable import row as MUT

        row_key = b"row_key"
        table = object()
        filter_ = object()
        row = self._make_one(row_key, table, filter_=filter_)
        row._true_pb_mutations = [1, 2, 3]
        num_mutations = len(row._true_pb_mutations)
        with _Monkey(MUT, MAX_MUTATIONS=num_mutations - 1):
            with self.assertRaises(ValueError):
                row.commit()

    def test_commit_no_mutations(self):
        from tests.unit._testing import _FakeStub

        project_id = "project-id"
        row_key = b"row_key"

        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(None, client=client)
        filter_ = object()
        row = self._make_one(row_key, table, filter_=filter_)
        self.assertEqual(row._true_pb_mutations, [])
        self.assertEqual(row._false_pb_mutations, [])

        # Patch the stub used by the API method.
        stub = _FakeStub()

        # Perform the method and check the result.
        result = row.commit()
        self.assertIsNone(result)
        # Make sure no request was sent.
        self.assertEqual(stub.method_calls, [])


class TestAppendRow(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row import AppendRow

        return AppendRow

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor(self):
        row_key = b"row_key"
        table = object()

        row = self._make_one(row_key, table)
        self.assertEqual(row._row_key, row_key)
        self.assertIs(row._table, table)
        self.assertEqual(row._rule_pb_list, [])

    def test_commit(self):
        from google.cloud._testing import _Monkey
        from google.cloud.bigtable import row as MUT
        from google.cloud.bigtable_v2.gapic import bigtable_client

        project_id = "project-id"
        row_key = b"row_key"
        table_name = "projects/more-stuff"
        app_profile_id = "app_profile_id"
        column_family_id = u"column_family_id"
        column = b"column"

        api = bigtable_client.BigtableClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(table_name, client=client, app_profile_id=app_profile_id)
        row = self._make_one(row_key, table)

        # Create request_pb
        value = b"bytes-value"

        # Create expected_result.
        row_responses = []
        expected_result = object()

        # Patch API calls
        client._table_data_client = api

        def mock_parse_rmw_row_response(row_response):
            row_responses.append(row_response)
            return expected_result

        # Perform the method and check the result.
        with _Monkey(MUT, _parse_rmw_row_response=mock_parse_rmw_row_response):
            row.append_cell_value(column_family_id, column, value)
            result = row.commit()
        call_args = api.transport.read_modify_write_row.call_args.args[0]
        self.assertEqual(app_profile_id, call_args.app_profile_id)
        self.assertEqual(result, expected_result)
        self.assertEqual(row._rule_pb_list, [])

    def test_commit_no_rules(self):
        from tests.unit._testing import _FakeStub

        project_id = "project-id"
        row_key = b"row_key"

        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(None, client=client)
        row = self._make_one(row_key, table)
        self.assertEqual(row._rule_pb_list, [])

        # Patch the stub used by the API method.
        stub = _FakeStub()

        # Perform the method and check the result.
        result = row.commit()
        self.assertEqual(result, {})
        # Make sure no request was sent.
        self.assertEqual(stub.method_calls, [])

    def test_commit_too_many_mutations(self):
        from google.cloud._testing import _Monkey
        from google.cloud.bigtable import row as MUT

        row_key = b"row_key"
        table = object()
        row = self._make_one(row_key, table)
        row._rule_pb_list = [1, 2, 3]
        num_mutations = len(row._rule_pb_list)
        with _Monkey(MUT, MAX_MUTATIONS=num_mutations - 1):
            with self.assertRaises(ValueError):
                row.commit()
