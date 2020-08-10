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

from tests.unit.test_base_column_family import (
    _ColumnFamilyPB,
    _Table,
)


class TestColumnFamily(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.column_family import ColumnFamily

        return ColumnFamily

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_constructor(self):
        column_family_id = u"column-family-id"
        table = object()
        gc_rule = object()
        column_family = self._make_one(column_family_id, table, gc_rule=gc_rule)

        self.assertEqual(column_family.column_family_id, column_family_id)
        self.assertIs(column_family._table, table)
        self.assertIs(column_family.gc_rule, gc_rule)

    def _create_test_helper(self, gc_rule=None):
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_table_admin_pb2 as table_admin_v2_pb2,
        )
        from tests.unit._testing import _FakeStub
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        project_id = "project-id"
        zone = "zone"
        cluster_id = "cluster-id"
        table_id = "table-id"
        column_family_id = "column-family-id"
        table_name = (
            "projects/"
            + project_id
            + "/zones/"
            + zone
            + "/clusters/"
            + cluster_id
            + "/tables/"
            + table_id
        )

        api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(table_name, client=client)
        column_family = self._make_one(column_family_id, table, gc_rule=gc_rule)

        # Create request_pb
        if gc_rule is None:
            column_family_pb = _ColumnFamilyPB()
        else:
            column_family_pb = _ColumnFamilyPB(gc_rule=gc_rule.to_pb())
        request_pb = table_admin_v2_pb2.ModifyColumnFamiliesRequest(name=table_name)
        request_pb.modifications.add(id=column_family_id, create=column_family_pb)

        # Create response_pb
        response_pb = _ColumnFamilyPB()

        # Patch the stub used by the API method.
        stub = _FakeStub(response_pb)
        client._table_admin_client = api
        client._table_admin_client.transport.create = stub

        # Create expected_result.
        expected_result = None  # create() has no return value.

        # Perform the method and check the result.
        self.assertEqual(stub.results, (response_pb,))
        result = column_family.create()
        self.assertEqual(result, expected_result)

    def test_create(self):
        self._create_test_helper(gc_rule=None)

    def test_create_with_gc_rule(self):
        from google.cloud.bigtable.base_column_family import MaxVersionsGCRule

        gc_rule = MaxVersionsGCRule(1337)
        self._create_test_helper(gc_rule=gc_rule)

    def _update_test_helper(self, gc_rule=None):
        from tests.unit._testing import _FakeStub
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_table_admin_pb2 as table_admin_v2_pb2,
        )
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        project_id = "project-id"
        zone = "zone"
        cluster_id = "cluster-id"
        table_id = "table-id"
        column_family_id = "column-family-id"
        table_name = (
            "projects/"
            + project_id
            + "/zones/"
            + zone
            + "/clusters/"
            + cluster_id
            + "/tables/"
            + table_id
        )

        api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(table_name, client=client)
        column_family = self._make_one(column_family_id, table, gc_rule=gc_rule)

        # Create request_pb
        if gc_rule is None:
            column_family_pb = _ColumnFamilyPB()
        else:
            column_family_pb = _ColumnFamilyPB(gc_rule=gc_rule.to_pb())
        request_pb = table_admin_v2_pb2.ModifyColumnFamiliesRequest(name=table_name)
        request_pb.modifications.add(id=column_family_id, update=column_family_pb)

        # Create response_pb
        response_pb = _ColumnFamilyPB()

        # Patch the stub used by the API method.
        stub = _FakeStub(response_pb)
        client._table_admin_client = api
        client._table_admin_client.transport.update = stub

        # Create expected_result.
        expected_result = None  # update() has no return value.

        # Perform the method and check the result.
        self.assertEqual(stub.results, (response_pb,))
        result = column_family.update()
        self.assertEqual(result, expected_result)

    def test_update(self):
        self._update_test_helper(gc_rule=None)

    def test_update_with_gc_rule(self):
        from google.cloud.bigtable.base_column_family import MaxVersionsGCRule

        gc_rule = MaxVersionsGCRule(1337)
        self._update_test_helper(gc_rule=gc_rule)

    def test_delete(self):
        from google.protobuf import empty_pb2
        from google.cloud.bigtable_admin_v2.proto import (
            bigtable_table_admin_pb2 as table_admin_v2_pb2,
        )
        from tests.unit._testing import _FakeStub
        from google.cloud.bigtable_admin_v2.gapic import bigtable_table_admin_client

        project_id = "project-id"
        zone = "zone"
        cluster_id = "cluster-id"
        table_id = "table-id"
        column_family_id = "column-family-id"
        table_name = (
            "projects/"
            + project_id
            + "/zones/"
            + zone
            + "/clusters/"
            + cluster_id
            + "/tables/"
            + table_id
        )

        api = bigtable_table_admin_client.BigtableTableAdminClient(mock.Mock())
        credentials = _make_credentials()
        client = self._make_client(
            project=project_id, credentials=credentials, admin=True
        )
        table = _Table(table_name, client=client)
        column_family = self._make_one(column_family_id, table)

        # Create request_pb
        request_pb = table_admin_v2_pb2.ModifyColumnFamiliesRequest(name=table_name)
        request_pb.modifications.add(id=column_family_id, drop=True)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Patch the stub used by the API method.
        stub = _FakeStub(response_pb)
        client._table_admin_client = api
        client._table_admin_client.transport.delete = stub

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # Perform the method and check the result.
        self.assertEqual(stub.results, (response_pb,))
        result = column_family.delete()
        self.assertEqual(result, expected_result)
