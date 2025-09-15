# -*- coding: utf-8 -*-
# Copyright 2023 Google LLC
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
#
import pytest
import uuid
from google.cloud.bigtable.data._cross_sync import CrossSync

TEST_FAMILY = "test-family"
TEST_FAMILY_2 = "test-family-2"
TEST_AGGREGATE_FAMILY = "test-aggregate-family"

class SystemTestRunner:
    """
    configures a system test class with configuration for clusters/tables/etc

    used by standard system tests, and metrics tests
    """

    @pytest.fixture(scope="session")
    def init_table_id(self):
        """
        The table_id to use when creating a new test table
        """
        return f"test-table-{uuid.uuid4().hex}"

    @pytest.fixture(scope="session")
    def cluster_config(self, project_id):
        """
        Configuration for the clusters to use when creating a new instance
        """
        from google.cloud.bigtable_admin_v2 import types

        cluster = {
            "test-cluster": types.Cluster(
                location=f"projects/{project_id}/locations/us-central1-b",
                serve_nodes=1,
            )
        }
        return cluster

    @pytest.fixture(scope="session")
    def column_family_config(self):
        """
        specify column families to create when creating a new test table
        """
        from google.cloud.bigtable_admin_v2 import types

        int_aggregate_type = types.Type.Aggregate(
            input_type=types.Type(int64_type={"encoding": {"big_endian_bytes": {}}}),
            sum={},
        )
        return {
            TEST_FAMILY: types.ColumnFamily(),
            TEST_FAMILY_2: types.ColumnFamily(),
            TEST_AGGREGATE_FAMILY: types.ColumnFamily(
                value_type=types.Type(aggregate_type=int_aggregate_type)
            ),
        }