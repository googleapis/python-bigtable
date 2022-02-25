# -*- coding: utf-8 -*-
# Copyright 2020 Google LLC
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
# Generated code. DO NOT EDIT!
#
# Snippet for CreateTableFromSnapshot
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-bigtable-admin


# [START bigtableadmin_v2_generated_BigtableTableAdmin_CreateTableFromSnapshot_sync]
from google.cloud import bigtable_admin_v2


def sample_create_table_from_snapshot():
    # Create a client
    client = bigtable_admin_v2.BigtableTableAdminClient()

    # Initialize request argument(s)
    request = bigtable_admin_v2.CreateTableFromSnapshotRequest(
        parent="parent_value",
        table_id="table_id_value",
        source_snapshot="source_snapshot_value",
    )

    # Make the request
    operation = client.create_table_from_snapshot(request=request)

    print("Waiting for operation to complete...")

    response = operation.result()

    # Handle the response
    print(response)

# [END bigtableadmin_v2_generated_BigtableTableAdmin_CreateTableFromSnapshot_sync]
