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
# Snippet for CreateBackup
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-bigtable-admin


# [START bigtableadmin_generated_bigtable_admin_v2_BigtableTableAdmin_CreateBackup_sync]
from google.cloud import bigtable_admin_v2


def sample_create_backup():
    # Create a client
    client = bigtable_admin_v2.BigtableTableAdminClient()

    # Initialize request argument(s)
    backup = bigtable_admin_v2.Backup()
    backup.source_table = "source_table_value"

    request = bigtable_admin_v2.CreateBackupRequest(
        parent="parent_value",
        backup_id="backup_id_value",
        backup=backup,
    )

    # Make the request
    operation = client.create_backup(request=request)

    print("Waiting for operation to complete...")

    response = operation.result()
    print(response)

# [END bigtableadmin_generated_bigtable_admin_v2_BigtableTableAdmin_CreateBackup_sync]
