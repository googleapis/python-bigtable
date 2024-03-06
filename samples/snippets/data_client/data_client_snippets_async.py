#!/usr/bin/env python

# Copyright 2023, Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


async def create_table(project, instance, table):
    # [START bigtable_data_create_table]
    from google.cloud.bigtable.data import BigtableDataClientAsync

    project_id = 'my_project'
    instance_id = 'my-instance'
    table_id = 'my-table'
    # [END bigtable_data_create_table]
    # replace placeholders outside sample
    project_id, instance_id, table_id = project, instance, table
    # [START bigtable_data_create_table]

    async with BigtableDataClientAsync(project=project_id) as client:
        async with client.get_table(instance_id, table_id) as table:
            print(f"table: {table}")
    # [END bigtable_data_create_table]


async def set_cell(table):
    # [START bigtable_data_set_cell]
    from google.cloud.bigtable.data import SetCell

    row_key = b"my_row"
    mutation = SetCell(
        family="family", qualifier="qualifier", new_value="value"
    )
    await table.mutate_row(row_key, mutation)
    # [END bigtable_data_set_cell]
