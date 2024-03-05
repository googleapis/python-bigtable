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


async def set_cell():
    # [START bigtable_data_set_cell]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import SetCell
    async with BigtableDataClientAsync(project="my-project") as client:
        async with client.get_table(instance_id='my-instance', table_id='my-table') as table:
            row_key = b"my_row"
            mutation = SetCell(family="family", qualifier="qualifier", new_value="value")
            await table.mutate_row(row_key, mutation)
    # [END bigtable_data_set_cell]
