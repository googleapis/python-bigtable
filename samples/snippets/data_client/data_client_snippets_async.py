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


async def write_simple(table):
    # [START bigtable_async_write_simple]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import SetCell

    async def write_simple(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:
                family_id = "stats_summary"
                row_key = b"phone#4c410523#20190501"

                cell_mutation = SetCell(family_id, "connected_cell", 1)
                wifi_mutation = SetCell(family_id, "connected_wifi", 1)
                os_mutation = SetCell(family_id, "os_build", "PQ2A.190405.003")

                await table.mutate_row(row_key, cell_mutation)
                await table.mutate_row(row_key, wifi_mutation)
                await table.mutate_row(row_key, os_mutation)

    # [END bigtable_async_write_simple]
    await write_simple(table.client.project, table.instance_id, table.table_id)


async def write_batch(table):
    # [START bigtable_async_writes_batch]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data.mutations import SetCell
    from google.cloud.bigtable.data.mutations import RowMutationEntry

    async def write_batch(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:
                family_id = "stats_summary"

                mutation_list = [
                    SetCell(family_id, "connected_cell", 1),
                    SetCell(family_id, "connected_wifi", 1),
                    SetCell(family_id, "os_build", "PQ2A.190405.003"),
                ]

                await table.bulk_mutate_rows(
                    [
                        RowMutationEntry("tablet#a0b81f74#20190501", mutation_list),
                        RowMutationEntry("tablet#a0b81f74#20190502", mutation_list),
                    ]
                )
    # [END bigtable_async_writes_batch]
    await write_batch(table.client.project, table.instance_id, table.table_id)


async def write_increment(table):
    # [START bigtable_async_write_increment]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule
    from google.cloud.bigtable.data import SetCell

    async def write_increment(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:
                family_id = "stats_summary"
                row_key = "phone#4c410523#20190501"

                # Decrement the connected_wifi value by 1.
                increment_rule = IncrementRule(
                    family_id, "connected_wifi", increment_amount=-1
                )
                result_row = await table.read_modify_write_row(row_key, increment_rule)

                # check result
                cell = result_row[0]
                print(f"{cell.row_key} value: {int(cell)}")
    # [END bigtable_async_write_increment]
    await write_increment(table.client.project, table.instance_id, table.table_id)


async def write_conditional(table):
    # [START bigtable_async_writes_conditional]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import row_filters
    from google.cloud.bigtable.data import SetCell

    async def write_conditional(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:
                family_id = "stats_summary"
                row_key = "phone#4c410523#20190501"

                row_filter = row_filters.RowFilterChain(
                    filters=[
                        row_filters.FamilyNameRegexFilter(family_id),
                        row_filters.ColumnQualifierRegexFilter("os_build"),
                        row_filters.ValueRegexFilter("PQ2A\\..*"),
                    ]
                )

                if_true = SetCell(family_id, "os_name", "android")
                result = await table.check_and_mutate_row(
                    row_key,
                    row_filter,
                    true_case_mutations=if_true,
                    false_case_mutations=None,
                )
                if result is True:
                    print("The row os_name was set to android")
    # [END bigtable_async_writes_conditional]
    await write_conditional(table.client.project, table.instance_id, table.table_id)

async def mutations_batcher(table):
    # [START bigtable_data_mutations_batcher]
    from google.cloud.bigtable.data.mutations import SetCell
    from google.cloud.bigtable.data import RowMutationEntry

    common_mutation = SetCell(family="family", qualifier="qualifier", new_value="value")

    async with table.mutations_batcher(
        flush_limit_mutation_count=2, flush_limit_bytes=1024
    ) as batcher:
        for i in range(10):
            row_key = f"row-{i}"
            batcher.append(RowMutationEntry(row_key, [common_mutation]))
        # [END bigtable_data_mutations_batcher]
        return batcher


async def read_row(table):
    # [START bigtable_data_read_row]
    row = await table.read_row(b"my_row")
    print(row.row_key)
    # [END bigtable_data_read_row]
    return row


async def read_rows_list(table):
    # [START bigtable_data_read_rows_list]
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange

    query = ReadRowsQuery(row_ranges=[RowRange("a", "z")])

    row_list = await table.read_rows(query)
    for row in row_list:
        print(row.row_key)
    # [END bigtable_data_read_rows_list]
    return row_list


async def read_rows_stream(table):
    # [START bigtable_data_read_rows_stream]
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange

    query = ReadRowsQuery(row_ranges=[RowRange("a", "z")])
    async for row in await table.read_rows_stream(query):
        print(row.row_key)
    # [END bigtable_data_read_rows_stream]


async def read_rows_sharded(table):
    # [START bigtable_data_read_rows_sharded]
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange

    # find shard keys for table
    table_shard_keys = await table.sample_row_keys()
    # construct shared query
    query = ReadRowsQuery(row_ranges=[RowRange("a", "z")])
    shard_queries = query.shard(table_shard_keys)
    # execute sharded query
    row_list = await table.read_rows_sharded(shard_queries)
    for row in row_list:
        print(row.row_key)
    # [END bigtable_data_read_rows_sharded]


async def row_exists(table):
    # [START bigtable_data_row_exists]
    row_key = b"my_row"
    exists = await table.row_exists(row_key)
    if exists:
        print(f"The row {row_key} exists")
    else:
        print(f"The row {row_key} does not exist")
    # [END bigtable_data_row_exists]
    return exists
