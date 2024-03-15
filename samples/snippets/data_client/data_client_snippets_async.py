#!/usr/bin/env python

# Copyright 2024, Google LLC
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

                async with table.mutations_batcher() as batcher:
                    mutation_list = [
                        SetCell(family_id, "connected_cell", 1),
                        SetCell(family_id, "connected_wifi", 1),
                        SetCell(family_id, "os_build", "12155.0.0-rc1"),
                    ]
                    batcher.append(
                        RowMutationEntry("tablet#a0b81f74#20190501", mutation_list)
                    )
                    batcher.append(
                        RowMutationEntry("tablet#a0b81f74#20190502", mutation_list)
                    )
    # [END bigtable_async_writes_batch]
    await write_batch(table.client.project, table.instance_id, table.table_id)


async def write_increment(table):
    # [START bigtable_async_write_increment]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule

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


async def read_row(table):
    # [START bigtable_async_reads_row]
    from google.cloud.bigtable.data import BigtableDataClientAsync

    async def read_row(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:
                row_key = "phone#4c410523#20190501"
                row = await table.read_row(row_key)
                print(row)
    # [END bigtable_async_reads_row]
    await read_row(table.client.project, table.instance_id, table.table_id)


async def read_row_partial(table):
    # [START bigtable_async_reads_row_partial]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import row_filters

    async def read_row_partial(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:
                row_key = "phone#4c410523#20190501"
                col_filter = row_filters.ColumnQualifierRegexFilter(b"os_build")

                row = await table.read_row(row_key, row_filter=col_filter)
                print(row)
    # [END bigtable_async_reads_row_partial]
    await read_row_partial(table.client.project, table.instance_id, table.table_id)


async def read_rows_multiple(table):
    # [START bigtable_async_reads_rows]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import ReadRowsQuery

    async def read_rows(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:

                query = ReadRowsQuery(row_keys=[
                    b"phone#4c410523#20190501",
                    b"phone#4c410523#20190502"
                ])
                async for row in await table.read_rows_stream(query):
                    print(row)

    # [END bigtable_async_reads_rows]
    await read_rows(table.client.project, table.instance_id, table.table_id)


async def read_row_range(table):
    # [START bigtable_async_reads_row_range]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange

    async def read_row_range(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:

                row_range = RowRange(
                    start_key=b"phone#4c410523#20190501",
                    end_key=b"phone#4c410523#201906201"
                )
                query = ReadRowsQuery(row_ranges=[row_range])

                async for row in await table.read_rows_stream(query):
                    print(row)
    # [END bigtable_async_reads_row_range]
    await read_row_range(table.client.project, table.instance_id, table.table_id)


async def read_with_prefix(table):
    # [START bigtable_async_reads_prefix]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange

    async def read_prefix(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:

                prefix = "phone#"
                end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
                prefix_range = RowRange(start_key=prefix, end_key=end_key)
                query = ReadRowsQuery(row_ranges=[prefix_range])

                async for row in await table.read_rows_stream(query):
                    print(row)
    # [END bigtable_async_reads_prefix]
    await read_prefix(table.client.project, table.instance_id, table.table_id)


async def read_with_filter(table):
    # [START bigtable_async_reads_filter]
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import row_filters

    async def read_with_filter(project_id, instance_id, table_id):
        async with BigtableDataClientAsync(project=project_id) as client:
            async with client.get_table(instance_id, table_id) as table:

                row_filter = row_filters.ValueRegexFilter(b"PQ2A.*$")
                query = ReadRowsQuery(row_filter=row_filter)

                async for row in await table.read_rows_stream(query):
                    print(row)
    # [END bigtable_async_reads_filter]
    await read_with_filter(table.client.project, table.instance_id, table.table_id)
