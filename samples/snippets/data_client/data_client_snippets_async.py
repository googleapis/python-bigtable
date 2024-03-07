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

    project_id = "my_project"
    instance_id = "my-instance"
    table_id = "my-table"
    # [END bigtable_data_create_table]
    # replace placeholders outside sample
    project_id, instance_id, table_id = project, instance, table
    # [START bigtable_data_create_table]

    async with BigtableDataClientAsync(project=project_id) as client:
        async with client.get_table(instance_id, table_id) as table:
            print(f"table: {table}")
            # [END bigtable_data_create_table]
            return table


async def set_cell(table):
    # [START bigtable_data_set_cell]
    from google.cloud.bigtable.data import SetCell

    row_key = b"my_row"
    mutation = SetCell(family="family", qualifier="qualifier", new_value="value")
    await table.mutate_row(row_key, mutation)
    # [END bigtable_data_set_cell]


async def bulk_mutate(table):
    # [START bigtable_data_bulk_mutate]
    from google.cloud.bigtable.data.mutations import SetCell
    from google.cloud.bigtable.data.mutations import RowMutationEntry

    row_key_1 = b"first_row"
    row_key_2 = b"second_row"

    common_mutation = SetCell(family="family", qualifier="qualifier", new_value="value")
    await table.bulk_mutate_rows(
        [
            RowMutationEntry(row_key_1, [common_mutation]),
            RowMutationEntry(row_key_2, [common_mutation]),
        ]
    )
    # [END bigtable_data_bulk_mutate]


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


async def read_modify_write_increment(table):
    # [START bigtable_data_read_modify_write_increment]
    from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule
    from google.cloud.bigtable.data import SetCell

    row_key = b"my_row"
    family = "family"
    qualifier = "qualifier"

    # initialize row with a starting value of 1
    await table.mutate_row(row_key, SetCell(family, qualifier, new_value=1))

    # use read_modify_write to increment the value by 2
    add_two_rule = IncrementRule(family, qualifier, increment_amount=2)
    result = await table.read_modify_write_row(row_key, add_two_rule)

    # check result
    cell = result[0]
    print(f"{cell.row_key} value: {int(cell)}")
    assert int(cell) == 3
    # [END bigtable_data_read_modify_write_increment]


async def read_modify_write_append(table):
    # [START bigtable_data_read_modify_write_append]
    from google.cloud.bigtable.data.read_modify_write_rules import AppendValueRule
    from google.cloud.bigtable.data import SetCell

    row_key = b"my_row"
    family = "family"
    qualifier = "qualifier"

    # initialize row with a starting value of "hello"
    await table.mutate_row(row_key, SetCell(family, qualifier, new_value="hello"))

    # use read_modify_write to append " world" to the value
    append_world_rule = AppendValueRule(family, qualifier, append_value=" world")
    result = await table.read_modify_write_row(row_key, append_world_rule)

    # check result
    cell = result[0]
    print(f"{cell.row_key} value: {cell.value}")
    assert cell.value == b"hello world"
    # [END bigtable_data_read_modify_append]


async def check_and_mutate(table):
    # [START bigtable_data_check_and_mutate]
    from google.cloud.bigtable.data.row_filters import ValueRangeFilter
    from google.cloud.bigtable.data import SetCell

    row_key = b"my_row"
    family = "family"
    qualifier = "qualifier"

    # create a predicate filter to test against
    # in this case, use a ValueRangeFilter to check if the value is positive or negative
    predicate = ValueRangeFilter(start_value=0, inclusive_start=True)
    # use check and mutate to change the value in the row based on the predicate
    was_true = await table.check_and_mutate_row(
        row_key,
        predicate,
        true_case_mutations=SetCell(family, qualifier, new_value="positive"),
        false_case_mutations=SetCell(family, qualifier, new_value="negative"),
    )
    if was_true:
        print("The value was positive")
    else:
        print("The value was negative")
    # [END bigtable_data_check_and_mutate]
