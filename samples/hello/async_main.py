#!/usr/bin/env python

# Copyright 2024 Google Inc.
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

"""Demonstrates how to connect to Cloud Bigtable and run some basic operations with the async APIs

Prerequisites:

- Create a Cloud Bigtable instance.
  https://cloud.google.com/bigtable/docs/creating-instance
- Set your Google Application Default Credentials.
  https://developers.google.com/identity/protocols/application-default-credentials
"""

import argparse
import asyncio

# [START bigtable_async_hw_imports]
from google.cloud import bigtable
from google.cloud.bigtable.data import row_filters
from google.cloud.bigtable.data import RowMutationEntry
from google.cloud.bigtable.data import SetCell
from google.cloud.bigtable.data import ReadRowsQuery

# [END bigtable_async_hw_imports]


async def main(project_id, instance_id, table_id):
    # [START bigtable_async_hw_connect]
    client = bigtable.data.BigtableDataClientAsync(project=project_id)
    table = client.get_table(instance_id, table_id)
    # [END bigtable_async_hw_connect]

    # [START bigtable_async_hw_create_table]
    from google.cloud.bigtable import column_family

    # the async client only supports the data API. Table creation as an admin operation
    # use admin client to create the table
    print("Creating the {} table.".format(table_id))
    admin_client = bigtable.Client(project=project_id, admin=True)
    admin_instance = admin_client.instance(instance_id)
    admin_table = admin_instance.table(table_id)

    print("Creating column family cf1 with Max Version GC rule...")
    # Create a column family with GC policy : most recent N versions
    # Define the GC policy to retain only the most recent 2 versions
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = "cf1"
    column_families = {column_family_id: max_versions_rule}
    if not admin_table.exists():
        admin_table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))
    # [END bigtable_async_hw_create_table]

    # [START bigtable_async_hw_write_rows]
    print("Writing some greetings to the table.")
    greetings = ["Hello World!", "Hello Cloud Bigtable!", "Hello Python!"]
    mutations = []
    column = "greeting"
    for i, value in enumerate(greetings):
        # Note: This example uses sequential numeric IDs for simplicity,
        # but this can result in poor performance in a production
        # application.  Since rows are stored in sorted order by key,
        # sequential keys can result in poor distribution of operations
        # across nodes.
        #
        # For more information about how to design a Bigtable schema for
        # the best performance, see the documentation:
        #
        #     https://cloud.google.com/bigtable/docs/schema-design
        row_key = "greeting{}".format(i).encode()
        row_mutation = RowMutationEntry(
            row_key, SetCell(column_family_id, column, value)
        )
        mutations.append(row_mutation)
    await table.bulk_mutate_rows(mutations)
    # [END bigtable_async_hw_write_rows]

    # [START bigtable_async_hw_create_filter]
    # Create a filter to only retrieve the most recent version of the cell
    # for each column across entire row.
    row_filter = row_filters.CellsColumnLimitFilter(1)
    # [END bigtable_async_hw_create_filter]

    # [START bigtable_async_hw_get_with_filter]
    # [START bigtable_async_hw_get_by_key]
    print("Getting a single greeting by row key.")
    key = "greeting0".encode()

    row = await table.read_row(key, row_filter=row_filter)
    cell = row.cells[0]
    print(cell.value.decode("utf-8"))
    # [END bigtable_async_hw_get_by_key]
    # [END bigtable_async_hw_get_with_filter]

    # [START bigtable_async_hw_scan_with_filter]
    # [START bigtable_async_hw_scan_all]
    print("Scanning for all greetings:")
    query = ReadRowsQuery(row_filter=row_filter)
    async for row in await table.read_rows_stream(query):
        cell = row.cells[0]
        print(cell.value.decode("utf-8"))
    # [END bigtable_async_hw_scan_all]
    # [END bigtable_async_hw_scan_with_filter]

    # [START bigtable_async_hw_delete_table]
    # the async client only supports the data API. Table deletion as an admin operation
    # use admin client to create the table
    print("Deleting the {} table.".format(table_id))
    admin_table.delete()
    # [END bigtable_async_hw_delete_table]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("project_id", help="Your Cloud Platform project ID.")
    parser.add_argument(
        "instance_id", help="ID of the Cloud Bigtable instance to connect to."
    )
    parser.add_argument(
        "--table", help="Table to create and destroy.", default="Hello-Bigtable"
    )

    args = parser.parse_args()
    asyncio.run(main(args.project_id, args.instance_id, args.table))
