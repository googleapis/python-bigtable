#!/usr/bin/env python

# Copyright 2022, Google LLC
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


from google.cloud import bigtable

# Write your code here.


# [START bigtable_delete_from_column_sample]
def delete_from_column_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    row = table.row("phone#4c410523#20190501")
    row.delete_cell(column_family_id="cell_plan", column="data_plan_01gb")
    row.commit()


# [END bigtable_delete_from_column_sample]

# [START bigtable_delete_from_column_family_sample]
def delete_from_column_family_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    row = table.row("phone#4c410523#20190501")
    row.delete_cells(
        column_family_id="cell_plan", columns=["data_plan_01gb", "data_plan_05gb"]
    )
    row.commit()


# [END bigtable_delete_from_column_family_sample]


# [START bigtable_delete_from_row_sample]
def delete_from_row_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    row = table.row("phone#4c410523#20190501")
    row.delete()
    row.commit()


# [END bigtable_delete_from_row_sample]

# [START bigtable_streaming_and_batching_sample]
def streaming_and_batching_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    batcher = table.mutations_batcher(flush_count=2)
    rows = table.read_rows()
    for row in rows:
        row = table.row(row.row_key)
        row.delete_cell(column_family_id="cell_plan", column="data_plan_01gb")

    batcher.mutate_rows(rows)


# [END bigtable_streaming_and_batching_sample]

# [START bigtable_check_and_mutate_sample]
def check_and_mutate_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    row = table.row("phone#4c410523#20190501")
    row.delete_cell(column_family_id="cell_plan", column="data_plan_01gb")
    row.delete_cell(column_family_id="cell_plan", column="data_plan_05gb")
    row.commit()


# [END bigtable_check_and_mutate_sample]


# [START bigtable_drop_row_range_sample]
def drop_row_range_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    row_key_prefix = b"phone#4c"
    table.drop_by_prefix(row_key_prefix, timeout=200)


# [END bigtable_drop_row_range_sample]

# [START bigtable_delete_column_family_sample]
def delete_column_family_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    column_family_id = "stats_summary"
    column = table.column_family(column_family_id)
    column.delete()


# [END bigtable_delete_column_family_sample]

# [START bigtable_delete_table_sample]
def delete_table_sample(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    table.delete()


# [END bigtable_delete_table_sample]
