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

import random
import asyncio
import string
from tqdm import tqdm

from google.cloud.bigtable.data import SetCell
from google.cloud.bigtable.data import RowMutationEntry

# the size of each value
FIELD_SIZE = 100

# number of qualifiers to use in test table
NUM_QUALIFIERS = 10

# The name of the column family used in the benchmark.
COLUMN_FAMILY = "cf"

KEY_WIDTH = 5

# The size of each BulkApply request.
BULK_SIZE = 1000

# How many shards to use for the table population.
POPULATE_SHARD_COUNT = 10


def random_value():
    valid_chars = list(string.ascii_letters + "-/_")
    return "".join(random.choices(valid_chars, k=FIELD_SIZE))


async def populate_table(table, table_size=100_000):
    """
    Populate the table with random test data

    Args:
    table: the Table object to populate with data
    table_size: the number of entries in the table. This value will be adjusted to be a multiple of 10, to ensure
        each shard has the same number of entries.
    """
    shard_size = max(table_size // POPULATE_SHARD_COUNT, 1)
    print(f"Populating table {table.table_id}...")
    async with table.mutations_batcher() as batcher:
        with tqdm(total=table_size) as pbar:
            task_list = []
            for shard_idx in range(POPULATE_SHARD_COUNT):
                new_task = asyncio.create_task(_populate_shard(batcher, shard_idx * shard_size, (shard_idx + 1) * shard_size, pbar))
                task_list.append(new_task)
            await asyncio.gather(*task_list)


async def _populate_shard(batcher, begin:int, end:int, pbar=None):
    for idx in range(begin, end):
        row_key = f"user{str(idx).zfill(KEY_WIDTH)}"
        row_mutations = [SetCell(COLUMN_FAMILY, f"field{qual}", random_value()) for qual in range(NUM_QUALIFIERS)]
        entry = RowMutationEntry(row_key, row_mutations)
        await batcher.append(entry)
        if pbar:
            pbar.update(1)
