import random
import asyncio
import string
import time

from google.cloud.bigtable.data import SetCell
from google.cloud.bigtable.data import RowMutationEntry
from google.cloud.bigtable.data import TableAsync

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
    print(f"Populating table {table.table_id} with {shard_size*POPULATE_SHARD_COUNT} entries...")
    for shard_idx in range(POPULATE_SHARD_COUNT):
        print(f"start: {shard_idx * shard_size}, end: {(shard_idx + 1) * shard_size}")
        await _populate_shard(table, shard_idx * shard_size, (shard_idx + 1) * shard_size)

async def _populate_shard(table, begin:int, end:int):
    entry_list = []
    for idx in range(begin, end):
        row_key = f"user{str(idx).zfill(KEY_WIDTH)}"
        row_mutations = [SetCell(COLUMN_FAMILY, f"field{qual}", random_value()) for qual in range(NUM_QUALIFIERS)]
        entry = RowMutationEntry(row_key, row_mutations)
        entry_list.append(entry)
    await table.bulk_mutate_rows(entry_list)

async def async_main():
    from google.cloud.bigtable.data import BigtableDataClientAsync
    client = BigtableDataClientAsync()
    table = client.get_table("sanche-test", "benchmarks")
    # await populate_shard(table, 0, 1000)
    await populate_table(table)

if __name__ == "__main__":
    asyncio.run(async_main())



