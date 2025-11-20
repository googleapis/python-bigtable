# [START bigtable_write_increment_async]
from google.cloud.bigtable.data import BigtableDataClientAsync
from google.cloud.bigtable.data.mutations import AddToCell, RowMutationEntry
from google.cloud.bigtable.data.exceptions import MutationsExceptionGroup

async def write_increment_async(project_id, instance_id, table_id):
    """Increments a value in a Bigtable table using the async client."""
    async with BigtableDataClientAsync(project=project_id) as client:
        table = client.get_table(instance_id, table_id)
        # Define the row key as a byte string
        row_key = b"unique_device_ids_1"
        try:
            async with table.mutations_batcher() as batcher:
                # The AddToCell mutation increments the value of a cell.
                # The value must be a positive or negative integer.
                reading = AddToCell(
                    family="counters",
                    qualifier="odometer",
                    value=32304
                )
                await batcher.append(RowMutationEntry(row_key, [reading]))
            print(f"Successfully incremented row {row_key.decode('utf-8')}.")
        except MutationsExceptionGroup as e:
            print(f"Failed to increment row {row_key.decode('utf-8')}")
            for sub_exception in e.exceptions:
                failed_entry: RowMutationEntry = sub_exception.entry
                cause: Exception = sub_exception.__cause__
                print(
                    f"Failed mutation for row {failed_entry.row_key!r} with error: {cause!r}"
                )
# [END bigtable_write_increment_async]
