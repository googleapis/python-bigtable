from typing import Tuple

from google.api_core import operation as api_core_operation

from google.cloud.bigtable import admin_v2
from google.cloud.bigtable.data._cross_sync import CrossSync
from google.cloud.bigtable.data import mutations, read_rows_query

import time

from .conftest import (
    INSTANCE_PREFIX,
    BACKUP_PREFIX,
    ROW_PREFIX,
    DEFAULT_CLUSTER_LOCATIONS,
    REPLICATION_CLUSTER_LOCATIONS,
    TEST_TABLE_NAME,
    TEST_BACKUP_TABLE_NAME,
    TEST_COLUMMN_FAMILY_NAME,
    TEST_COLUMN_NAME,
    NUM_ROWS,
    INITIAL_CELL_VALUE,
    NEW_CELL_VALUE,
)

from datetime import datetime, timedelta

import asyncio
import pytest

# TODO: Finish CrossSync integration after moving to new directory


_TABLE_ADMIN_CLIENT = admin_v2.BigtableTableAdminAsyncClient()
_INSTANCE_ADMIN_CLIENT = admin_v2.BigtableInstanceAdminAsyncClient()
_DATA_CLIENT = CrossSync.DataClient()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.stop()
    loop.close()


@pytest.fixture(scope="function")
async def instances_to_delete():
    instances_to_delete = []
    
    yield instances_to_delete

    for instance in instances_to_delete:
        await _INSTANCE_ADMIN_CLIENT.delete_instance(name=instance.name)


@pytest.fixture(scope="function")
async def backups_to_delete():
    backups_to_delete = []

    yield backups_to_delete

    for backup in backups_to_delete:
        await _TABLE_ADMIN_CLIENT.delete_backup(name=backup.name)


def ctime() -> int:
    return int(time.time())


async def create_instance(
    project_id,
    instances_to_delete,
    storage_type=admin_v2.StorageType.HDD,
    cluster_locations=DEFAULT_CLUSTER_LOCATIONS,
) -> Tuple[admin_v2.Instance, admin_v2.Table]:
    clusters = {}

    instance_id = f"{INSTANCE_PREFIX}-{ctime()}"

    for idx, location in enumerate(cluster_locations):
        clusters[location] = admin_v2.Cluster(
            name=_INSTANCE_ADMIN_CLIENT.cluster_path(project_id, instance_id, f"{instance_id}-{idx}"),
            location=_INSTANCE_ADMIN_CLIENT.common_location_path(project_id, location),
            default_storage_type=storage_type
        )

    create_instance_request = admin_v2.CreateInstanceRequest(
        parent=_INSTANCE_ADMIN_CLIENT.common_project_path(project_id),
        instance_id=instance_id,
        instance=admin_v2.Instance(
            display_name=instance_id[:30],  # truncate to 30 characters because of character limit
        ),
        clusters=clusters
    )
    operation = await _INSTANCE_ADMIN_CLIENT.create_instance(create_instance_request)
    instance = await operation.result()

    instances_to_delete.append(instance)

    create_table_request = admin_v2.CreateTableRequest(
        parent=_INSTANCE_ADMIN_CLIENT.instance_path(project_id, instance_id),
        table_id=TEST_TABLE_NAME,
        table=admin_v2.Table(
            column_families={
                TEST_COLUMMN_FAMILY_NAME: admin_v2.ColumnFamily(),
            }
        )
    )

    table = await _TABLE_ADMIN_CLIENT.create_table(create_table_request)

    # Populate with dummy data
    await populate_table(instance, table, INITIAL_CELL_VALUE)

    return instance, table


async def populate_table(instance, table, cell_value):
    data_client_table = _DATA_CLIENT.get_table(
        _TABLE_ADMIN_CLIENT.parse_instance_path(instance.name)["instance"],
        _TABLE_ADMIN_CLIENT.parse_table_path(table.name)["table"],
    )
    row_mutation_entries = []
    for i in range(0, NUM_ROWS):
        row_mutation_entries.append(
            mutations.RowMutationEntry(
                row_key=f"{ROW_PREFIX}-{i}",
                mutations=[
                    mutations.SetCell(
                        family=TEST_COLUMMN_FAMILY_NAME,
                        qualifier=TEST_COLUMN_NAME,
                        new_value=cell_value,
                        timestamp_micros=-1
                    )
                ]
            )
        )

    await data_client_table.bulk_mutate_rows(row_mutation_entries)


async def create_backup(instance, table, backups_to_delete) -> admin_v2.Backup:
    list_clusters_response = await _INSTANCE_ADMIN_CLIENT.list_clusters(parent=instance.name)
    cluster_name = list_clusters_response.clusters[0].name

    backup_id = f"{BACKUP_PREFIX}-{ctime()}"

    operation = await _TABLE_ADMIN_CLIENT.create_backup(
        admin_v2.CreateBackupRequest(
            parent=cluster_name,
            backup_id=backup_id,
            backup=admin_v2.Backup(
                name=f"{cluster_name}/backups/{backup_id}",
                source_table=table.name,
                expire_time=datetime.now() + timedelta(hours=7)
            )
        )
    )

    backup = await operation.result()
    backups_to_delete.append(backup)
    return backup


async def assert_table_cell_value_equal_to(instance, table, value):
    data_client_table = await _DATA_CLIENT.get_table(
        _TABLE_ADMIN_CLIENT.parse_instance_path(instance.name)["instance"],
        _TABLE_ADMIN_CLIENT.parse_table_path(table.name)["table"],
    )

    # Read all the rows; there shouldn't be that many of them
    query = read_rows_query.ReadRowsQuery(limit=NUM_ROWS)
    for row in await data_client_table.read_rows_stream(query):
        latest_cell = row[TEST_COLUMMN_FAMILY_NAME, TEST_COLUMN_NAME][0]
        assert latest_cell.value.decode("utf-8") == value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second_instance_storage_type,expect_optimize_operation",
    [
        (admin_v2.StorageType.HDD, False),
        (admin_v2.StorageType.SSD, True),
    ]
)
async def test_optimize_restored_table(
    admin_overlay_project_id,
    instances_to_delete,
    backups_to_delete,
    second_instance_storage_type,
    expect_optimize_operation,
):
    # Create two instances. We backup a table from the first instance to a new table in the
    # second instance. This is to test whether or not different scenarios trigger an
    # optimize_restored_table operation
    instance_with_backup, table_to_backup = await create_instance(
        admin_overlay_project_id,
        instances_to_delete,
        admin_v2.StorageType.HDD
    )

    instance_to_restore, _ = await create_instance(
        admin_overlay_project_id,
        instances_to_delete,
        second_instance_storage_type,
    )

    backup = await create_backup(instance_with_backup, table_to_backup, backups_to_delete)

    # Restore to other instance
    restore_operation = await _TABLE_ADMIN_CLIENT.restore_table(
        admin_v2.RestoreTableRequest(
            parent=instance_to_restore.name,
            table_id=TEST_BACKUP_TABLE_NAME,
            backup=backup.name,
        )
    )

    assert isinstance(restore_operation, admin_v2.RestoreTableOperation)
    restored_table = await restore_operation.result()

    optimize_operation = await restore_operation.optimize_restored_table_operation()
    if expect_optimize_operation:
        assert isinstance(optimize_operation, api_core_operation.Operation)
        await optimize_operation.result()
    else:
        assert optimize_operation is None
    
    # Test that the new table exists
    assert restored_table.name == f"{instance_to_restore.name}/tables/{TEST_BACKUP_TABLE_NAME}"
    await assert_table_cell_value_equal_to(instance_to_restore, restored_table, INITIAL_CELL_VALUE)


@pytest.mark.asyncio
async def test_wait_for_consistency_async(instances_to_delete, admin_overlay_project_id):
    instance, table = await create_instance(
        admin_overlay_project_id,
        instances_to_delete,
        cluster_locations=REPLICATION_CLUSTER_LOCATIONS,
    )

    await populate_table(instance, table, NEW_CELL_VALUE)
    wait_for_consistency_request = admin_v2.WaitForConsistencyRequest(
        name=table.name,
        standard_read_remote_writes=admin_v2.StandardReadRemoteWrites(),
    )

    await _TABLE_ADMIN_CLIENT.wait_for_consistency(wait_for_consistency_request)
    await assert_table_cell_value_equal_to(instance, table, NEW_CELL_VALUE)
