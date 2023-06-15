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

import pytest
import pytest_asyncio
import os
import asyncio
import uuid
from google.api_core import retry
from google.api_core.exceptions import ClientError

TEST_FAMILY = "test-family"
TEST_FAMILY_2 = "test-family-2"


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope="session")
def instance_admin_client():
    """Client for interacting with the Instance Admin API."""
    from google.cloud.bigtable_admin_v2 import BigtableInstanceAdminClient

    with BigtableInstanceAdminClient() as client:
        yield client


@pytest.fixture(scope="session")
def table_admin_client():
    """Client for interacting with the Table Admin API."""
    from google.cloud.bigtable_admin_v2 import BigtableTableAdminClient

    with BigtableTableAdminClient() as client:
        yield client


@pytest.fixture(scope="session")
def instance_id(instance_admin_client, project_id):
    """
    Returns BIGTABLE_TEST_INSTANCE if set, otherwise creates a new temporary instance for the test session
    """
    from google.cloud.bigtable_admin_v2 import types
    from google.api_core import exceptions

    # use user-specified instance if available
    user_specified_instance = os.getenv("BIGTABLE_TEST_INSTANCE")
    if user_specified_instance:
        print("Using user-specified instance: {}".format(user_specified_instance))
        yield user_specified_instance
        return

    # create a new temporary test instance
    instance_id = "test-instance"
    try:
        operation = instance_admin_client.create_instance(
            parent=f"projects/{project_id}",
            instance_id=instance_id,
            instance=types.Instance(
                display_name="Test Instance",
                labels={"python-system-test": "true"},
            ),
            clusters={
                "test-cluster": types.Cluster(
                    location=f"projects/{project_id}/locations/us-central1-b",
                    serve_nodes=3,
                )
            },
        )
        operation.result(timeout=240)
    except exceptions.AlreadyExists:
        pass
    yield instance_id
    instance_admin_client.delete_instance(
        name=f"projects/{project_id}/instances/{instance_id}"
    )


@pytest.fixture(scope="session")
def table_id(table_admin_client, project_id, instance_id):
    """
    Returns BIGTABLE_TEST_TABLE if set, otherwise creates a new temporary table for the test session
    """
    from google.cloud.bigtable_admin_v2 import types
    from google.api_core import exceptions
    from google.api_core import retry

    # use user-specified instance if available
    user_specified_table = os.getenv("BIGTABLE_TEST_TABLE")
    if user_specified_table:
        print("Using user-specified table: {}".format(user_specified_table))
        yield user_specified_table
        return

    table_id = "test-table"
    retry = retry.Retry(
        predicate=retry.if_exception_type(exceptions.FailedPrecondition)
    )
    try:
        table_admin_client.create_table(
            parent=f"projects/{project_id}/instances/{instance_id}",
            table_id=table_id,
            table=types.Table(
                column_families={
                    TEST_FAMILY: types.ColumnFamily(),
                    TEST_FAMILY_2: types.ColumnFamily(),
                },
            ),
            retry=retry,
        )
    except exceptions.AlreadyExists:
        pass
    yield table_id
    table_admin_client.delete_table(
        name=f"projects/{project_id}/instances/{instance_id}/tables/{table_id}"
    )


@pytest_asyncio.fixture(scope="session")
async def client():
    from google.cloud.bigtable import BigtableDataClient

    project = os.getenv("GOOGLE_CLOUD_PROJECT") or None
    async with BigtableDataClient(project=project) as client:
        yield client


@pytest.fixture(scope="session")
def project_id(client):
    """Returns the project ID from the client."""
    yield client.project


@pytest_asyncio.fixture(scope="session")
async def table(client, table_id, instance_id):
    async with client.get_table(instance_id, table_id) as table:
        yield table


class TempRowBuilder:
    """
    Used to add rows to a table for testing purposes.
    """

    def __init__(self, table):
        self.rows = []
        self.table = table

    async def add_row(
        self, row_key, family=TEST_FAMILY, qualifier=b"q", value=b"test-value"
    ):
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif isinstance(value, int):
            value = value.to_bytes(8, byteorder="big", signed=True)
        request = {
            "table_name": self.table.table_name,
            "row_key": row_key,
            "mutations": [
                {
                    "set_cell": {
                        "family_name": family,
                        "column_qualifier": qualifier,
                        "value": value,
                    }
                }
            ],
        }
        await self.table.client._gapic_client.mutate_row(request)
        self.rows.append(row_key)

    async def delete_rows(self):
        request = {
            "table_name": self.table.table_name,
            "entries": [
                {"row_key": row, "mutations": [{"delete_from_row": {}}]}
                for row in self.rows
            ],
        }
        await self.table.client._gapic_client.mutate_rows(request)


async def _retrieve_cell_value(table, row_key):
    """
    Helper to read an individual row
    """
    from google.cloud.bigtable import ReadRowsQuery

    row_list = await table.read_rows(ReadRowsQuery(row_keys=row_key))
    assert len(row_list) == 1
    row = row_list[0]
    cell = row.cells[0]
    return cell.value


async def _create_row_and_mutation(
    table, temp_rows, *, start_value=b"start", new_value=b"new_value"
):
    """
    Helper to create a new row, and a sample set_cell mutation to change its value
    """
    from google.cloud.bigtable.mutations import SetCell

    row_key = uuid.uuid4().hex.encode()
    family = TEST_FAMILY
    qualifier = b"test-qualifier"
    await temp_rows.add_row(
        row_key, family=family, qualifier=qualifier, value=start_value
    )
    # ensure cell is initialized
    assert (await _retrieve_cell_value(table, row_key)) == start_value

    mutation = SetCell(family=TEST_FAMILY, qualifier=qualifier, new_value=new_value)
    return row_key, mutation


@pytest_asyncio.fixture(scope="function")
async def temp_rows(table):
    builder = TempRowBuilder(table)
    yield builder
    await builder.delete_rows()


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=10)
@pytest.mark.asyncio
async def test_ping_and_warm_gapic(client, table):
    """
    Simple ping rpc test
    This test ensures channels are able to authenticate with backend
    """
    request = {"name": table.instance_name}
    await client._gapic_client.ping_and_warm(request)


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutation_set_cell(table, temp_rows):
    """
    Ensure cells can be set properly
    """
    row_key = b"bulk_mutate"
    new_value = uuid.uuid4().hex.encode()
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value
    )

    await table.mutate_row(row_key, mutation)

    # ensure cell is updated
    assert (await _retrieve_cell_value(table, row_key)) == new_value


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_bulk_mutations_set_cell(client, table, temp_rows):
    """
    Ensure cells can be set properly
    """
    from google.cloud.bigtable.mutations import RowMutationEntry

    new_value = uuid.uuid4().hex.encode()
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])

    await table.bulk_mutate_rows([bulk_mutation])

    # ensure cell is updated
    assert (await _retrieve_cell_value(table, row_key)) == new_value


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutations_batcher_context_manager(client, table, temp_rows):
    """
    test batcher with context manager. Should flush on exit
    """
    from google.cloud.bigtable.mutations import RowMutationEntry

    new_value, new_value2 = [uuid.uuid4().hex.encode() for _ in range(2)]
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value
    )
    row_key2, mutation2 = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value2
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])
    bulk_mutation2 = RowMutationEntry(row_key2, [mutation2])

    async with table.mutations_batcher() as batcher:
        batcher.append(bulk_mutation)
        batcher.append(bulk_mutation2)
    # ensure cell is updated
    assert (await _retrieve_cell_value(table, row_key)) == new_value
    assert len(batcher._staged_entries) == 0


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutations_batcher_manual_flush(client, table, temp_rows):
    """
    batcher should flush when manually requested
    """
    from google.cloud.bigtable.mutations import RowMutationEntry

    new_value = uuid.uuid4().hex.encode()
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])
    async with table.mutations_batcher() as batcher:
        batcher.append(bulk_mutation)
        assert len(batcher._staged_entries) == 1
        await batcher.flush()
        assert len(batcher._staged_entries) == 0
        # ensure cell is updated
        assert (await _retrieve_cell_value(table, row_key)) == new_value


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutations_batcher_timer_flush(client, table, temp_rows):
    """
    batch should occur after flush_interval seconds
    """
    from google.cloud.bigtable.mutations import RowMutationEntry

    new_value = uuid.uuid4().hex.encode()
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])
    flush_interval = 0.1
    async with table.mutations_batcher(flush_interval=flush_interval) as batcher:
        batcher.append(bulk_mutation)
        await asyncio.sleep(0)
        assert len(batcher._staged_entries) == 1
        await asyncio.sleep(flush_interval + 0.1)
        assert len(batcher._staged_entries) == 0
        # ensure cell is updated
        assert (await _retrieve_cell_value(table, row_key)) == new_value


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutations_batcher_count_flush(client, table, temp_rows):
    """
    batch should flush after flush_limit_mutation_count mutations
    """
    from google.cloud.bigtable.mutations import RowMutationEntry

    new_value, new_value2 = [uuid.uuid4().hex.encode() for _ in range(2)]
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])
    row_key2, mutation2 = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value2
    )
    bulk_mutation2 = RowMutationEntry(row_key2, [mutation2])

    async with table.mutations_batcher(flush_limit_mutation_count=2) as batcher:
        batcher.append(bulk_mutation)
        # should be noop; flush not scheduled
        await batcher._prev_flush
        assert len(batcher._staged_entries) == 1
        batcher.append(bulk_mutation2)
        # task should now be scheduled
        await batcher._prev_flush
        assert len(batcher._staged_entries) == 0
        # ensure cells were updated
        assert (await _retrieve_cell_value(table, row_key)) == new_value
        assert (await _retrieve_cell_value(table, row_key2)) == new_value2


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutations_batcher_bytes_flush(client, table, temp_rows):
    """
    batch should flush after flush_limit_bytes bytes
    """
    from google.cloud.bigtable.mutations import RowMutationEntry

    new_value, new_value2 = [uuid.uuid4().hex.encode() for _ in range(2)]
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])
    row_key2, mutation2 = await _create_row_and_mutation(
        table, temp_rows, new_value=new_value2
    )
    bulk_mutation2 = RowMutationEntry(row_key2, [mutation2])

    flush_limit = bulk_mutation.size() + bulk_mutation2.size() - 1

    async with table.mutations_batcher(flush_limit_bytes=flush_limit) as batcher:
        batcher.append(bulk_mutation)
        # should be noop; flush not scheduled
        await batcher._prev_flush
        assert len(batcher._staged_entries) == 1
        batcher.append(bulk_mutation2)
        # task should now be scheduled
        await batcher._prev_flush
        assert len(batcher._staged_entries) == 0
        # ensure cells were updated
        assert (await _retrieve_cell_value(table, row_key)) == new_value
        assert (await _retrieve_cell_value(table, row_key2)) == new_value2


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutations_batcher_no_flush(client, table, temp_rows):
    """
    test with no flush requirements met
    """
    from google.cloud.bigtable.mutations import RowMutationEntry

    new_value = uuid.uuid4().hex.encode()
    start_value = b"unchanged"
    row_key, mutation = await _create_row_and_mutation(
        table, temp_rows, start_value=start_value, new_value=new_value
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])
    row_key2, mutation2 = await _create_row_and_mutation(
        table, temp_rows, start_value=start_value, new_value=new_value
    )
    bulk_mutation2 = RowMutationEntry(row_key2, [mutation2])

    size_limit = bulk_mutation.size() + bulk_mutation2.size() + 1
    async with table.mutations_batcher(
        flush_limit_bytes=size_limit, flush_limit_mutation_count=3, flush_interval=1
    ) as batcher:
        batcher.append(bulk_mutation)
        assert len(batcher._staged_entries) == 1
        batcher.append(bulk_mutation2)
        # should be noop; flush not scheduled
        await batcher._prev_flush
        await asyncio.sleep(0.01)
        assert len(batcher._staged_entries) == 2
        # ensure cells were updated
        assert (await _retrieve_cell_value(table, row_key)) == start_value
        assert (await _retrieve_cell_value(table, row_key2)) == start_value


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows_stream(table, temp_rows):
    """
    Ensure that the read_rows_stream method works
    """
    await temp_rows.add_row(b"row_key_1")
    await temp_rows.add_row(b"row_key_2")

    # full table scan
    generator = await table.read_rows_stream({})
    first_row = await generator.__anext__()
    second_row = await generator.__anext__()
    assert first_row.row_key == b"row_key_1"
    assert second_row.row_key == b"row_key_2"
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows(table, temp_rows):
    """
    Ensure that the read_rows method works
    """
    await temp_rows.add_row(b"row_key_1")
    await temp_rows.add_row(b"row_key_2")
    # full table scan
    row_list = await table.read_rows({})
    assert len(row_list) == 2
    assert row_list[0].row_key == b"row_key_1"
    assert row_list[1].row_key == b"row_key_2"


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows_range_query(table, temp_rows):
    """
    Ensure that the read_rows method works
    """
    from google.cloud.bigtable import ReadRowsQuery
    from google.cloud.bigtable import RowRange

    await temp_rows.add_row(b"a")
    await temp_rows.add_row(b"b")
    await temp_rows.add_row(b"c")
    await temp_rows.add_row(b"d")
    # full table scan
    query = ReadRowsQuery(row_ranges=RowRange(start_key=b"b", end_key=b"d"))
    row_list = await table.read_rows(query)
    assert len(row_list) == 2
    assert row_list[0].row_key == b"b"
    assert row_list[1].row_key == b"c"


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows_key_query(table, temp_rows):
    """
    Ensure that the read_rows method works
    """
    from google.cloud.bigtable import ReadRowsQuery

    await temp_rows.add_row(b"a")
    await temp_rows.add_row(b"b")
    await temp_rows.add_row(b"c")
    await temp_rows.add_row(b"d")
    # full table scan
    query = ReadRowsQuery(row_keys=[b"a", b"c"])
    row_list = await table.read_rows(query)
    assert len(row_list) == 2
    assert row_list[0].row_key == b"a"
    assert row_list[1].row_key == b"c"


@pytest.mark.asyncio
async def test_read_rows_stream_close(table, temp_rows):
    """
    Ensure that the read_rows_stream can be closed
    """
    await temp_rows.add_row(b"row_key_1")
    await temp_rows.add_row(b"row_key_2")

    # full table scan
    generator = await table.read_rows_stream({})
    first_row = await generator.__anext__()
    assert first_row.row_key == b"row_key_1"
    await generator.aclose()
    assert generator.active is False
    with pytest.raises(StopAsyncIteration) as e:
        await generator.__anext__()
        assert "closed" in str(e)


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows_stream_inactive_timer(table, temp_rows):
    """
    Ensure that the read_rows_stream method works
    """
    from google.cloud.bigtable.exceptions import IdleTimeout

    await temp_rows.add_row(b"row_key_1")
    await temp_rows.add_row(b"row_key_2")

    generator = await table.read_rows_stream({})
    await generator._start_idle_timer(0.05)
    await asyncio.sleep(0.2)
    assert generator.active is False
    with pytest.raises(IdleTimeout) as e:
        await generator.__anext__()
        assert "inactivity" in str(e)
        assert "idle_timeout=0.1" in str(e)


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.parametrize(
    "cell_value,filter_input,expect_match",
    [
        (b"abc", b"abc", True),
        (b"abc", "abc", True),
        (b".", ".", True),
        (".*", ".*", True),
        (".*", b".*", True),
        ("a", ".*", False),
        (b".*", b".*", True),
        (r"\a", r"\a", True),
        (b"\xe2\x98\x83", "☃", True),
        ("☃", "☃", True),
        (r"\C☃", r"\C☃", True),
        (1, 1, True),
        (2, 1, False),
        (68, 68, True),
        ("D", 68, False),
        (68, "D", False),
        (-1, -1, True),
        (2852126720, 2852126720, True),
        (-1431655766, -1431655766, True),
        (-1431655766, -1, False),
    ],
)
@pytest.mark.asyncio
async def test_literal_value_filter(
    table, temp_rows, cell_value, filter_input, expect_match
):
    """
    Literal value filter does complex escaping on re2 strings.
    Make sure inputs are properly interpreted by the server
    """
    from google.cloud.bigtable.row_filters import LiteralValueFilter
    from google.cloud.bigtable import ReadRowsQuery

    f = LiteralValueFilter(filter_input)
    await temp_rows.add_row(b"row_key_1", value=cell_value)
    query = ReadRowsQuery(row_filter=f)
    row_list = await table.read_rows(query)
    assert len(row_list) == bool(
        expect_match
    ), f"row {type(cell_value)}({cell_value}) not found with {type(filter_input)}({filter_input}) filter"
