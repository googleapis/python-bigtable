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
from google.api_core import retry
from google.api_core.exceptions import ClientError

from google.cloud.bigtable.read_modify_write_rules import MAX_INCREMENT_VALUE

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
        self, row_key, *, family=TEST_FAMILY, qualifier=b"q", value=b"test-value"
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


@pytest_asyncio.fixture(scope="function")
async def temp_rows(table):
    builder = TempRowBuilder(table)
    yield builder
    await builder.delete_rows()


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
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
async def test_ping_and_warm(client, table):
    """
    Test ping and warm from handwritten client
    """
    try:
        channel = client.transport._grpc_channel.pool[0]
    except Exception:
        # for sync client
        channel = client.transport._grpc_channel
    results = await client._ping_and_warm_instances(channel)
    assert len(results) == 1
    assert results[0] is None


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_mutation_set_cell(table, temp_rows):
    """
    Ensure cells can be set properly
    """
    from google.cloud.bigtable.mutations import SetCell

    row_key = b"mutate"
    family = TEST_FAMILY
    qualifier = b"test-qualifier"
    start_value = b"start"
    await temp_rows.add_row(
        row_key, family=family, qualifier=qualifier, value=start_value
    )

    # ensure cell is initialized
    assert (await _retrieve_cell_value(table, row_key)) == start_value

    expected_value = b"new-value"
    mutation = SetCell(
        family=TEST_FAMILY, qualifier=b"test-qualifier", new_value=expected_value
    )
    await table.mutate_row(row_key, mutation)

    # ensure cell is updated
    assert (await _retrieve_cell_value(table, row_key)) == expected_value


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_sample_row_keys(client, table, temp_rows):
    """
    Sample keys should return a single sample in small test tables
    """
    await temp_rows.add_row(b"row_key_1")
    await temp_rows.add_row(b"row_key_2")

    results = await table.sample_row_keys()
    assert len(results) == 1
    sample = results[0]
    assert isinstance(sample[0], bytes)
    assert isinstance(sample[1], int)


@pytest.mark.asyncio
async def test_bulk_mutations_set_cell(client, table, temp_rows):
    """
    Ensure cells can be set properly
    """
    from google.cloud.bigtable.mutations import SetCell, RowMutationEntry

    row_key = b"bulk_mutate"
    family = TEST_FAMILY
    qualifier = b"test-qualifier"
    start_value = b"start"
    await temp_rows.add_row(
        row_key, family=family, qualifier=qualifier, value=start_value
    )

    # ensure cell is initialized
    assert (await _retrieve_cell_value(table, row_key)) == start_value

    expected_value = b"new-value"
    mutation = SetCell(
        family=TEST_FAMILY, qualifier=b"test-qualifier", new_value=expected_value
    )
    bulk_mutation = RowMutationEntry(row_key, [mutation])
    await table.bulk_mutate_rows([bulk_mutation])

    # ensure cell is updated
    assert (await _retrieve_cell_value(table, row_key)) == expected_value


@pytest.mark.parametrize(
    "start,increment,expected",
    [
        (0, 0, 0),
        (0, 1, 1),
        (0, -1, -1),
        (1, 0, 1),
        (0, -100, -100),
        (0, 3000, 3000),
        (10, 4, 14),
        (MAX_INCREMENT_VALUE, -MAX_INCREMENT_VALUE, 0),
        (MAX_INCREMENT_VALUE, 2, -MAX_INCREMENT_VALUE),
        (-MAX_INCREMENT_VALUE, -2, MAX_INCREMENT_VALUE),
    ],
)
@pytest.mark.asyncio
async def test_read_modify_write_row_increment(
    client, table, temp_rows, start, increment, expected
):
    """
    test read_modify_write_row
    """
    from google.cloud.bigtable.read_modify_write_rules import IncrementRule

    row_key = b"test-row-key"
    family = TEST_FAMILY
    qualifier = b"test-qualifier"
    await temp_rows.add_row(row_key, value=start, family=family, qualifier=qualifier)

    rule = IncrementRule(family, qualifier, increment)
    result = await table.read_modify_write_row(row_key, rule)
    assert result.row_key == row_key
    assert len(result) == 1
    assert result[0].family == family
    assert result[0].qualifier == qualifier
    assert int(result[0]) == expected
    # ensure that reading from server gives same value
    assert (await _retrieve_cell_value(table, row_key)) == result[0].value


@pytest.mark.parametrize(
    "start,append,expected",
    [
        (b"", b"", b""),
        ("", "", b""),
        (b"abc", b"123", b"abc123"),
        (b"abc", "123", b"abc123"),
        ("", b"1", b"1"),
        (b"abc", "", b"abc"),
        (b"hello", b"world", b"helloworld"),
    ],
)
@pytest.mark.asyncio
async def test_read_modify_write_row_append(
    client, table, temp_rows, start, append, expected
):
    """
    test read_modify_write_row
    """
    from google.cloud.bigtable.read_modify_write_rules import AppendValueRule

    row_key = b"test-row-key"
    family = TEST_FAMILY
    qualifier = b"test-qualifier"
    await temp_rows.add_row(row_key, value=start, family=family, qualifier=qualifier)

    rule = AppendValueRule(family, qualifier, append)
    result = await table.read_modify_write_row(row_key, rule)
    assert result.row_key == row_key
    assert len(result) == 1
    assert result[0].family == family
    assert result[0].qualifier == qualifier
    assert result[0].value == expected
    # ensure that reading from server gives same value
    assert (await _retrieve_cell_value(table, row_key)) == result[0].value


@pytest.mark.asyncio
async def test_read_modify_write_row_chained(client, table, temp_rows):
    """
    test read_modify_write_row with multiple rules
    """
    from google.cloud.bigtable.read_modify_write_rules import AppendValueRule
    from google.cloud.bigtable.read_modify_write_rules import IncrementRule

    row_key = b"test-row-key"
    family = TEST_FAMILY
    qualifier = b"test-qualifier"
    start_amount = 1
    increment_amount = 10
    await temp_rows.add_row(
        row_key, value=start_amount, family=family, qualifier=qualifier
    )
    rule = [
        IncrementRule(family, qualifier, increment_amount),
        AppendValueRule(family, qualifier, "hello"),
        AppendValueRule(family, qualifier, "world"),
        AppendValueRule(family, qualifier, "!"),
    ]
    result = await table.read_modify_write_row(row_key, rule)
    assert result.row_key == row_key
    assert result[0].family == family
    assert result[0].qualifier == qualifier
    # result should be a bytes number string for the IncrementRules, followed by the AppendValueRule values
    assert (
        result[0].value
        == (start_amount + increment_amount).to_bytes(8, "big", signed=True)
        + b"helloworld!"
    )
    # ensure that reading from server gives same value
    assert (await _retrieve_cell_value(table, row_key)) == result[0].value


@pytest.mark.parametrize(
    "start_val,predicate_range,expected_result",
    [
        (1, (0, 2), True),
        (-1, (0, 2), False),
    ],
)
@pytest.mark.asyncio
async def test_check_and_mutate(
    client, table, temp_rows, start_val, predicate_range, expected_result
):
    """
    test that check_and_mutate_row works applies the right mutations, and returns the right result
    """
    from google.cloud.bigtable.mutations import SetCell
    from google.cloud.bigtable.row_filters import ValueRangeFilter

    row_key = b"test-row-key"
    family = TEST_FAMILY
    qualifier = b"test-qualifier"

    await temp_rows.add_row(
        row_key, value=start_val, family=family, qualifier=qualifier
    )

    false_mutation_value = b"false-mutation-value"
    false_mutation = SetCell(
        family=TEST_FAMILY, qualifier=qualifier, new_value=false_mutation_value
    )
    true_mutation_value = b"true-mutation-value"
    true_mutation = SetCell(
        family=TEST_FAMILY, qualifier=qualifier, new_value=true_mutation_value
    )
    predicate = ValueRangeFilter(predicate_range[0], predicate_range[1])
    result = await table.check_and_mutate_row(
        row_key,
        predicate,
        true_case_mutations=true_mutation,
        false_case_mutations=false_mutation,
    )
    assert result == expected_result
    # ensure cell is updated
    expected_value = true_mutation_value if expected_result else false_mutation_value
    assert (await _retrieve_cell_value(table, row_key)) == expected_value


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
async def test_read_rows_sharded_simple(table, temp_rows):
    """
    Test read rows sharded with two queries
    """
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery

    await temp_rows.add_row(b"a")
    await temp_rows.add_row(b"b")
    await temp_rows.add_row(b"c")
    await temp_rows.add_row(b"d")
    query1 = ReadRowsQuery(row_keys=[b"a", b"c"])
    query2 = ReadRowsQuery(row_keys=[b"b", b"d"])
    row_list = await table.read_rows_sharded([query1, query2])
    assert len(row_list) == 4
    assert row_list[0].row_key == b"a"
    assert row_list[1].row_key == b"c"
    assert row_list[2].row_key == b"b"
    assert row_list[3].row_key == b"d"


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows_sharded_from_sample(table, temp_rows):
    """
    Test end-to-end sharding
    """
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery
    from google.cloud.bigtable.read_rows_query import RowRange

    await temp_rows.add_row(b"a")
    await temp_rows.add_row(b"b")
    await temp_rows.add_row(b"c")
    await temp_rows.add_row(b"d")

    table_shard_keys = await table.sample_row_keys()
    query = ReadRowsQuery(row_ranges=[RowRange(start_key=b"b", end_key=b"z")])
    shard_queries = query.shard(table_shard_keys)
    row_list = await table.read_rows_sharded(shard_queries)
    assert len(row_list) == 3
    assert row_list[0].row_key == b"b"
    assert row_list[1].row_key == b"c"
    assert row_list[2].row_key == b"d"


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows_sharded_filters_limits(table, temp_rows):
    """
    Test read rows sharded with filters and limits
    """
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery
    from google.cloud.bigtable.row_filters import ApplyLabelFilter

    await temp_rows.add_row(b"a")
    await temp_rows.add_row(b"b")
    await temp_rows.add_row(b"c")
    await temp_rows.add_row(b"d")

    label_filter1 = ApplyLabelFilter("first")
    label_filter2 = ApplyLabelFilter("second")
    query1 = ReadRowsQuery(row_keys=[b"a", b"c"], limit=1, row_filter=label_filter1)
    query2 = ReadRowsQuery(row_keys=[b"b", b"d"], row_filter=label_filter2)
    row_list = await table.read_rows_sharded([query1, query2])
    assert len(row_list) == 3
    assert row_list[0].row_key == b"a"
    assert row_list[1].row_key == b"b"
    assert row_list[2].row_key == b"d"
    assert row_list[0][0].labels == ["first"]
    assert row_list[1][0].labels == ["second"]
    assert row_list[2][0].labels == ["second"]


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
async def test_read_rows_single_key_query(table, temp_rows):
    """
    Ensure that the read_rows method works with specified query
    """
    from google.cloud.bigtable import ReadRowsQuery

    await temp_rows.add_row(b"a")
    await temp_rows.add_row(b"b")
    await temp_rows.add_row(b"c")
    await temp_rows.add_row(b"d")
    # retrieve specific keys
    query = ReadRowsQuery(row_keys=[b"a", b"c"])
    row_list = await table.read_rows(query)
    assert len(row_list) == 2
    assert row_list[0].row_key == b"a"
    assert row_list[1].row_key == b"c"


@retry.Retry(predicate=retry.if_exception_type(ClientError), initial=1, maximum=5)
@pytest.mark.asyncio
async def test_read_rows_with_filter(table, temp_rows):
    """
    ensure filters are applied
    """
    from google.cloud.bigtable import ReadRowsQuery
    from google.cloud.bigtable.row_filters import ApplyLabelFilter

    await temp_rows.add_row(b"a")
    await temp_rows.add_row(b"b")
    await temp_rows.add_row(b"c")
    await temp_rows.add_row(b"d")
    # retrieve keys with filter
    expected_label = "test-label"
    row_filter = ApplyLabelFilter(expected_label)
    query = ReadRowsQuery(row_filter=row_filter)
    row_list = await table.read_rows(query)
    assert len(row_list) == 4
    for row in row_list:
        assert row[0].labels == [expected_label]


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


@pytest.mark.asyncio
async def test_read_row(table, temp_rows):
    """
    Test read_row (single row helper)
    """
    from google.cloud.bigtable import Row

    await temp_rows.add_row(b"row_key_1", value=b"value")
    row = await table.read_row(b"row_key_1")
    assert isinstance(row, Row)
    assert row.row_key == b"row_key_1"
    assert row.cells[0].value == b"value"


@pytest.mark.asyncio
async def test_read_row_missing(table):
    """
    Test read_row when row does not exist
    """
    from google.api_core import exceptions

    row_key = "row_key_not_exist"
    result = await table.read_row(row_key)
    assert result is None
    with pytest.raises(exceptions.InvalidArgument) as e:
        await table.read_row("")
        assert "Row key must be non-empty" in str(e)


@pytest.mark.asyncio
async def test_read_row_w_filter(table, temp_rows):
    """
    Test read_row (single row helper)
    """
    from google.cloud.bigtable import Row
    from google.cloud.bigtable.row_filters import ApplyLabelFilter

    await temp_rows.add_row(b"row_key_1", value=b"value")
    expected_label = "test-label"
    label_filter = ApplyLabelFilter(expected_label)
    row = await table.read_row(b"row_key_1", row_filter=label_filter)
    assert isinstance(row, Row)
    assert row.row_key == b"row_key_1"
    assert row.cells[0].value == b"value"
    assert row.cells[0].labels == [expected_label]


@pytest.mark.asyncio
async def test_row_exists(table, temp_rows):
    from google.api_core import exceptions

    """Test row_exists with rows that exist and don't exist"""
    assert await table.row_exists(b"row_key_1") is False
    await temp_rows.add_row(b"row_key_1")
    assert await table.row_exists(b"row_key_1") is True
    assert await table.row_exists("row_key_1") is True
    assert await table.row_exists(b"row_key_2") is False
    assert await table.row_exists("row_key_2") is False
    assert await table.row_exists("3") is False
    await temp_rows.add_row(b"3")
    assert await table.row_exists(b"3") is True
    with pytest.raises(exceptions.InvalidArgument) as e:
        await table.row_exists("")
        assert "Row kest must be non-empty" in str(e)


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
