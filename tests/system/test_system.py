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

TEST_FAMILY = "test-family"
TEST_FAMILY_2 = "test-family-2"

from google.cloud.bigtable.read_modify_write_rules import MAX_INCREMENT_VALUE

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
        if isinstance(value, int):
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


@pytest_asyncio.fixture(scope="function")
async def temp_rows(table):
    builder = TempRowBuilder(table)
    yield builder
    await builder.delete_rows()

@pytest.mark.asyncio
async def test_ping_and_warm_gapic(client, table):
    """
    Simple ping rpc test
    This test ensures channels are able to authenticate with backend
    """
    request = {"name": table.instance_name}
    await client._gapic_client.ping_and_warm(request)


@pytest.mark.asyncio
async def test_mutation_set_cell(client, table):
    """
    Ensure cells can be set properly
    """
    from google.cloud.bigtable.mutations import SetCell

    mutation = SetCell(
        family=TEST_FAMILY, qualifier=b"test-qualifier", new_value=b"test-value"
    )
    await table.mutate_row("abc", mutation)


@pytest.mark.asyncio
async def test_bulk_mutations_set_cell(client, table):
    """
    Ensure cells can be set properly
    """
    from google.cloud.bigtable.mutations import SetCell, BulkMutationsEntry

    mutation = SetCell(
        family=TEST_FAMILY, qualifier=b"test-qualifier", new_value=b"test-value"
    )
    bulk_mutation = BulkMutationsEntry(b"abc", [mutation])
    await table.bulk_mutate_rows([bulk_mutation])

@pytest.mark.parametrize("start,increment,expected", [
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
])
@pytest.mark.asyncio
async def test_read_modify_write_row_increment(client, table, temp_rows, start, increment, expected):
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
    assert result[0].column_qualifier == qualifier
    assert int(result[0]) == expected

@pytest.mark.parametrize("start,append,expected", [
    (b'', b'', b''),
    ("", "", b""),
    (b'abc', b'123', b'abc123'),
    (b'abc', '123', b'abc123'),
    ('', b'1', b'1'),
    (b'abc', '', b'abc'),
    (b"hello", b"world", b"helloworld"),
])
@pytest.mark.asyncio
async def test_read_modify_write_row_append(client, table, temp_rows, start, append, expected):
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
    assert result[0].column_qualifier == qualifier
    assert result[0].value == expected


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
    await temp_rows.add_row(row_key, value=start_amount, family=family, qualifier=qualifier)
    rule = [IncrementRule(family, qualifier, increment_amount), AppendValueRule(family, qualifier, "hello"), AppendValueRule(family, qualifier, "world"), AppendValueRule(family, qualifier, "!")]
    result = await table.read_modify_write_row(row_key, rule)
    assert result.row_key == row_key
    assert result[0].family == family
    assert result[0].column_qualifier == qualifier
    # result should be a bytes number string for the IncrementRules, followed by the AppendValueRule values
    assert result[0].value == (start_amount+increment_amount).to_bytes(8, "big", signed=True) +  b"helloworld!"
