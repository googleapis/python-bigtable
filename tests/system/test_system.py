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


@pytest_asyncio.fixture
async def client():
    from google.cloud.bigtable import BigtableDataClient

    project = os.getenv("GOOGLE_CLOUD_PROJECT") or None
    async with BigtableDataClient(project=project) as client:
        yield client


@pytest_asyncio.fixture
async def table(client):
    instance = os.getenv("BIGTABLE_TEST_INSTANCE") or "test-instance"
    table = os.getenv("BIGTABLE_TEST_TABLE") or "test-table"
    async with client.get_table(instance, table) as table:
        yield table


class TempRowBuilder:
    """
    Used to add rows to a table for testing purposes.
    """
    def __init__(self, table):
        self.rows = []
        self.table = table

    async def add_row(self, row_key, family, qualifier, value):
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
async def test_read_rows_stream(table, temp_rows):
    """
    Ensure that the read_rows_stream method works
    """
    await temp_rows.add_row(b"row_key_1", "cf1", "c1", b"value1")
    await temp_rows.add_row(b"row_key_2", "cf1", "c1", b"value2")

    # full table scan
    generator = await table.read_rows_stream({})
    first_row = await generator.__anext__()
    second_row = await generator.__anext__()
    assert first_row.row_key == b"row_key_1"
    assert second_row.row_key == b"row_key_2"
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_read_rows(table, temp_rows):
    """
    Ensure that the read_rows method works
    """
    await temp_rows.add_row(b"row_key_1", "cf1", "c1", b"value1")
    await temp_rows.add_row(b"row_key_2", "cf1", "c1", b"value2")
    # full table scan
    row_list = await table.read_rows({})
    assert len(row_list) == 2
    assert row_list[0].row_key == b"row_key_1"
    assert row_list[1].row_key == b"row_key_2"
