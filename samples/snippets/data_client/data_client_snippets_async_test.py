# Copyright 2023, Google LLC
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
import pytest
import pytest_asyncio
import uuid
import os
import asyncio

import data_client_snippets_async as data_snippets


PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BIGTABLE_INSTANCE = os.environ["BIGTABLE_INSTANCE"]
TABLE_ID_STATIC = os.getenv(
    "BIGTABLE_TABLE", None
)  # if not set, a temproary table will be generated


@pytest.fixture(scope="session")
def table_id():
    from google.cloud import bigtable

    client = bigtable.Client(project=PROJECT, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE)
    table_id = TABLE_ID_STATIC or f"data-client-{str(uuid.uuid4())[:16]}"

    admin_table = instance.table(table_id)
    if not admin_table.exists():
        admin_table.create(column_families={"family": None})

    yield table_id

    if not table_id == TABLE_ID_STATIC:
        # clean up table when finished
        admin_table.delete()


@pytest_asyncio.fixture
async def table(table_id):
    from google.cloud.bigtable.data import BigtableDataClientAsync

    async with BigtableDataClientAsync(project=PROJECT) as client:
        async with client.get_table(BIGTABLE_INSTANCE, table_id) as table:
            yield table


@pytest.mark.asyncio
async def test_create_table(table_id):
    from google.cloud.bigtable.data import TableAsync

    result = await data_snippets.create_table(PROJECT, BIGTABLE_INSTANCE, table_id)
    assert isinstance(result, TableAsync)
    assert result.table_id == table_id
    assert result.instance_id == BIGTABLE_INSTANCE
    assert result.client.project == PROJECT


@pytest.mark.asyncio
async def test_set_cell(table):
    await data_snippets.set_cell(table)


@pytest.mark.asyncio
async def test_bulk_mutate(table):
    await data_snippets.bulk_mutate(table)


@pytest.mark.asyncio
async def test_mutations_batcher(table):
    from google.cloud.bigtable.data import MutationsBatcherAsync

    batcher = await data_snippets.mutations_batcher(table)
    assert isinstance(batcher, MutationsBatcherAsync)


@pytest.mark.asyncio
async def test_read_row(table):
    await data_snippets.read_row(table)


@pytest.mark.asyncio
async def test_read_rows_list(table):
    await data_snippets.read_rows_list(table)


@pytest.mark.asyncio
async def test_read_rows_stream(table):
    await data_snippets.read_rows_stream(table)


@pytest.mark.asyncio
async def test_read_rows_sharded(table):
    await data_snippets.read_rows_sharded(table)


@pytest.mark.asyncio
async def test_row_exists(table):
    await data_snippets.row_exists(table)


@pytest.mark.asyncio
async def test_read_modify_write_increment(table):
    await data_snippets.read_modify_write_increment(table)


@pytest.mark.asyncio
async def test_read_modify_write_append(table):
    await data_snippets.read_modify_write_append(table)


@pytest.mark.asyncio
async def test_check_and_mutate(table):
    await data_snippets.check_and_mutate(table)
