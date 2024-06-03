# Copyright 2024 Google Inc.
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

import os
from typing import AsyncGenerator

from google.cloud.bigtable.data import BigtableDataClientAsync, SetCell
import pytest, pytest_asyncio

from main_async import main


PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BIGTABLE_INSTANCE = os.environ["BIGTABLE_INSTANCE"]
TABLE_ID_FORMAT = "quickstart-test-{}"


@pytest_asyncio.fixture
async def table_id() -> AsyncGenerator[str, None]:
    table_id = _create_table()
    await _populate_table(table_id)

    yield table_id

    _delete_table(table_id)


def _create_table():
    from google.cloud import bigtable
    import uuid

    client = bigtable.Client(project=PROJECT, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE)

    table_id = TABLE_ID_FORMAT.format(uuid.uuid4().hex[:8])
    table = instance.table(table_id)
    if table.exists():
        table.delete()

    table.create(column_families={"cf1": None})

    client.close()
    return table_id


async def _populate_table(table_id: str):
    async with BigtableDataClientAsync(project=PROJECT) as client:
        async with client.get_table(BIGTABLE_INSTANCE, table_id) as table:
            await table.mutate_row("r1", SetCell("cf1", "c1", "test-value"))


def _delete_table(table_id: str):
    from google.cloud import bigtable

    client = bigtable.Client(project=PROJECT, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE)
    table = instance.table(table_id)
    table.delete()
    client.close()


@pytest.mark.asyncio
async def test_main(capsys, table_id):
    await main(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    assert "Row key: r1\nData: test-value\n" in out
