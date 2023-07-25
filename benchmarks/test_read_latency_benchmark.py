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
import uuid
import os

from populate_table import populate_table


@pytest.fixture(scope="session")
def init_column_families():
    """
    specify column families to create when creating a new test table
    """
    from google.cloud.bigtable_admin_v2 import types

    return {"cf": types.ColumnFamily()}


@pytest.fixture(scope="session")
def init_table_id():
    """
    The table_id to use when creating a new test table
    """
    return f"benchmark-table-{uuid.uuid4().hex}"


@pytest.mark.usefixtures("table")
@pytest_asyncio.fixture(scope="session")
async def populated_table(table):
    """
    populate a table with benchmark data
    """
    user_specified_table = os.getenv("BIGTABLE_TEST_TABLE")
    if table.table_id != user_specified_table:
        # only populate table if it is auto-generated
        # user-specified tables are assumed to be pre-populated
        await populate_table(table)
    yield table


@pytest.mark.asyncio
async def test_read_latency_benchmark(populated_table):
    print(populated_table.table_id)
