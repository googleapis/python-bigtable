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
import random
import time

from populate_table import populate_table
from populate_table import KEY_WIDTH


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

    Table details:
      - 10,000 rows
      - single column family
      - 10 column qualifiers
    """
    user_specified_table = os.getenv("BIGTABLE_TEST_TABLE")
    if table.table_id != user_specified_table:
        # only populate table if it is auto-generated
        # user-specified tables are assumed to be pre-populated
        await populate_table(table, 10_000)
    yield table


@pytest.mark.asyncio
async def test_scan_throughput_benchmark(populated_table, duration=5):
    """
    This benchmark measures the throughput of read_rows against
    a typical table

    The benchmark will:
      - for each `scan_size` in `[100, 1000, 10_000]`, execute the following loop
        for `duration` seconds:
        - pick one of the 10,000 keys at random, with uniform probability
            - keys within `scan_size` of the end of the table are excluded from sampling
        - scan `scan_size` rows starting at the chosen key

    The benchmark will report throughput in rows per second for each `scan_size` value.

    Table details:
      - 10,000 rows
      - single column family
      - 10 column qualifiers
    """
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange
    from google.api_core.exceptions import DeadlineExceeded
    for scan_size in [100, 1000, 10_000]:
        print(f"running scan throughput benchmark with scan_size={scan_size}")
        deadline = time.monotonic() + duration
        total_rows = 0
        while time.monotonic() < deadline:
            start_idx = random.randint(0, max(10_000 - scan_size, 0))
            start_key = f"user{str(start_idx).zfill(KEY_WIDTH)}"
            query = ReadRowsQuery(row_ranges=RowRange(start_key=start_key), limit=scan_size)
            try:
                results = await populated_table.read_rows(query, operation_timeout=deadline - time.monotonic())
                total_rows += len(results)
            except DeadlineExceeded as e:
                exc_group = e.__cause__
                if exc_group and any(not isinstance(exc, DeadlineExceeded) for exc in exc_group.exceptions):
                    # found error other than deadline exceeded
                    raise
        print(f"total rows: {total_rows}")
