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

import rich

from populate_table import populate_table


@pytest.fixture(scope="session")
def column_family_config():
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

@pytest.fixture(scope="session")
def cluster_config(project_id):
    """
    Configuration for the clusters to use when creating a new instance
    """
    from google.cloud.bigtable_admin_v2 import types
    cluster = {
        "benchmark-cluster": types.Cluster(
            location=f"projects/{project_id}/locations/us-central1-b",
            serve_nodes=3,
        )
    }
    return cluster


@pytest.mark.usefixtures("table")
@pytest_asyncio.fixture(scope="session")
async def populated_table(table):
    """
    populate a table with benchmark data

    Table details:
      - 10,000 rows
      - single column family
      - single column qualifier
      - splits at 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, and 9000
    """
    user_specified_table = os.getenv("BIGTABLE_TEST_TABLE")
    if table.table_id != user_specified_table:
        # only populate table if it is auto-generated
        # user-specified tables are assumed to be pre-populated
        await populate_table(table, 10_000)
    yield table


@pytest.mark.parametrize("scan_size", [100, 1000, 10_000])
@pytest.mark.asyncio
async def test_scan_throughput_benchmark(populated_table, scan_size, duration=5):
    """
    This benchmark measures the throughput of read_rows against
    a typical table

    The benchmark will:
      - for each `scan_size` in `[100, 1000, 10_000]`, execute the following loop
        for `duration` seconds:
        - pick one of the keys at random, with uniform probability
            - keys within `scan_size` of the end of the table are excluded from sampling
        - scan `scan_size` rows starting at the chosen key

    The benchmark will report throughput in rows per second for each `scan_size` value.
    """
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange
    from google.api_core.exceptions import DeadlineExceeded
    print(f"\nrunning test_scan_throughput_benchmark for {duration}s with scan_size={scan_size}")
    deadline = time.monotonic() + duration
    total_rows = 0
    total_operations = 0
    total_op_time = 0
    while time.monotonic() < deadline:
        start_idx = random.randint(0, max(10_000 - scan_size, 0))
        start_key = start_idx.to_bytes(8, byteorder="big")
        query = ReadRowsQuery(row_ranges=RowRange(start_key=start_key), limit=scan_size)
        try:
            total_operations += 1
            start_time = time.perf_counter()
            async for row in await populated_table.read_rows_stream(query, operation_timeout=deadline - time.monotonic()):
                total_rows += 1
        except DeadlineExceeded as e:
            exc_group = e.__cause__
            if exc_group and any(not isinstance(exc, DeadlineExceeded) for exc in exc_group.exceptions):
                # found error other than deadline exceeded
                raise
        finally:
            total_op_time += time.perf_counter() - start_time
    rich.print(f"[blue]total rows: {total_rows}. total operations: {total_operations} time in operation: {total_op_time:0.2f}s throughput: {total_rows / total_op_time:0.2f} rows/s")


@pytest.mark.asyncio
async def test_sharded_scan_throughput_benchmark(populated_table, duration=10):
    """
    This benchmark measures the throughput of read_rows_sharded against
    a typical table

    The benchmark will:
      - for `duration` seconds, execute the following loop:
        - pick one of the keys at random, with uniform probability
        - build a sharded query using the row key samples
        - scan rows to the end of the table starting at the chosen key

    The benchmark will report throughput in rows per second for each `scan_size` value.
    """
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange
    from google.cloud.bigtable.data.exceptions import ShardedReadRowsExceptionGroup
    from google.api_core.exceptions import DeadlineExceeded
    print(f"\nrunning test_sharded_scan_throughput_benchmark for {duration}s")
    deadline = time.monotonic() + duration
    total_rows = 0
    total_operations = 0
    total_op_time = 0
    table_shard_keys = await populated_table.sample_row_keys()
    while time.monotonic() < deadline:
        start_idx = random.randint(0, 10_000)
        start_key = start_idx.to_bytes(8, byteorder="big")
        query = ReadRowsQuery(row_ranges=RowRange(start_key=start_key))
        shard_query = query.shard(table_shard_keys)
        try:
            total_operations += 1
            start_timestamp = time.perf_counter()
            results = await populated_table.read_rows_sharded(shard_query, operation_timeout=deadline - time.monotonic())
            total_rows += len(results)
        except ShardedReadRowsExceptionGroup as e:
            for sub_exc in e.exceptions:
                if sub_exc.__cause__ and not isinstance(sub_exc.__cause__, DeadlineExceeded):
                    # found error other than deadline exceeded
                    raise
        finally:
            total_op_time += time.perf_counter() - start_timestamp
    rich.print(f"[blue]total rows: {total_rows}. total operations: {total_operations} time in operation: {total_op_time:0.2f}s throughput: {total_rows / total_op_time:0.2f} rows/s")
