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
import asyncio

import rich

from populate_table import populate_table

TEST_DURATION = int(os.getenv("TEST_DURATION", 30))
ENABLE_PROFILING = os.getenv("ENABLE_PROFILING", 0) in [1, "1", "true", "True", "TRUE"]

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
      - $ROW_SIZE bytes per row (default: 100b)
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


@pytest.mark.parametrize("scan_size", [100, 10_000])
@pytest.mark.asyncio
async def test_scan_throughput_benchmark(populated_table, scan_size, duration=TEST_DURATION, batch_size=20):
    """
    This benchmark measures the throughput of read_rows against
    a typical table

    The benchmark will:
      - for each `scan_size`, execute the following loop
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
    total_rows = 0
    total_operations = 0
    total_op_time = 0
    while total_op_time < duration:
        test_keys = [random.randint(0, max(10_000 - scan_size, 0)).to_bytes(8, byteorder="big") for _ in range(batch_size)]
        test_queries = [ReadRowsQuery(row_ranges=RowRange(start_key=key), limit=scan_size) for key in test_keys]
        total_operations += batch_size
        task_list = [asyncio.create_task(populated_table.read_rows(q)) for q in test_queries]
        start_time = time.perf_counter()
        if ENABLE_PROFILING:
            import yappi
            yappi.start()
        await asyncio.gather(*task_list)
        if ENABLE_PROFILING:
            import yappi
            import io
            yappi.stop()
        total_op_time += time.perf_counter() - start_time
        total_rows += scan_size * batch_size
    if ENABLE_PROFILING:
        result = io.StringIO()
        yappi.get_func_stats().print_all(out=result)
        yappi.get_func_stats().save(f"table_scan_{scan_size}.stats", type="pstat")
        print(result.getvalue())
    # rich.print(f"[blue]total rows: {total_rows}. total operations: {total_operations} time in operation: {total_op_time:0.2f}s throughput: {total_rows / total_op_time:0.2f} rows/s QPS: {total_operations / total_op_time:0.2f} ops/s")
    rich.print(f"[blue]throughput: {total_rows / total_op_time:,.2f} rows/s QPS: {total_operations / total_op_time:,.2f} ops/s")


@pytest.mark.asyncio
async def test_point_read_throughput_benchmark(populated_table, batch_count=1000, duration=TEST_DURATION):
    """
    This benchmark measures the throughput of read_row against
    a typical table

    The benchmark will:
      - for `duration` seconds:
        - pick one of the keys at random, with uniform probability
        - call `read_row` on the chosen key

    The benchmark will report throughput in rows per second.
    """
    print(f"\nrunning test_point_read_throughput for {duration}s")
    total_rows = 0
    total_operations = 0
    total_op_time = 0
    while total_op_time < duration:
        test_keys = [random.randint(0, 10_000).to_bytes(8, "big") for _ in range(batch_count)]
        task_list = [asyncio.create_task(populated_table.read_row(k)) for k in test_keys]
        start_time = time.perf_counter()
        if ENABLE_PROFILING:
            import yappi
            yappi.start()
        await asyncio.gather(*task_list)
        if ENABLE_PROFILING:
            import yappi
            import io
            yappi.stop()
        total_op_time += time.perf_counter() - start_time
        total_rows += batch_count
        total_operations += batch_count
    if ENABLE_PROFILING:
        result = io.StringIO()
        yappi.get_func_stats().print_all(out=result)
        yappi.get_func_stats().save("point_read.stats", type="pstat")
        print(result.getvalue())
    # rich.print(f"[blue]total rows: {total_rows}. total operations: {total_operations} time in operation: {total_op_time:0.2f}s throughput: {total_rows / total_op_time:0.2f} rows/s")
    rich.print(f"[blue]throughput: {total_rows / total_op_time:,.2f}")

@pytest.mark.asyncio
async def test_sharded_scan_throughput_benchmark(populated_table, duration=TEST_DURATION, batch_size=100):
    """
    This benchmark measures the throughput of read_rows_sharded against
    a typical table

    The benchmark will:
      - for `duration` seconds, execute the following loop:
        - pick one of the keys at random, with uniform probability
        - build a sharded query using the row key samples
        - scan rows to the end of the table starting at the chosen key

    The benchmark will report throughput in rows per second.
    """
    from google.cloud.bigtable.data import ReadRowsQuery
    from google.cloud.bigtable.data import RowRange
    from google.cloud.bigtable.data.exceptions import ShardedReadRowsExceptionGroup
    from google.api_core.exceptions import DeadlineExceeded
    print(f"\nrunning test_sharded_scan_throughput_benchmark for {duration}s")
    total_rows = 0
    total_operations = 0
    total_op_time = 0
    table_shard_keys = await populated_table.sample_row_keys()
    table_scan = ReadRowsQuery()
    sharded_scan = table_scan.shard(table_shard_keys)
    while total_op_time < duration:
        total_operations += batch_size
        start_timestamp = time.perf_counter()
        task_list = [asyncio.create_task(populated_table.read_rows_sharded(sharded_scan)) for _ in range(batch_size)]
        results = await asyncio.gather(*task_list)
        total_op_time += time.perf_counter() - start_timestamp
        total_rows += len(results) * len(results[0])
    # rich.print(f"[blue]total rows: {total_rows}. total operations: {total_operations} time in operation: {total_op_time:0.2f}s throughput: {total_rows / total_op_time:0.2f} rows/s")
    rich.print(f"[blue]throughput: {total_rows / total_op_time:,.2f} rows/s QPS: {total_operations / total_op_time:,.2f} ops/s")
