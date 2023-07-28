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
import threading
from itertools import product

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


@pytest.fixture(scope="session")
def legacy_client(project_id):
    from google.cloud.bigtable.client import Client
    client = Client(project=project_id)
    yield client


@pytest.fixture(scope="session")
def legacy_table(legacy_client, table_id, instance_id):
    instance = legacy_client.instance(instance_id)
    table = instance.table(table_id)
    yield table


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


@pytest.mark.parametrize("scan_size,num_threads", product([100, 10_000], [1,8]))
def test_legacy_scan_throughput_benchmark(populated_table, legacy_table, scan_size, num_threads, duration=TEST_DURATION):
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
    print(f"\nrunning test_legacy_scan_throughput_benchmark for {duration}s with scan_size={scan_size} num_threads={num_threads}")
    total_rows = 0
    total_operations = 0
    total_op_time = 0
    def _inner(table, start_key, scan_size):
        for row in table.read_rows(start_key=start_key, limit=scan_size):
            pass
    while total_op_time < duration:
        key_list = [random.randint(0, max(10_000 - scan_size, 0)).to_bytes(8, "big") for _ in range(num_threads)]
        thread_list = [threading.Thread(target=_inner, args=(legacy_table, key, scan_size)) for key in key_list]
        total_operations += num_threads
        start_time = time.perf_counter()
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        total_rows += scan_size * num_threads
        total_op_time += time.perf_counter() - start_time
    rich.print(f"[blue]throughput: {total_rows / total_op_time:,.2f} rows/s QPS: {total_operations / total_op_time:,.2f} ops/s")

@pytest.mark.parametrize("num_threads",[1,8])
def test_legacy_point_read_throughput_benchmark(populated_table, legacy_table, num_threads, duration=TEST_DURATION):
    """
    This benchmark measures the throughput of read_row against
    a typical table

    The benchmark will:
      - for `duration` seconds:
        - pick one of the keys at random, with uniform probability
        - call `read_row` on the chosen key

    The benchmark will report throughput in rows per second.
    """
    print(f"\nrunning test_legacy_point_read_throughput for {duration}s with num_threads={num_threads}")
    total_rows = 0
    total_op_time = 0
    def _inner(table, key):
        table.read_row(key)
    while total_op_time < duration:
        key_list = [random.randint(0, 10_000).to_bytes(8, "big") for _ in range(num_threads)]
        thread_list = [threading.Thread(target=_inner, args=(legacy_table, key)) for key in key_list]
        start_time = time.perf_counter()
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()
        total_op_time += time.perf_counter() - start_time
        total_rows += 1
    # rich.print(f"[blue]total rows: {total_rows}. total operations: {total_operations} time in operation: {total_op_time:0.2f}s throughput: {total_rows / total_op_time:0.2f} rows/s")
    rich.print(f"[blue]throughput: {total_rows / total_op_time:,.2f}")
