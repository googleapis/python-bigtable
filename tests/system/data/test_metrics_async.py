# Copyright 2025 Google LLC
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

import asyncio
import pytest
import os

from . import TEST_FAMILY, SystemTestRunner

from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery

from google.cloud.bigtable.data._cross_sync import CrossSync

__CROSS_SYNC_OUTPUT__ = "tests.system.data.test_metrics_autogen"


@CrossSync.convert_class(sync_name="TestExportedMetrics")
class TestExportedExportedMetricsAsync(SystemTestRunner):

    @CrossSync.drop
    @pytest.fixture(scope="session")
    def event_loop(self):
        loop = asyncio.get_event_loop()
        yield loop
        loop.stop()
        loop.close()

    def _make_client(self):
        project = os.getenv("GOOGLE_CLOUD_PROJECT") or None
        return CrossSync.DataClient(project=project)

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="session")
    async def client(self):
        async with self._make_client() as client:
            yield client

    @pytest.fixture(scope="session")
    def metrics_client(self, client):
        yield client._gcp_metrics_exporter.client


    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="function")
    async def temp_rows(self, table):
        builder = CrossSync.TempRowBuilder(table)
        yield builder
        await builder.delete_rows()

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="session")
    async def table(self, client, table_id, instance_id):
        async with client.get_table(instance_id, table_id) as table:
            yield table

    @CrossSync.pytest
    async def test_read_rows(self, table, temp_rows, metrics_client):
        from datetime import datetime, timedelta, timezone
        from google.cloud import monitoring_v3

        await temp_rows.add_row(b"row_key_1")
        await temp_rows.add_row(b"row_key_2")
        row_list = await table.read_rows(ReadRowsQuery())
        # read back metrics

        # 1. Define the Time Interval
        now = datetime.now(timezone.utc)
        # The end time is inclusive
        end_time = now
        # The start time is exclusive, for an interval (startTime, endTime]
        start_time = now - timedelta(minutes=5)

        interval = {"start_time": start_time, "end_time": end_time}
        metric_filter = (
            'metric.type = "bigtable.googleapis.com/client/attempt_latencies" AND metric.labels.client_name != "go-bigtable/1.40.0"'
        )
        results = metrics_client.list_time_series(
            name=f"projects/{table.client.project}",
            filter=metric_filter,
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        )
        print(results)