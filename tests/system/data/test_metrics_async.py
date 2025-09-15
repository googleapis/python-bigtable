# Copyright 2024 Google LLC
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
import os
import pytest
import uuid

from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler
from google.cloud.bigtable.data._metrics.data_model import CompletedOperationMetric

from google.cloud.bigtable.data._cross_sync import CrossSync


from . import TEST_FAMILY, SystemTestRunner

__CROSS_SYNC_OUTPUT__ = "tests.system.data.test_metrics_autogen"


class _MetricsTestHandler(MetricsHandler):
    """
    Store completed metrics events in internal lists for testing
    """

    def __init__(self, **kwargs):
        self.completed_operations = []
        self.completed_attempts = []
        self.cancelled_operations = []

    def on_operation_complete(self, op):
        self.completed_operations.append(op)

    def on_operation_cancelled(self, op):
        self.cancelled_operations.append(op)

    def on_attempt_complete(self, attempt, op):
        self.completed_attempts.append((attempt, op))

    def total(self):
        return len(self.completed_operations) + len(self._ancelled_operations) + len(self.completed_attempts)

    def clear(self):
        self.cancelled_operations.clear()
        self.completed_operations.clear()
        self.completed_attempts.clear()

    def __repr__(self):
        return f"{self.__class__}(completed_operations={len(self.completed_operations)}, cancelled_operations={len(self.cancelled_operations)}, completed_attempts={len(self.completed_attempts)}"


@CrossSync.convert_class(sync_name="TestMetrics")
class TestMetricsAsync(SystemTestRunner):

    def _make_client(self):
        project = os.getenv("GOOGLE_CLOUD_PROJECT") or None
        return CrossSync.DataClient(project=project)

    @pytest.fixture(scope="session")
    def handler(self):
        return _MetricsTestHandler()

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="function", autouse=True)
    async def _clear_handler(self, handler):
        """Clear handler between each test"""
        handler.clear()

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="session")
    async def client(self):
        async with self._make_client() as client:
            yield client

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="function")
    async def temp_rows(self, target):
        builder = CrossSync.TempRowBuilder(target)
        yield builder
        await builder.delete_rows()

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="session")
    async def target(self, client, table_id, instance_id, handler):
        """
        This fixture runs twice: once for a standard table, and once with an authorized view

        Note: emulator doesn't support authorized views. Only use target
        """
        async with client.get_table(instance_id, table_id) as table:
            table._metrics.add_handler(handler)
            yield table

    @CrossSync.pytest
    async def test_read_modify_write(self, target, temp_rows, handler):
        from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule

        row_key = b"test-row-key"
        family = TEST_FAMILY
        qualifier = b"test-qualifier"
        await temp_rows.add_row(
            row_key, value=0, family=family, qualifier=qualifier
        )
        rule = IncrementRule(family, qualifier, 1)
        await target.read_modify_write_row(row_key, rule)
        breakpoint()
        assert handler.total() == 1
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        operation = handler.completed_operations[0]
        assert operation.final_status.value[0] == 0
        assert operation.final_status.value[0] == 0
