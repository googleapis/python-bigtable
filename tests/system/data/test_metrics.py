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
import re
import mock
import asyncio
import time
from functools import partial

from google.api_core.exceptions import NotFound, ServiceUnavailable, DeadlineExceeded
from google.cloud.bigtable.data import MutationsExceptionGroup
from google.api_core import retry

from google.cloud.bigtable.data import SetCell, ReadRowsQuery, RowMutationEntry
from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule
from google.cloud.bigtable.data.row_filters import PassAllFilter
from google.cloud.bigtable_v2 import BigtableAsyncClient

from .test_system import _create_row_and_mutation, init_table_id, column_family_config, cluster_config, temp_rows, TEST_FAMILY, TEST_CLUSTER, TEST_ZONE

# use this value to make sure we are testing against a consistent test of metrics
# if _populate_calls is modified, this value will have to change
EXPECTED_METRIC_COUNT = 172


async def _populate_calls(client, instance_id, table_id):
    """
    Call rpcs to populate the backend with a set of metrics to run tests against:
        - succcessful rpcs
        - rpcs that raise terminal NotFound error
        - rpcs that raise retryable ServiceUnavailable error and then succeed
        - rpcs that raise retryable ServiceUnavailable error and then timeout
        - rpcs that raise retryable ServiceUnavailable error and then raise terminal NotFound error

    Each set of calls is made with a unique app profile to make assertions about each easier
    This app profile is not set on the backend, but only passed to the metric handler
    """
    # helper function to build separate table instances for each app profile
    def table_with_profile(app_profile_id):
        from google.cloud.bigtable.data._metrics import GoogleCloudMetricsHandler
        table = client.get_table(instance_id, table_id)
        kwargs = {
            "project_id": client.project,
            "instance_id": instance_id,
            "table_id": table_id,
            "app_profile_id": app_profile_id,  # use artificial app profile for metrics, not in backend
        }
        # create a GCP exporter with 1 second interval
        table._metrics.handlers = [
            GoogleCloudMetricsHandler(**kwargs, export_interval=1),
        ]
        return table
    # helper function that builds callables to execute each type of rpc
    def _get_stubs_for_table(table):
        family = TEST_FAMILY
        qualifier = b"qualifier"
        init_cell = SetCell(family=family, qualifier=qualifier, new_value=0)
        rpc_stubs = {
            "mutate_row": [partial(table.mutate_row, b'row1', init_cell)],
            "mutate_rows": [partial(table.bulk_mutate_rows, [RowMutationEntry(b'row2', [init_cell])])],
            "read_rows": [partial(table.read_row, b'row1'), partial(table.read_rows, ReadRowsQuery(row_keys=[b'row1', b'row2']))],
            "sample_row_keys": [table.sample_row_keys],
            "read_modify_write_row": [partial(table.read_modify_write_row, b'row1', IncrementRule(family, qualifier, 1))],
            "check_and_mutate_row": [partial(table.check_and_mutate_row, b'row1', PassAllFilter(True), true_case_mutations=[init_cell])],
        }
        return rpc_stubs

    # Call each rpc with no errors. Should be successful.
    print("populating successful rpcs...")
    async with table_with_profile("success") as table:
        stubs = _get_stubs_for_table(table)
        for stub_list in stubs.values():
            for stub in stub_list:
                await stub()

    # Call each rpc with a terminal exception. Does not hit gcp servers
    print("populating terminal NotFound rpcs...")
    async with table_with_profile("terminal_exception") as table:
        stubs = _get_stubs_for_table(table)
        for rpc_name, stub_list in stubs.items():
            for stub in stub_list:
                with mock.patch(f"google.cloud.bigtable_v2.BigtableAsyncClient.{rpc_name}", side_effect=NotFound("test")):
                    with pytest.raises((NotFound, MutationsExceptionGroup)):
                        await stub()

    non_retryable_rpcs = ["read_modify_write_row", "check_and_mutate_row"]
    # Calls hit retryable errors, then succeed
    print("populating retryable success rpcs...")
    async with table_with_profile("retry_then_success") as table:
        stubs = {k:v for k,v in _get_stubs_for_table(table).items() if k not in non_retryable_rpcs}
        for rpc_name, stub_list in stubs.items():
            for stub in stub_list:
                true_fn = BigtableAsyncClient.__dict__[rpc_name]
                counter = 0
                # raise errors twice, then call true function
                def side_effect(*args, **kwargs):
                    nonlocal counter
                    nonlocal true_fn
                    if counter < 2:
                        counter += 1
                        raise ServiceUnavailable("test")
                    return true_fn(table.client._gapic_client, *args, **kwargs)
                with mock.patch(f"google.cloud.bigtable_v2.BigtableAsyncClient.{rpc_name}", side_effect=side_effect):
                    await stub(retryable_errors=(ServiceUnavailable,))

    # Calls hit retryable errors, then hit deadline
    print("populating retryable timeout rpcs...")
    async with table_with_profile("retry_then_timeout") as table:
        stubs = {k:v for k,v in _get_stubs_for_table(table).items() if k not in non_retryable_rpcs}
        for rpc_name, stub_list in stubs.items():
            with mock.patch(f"google.cloud.bigtable_v2.BigtableAsyncClient.{rpc_name}", side_effect=ServiceUnavailable("test")):
                for stub in stub_list:
                    with pytest.raises((DeadlineExceeded, MutationsExceptionGroup)):
                        await stub(operation_timeout=0.5, retryable_errors=(ServiceUnavailable,))

    # Calls hit retryable errors, then hit terminal exception
    print("populating retryable then terminal error rpcs...")
    async with table_with_profile("retry_then_terminal") as table:
        stubs = {k:v for k,v in _get_stubs_for_table(table).items() if k not in non_retryable_rpcs}
        for rpc_name, stub_list in stubs.items():
            for stub in stub_list:
                error_list = [ServiceUnavailable("test")] * 2 + [NotFound("test")]
                with mock.patch(f"google.cloud.bigtable_v2.BigtableAsyncClient.{rpc_name}", side_effect=error_list):
                    with pytest.raises((NotFound, MutationsExceptionGroup)):
                        await stub(retryable_errors=(ServiceUnavailable,))


@pytest_asyncio.fixture(scope="session")
async def get_all_metrics(client, instance_id, table_id, project_id):
    from google.protobuf.timestamp_pb2 import Timestamp
    from google.cloud.monitoring_v3.types.common import TimeInterval
    # populate table with metrics
    start_time = Timestamp()
    start_time.GetCurrentTime()
    await _populate_calls(client, instance_id, table_id)

    # read them down and save to a list. Retry until ready
    @retry.Retry(predicate=retry.if_exception_type(NotFound), maximum=5, timeout=2 * 60)
    def _read_metrics():
        from google.cloud.monitoring_v3 import MetricServiceClient
        all_responses = []
        client = MetricServiceClient()
        end_time = Timestamp()
        end_time.GetCurrentTime()
        all_instruments = [
            "operation_latencies",
            "attempt_latencies",
            "server_latencies",
            "first_response_latencies",
            "application_blocking_latencies",
            "client_blocking_latencies",
            "retry_count",
            "connectivity_error_count"
        ]

        for instrument in all_instruments:
            response = client.list_time_series(
                name=f"projects/{project_id}",
                filter=f'metric.type="bigtable.googleapis.com/client/{instrument}" AND resource.labels.instance="{instance_id}" AND resource.labels.table="{table_id}"',
                interval=TimeInterval(
                    start_time=start_time, end_time=end_time
                ),
            )
            response = list(response)
            if not response:
                print(f"no data for {instrument}")
                raise NotFound("No metrics found")
            all_responses.extend(response)
        if len(all_responses) < EXPECTED_METRIC_COUNT:
            print(f"Only found {len(all_responses)} of {EXPECTED_METRIC_COUNT} metrics. Retrying...")
            raise NotFound("Not all metrics found")
        elif len(all_responses) > EXPECTED_METRIC_COUNT:
            raise ValueError(f"Found more metrics than expected: {len(all_responses)}")
        return all_responses
    print("waiting for metrics to be ready...")
    metrics = _read_metrics()
    print("metrics ready")
    return metrics


@pytest.mark.asyncio
async def test_resource(get_all_metrics, instance_id, table_id, project_id):
    """
    all metrics should have monitored resource populated consistently
    """
    for m in get_all_metrics:
        resource = m.resource
        assert resource.type == "bigtable_table"
        assert len(resource.labels) == 5
        assert resource.labels["instance"] == instance_id
        assert resource.labels["table"] == table_id
        assert resource.labels["project_id"] == project_id
        if 'success' in m.metric.labels["app_profile_id"]:
            # for attempts that succeeded, zone and cluster should be populated
            assert resource.labels["zone"] == TEST_ZONE
            assert resource.labels["cluster"] == TEST_CLUSTER
        else:
            # others should fall back to defaults
            assert resource.labels["zone"] == "unspecified"
            assert resource.labels["cluster"] == "global"

