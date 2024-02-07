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
import mock
from functools import partial

from google.api_core.exceptions import NotFound, ServiceUnavailable, DeadlineExceeded
from google.cloud.bigtable.data._metrics.data_model import OperationType
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

ALL_INSTRUMENTS = [
    "operation_latencies",
    "attempt_latencies",
    "server_latencies",
    "first_response_latencies",
    "application_blocking_latencies",
    "client_blocking_latencies",
    "retry_count",
    "connectivity_error_count"
]

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
    # should have 2 attempts, through mocked backoff
    print("populating retryable timeout rpcs...")
    async with table_with_profile("retry_then_timeout") as table:
        stubs = {k:v for k,v in _get_stubs_for_table(table).items() if k not in non_retryable_rpcs}
        for rpc_name, stub_list in stubs.items():
                with mock.patch(f"google.cloud.bigtable_v2.BigtableAsyncClient.{rpc_name}", side_effect=ServiceUnavailable("test")):
                    for stub in stub_list:
                        with mock.patch("google.cloud.bigtable.data._helpers.exponential_sleep_generator", return_value=iter([0.01, 0.01, 5])):
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

        for instrument in ALL_INSTRUMENTS:
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
        # zone and cluster should use default values for failed attempts
        assert resource.labels["zone"] in [TEST_ZONE, 'global']
        assert resource.labels["cluster"] in [TEST_CLUSTER, 'unspecified']


@pytest.mark.asyncio
async def test_client_name(get_all_metrics):
    """
    all metrics should have client_name populated consistently
    """
    from google.cloud.bigtable import __version__
    for m in get_all_metrics:
        client_name = m.metric.labels["client_name"]
        assert client_name == "python-bigtable/" + __version__

@pytest.mark.asyncio
async def test_app_profile(get_all_metrics):
    """
    all metrics should have app_profile populated with one of the test values
    """
    supported_app_profiles = ["success", "terminal_exception", "retry_then_success", "retry_then_timeout", "retry_then_terminal"]
    for m in get_all_metrics:
        app_profile = m.metric.labels["app_profile"]
        assert app_profile in supported_app_profiles

@pytest.mark.asyncio
async def test_latency_data_types(get_all_metrics):
    """
    all latency metrics should have metric_kind DELTA and value_type DISTRIBUTION
    """
    latency_metrics = [m for m in get_all_metrics if "latencies" in m.metric.type]
    # ensure we got all metrics
    assert len(latency_metrics) > 100
    assert any("operation_latencies" in m.metric.type for m in latency_metrics)
    assert any("attempt_latencies" in m.metric.type for m in latency_metrics)
    assert any("server_latencies" in m.metric.type for m in latency_metrics)
    assert any("first_response_latencies" in m.metric.type for m in latency_metrics)
    assert any("application_blocking_latencies" in m.metric.type for m in latency_metrics)
    assert any("client_blocking_latencies" in m.metric.type for m in latency_metrics)
    # ensure all types are correct
    for m in latency_metrics:
        assert m.metric_kind == 2  # DELTA
        assert m.value_type == 5  # DISTRIBUTION

@pytest.mark.asyncio
async def test_count_data_types(get_all_metrics):
    """
    all count metrics should have metric_kind DELTA and value_type INT64
    """
    count_metrics = [m for m in get_all_metrics if "count" in m.metric.type]
    # ensure we got all metrics
    assert len(count_metrics) > 25
    assert any("retry_count" in m.metric.type for m in count_metrics)
    assert any("connectivity_error_count" in m.metric.type for m in count_metrics)
    # ensure all types are correct
    for m in count_metrics:
        assert m.metric_kind == 2  # DELTA
        assert m.value_type == 2  # INT64

@pytest.mark.asyncio
@pytest.mark.parametrize("instrument,methods", [
    ("operation_latencies", list(OperationType)),  # all operation types
    ("attempt_latencies", list(OperationType)),
    ("server_latencies", list(OperationType)),
    ("application_blocking_latencies", list(OperationType)),
    ("client_blocking_latencies", list(OperationType)),
    ("first_response_latencies", [OperationType.READ_ROWS]),  # only valid for ReadRows
    ("connectivity_error_count", list(OperationType)),
    ("retry_count", [OperationType.READ_ROWS, OperationType.SAMPLE_ROW_KEYS, OperationType.BULK_MUTATE_ROWS, OperationType.MUTATE_ROW]),  # only valid for retryable operations
])
async def test_full_method_coverage(get_all_metrics, instrument, methods):
    """
    ensure that each instrument type has data for all expected rpc methods
    """
    filtered_metrics = [m for m in get_all_metrics if instrument in m.metric.type]
    assert len(filtered_metrics) > 0
    # ensure all methods are covered
    for method in methods:
        assert any(method.value in m.metric.labels["method"] for m in filtered_metrics), f"{method} not found in {instrument}"
    # ensure no unexpected methods are covered
    for m in filtered_metrics:
        assert m.metric.labels["method"] in [m.value for m in methods], f"unexpected method {m.metric.labels['method']}"

@pytest.mark.asyncio
@pytest.mark.parametrize("instrument,include_status,include_streaming", [
    ("operation_latencies", True, True),
    ("attempt_latencies", True, True),
    ("server_latencies", True, True),
    ("first_response_latencies", True, False),
    ("connectivity_error_count", True, False),
    ("retry_count", True, False),
    ("application_blocking_latencies", False, False),
    ("client_blocking_latencies", False, False),
])
async def test_labels(get_all_metrics, instrument, include_status, include_streaming):
    """
    all metrics have 3 common labels: method, client_name, app_profile

    some metrics also have status and streaming labels
    """
    assert len(get_all_metrics) > 0
    filtered_metrics = [m for m in get_all_metrics if instrument in m.metric.type]
    expected_num = 3 + int(include_status) + int(include_streaming)
    for m in filtered_metrics:
        labels = m.metric.labels
        # check for count
        assert len(labels) == expected_num
        # check for common labels
        assert "client_name" in labels
        assert "method" in labels
        assert "app_profile" in labels
        # check for optional labels
        if include_status:
            assert "status" in labels
        if include_streaming:
            assert "streaming" in labels

@pytest.mark.asyncio
async def test_streaming_label(get_all_metrics):
    """
    streaming=True indicates point-reads using ReadRows rpc

    We should only set it set on ReadRows, and we should see a mix of True and False in
    the dataset
    """
    # find set of metrics that support streaming tag
    streaming_instruments = ["operation_latencies", "attempt_latencies", "server_latencies"]
    streaming_metrics = [m for m in get_all_metrics if any(i in m.metric.type for i in streaming_instruments)]
    non_read_rows = [m for m in streaming_metrics if  m.metric.labels["method"] != OperationType.READ_ROWS.value]
    assert len(non_read_rows) > 50
    # ensure all non-read-rows have streaming=False
    assert all(m.metric.labels["streaming"] == "false" for m in non_read_rows)
    # ensure read-rows have a mix of True and False, for each instrument
    for instrument in streaming_instruments:
        filtered_read_rows = [m for m in streaming_metrics if instrument in m.metric.type and m.metric.labels["method"] == OperationType.READ_ROWS.value]
        assert len(filtered_read_rows) > 0
        assert any(m.metric.labels["streaming"] == "true" for m in filtered_read_rows)
        assert any(m.metric.labels["streaming"] == "false" for m in filtered_read_rows)

@pytest.mark.asyncio
async def test_status_success(get_all_metrics):
    """
    check the subset of successful rpcs

    They should have no retries, no connectivity errors, a status of OK
    Should have cluster and zone properly set
    """
    success_metrics = [m for m in get_all_metrics if m.metric.labels["app_profile"] == "success"]
    # ensure each expected instrument is present in data
    assert any("operation_latencies" in m.metric.type for m in success_metrics)
    assert any("attempt_latencies" in m.metric.type for m in success_metrics)
    assert any("server_latencies" in m.metric.type for m in success_metrics)
    assert any("first_response_latencies" in m.metric.type for m in success_metrics)
    assert any("application_blocking_latencies" in m.metric.type for m in success_metrics)
    assert any("client_blocking_latencies" in m.metric.type for m in success_metrics)
    for m in success_metrics:
        # ensure no retries or connectivity errors recorded
        assert "connectivity_error_count" not in m.metric.type
        assert "retry_count" not in m.metric.type
        # if instrument has status label, should be OK
        if "status" in m.metric.labels:
            assert m.metric.labels["status"] == "OK"
        # check for cluster and zone
        assert m.resource.labels["zone"] == TEST_ZONE
        assert m.resource.labels["cluster"] == TEST_CLUSTER

@pytest.mark.asyncio
async def test_status_exception(get_all_metrics):
    """
    check the subset of rpcs with a single terminal exception

    They should have no retries, 1+ connectivity errors, a status of NOT_FOUND
    Should have default values for cluster and zone
    """
    fail_metrics = [m for m in get_all_metrics if m.metric.labels["app_profile"] == "terminal_exception"]
    # ensure each expected instrument is present in data
    assert any("operation_latencies" in m.metric.type for m in fail_metrics)
    assert any("attempt_latencies" in m.metric.type for m in fail_metrics)
    assert any("application_blocking_latencies" in m.metric.type for m in fail_metrics)
    assert any("client_blocking_latencies" in m.metric.type for m in fail_metrics)
    assert any("connectivity_error_count" in m.metric.type for m in fail_metrics)
    # server_latencies, first_response_latencies and retry_count are not expected
    assert not any("server_latencies" in m.metric.type for m in fail_metrics)
    assert not any("retry_count" in m.metric.type for m in fail_metrics)
    assert not any("first_response_latencies" in m.metric.type for m in fail_metrics)
    for m in fail_metrics:
        # if instrument has status label, should be UNAVAILABLE
        if "status" in m.metric.labels:
            assert m.metric.labels["status"] == "NOT_FOUND"
        # check for cluster and zone
        assert m.resource.labels["zone"] == 'global'
        assert m.resource.labels["cluster"] == 'unspecified'
    # each rpc should have at least one connectivity error
    # ReadRows will have more, since we test point reads and streams
    connectivity_error_counts = [m for m in fail_metrics if "connectivity_error_count" in m.metric.type]
    for error_metric in connectivity_error_counts:
        total_points = sum([int(pt.value.int64_value) for pt in error_metric.points])
        assert total_points >= 1
    # ensure each rpc reported connectivity errors
    for prc in OperationType:
        assert any(m.metric.labels["method"] == prc.value for m in connectivity_error_counts)


@pytest.mark.asyncio
@pytest.mark.parametrize("app_profile,final_status", [
    ("retry_then_success", "OK"),
    ("retry_then_terminal", "NOT_FOUND"),
    ("retry_then_timeout", "DEADLINE_EXCEEDED"),
])
async def test_status_retry(get_all_metrics, app_profile, final_status):
    """
    check the subset of calls that fail and retry

    All retry metrics should have 2 attempts before reaching final status

    Should have retries, connectivity_errors, and status of `final_status`.
    cluster and zone may or may not change from the default value, depending on the final status
    """
    # get set of all retry_then_success metrics
    retry_metrics = [m for m in get_all_metrics if m.metric.labels["app_profile"] == app_profile]
    # find each relevant instrument
    retry_counts = [m for m in retry_metrics if "retry_count" in m.metric.type]
    assert len(retry_counts) > 0
    connectivity_error_counts = [m for m in retry_metrics if "connectivity_error_count" in m.metric.type]
    assert len(connectivity_error_counts) > 0
    operation_latencies = [m for m in retry_metrics if "operation_latencies" in m.metric.type]
    assert len(operation_latencies) > 0
    attempt_latencies = [m for m in retry_metrics if "attempt_latencies" in m.metric.type]
    assert len(attempt_latencies) > 0
    server_latencies = [m for m in retry_metrics if "server_latencies" in m.metric.type]  # may not be present. only if reached server
    first_response_latencies = [m for m in retry_metrics if "first_response_latencies" in m.metric.type]  # may not be present

    # should have at least 2 retry attempts
    # ReadRows will have more, because it is called multiple times in the test data
    for m in retry_counts:
        total_errors = sum([int(pt.value.int64_value) for pt in m.points])
        assert total_errors >= 2
    # each rpc should have at least one connectivity error
    # most will have 2, but will have 1 if status == NOT_FOUND
    for m in connectivity_error_counts:
        total_errors = sum([int(pt.value.int64_value) for pt in m.points])
        assert total_errors >= 1

    # all operation-level status should be final_status
    for m in operation_latencies + retry_counts:
        assert m.metric.labels["status"] == final_status

    # check attempt statuses
    attempt_statuses = set([m.metric.labels["status"] for m in attempt_latencies + server_latencies + first_response_latencies + connectivity_error_counts])
    if final_status == "DEADLINE_EXCEEDED":
        # operation DEADLINE_EXCEEDED never shows up in attempts
        assert len(attempt_statuses) == 1
        assert "UNAVAILABLE" in attempt_statuses
    else:
        # all other attempt-level status should have a mix of final_status and UNAVAILABLE
        assert len(attempt_statuses) == 2
        assert "UNAVAILABLE" in attempt_statuses
        assert final_status in attempt_statuses


@pytest.mark.asyncio
async def test_latency_metric_histogram_buckets(get_all_metrics):
    """
    latency metrics should all have histogram buckets set up properly
    """
    from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import MILLIS_AGGREGATION
    filtered = [m for m in get_all_metrics if "latency" in m.metric.type]
    all_values = [pt.value.distribution_value for m in filtered for pt in m.points]
    for v in all_values:
        # check bucket schema
        assert v.bucket_options.explicit_buckets.bounds == MILLIS_AGGREGATION.boundaries
        # check for reasobble values
        assert v.count > 0
        assert v.mean > 0
        assert v.mean < 5000
