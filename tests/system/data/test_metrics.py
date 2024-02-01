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

from google.api_core.exceptions import NotFound
from google.api_core import retry

from .test_system import _create_row_and_mutation, init_table_id, column_family_config, cluster_config, temp_rows, TEST_FAMILY, TEST_CLUSTER, TEST_ZONE


# use unique app profile for each run, to avoid conflicts
APP_PROFILE = str(uuid.uuid4())


@pytest_asyncio.fixture(scope="session")
async def table_with_metrics(client, table_id, instance_id):
    from google.cloud.bigtable.data._metrics import GoogleCloudMetricsHandler
    from google.cloud.bigtable.data import SetCell, ReadRowsQuery, RowMutationEntry
    from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule
    from google.cloud.bigtable.data.row_filters import PassAllFilter
    async with client.get_table(instance_id, table_id) as table:
        kwargs = {
            "project_id": client.project,
            "instance_id": instance_id,
            "table_id": table_id,
            "app_profile_id": APP_PROFILE,  # pass in artificial app profile, unique to this run
        }
        # create a GCP exporter with 1 second interval
        table._metrics.handlers = [
            GoogleCloudMetricsHandler(**kwargs, export_interval=1),
        ]
        # run some example operations
        family = TEST_FAMILY
        qualifier = b"qualifier"
        init_cell = SetCell(family=family, qualifier=qualifier, new_value=0)
        await table.mutate_row(b'r-1', init_cell)
        await table.bulk_mutate_rows([RowMutationEntry(b'r-2', [init_cell])])
        await table.read_row(b'r-1')
        await table.read_rows(ReadRowsQuery(row_keys=[b'r-1', b'r-2']))
        await table.sample_row_keys()
        await table.read_modify_write_row(b'r-1', IncrementRule(family, qualifier, 1))
        await table.check_and_mutate_row(b'r-1', PassAllFilter(True), true_case_mutations=[init_cell])
        yield table


@retry.Retry(predicate=retry.if_exception_type(NotFound), initial=1, maximum=5, timeout=5*60)
def get_metric(metric_name, project_id, instance_id, table_id, expect_all_methods=True):
    from google.cloud.monitoring_v3 import MetricServiceClient
    from google.cloud.monitoring_v3.types.common import TimeInterval
    from google.protobuf.timestamp_pb2 import Timestamp
    client = MetricServiceClient()
    now_time = Timestamp()
    now_time.GetCurrentTime()
    response = client.list_time_series(
        name=f"projects/{project_id}",
        filter=f'metric.type="bigtable.googleapis.com/client/{metric_name}" AND metric.labels.app_profile={APP_PROFILE} AND resource.labels.instance="{instance_id}" AND resource.labels.table="{table_id}"',
        interval=TimeInterval(
            start_time="2024-01-01T00:00:00Z", end_time=now_time
        ),
    )
    metric_map = {}
    response = list(response)
    for r in response:
        metric_map.setdefault(r.metric.labels["method"], []).append(r)
    if expect_all_methods and len(metric_map.keys()) < 6:
        raise NotFound("Missing expected metrics")
    return response


@pytest.mark.asyncio
@pytest.mark.parametrize("latency_type", ["operation", "attempt", "server"])
async def test_generic_latency_metric(table_with_metrics, project_id, instance_id, table_id, latency_type):
    """
    Shared tests for operation_latencies, attempt_latencies, and server_latencies

    These values should all have the same metadata, so we can share test logic
    """
    from google.cloud.bigtable import __version__
    from google.cloud.bigtable.data._metrics.data_model import OperationType
    from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import MILLIS_AGGREGATION
    found_metrics = get_metric(f"{latency_type}_latencies", project_id, instance_id, table_id)
    # check proper units
    assert all(r.metric_kind == 2 for r in found_metrics)  # DELTA
    assert all(r.value_type == 5 for r in found_metrics)  # DISTRIBUTION

    # should have at least one example for each metric type
    for op_type in OperationType:
        assert any(r.metric.labels["method"] == op_type.value for r in found_metrics)
    # should have 5 labels: status, method, client_name, streaming, app_profile
    assert all(len(r.metric.labels) == 5 for r in found_metrics)
    # should all have successful status
    assert all(r.metric.labels["status"] == "OK" for r in found_metrics)
    # should all have client_name set
    assert all(r.metric.labels["client_name"] == f"python-bigtable/{__version__}" for r in found_metrics)
    # should have streaming set to false or true. Only ReadRows should have true set, when doing bulk reads
    assert all(re.match("(false|true)", r.metric.labels["streaming"]) for r in found_metrics)
    assert all(r.metric.labels["streaming"] == "false" for r in found_metrics if r.metric.labels["method"] != "ReadRows")
    assert any(r.metric.labels["streaming"] == "false" for r in found_metrics if r.metric.labels["method"] == "ReadRows")
    assert any(r.metric.labels["streaming"] == "true" for r in found_metrics if r.metric.labels["method"] == "ReadRows")
    # should have app_profile set
    assert all(r.metric.labels["app_profile"] == APP_PROFILE for r in found_metrics)
    # should have resouce populated
    assert all(r.resource.type == "bigtable_table" for r in found_metrics)
    assert all(len(r.resource.labels) == 5 for r in found_metrics)
    assert all(r.resource.labels["instance"] == instance_id for r in found_metrics)
    assert all(r.resource.labels["table"] == table_id for r in found_metrics)
    assert all(r.resource.labels["project_id"] == project_id for r in found_metrics)
    assert all(r.resource.labels["zone"] == TEST_ZONE for r in found_metrics)
    assert all(r.resource.labels["cluster"] == TEST_CLUSTER for r in found_metrics)
    # should have reasonable point values
    all_values = [pt.value.distribution_value for r in found_metrics for pt in r.points]
    assert all(v.count > 0 for v in all_values)
    assert all(v.mean > 0 for v in all_values)
    assert all(v.mean < 5000 for v in all_values)
    # should have buckets populated
    assert all(v.bucket_options.explicit_buckets.bounds == MILLIS_AGGREGATION._boundaries for v in all_values)


@pytest.mark.asyncio
async def test_first_response_latency_metric(table_with_metrics, project_id, instance_id, table_id):
    """
    Shared tests for first_response_latencies

    Unlike other latencies, this one is only tracked for read_rows requests. And does not track streaming
    """
    from google.cloud.bigtable import __version__
    from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import MILLIS_AGGREGATION
    found_metrics = get_metric("first_response_latencies", project_id, instance_id, table_id, expect_all_methods=False)
    # check proper units
    assert all(r.metric_kind == 2 for r in found_metrics)  # DELTA
    assert all(r.value_type == 5 for r in found_metrics)  # DISTRIBUTION

    # should only have ReadRows
    assert all(r.metric.labels["method"] == "ReadRows" for r in found_metrics)
    # should have 5 labels: status, method, client_name, app_profile
    assert all(len(r.metric.labels) == 4 for r in found_metrics)
    # should all have successful status
    assert all(r.metric.labels["status"] == "OK" for r in found_metrics)
    # should all have client_name set
    assert all(r.metric.labels["client_name"] == f"python-bigtable/{__version__}" for r in found_metrics)
    # should have app_profile set
    assert all(r.metric.labels["app_profile"] == APP_PROFILE for r in found_metrics)
    # should have resouce populated
    assert all(r.resource.type == "bigtable_table" for r in found_metrics)
    assert all(len(r.resource.labels) == 5 for r in found_metrics)
    assert all(r.resource.labels["instance"] == instance_id for r in found_metrics)
    assert all(r.resource.labels["table"] == table_id for r in found_metrics)
    assert all(r.resource.labels["project_id"] == project_id for r in found_metrics)
    assert all(r.resource.labels["zone"] == TEST_ZONE for r in found_metrics)
    assert all(r.resource.labels["cluster"] == TEST_CLUSTER for r in found_metrics)
    # should have reasonable point values
    all_values = [pt.value.distribution_value for r in found_metrics for pt in r.points]
    assert all(v.count > 0 for v in all_values)
    assert all(v.mean > 0 for v in all_values)
    assert all(v.mean < 2000 for v in all_values)
    # should have buckets populated
    assert all(v.bucket_options.explicit_buckets.bounds == MILLIS_AGGREGATION._boundaries for v in all_values)


@pytest.mark.asyncio
@pytest.mark.parametrize("latency_type", ["application_blocking", "client_blocking"])
async def test_blocking_latency_metrics(table_with_metrics, project_id, instance_id, table_id, latency_type):
    """
    Shared tests for application_blocking_latencies and client_blocking_latencies

    These values should all have the same metadata, so we can share test logic
    Unlike the other latencies, these don't track status or streaming fields
    """
    from google.cloud.bigtable import __version__
    from google.cloud.bigtable.data._metrics.data_model import OperationType
    from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import MILLIS_AGGREGATION
    found_metrics = get_metric(f"{latency_type}_latencies", project_id, instance_id, table_id)
    # check proper units
    assert all(r.metric_kind == 2 for r in found_metrics)  # DELTA
    assert all(r.value_type == 5 for r in found_metrics)  # DISTRIBUTION

    # should have at least one example for each metric type
    for op_type in OperationType:
        assert any(r.metric.labels["method"] == op_type.value for r in found_metrics)
    # should have 3 labels: method, client_name, app_profile
    assert all(len(r.metric.labels) == 3 for r in found_metrics)
    # should all have client_name set
    assert all(r.metric.labels["client_name"] == f"python-bigtable/{__version__}" for r in found_metrics)
    # should have app_profile set
    assert all(r.metric.labels["app_profile"] == APP_PROFILE for r in found_metrics)
    # should have resouce populated
    assert all(r.resource.type == "bigtable_table" for r in found_metrics)
    assert all(len(r.resource.labels) == 5 for r in found_metrics)
    assert all(r.resource.labels["instance"] == instance_id for r in found_metrics)
    assert all(r.resource.labels["table"] == table_id for r in found_metrics)
    assert all(r.resource.labels["project_id"] == project_id for r in found_metrics)
    assert all(r.resource.labels["zone"] == TEST_ZONE for r in found_metrics)
    assert all(r.resource.labels["cluster"] == TEST_CLUSTER for r in found_metrics)
    # should have reasonable point values
    all_values = [pt.value.distribution_value for r in found_metrics for pt in r.points]
    assert all(v.count > 0 for v in all_values)
    assert all(v.mean >= 0 for v in all_values)
    assert all(v.mean < 2000 for v in all_values)
    # should have buckets populated
    assert all(v.bucket_options.explicit_buckets.bounds == MILLIS_AGGREGATION._boundaries for v in all_values)


@pytest.mark.asyncio
@pytest.mark.parametrize("count_type", ["retry", "connectivity_error"])
async def test_count_metrics(table_with_metrics, project_id, instance_id, table_id, count_type):
    """
    Shared tests for retry_count and connectivity_error_count

    These values should all have the same metadata, so we can share test logic.
    Count metrics do not include the streaming field
    """
    from google.cloud.bigtable import __version__
    from google.cloud.bigtable.data._metrics.data_model import OperationType
    found_metrics = get_metric(f"{count_type}_count", project_id, instance_id, table_id)
    # check proper units
    assert all(r.metric_kind == 2 for r in found_metrics)  # DELTA
    assert all(r.value_type == 2 for r in found_metrics)  # INT


    # should have at least one example for each metric type
    for op_type in OperationType:
        assert any(r.metric.labels["method"] == op_type.value for r in found_metrics)
    # should have 5 labels: status, method, client_name, app_profile
    assert all(len(r.metric.labels) == 4 for r in found_metrics)
    # should all have successful status
    assert all(r.metric.labels["status"] == "OK" for r in found_metrics)
    # should all have client_name set
    assert all(r.metric.labels["client_name"] == f"python-bigtable/{__version__}" for r in found_metrics)
    # should have app_profile set
    assert all(r.metric.labels["app_profile"] == APP_PROFILE for r in found_metrics)
    # should have resouce populated
    assert all(r.resource.type == "bigtable_table" for r in found_metrics)
    assert all(len(r.resource.labels) == 5 for r in found_metrics)
    assert all(r.resource.labels["instance"] == instance_id for r in found_metrics)
    assert all(r.resource.labels["table"] == table_id for r in found_metrics)
    assert all(r.resource.labels["project_id"] == project_id for r in found_metrics)
    assert all(r.resource.labels["zone"] == TEST_ZONE for r in found_metrics)
    assert all(r.resource.labels["cluster"] == TEST_CLUSTER for r in found_metrics)
    # should have no errors, so all values should be 0
    all_values = [pt.value.int64_value for r in found_metrics for pt in r.points]
    assert all(v == 0 for v in all_values)
