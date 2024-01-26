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

"""
This file tests each rpc method to ensure they support metrics properly
"""

import pytest
import mock
import datetime
from grpc import StatusCode
from grpc.aio import Metadata

from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data import mutations
from google.cloud.bigtable.data._metrics import OperationType
from google.cloud.bigtable.data._metrics.data_model import BIGTABLE_METADATA_KEY
from google.cloud.bigtable.data._metrics.data_model import SERVER_TIMING_METADATA_KEY

from .._async.test_client import mock_grpc_call


RPC_ARGS = "fn_name,fn_args,gapic_fn,is_unary,expected_type"
RETRYABLE_RPCS = [
    (
        "read_rows_stream",
        (ReadRowsQuery(),),
        "read_rows",
        False,
        OperationType.READ_ROWS,
    ),
    ("read_rows", (ReadRowsQuery(),), "read_rows", False, OperationType.READ_ROWS),
    ("read_row", (b"row_key",), "read_rows", False, OperationType.READ_ROWS),
    (
        "read_rows_sharded",
        ([ReadRowsQuery()],),
        "read_rows",
        False,
        OperationType.READ_ROWS,
    ),
    ("row_exists", (b"row_key",), "read_rows", False, OperationType.READ_ROWS),
    ("sample_row_keys", (), "sample_row_keys", False, OperationType.SAMPLE_ROW_KEYS),
    (
        "mutate_row",
        (b"row_key", [mutations.DeleteAllFromRow()]),
        "mutate_row",
        False,
        OperationType.MUTATE_ROW,
    ),
    (
        "bulk_mutate_rows",
        ([mutations.RowMutationEntry(b"key", [mutations.DeleteAllFromRow()])],),
        "mutate_rows",
        False,
        OperationType.BULK_MUTATE_ROWS,
    ),
]
ALL_RPCS = RETRYABLE_RPCS + [
    (
        "check_and_mutate_row",
        (b"row_key", None),
        "check_and_mutate_row",
        True,
        OperationType.CHECK_AND_MUTATE,
    ),
    (
        "read_modify_write_row",
        (b"row_key", mock.Mock()),
        "read_modify_write_row",
        True,
        OperationType.READ_MODIFY_WRITE,
    ),
]


@pytest.mark.parametrize(RPC_ARGS, ALL_RPCS)
@pytest.mark.asyncio
async def test_rpc_instrumented(fn_name, fn_args, gapic_fn, is_unary, expected_type):
    """check that all requests attach proper metadata headers"""
    from google.cloud.bigtable.data import TableAsync
    from google.cloud.bigtable.data import BigtableDataClientAsync

    cluster_data = "my-cluster"
    zone_data = "my-zone"
    expected_gfe_latency = 123

    with mock.patch(
        f"google.cloud.bigtable_v2.BigtableAsyncClient.{gapic_fn}"
    ) as gapic_mock:
        if is_unary:
            unary_response = mock.Mock()
            unary_response.row.families = []  # patch for read_modify_write_row
        else:
            unary_response = None
        # populate metadata fields
        initial_metadata = Metadata(
            (BIGTABLE_METADATA_KEY, f"{zone_data} {cluster_data}".encode("utf-8"))
        )
        trailing_metadata = Metadata(
            (SERVER_TIMING_METADATA_KEY, f"gfet4t7; dur={expected_gfe_latency*1000}")
        )
        grpc_call = mock_grpc_call(
            unary_response=unary_response,
            initial_metadata=initial_metadata,
            trailing_metadata=trailing_metadata,
        )
        gapic_mock.return_value = grpc_call
        async with BigtableDataClientAsync() as client:
            table = TableAsync(client, "instance-id", "table-id")
            # customize metrics handlers
            mock_metric_handler = mock.Mock()
            table._metrics.handlers = [mock_metric_handler]
            test_fn = table.__getattribute__(fn_name)
            maybe_stream = await test_fn(*fn_args)
            # iterate stream if it exists
            try:
                [i async for i in maybe_stream]
            except TypeError:
                pass
            # check for recorded metrics values
            assert mock_metric_handler.on_operation_complete.call_count == 1
            found_operation = mock_metric_handler.on_operation_complete.call_args[0][0]
            # make sure expected fields were set properly
            assert found_operation.op_type == expected_type
            now = datetime.datetime.now(datetime.timezone.utc)
            assert found_operation.start_time - now < datetime.timedelta(seconds=1)
            assert found_operation.duration < 0.1
            assert found_operation.duration > 0
            assert found_operation.final_status == StatusCode.OK
            assert found_operation.cluster_id == cluster_data
            assert found_operation.zone == zone_data
            # is_streaming should only be true for read_rows, read_rows_stream, and read_rows_sharded
            assert found_operation.is_streaming == ("read_rows" in fn_name)
            # check attempts
            assert len(found_operation.completed_attempts) == 1
            found_attempt = found_operation.completed_attempts[0]
            assert found_attempt.end_status == StatusCode.OK
            assert found_attempt.start_time - now < datetime.timedelta(seconds=1)
            assert found_attempt.duration < 0.1
            assert found_attempt.duration > 0
            assert found_attempt.start_time >= found_operation.start_time
            assert found_attempt.duration <= found_operation.duration
            assert found_attempt.gfe_latency == expected_gfe_latency
            # first response latency not populated, because no real read_rows chunks processed
            assert found_attempt.first_response_latency is None
            # no application blocking time or backoff time expected
            assert found_attempt.application_blocking_time == 0
            assert found_attempt.backoff_before_attempt == 0
            # no throttling expected
            assert found_attempt.grpc_throttling_time == 0
            assert found_operation.flow_throttling_time == 0


@pytest.mark.parametrize(RPC_ARGS, RETRYABLE_RPCS)
@pytest.mark.asyncio
async def test_rpc_instrumented_multiple_attempts(
    fn_name, fn_args, gapic_fn, is_unary, expected_type
):
    """check that all requests attach proper metadata headers, with a retry"""
    from google.cloud.bigtable.data import TableAsync
    from google.cloud.bigtable.data import BigtableDataClientAsync
    from google.api_core.exceptions import Aborted
    from google.cloud.bigtable_v2.types import MutateRowsResponse
    from google.rpc.status_pb2 import Status

    with mock.patch(
        f"google.cloud.bigtable_v2.BigtableAsyncClient.{gapic_fn}"
    ) as gapic_mock:
        if is_unary:
            unary_response = mock.Mock()
            unary_response.row.families = []  # patch for read_modify_write_row
        else:
            unary_response = None
        grpc_call = mock_grpc_call(unary_response=unary_response)
        if gapic_fn == "mutate_rows":
            # patch response to send success
            grpc_call.stream_response = [
                MutateRowsResponse(
                    entries=[MutateRowsResponse.Entry(index=0, status=Status(code=0))]
                )
            ]
        gapic_mock.side_effect = [Aborted("first attempt failed"), grpc_call]
        async with BigtableDataClientAsync() as client:
            table = TableAsync(client, "instance-id", "table-id")
            # customize metrics handlers
            mock_metric_handler = mock.Mock()
            table._metrics.handlers = [mock_metric_handler]
            test_fn = table.__getattribute__(fn_name)
            maybe_stream = await test_fn(*fn_args, retryable_errors=(Aborted,))
            # iterate stream if it exists
            try:
                [_ async for _ in maybe_stream]
            except TypeError:
                pass
            # check for recorded metrics values
            assert mock_metric_handler.on_operation_complete.call_count == 1
            found_operation = mock_metric_handler.on_operation_complete.call_args[0][0]
            # make sure expected fields were set properly
            assert found_operation.op_type == expected_type
            now = datetime.datetime.now(datetime.timezone.utc)
            assert found_operation.start_time - now < datetime.timedelta(seconds=1)
            assert found_operation.duration < 0.1
            assert found_operation.duration > 0
            assert found_operation.final_status == StatusCode.OK
            # metadata wasn't set, should see default values
            assert found_operation.cluster_id == "unspecified"
            assert found_operation.zone == "global"
            # is_streaming should only be true for read_rows, read_rows_stream, and read_rows_sharded
            assert found_operation.is_streaming == ("read_rows" in fn_name)
            # check attempts
            assert len(found_operation.completed_attempts) == 2
            failure, success = found_operation.completed_attempts
            for attempt in [success, failure]:
                # check things that should be consistent across attempts
                assert attempt.start_time - now < datetime.timedelta(seconds=1)
                assert attempt.duration < 0.1
                assert attempt.duration > 0
                assert attempt.start_time >= found_operation.start_time
                assert attempt.duration <= found_operation.duration
                assert attempt.application_blocking_time == 0
            assert success.end_status == StatusCode.OK
            assert failure.end_status == StatusCode.ABORTED
            assert success.start_time > failure.start_time + datetime.timedelta(
                seconds=failure.duration
            )
            assert success.backoff_before_attempt > 0
            assert failure.backoff_before_attempt == 0


@pytest.mark.asyncio
async def test_batcher_rpcs_instrumented():
    """check that all requests attach proper metadata headers"""
    from google.cloud.bigtable.data import TableAsync
    from google.cloud.bigtable.data import BigtableDataClientAsync

    cluster_data = "my-cluster"
    zone_data = "my-zone"
    expected_gfe_latency = 123

    with mock.patch(
        "google.cloud.bigtable_v2.BigtableAsyncClient.mutate_rows"
    ) as gapic_mock:
        # populate metadata fields
        initial_metadata = Metadata(
            (BIGTABLE_METADATA_KEY, f"{zone_data} {cluster_data}".encode("utf-8"))
        )
        trailing_metadata = Metadata(
            (SERVER_TIMING_METADATA_KEY, f"gfet4t7; dur={expected_gfe_latency*1000}")
        )
        grpc_call = mock_grpc_call(
            initial_metadata=initial_metadata, trailing_metadata=trailing_metadata
        )
        gapic_mock.return_value = grpc_call
        async with BigtableDataClientAsync() as client:
            table = TableAsync(client, "instance-id", "table-id")
            # customize metrics handlers
            mock_metric_handler = mock.Mock()
            table._metrics.handlers = [mock_metric_handler]
            async with table.mutations_batcher() as batcher:
                await batcher.append(
                    mutations.RowMutationEntry(
                        b"row-key", [mutations.DeleteAllFromRow()]
                    )
                )
            # check for recorded metrics values
            assert mock_metric_handler.on_operation_complete.call_count == 1
            found_operation = mock_metric_handler.on_operation_complete.call_args[0][0]
            # make sure expected fields were set properly
            assert found_operation.op_type == OperationType.BULK_MUTATE_ROWS
            now = datetime.datetime.now(datetime.timezone.utc)
            assert found_operation.start_time - now < datetime.timedelta(seconds=1)
            assert found_operation.duration < 0.1
            assert found_operation.duration > 0
            assert found_operation.final_status == StatusCode.OK
            assert found_operation.cluster_id == cluster_data
            assert found_operation.zone == zone_data
            assert found_operation.is_streaming is False
            # check attempts
            assert len(found_operation.completed_attempts) == 1
            found_attempt = found_operation.completed_attempts[0]
            assert found_attempt.end_status == StatusCode.OK
            assert found_attempt.start_time - now < datetime.timedelta(seconds=1)
            assert found_attempt.duration < 0.1
            assert found_attempt.duration > 0
            assert found_attempt.start_time >= found_operation.start_time
            assert found_attempt.duration <= found_operation.duration
            assert found_attempt.gfe_latency == expected_gfe_latency
            # first response latency not populated, because no real read_rows chunks processed
            assert found_attempt.first_response_latency is None
            # no application blocking time or backoff time expected
            assert found_attempt.application_blocking_time == 0
            assert found_attempt.backoff_before_attempt == 0
