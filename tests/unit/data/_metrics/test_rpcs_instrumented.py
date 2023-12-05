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
from grpc import StatusCode

from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data import mutations
from google.cloud.bigtable.data.exceptions import _BigtableExceptionGroup
from google.cloud.bigtable.data._metrics import OperationType

from .._async.test_client import mock_grpc_call


@pytest.mark.parametrize(
    "fn_name,fn_args,gapic_fn,is_unary,expected_type",
    [
        ("read_rows_stream", (ReadRowsQuery(),), "read_rows", False, OperationType.READ_ROWS),
        ("read_rows", (ReadRowsQuery(),), "read_rows", False, OperationType.READ_ROWS),
        ("read_row", (b"row_key",), "read_rows", False, OperationType.READ_ROWS),
        ("read_rows_sharded", ([ReadRowsQuery()],), "read_rows", False, OperationType.READ_ROWS),
        ("row_exists", (b"row_key",), "read_rows", False, OperationType.READ_ROWS),
        ("sample_row_keys", (), "sample_row_keys", False, OperationType.SAMPLE_ROW_KEYS),
        ("mutate_row", (b"row_key", [mutations.DeleteAllFromRow()]), "mutate_row", False, OperationType.MUTATE_ROW),
        (
            "bulk_mutate_rows",
            ([mutations.RowMutationEntry(b"key", [mutations.DeleteAllFromRow()])],),
            "mutate_rows",
            False,
            OperationType.BULK_MUTATE_ROWS
        ),
        ("check_and_mutate_row", (b"row_key", None), "check_and_mutate_row", True, OperationType.CHECK_AND_MUTATE),
        (
            "read_modify_write_row",
            (b"row_key", mock.Mock()),
            "read_modify_write_row",
            True,
            OperationType.READ_MODIFY_WRITE
        ),
    ],
)
@pytest.mark.asyncio
async def test_rpc_instrumented(fn_name, fn_args, gapic_fn, is_unary, expected_type):
    """check that all requests attach proper metadata headers"""
    from google.cloud.bigtable.data import TableAsync
    from google.cloud.bigtable.data import BigtableDataClientAsync

    with mock.patch(f"google.cloud.bigtable_v2.BigtableAsyncClient.{gapic_fn}") as gapic_mock:
        if is_unary:
            unary_response = mock.Mock()
            unary_response.row.families = []  # patch for read_modify_write_row
        else:
            unary_response = None
        gapic_mock.return_value = mock_grpc_call(unary_response)
        # gapic_mock.side_effect = RuntimeError("stop early")
        async with BigtableDataClientAsync() as client:
            async with TableAsync(client, "instance-id", "table-id") as table:
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
                assert found_operation.duration < 0.1
                assert found_operation.final_status == StatusCode.OK
                assert found_operation.cluster_id == "unspecified"
                assert found_operation.zone == "global"
                # is_streaming should only be true for read_rows, read_rows_stream, and read_rows_sharded
                assert found_operation.is_streaming == ("read_rows" in fn_name)
                # check attempts
                assert len(found_operation.completed_attempts) == 1
                found_attempt = found_operation.completed_attempts[0]
                assert found_attempt.end_status == StatusCode.OK
