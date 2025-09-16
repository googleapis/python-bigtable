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
from grpc import RpcError
from grpc.aio import AioRpcError
from grpc.aio import Metadata

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigtable_v2.types import ResponseParams
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler
from google.cloud.bigtable.data._metrics.data_model import CompletedOperationMetric, CompletedAttemptMetric, ActiveOperationMetric, OperationState

from google.cloud.bigtable.data._cross_sync import CrossSync

from . import TEST_FAMILY, SystemTestRunner

if CrossSync.is_async:
    from grpc.aio import UnaryUnaryClientInterceptor
    from grpc.aio import UnaryStreamClientInterceptor
else:
    from grpc import UnaryUnaryClientInterceptor
    from grpc import UnaryStreamClientInterceptor

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

    def on_attempt_complete(self, attempt, _):
        self.completed_attempts.append(attempt)

    def clear(self):
        self.cancelled_operations.clear()
        self.completed_operations.clear()
        self.completed_attempts.clear()

    def __repr__(self):
        return f"{self.__class__}(completed_operations={len(self.completed_operations)}, cancelled_operations={len(self.cancelled_operations)}, completed_attempts={len(self.completed_attempts)}"


class _ErrorInjectorInterceptor(UnaryUnaryClientInterceptor):
    """
    Gprc interceptor used to inject errors into rpc calls, to test failures
    """

    def __init__(self):
        self._exc_list = []

    def push(self, exc: Exception):
        self._exc_list.append(exc)

    def clear(self):
        self._exc_list.clear()

    async def intercept_unary_unary(
        self, continuation, client_call_details, request
    ):
        if self._exc_list:
            raise self._exc_list.pop(0)
        return await continuation(client_call_details, request)

@CrossSync.convert_class(sync_name="TestMetrics")
class TestMetricsAsync(SystemTestRunner):

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

    @pytest.fixture(scope="session")
    def handler(self):
        return _MetricsTestHandler()

    @pytest.fixture(scope="session")
    def error_injector(self):
        return _ErrorInjectorInterceptor()

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="function", autouse=True)
    async def _clear_state(self, handler, error_injector):
        """Clear handler and interceptor between each test"""
        handler.clear()
        error_injector.clear()

    @CrossSync.convert
    @CrossSync.pytest_fixture(scope="session")
    async def client(self, error_injector):
        async with self._make_client() as client:
            if CrossSync.is_async:
                client.transport.grpc_channel._unary_unary_interceptors.append(error_injector)
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
        async with client.get_table(instance_id, table_id) as table:
            table._metrics.add_handler(handler)
            yield table

    @CrossSync.pytest
    async def test_read_rows(self, target, temp_rows, handler, cluster_config):
        await temp_rows.add_row(b"row_key_1")
        await temp_rows.add_row(b"row_key_2")
        handler.clear()
        row_list = await target.read_rows({})
        assert len(row_list) == 2
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is True
        assert operation.op_type.value == "ReadRows"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is not None and operation.first_response_latency_ns < operation.duration_ns
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns > 0 and attempt.application_blocking_time_ns < operation.duration_ns
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_read_rows_stream(self, target, temp_rows, handler, cluster_config):
        await temp_rows.add_row(b"row_key_1")
        await temp_rows.add_row(b"row_key_2")
        handler.clear()
        # full table scan
        generator = await target.read_rows_stream({})
        row_list = [r async for r in generator]
        assert len(row_list) == 2
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is True
        assert operation.op_type.value == "ReadRows"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is not None and operation.first_response_latency_ns < operation.duration_ns
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns > 0 and attempt.application_blocking_time_ns < operation.duration_ns
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_read_row(self, target, temp_rows, handler, cluster_config):
        await temp_rows.add_row(b"row_key_1")
        handler.clear()
        await target.read_row(b"row_key_1")
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is False
        assert operation.op_type.value == "ReadRows"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns > 0 and operation.first_response_latency_ns < operation.duration_ns
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns > 0 and attempt.application_blocking_time_ns < operation.duration_ns
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_read_rows_sharded(self, target, temp_rows, handler, cluster_config):
        from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
        await temp_rows.add_row(b"a")
        await temp_rows.add_row(b"b")
        await temp_rows.add_row(b"c")
        await temp_rows.add_row(b"d")
        query1 = ReadRowsQuery(row_keys=[b"a", b"c"])
        query2 = ReadRowsQuery(row_keys=[b"b", b"d"])
        handler.clear()
        row_list = await target.read_rows_sharded([query1, query2])
        assert len(row_list) == 4
        # validate counts
        assert len(handler.completed_operations) == 2
        assert len(handler.completed_attempts) == 2
        assert len(handler.cancelled_operations) == 0
        # validate operations
        for operation in handler.completed_operations:
            assert isinstance(operation, CompletedOperationMetric)
            assert operation.final_status.value[0] == 0
            assert operation.is_streaming is True
            assert operation.op_type.value == "ReadRows"
            assert len(operation.completed_attempts) == 1
            attempt = operation.completed_attempts[0]
            assert attempt in handler.completed_attempts
            assert operation.cluster_id == next(iter(cluster_config.keys()))
            assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
            assert operation.duration_ns > 0 and operation.duration_ns < 1e9
            assert operation.first_response_latency_ns is not None and operation.first_response_latency_ns < operation.duration_ns
            assert operation.flow_throttling_time_ns == 0
            # validate attempt
            assert isinstance(attempt, CompletedAttemptMetric)
            assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
            assert attempt.end_status.value[0] == 0
            assert attempt.backoff_before_attempt_ns == 0
            assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
            assert attempt.application_blocking_time_ns > 0 and attempt.application_blocking_time_ns < operation.duration_ns
            assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_bulk_mutate_rows(self, target, temp_rows, handler, cluster_config):
        from google.cloud.bigtable.data.mutations import RowMutationEntry

        new_value = uuid.uuid4().hex.encode()
        row_key, mutation = await temp_rows.create_row_and_mutation(
            target, new_value=new_value
        )
        bulk_mutation = RowMutationEntry(row_key, [mutation])

        handler.clear()
        await target.bulk_mutate_rows([bulk_mutation])
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is False
        assert operation.op_type.value == "MutateRows"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is None  # populated for read_rows only
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns == 0
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_mutate_rows_batcher(self, target, temp_rows, handler, cluster_config):
        from google.cloud.bigtable.data.mutations import RowMutationEntry

        new_value, new_value2 = [uuid.uuid4().hex.encode() for _ in range(2)]
        row_key, mutation = await temp_rows.create_row_and_mutation(
            target, new_value=new_value
        )
        row_key2, mutation2 = await temp_rows.create_row_and_mutation(
            target, new_value=new_value2
        )
        bulk_mutation = RowMutationEntry(row_key, [mutation])
        bulk_mutation2 = RowMutationEntry(row_key2, [mutation2])

        handler.clear()
        async with target.mutations_batcher() as batcher:
            await batcher.append(bulk_mutation)
            await batcher.append(bulk_mutation2)
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        # bacher expects to cancel staged operation on close
        assert len(handler.cancelled_operations) == 1
        cancelled = handler.cancelled_operations[0]
        assert isinstance(cancelled, ActiveOperationMetric)
        assert cancelled.state == OperationState.CREATED
        assert not cancelled.completed_attempts
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is False
        assert operation.op_type.value == "MutateRows"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is None  # populated for read_rows only
        assert operation.flow_throttling_time_ns > 0 and operation.flow_throttling_time_ns < operation.duration_ns
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns == 0
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_mutate_row(self, target, temp_rows, handler, cluster_config):
        row_key = b"bulk_mutate"
        new_value = uuid.uuid4().hex.encode()
        row_key, mutation = await temp_rows.create_row_and_mutation(
            target, new_value=new_value
        )
        handler.clear()
        await target.mutate_row(row_key, mutation)
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is False
        assert operation.op_type.value == "MutateRow"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is None  # populated for read_rows only
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns == 0
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_sample_row_keys(self, target, temp_rows, handler, cluster_config):
        await target.sample_row_keys()
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is False
        assert operation.op_type.value == "SampleRowKeys"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is None  # populated for read_rows only
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns == 0
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_read_modify_write(self, target, temp_rows, handler, cluster_config):
        from google.cloud.bigtable.data.read_modify_write_rules import IncrementRule

        row_key = b"test-row-key"
        family = TEST_FAMILY
        qualifier = b"test-qualifier"
        await temp_rows.add_row(
            row_key, value=0, family=family, qualifier=qualifier
        )
        rule = IncrementRule(family, qualifier, 1)
        await target.read_modify_write_row(row_key, rule)
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is False
        assert operation.op_type.value == "ReadModifyWriteRow"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is None  # populated for read_rows only
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns == 0
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_check_and_mutate_row(self, target, temp_rows, handler, cluster_config):
        from google.cloud.bigtable.data.mutations import SetCell
        from google.cloud.bigtable.data.row_filters import ValueRangeFilter

        row_key = b"test-row-key"
        family = TEST_FAMILY
        qualifier = b"test-qualifier"
        await temp_rows.add_row(
            row_key, value=1, family=family, qualifier=qualifier
        )

        true_mutation_value = b"true-mutation-value"
        true_mutation = SetCell(
            family=TEST_FAMILY, qualifier=qualifier, new_value=true_mutation_value
        )
        predicate = ValueRangeFilter(0, 2)
        await target.check_and_mutate_row(
            row_key,
            predicate,
            true_case_mutations=true_mutation,
        )
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.value[0] == 0
        assert operation.is_streaming is False
        assert operation.op_type.value == "CheckAndMutateRow"
        assert len(operation.completed_attempts) == 1
        assert operation.completed_attempts[0] == handler.completed_attempts[0]
        assert operation.cluster_id == next(iter(cluster_config.keys()))
        assert operation.zone == cluster_config[operation.cluster_id].location.split("/")[-1]
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert operation.first_response_latency_ns is None  # populated for read_rows only
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0 and attempt.duration_ns < operation.duration_ns
        assert attempt.end_status.value[0] == 0
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns > 0 and attempt.gfe_latency_ns < attempt.duration_ns
        assert attempt.application_blocking_time_ns == 0
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm

    @CrossSync.pytest
    async def test_check_and_mutate_row_failure_grpc(
        self, target, temp_rows, handler, error_injector
    ):
        """
        Test failure in grpc layer by injecting an error into an interceptor

        No headers expected
        """
        from google.cloud.bigtable.data.mutations import SetCell
        from google.cloud.bigtable.data.row_filters import ValueRangeFilter

        row_key = b"test-row-key"
        family = TEST_FAMILY
        qualifier = b"test-qualifier"
        await temp_rows.add_row(row_key, value=1, family=family, qualifier=qualifier)

        # trigger an exception
        exc = RuntimeError("injected")
        error_injector.push(exc)
        with pytest.raises(RuntimeError):
            await target.check_and_mutate_row(
                row_key,
                predicate=ValueRangeFilter(0,2),
            )
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.name == "UNKNOWN"
        assert operation.is_streaming is False
        assert operation.op_type.value == "CheckAndMutateRow"
        assert len(operation.completed_attempts) == len(handler.completed_attempts)
        assert operation.completed_attempts == handler.completed_attempts
        assert operation.cluster_id == "unspecified"
        assert operation.zone == "global"
        assert operation.duration_ns > 0 and operation.duration_ns < 1e9
        assert (
            operation.first_response_latency_ns is None
        )  # populated for read_rows only
        assert operation.flow_throttling_time_ns == 0
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert isinstance(attempt, CompletedAttemptMetric)
        assert attempt.duration_ns > 0
        assert attempt.end_status.name == "UNKNOWN"
        assert attempt.backoff_before_attempt_ns == 0
        assert attempt.gfe_latency_ns is None
        assert attempt.application_blocking_time_ns == 0
        assert attempt.grpc_throttling_time_ns == 0  # TODO: confirm


    @CrossSync.pytest
    async def test_check_and_mutate_row_failure_invalid_argument(
        self, target, temp_rows, handler
    ):
        """
        Test failure on backend by passing invalid argument

        We expect a server-timing header, but no cluster/zone info
        """
        from google.cloud.bigtable.data.mutations import SetCell
        from google.cloud.bigtable.data.row_filters import ValueRangeFilter

        row_key = b"test-row-key"
        family = TEST_FAMILY
        qualifier = b"test-qualifier"
        await temp_rows.add_row(row_key, value=1, family=family, qualifier=qualifier)

        predicate = ValueRangeFilter(-1, -1)
        with pytest.raises(GoogleAPICallError):
            await target.check_and_mutate_row(
                row_key,
                predicate,
            )
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.name == "INVALID_ARGUMENT"
        assert operation.cluster_id == "unspecified"
        assert operation.zone == "global"
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert attempt.gfe_latency_ns >= 0 and attempt.gfe_latency_ns < operation.duration_ns


    @CrossSync.pytest
    async def test_check_and_mutate_row_failure_timeout(
        self, target, temp_rows, handler, error_injector
    ):
        """
        Test failure in gapic layer by passing very low timeout

        No grpc headers expected
        """
        from google.cloud.bigtable.data.mutations import SetCell
        from google.cloud.bigtable.data.row_filters import ValueRangeFilter

        row_key = b"test-row-key"
        family = TEST_FAMILY
        qualifier = b"test-qualifier"
        await temp_rows.add_row(row_key, value=1, family=family, qualifier=qualifier)

        true_mutation_value = b"true-mutation-value"
        true_mutation = SetCell(
            family=TEST_FAMILY, qualifier=qualifier, new_value=true_mutation_value
        )
        with pytest.raises(GoogleAPICallError):
            await target.check_and_mutate_row(
                row_key,
                predicate=ValueRangeFilter(0, 2),
                true_case_mutations=true_mutation,
                operation_timeout=0.001
            )
        # validate counts
        assert len(handler.completed_operations) == 1
        assert len(handler.completed_attempts) == 1
        assert len(handler.cancelled_operations) == 0
        # validate operation
        operation = handler.completed_operations[0]
        assert isinstance(operation, CompletedOperationMetric)
        assert operation.final_status.name == "DEADLINE_EXCEEDED"
        assert operation.cluster_id == "unspecified"
        assert operation.zone == "global"
        # validate attempt
        attempt = handler.completed_attempts[0]
        assert attempt.gfe_latency_ns is None