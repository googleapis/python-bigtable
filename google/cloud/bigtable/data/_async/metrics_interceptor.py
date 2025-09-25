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
# limitations under the License
from __future__ import annotations

from typing import Sequence

import time
from functools import wraps
from grpc import StatusCode

from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.data_model import OperationState
from google.cloud.bigtable.data._metrics.data_model import OperationType
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler

from google.cloud.bigtable.data._cross_sync import CrossSync

if CrossSync.is_async:
    from grpc.aio import UnaryUnaryClientInterceptor
    from grpc.aio import UnaryStreamClientInterceptor
    from grpc.aio import AioRpcError
else:
    from grpc import UnaryUnaryClientInterceptor
    from grpc import UnaryStreamClientInterceptor


__CROSS_SYNC_OUTPUT__ = "google.cloud.bigtable.data._sync_autogen.metrics_interceptor"


def _with_operation_from_metadata(func):
    """
    Decorator for interceptor methods to extract the active operation
    from metadata and pass it to the decorated function.
    """

    @wraps(func)
    def wrapper(self, continuation, client_call_details, request):
        operation: "ActiveOperationMetric" | None = ActiveOperationMetric.get_active()

        if operation:
            # start a new attempt if not started
            if (
                operation.state == OperationState.CREATED
                or operation.state == OperationState.BETWEEN_ATTEMPTS
            ):
                operation.start_attempt()
            # wrap continuation in logic to process the operation
            return func(self, operation, continuation, client_call_details, request)
        else:
            # if operation not found, return unwrapped continuation
            return continuation(client_call_details, request)

    return wrapper

@CrossSync.convert
async def _get_metadata(source) -> dict[str, str|bytes] | None:
    """Helper to extract metadata from a call or RpcError"""
    try:
        if CrossSync.is_async:
            # grpc.aio returns metadata in Metadata objects
            if isinstance(source, AioRpcError):
                metadata = list(source.trailing_metadata()) + list(source.initial_metadata())
            else:
                metadata = list(await source.trailing_metadata()) + list(await source.initial_metadata())
        else:
            # sync grpc returns metadata as a sequence of tuples
            metadata: Sequence[tuple[str. str|bytes]] = source.trailing_metadata() + source.initial_metadata()
        # convert metadata to dict format
        return {k:v for k,v in metadata}
    except Exception:
        # ignore errors while fetching metadata
        return None


@CrossSync.convert_class(sync_name="BigtableMetricsInterceptor")
class AsyncBigtableMetricsInterceptor(
    UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor, MetricsHandler
):
    """
    An async gRPC interceptor to add client metadata and print server metadata.
    """

    @CrossSync.convert
    @_with_operation_from_metadata
    async def intercept_unary_unary(
        self, operation, continuation, client_call_details, request
    ):
        metadata = None
        try:
            call = await continuation(client_call_details, request)
            metadata = await _get_metadata(call)
            return call
        except Exception as rpc_error:
            metadata = await _get_metadata(rpc_error)
            raise rpc_error
        finally:
            if metadata is not None:
                operation.add_response_metadata(metadata)

    @CrossSync.convert
    @_with_operation_from_metadata
    async def intercept_unary_stream(
        self, operation, continuation, client_call_details, request
    ):
        async def response_wrapper(call):
            # only track has_first response for READ_ROWS
            has_first_response = (
                operation.first_response_latency_ns is not None
                or operation.op_type != OperationType.READ_ROWS
            )
            encountered_exc = None
            try:
                async for response in call:
                    # record time to first response. Currently only used for READ_ROWs
                    if not has_first_response:
                        operation.first_response_latency_ns = (
                            time.monotonic_ns() - operation.start_time_ns
                        )
                        has_first_response = True
                    yield response
            except Exception as e:
                # handle errors while processing stream
                encountered_exc = e
                raise
            finally:
                if call is not None:
                    metadata = await _get_metadata(encountered_exc or call)
                    if metadata is not None:
                        operation.add_response_metadata(metadata)

        try:
            return response_wrapper(await continuation(client_call_details, request))
        except Exception as rpc_error:
            metadata = await _get_metadata(rpc_error)
            if metadata is not None:
                operation.add_response_metadata(metadata)
            raise rpc_error
