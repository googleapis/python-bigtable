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

import time
from functools import wraps
from grpc import RpcError
from google.cloud.bigtable.data._metrics.data_model import (
    OPERATION_INTERCEPTOR_METADATA_KEY,
)
from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.data_model import OperationState
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler

from google.cloud.bigtable.data._cross_sync import CrossSync

if CrossSync.is_async:
    from grpc.aio import UnaryUnaryClientInterceptor
    from grpc.aio import UnaryStreamClientInterceptor
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
        key = next(
            (
                m[1]
                for m in client_call_details.metadata
                if m[0] == OPERATION_INTERCEPTOR_METADATA_KEY
            ),
            None,
        )
        operation: "ActiveOperationMetric" = self.operation_map.get(key)
        if operation:
            # start a new attempt if not started
            if operation.state != OperationState.ACTIVE_ATTEMPT:
                operation.start_attempt()
            # wrap continuation in logic to process the operation
            return func(self, operation, continuation, client_call_details, request)
        else:
            # if operation not found, return unwrapped continuation
            return continuation(client_call_details, request)

    return wrapper


@CrossSync.convert_class(sync_name="BigtableMetricsInterceptor")
class AsyncBigtableMetricsInterceptor(
    UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor, MetricsHandler
):
    """
    An async gRPC interceptor to add client metadata and print server metadata.
    """

    def __init__(self):
        super().__init__()
        self.operation_map = {}

    def register_operation(self, operation):
        """
        Register an operation object to be tracked my the interceptor

        When registered, the operation will receive metadata updates:
        - start_attempt if attempt not started when rpc is being sent
        - add_response_metadata after call is complete
        - end_attempt_with_status if attempt receives an error

        The interceptor will register itself as a handeler for the operation,
        so it can unregister the operation when it is complete
        """
        self.operation_map[operation.uuid] = operation
        operation.handlers.append(self)

    def on_operation_complete(self, op):
        if op.uuid in self.operation_map:
            del self.operation_map[op.uuid]

    def on_operation_cancelled(self, op):
        self.on_operation_complete(op)

    @CrossSync.convert
    @_with_operation_from_metadata
    async def intercept_unary_unary(
        self, operation, continuation, client_call_details, request
    ):
        encountered_exc: Exception | None = None
        metadata = None
        try:
            call = await continuation(client_call_details, request)
            metadata = (await call.trailing_metadata() or []) + (await call.initial_metadata() or [])
            return call
        except RpcError as rpc_error:
            # attempt extracting metadata from error
            try:
                metadata = (await rpc_error.trailing_metadata() or []) + (await rpc_error.initial_metadata() or [])
            except Exception:
                pass
            encountered_exc = rpc_error
            raise rpc_error
        except Exception as e:
            encountered_exc = e
            raise
        finally:
            if metadata is not None:
                operation.add_response_metadata(metadata)
            if encountered_exc is not None:
                # end attempt. If it succeeded, let higher levels decide when to end operation
                operation.end_attempt_with_status(encountered_exc)

    @CrossSync.convert
    @_with_operation_from_metadata
    async def intercept_unary_stream(
        self, operation, continuation, client_call_details, request
    ):
        async def response_wrapper(call):
            has_first_response = operation.first_response_latency is not None
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
                encountered_exc = e
                raise
            finally:
                if call is not None:
                    metadata = (await call.trailing_metadata() or []) + (await call.initial_metadata() or [])
                    operation.add_response_metadata(metadata)
                    if encountered_exc is not None:
                        # end attempt. If it succeeded, let higher levels decide when to end operation
                        operation.end_attempt_with_status(encountered_exc)

        try:
            return response_wrapper(await continuation(client_call_details, request))
        except RpcError as rpc_error:
            # attempt extracting metadata from error
            try:
                metadata = (await rpc_error.trailing_metadata() or []) + (await rpc_error.initial_metadata() or [])
                operation.add_response_metadata(metadata)
            except Exception:
                pass
            operation.end_attempt_with_status(rpc_error)
            raise rpc_error
        except Exception as e:
            operation.end_attempt_with_status(e)
            raise