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

from functools import wraps
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
        del self.operation_map[op.uuid]

    def on_operation_cancelled(self, op):
        self.on_operation_complete(op)

    @CrossSync.convert
    @_with_operation_from_metadata
    async def intercept_unary_unary(
        self, operation, continuation, client_call_details, request
    ):
        encountered_exc: Exception | None = None
        call = None
        try:
            call = await continuation(client_call_details, request)
            return call
        except Exception as e:
            encountered_exc = e
            raise
        finally:
            if call is not None:
                metadata = (
                    await call.trailing_metadata() + await call.initial_metadata()
                )
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
            encountered_exc = None
            try:
                async for response in call:
                    yield response

            except Exception as e:
                encountered_exc = e
                raise
            finally:
                metadata = (
                    await call.trailing_metadata() + await call.initial_metadata()
                )
                operation.add_response_metadata(metadata)
                if encountered_exc is not None:
                    # end attempt. If it succeeded, let higher levels decide when to end operation
                    operation.end_attempt_with_status(encountered_exc)

        return response_wrapper(await continuation(client_call_details, request))
