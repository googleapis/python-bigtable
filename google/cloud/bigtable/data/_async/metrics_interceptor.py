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

from google.cloud.bigtable.data._cross_sync import CrossSync

if CrossSync.is_async:
    from grpc.aio import UnaryUnaryClientInterceptor
    from grpc.aio import UnaryStreamClientInterceptor
else:
    from grpc import UnaryUnaryClientInterceptor
    from grpc import UnaryStreamClientInterceptor


__CROSS_SYNC_OUTPUT__ = "google.cloud.bigtable.data._sync_autogen.metrics_interceptor"


@CrossSync.convert_class(sync_name="BigtableMetricsInterceptor")
class AsyncBigtableMetricsInterceptor(
    UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor
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
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        try:
            call = await continuation(client_call_details, request)
            return call
        except Exception as rpc_error:
            raise rpc_error

    @CrossSync.convert
    async def intercept_unary_stream(self, continuation, client_call_details, request):
        async def response_wrapper(call):
            try:
                async for response in call:
                    yield response
            except Exception as e:
                # handle errors while processing stream
                raise e

        try:
            return response_wrapper(await continuation(client_call_details, request))
        except Exception as rpc_error:
            # handle errors while intializing stream
            raise rpc_error
