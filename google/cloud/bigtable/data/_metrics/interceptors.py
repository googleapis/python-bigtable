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

import grpc
import time
from typing import Any, Callable
from google.cloud.bigtable.data._metrics.data_model import OPERATION_INTERCEPTOR_METADATA_KEY
from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.data_model import OperationState

# class MetadataInterceptor(grpc.UnaryUnaryClientInterceptor):
#     """
#     An interceptor to add client metadata and print metadata received from the server.
#     """
#     def intercept_unary_unary(
#         self, continuation: Continuation, client_call_details: grpc.ClientCallDetails, request: Any
#     ):
#         """
#         Intercepts a unary RPC to handle metadata.
#         """
#         print("--- Interceptor: Before RPC ---")
#         print(f"Calling method: {client_call_details.method}")

#         # Proceed with the RPC invocation
#         response_future = continuation(client_call_details, request)

#         # 2. REGISTER A CALLBACK TO READ METADATA FROM THE SERVER RESPONSE
#         def log_server_metadata(future: grpc.Future):
#             print("\n--- Interceptor: After RPC ---")
#             # Read initial metadata (sent by the server before the response message)
#             initial_md = future.initial_metadata()
#             print(f"<- Received initial metadata from server: {initial_md}")

#             # Read trailing metadata (sent by the server after the response message)
#             trailing_md = future.trailing_metadata()
#             print(f"<- Received trailing metadata from server: {trailing_md}")

#         response_future.add_done_callback(log_server_metadata)

#         return response_future


class AsyncMetadataInterceptor(grpc.aio.UnaryUnaryClientInterceptor, grpc.aio.UnaryStreamClientInterceptor):
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

    def on_attempt_complete(self, attempt, operation):
        pass


    async def intercept_unary_unary(self, continuation, client_call_details, request):
        """
        Intercepts a unary RPC to handle metadata asynchronously.
        """
        print("--- Interceptor: Before RPC ---")
        print(f"Calling method: {client_call_details.method}")

        # somehow get a reference to the operation
        key = next((m[1] for m in client_call_details.metadata if m[0] == OPERATION_INTERCEPTOR_METADATA_KEY), None)
        # update metadata
        # client_call_details.metadata = [m for m in client_call_details.metadata if m[0] != OPERATION_INTERCEPTOR_METADATA_KEY]
        operation: "ActiveOperationMetric" = self.operation_map.get(key)
        if operation:
            # start a new attempt if not started
            if operation.state != OperationState.ACTIVE_ATTEMPT:
                operation.start_attempt()
        encountered_exc: Exception | None = None
        call = None
        try:
            call = await continuation(client_call_details, request)
            print("\n--- Interceptor: After RPC ---")
            return call
        except Exception as e:
            encountered_exc = e
            raise
        finally:
            if call is not None and operation:
                metadata = (
                    await call.trailing_metadata()
                    + await call.initial_metadata()
                )
                print(f"<- Received metadata: {metadata}")
                operation.add_response_metadata(metadata)
                if encountered_exc is not None:
                    # end attempt. If it succeeded, let higher levels decide when to end operation
                    operation.end_attempt_with_status(encountered_exc)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        print("--- Interceptor: Before RPC ---")
        print(f"Calling method: {client_call_details.method}")

        # somehow get a reference to the operation
        key = next((m[1] for m in client_call_details.metadata if m[0] == OPERATION_INTERCEPTOR_METADATA_KEY), None)
        # update metadata
        # client_call_details.metadata = [m for m in client_call_details.metadata if m[0] != OPERATION_INTERCEPTOR_METADATA_KEY]
        operation: "ActiveOperationMetric" = self.operation_map.get(key)
        if operation:            
            # start a new attempt if not started
            if operation.state != OperationState.ACTIVE_ATTEMPT:
                operation.start_attempt()

        async def response_wrapper(call):
            encountered_exc = None
            try:
                async for response in call:
                    yield response

            except Exception as e:
                encountered_exc = e
                raise
            finally:
                if operation:
                    metadata = (
                        await call.trailing_metadata()
                        + await call.initial_metadata()
                    )
                    operation.add_response_metadata(metadata)
                    if encountered_exc is not None:
                        # end attempt. If it succeeded, let higher levels decide when to end operation
                        operation.end_attempt_with_status(encountered_exc)

        return response_wrapper(await continuation(client_call_details, request))