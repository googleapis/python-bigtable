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
The Python implementation of the `cloud-bigtable-clients-test` proxy server.

https://github.com/googleapis/cloud-bigtable-clients-test

This server is intended to be used to test the correctness of Bigtable
clients across languages.

Contributor Note: the proxy implementation is split across TestProxyClientHandler
and TestProxyGrpcServer. This is due to the fact that generated protos and proto-plus
objects cannot be used in the same process, so we had to make use of the
multiprocessing module to allow them to work together.
"""

import multiprocessing
import time
import sys
import client_handler


def grpc_server_process(request_q, queue_pool, port=50055):
    """
    Defines a process that hosts a grpc server
    proxies requests to a client_handler_process
    """
    from concurrent import futures

    import grpc
    import test_proxy_pb2
    import test_proxy_pb2_grpc
    import data_pb2
    from google.rpc.status_pb2 import Status

    from google.protobuf import json_format

    class TestProxyGrpcServer(test_proxy_pb2_grpc.CloudBigtableV2TestProxyServicer):
        """
        Implements a grpc server that proxies conformance test requests to the client library

        Due to issues with using protoc-compiled protos and client-library
        proto-plus objects in the same process, this server defers requests to
        matching methods in  a TestProxyClientHandler instance in a separate
        process.
        This happens invisbly in the decorator @defer_to_client, with the
        results attached to each request as a client_response kwarg
        """

        def __init__(self, queue_pool):
            self.open_queues = list(range(len(queue_pool)))
            self.queue_pool = queue_pool

        def delegate_to_client_handler(func, timeout_seconds=300):
            """
            Decorator that transparently passes a request to the client
            handler process, and then attaches the resonse to the wrapped call
            """

            def wrapper(self, request, context, **kwargs):
                deadline = time.time() + timeout_seconds
                json_dict = json_format.MessageToDict(request)
                out_idx = self.open_queues.pop()
                json_dict["proxy_request"] = func.__name__
                json_dict["response_queue_idx"] = out_idx
                out_q = queue_pool[out_idx]
                request_q.put(json_dict)
                # wait for response
                while time.time() < deadline:
                    if not out_q.empty():
                        response = out_q.get()
                        self.open_queues.append(out_idx)
                        if isinstance(response, Exception):
                            raise response
                        else:
                            return func(
                                self,
                                request,
                                context,
                                client_response=response,
                                **kwargs,
                            )
                    time.sleep(1e-4)

            return wrapper

        @delegate_to_client_handler
        def CreateClient(self, request, context, client_response=None):
            return test_proxy_pb2.CreateClientResponse()

        @delegate_to_client_handler
        def CloseClient(self, request, context, client_response=None):
            return test_proxy_pb2.CloseClientResponse()

        @delegate_to_client_handler
        def RemoveClient(self, request, context, client_response=None):
            return test_proxy_pb2.RemoveClientResponse()

        @delegate_to_client_handler
        def ReadRows(self, request, context, client_response=None):
            status = Status()
            rows = []
            if isinstance(client_response, dict) and "error" in client_response:
                status = Status(code=5, message=client_response["error"])
            else:
                rows = [data_pb2.Row(**d) for d in client_response]
            result = test_proxy_pb2.RowsResult(row=rows, status=status)
            return result

        def ReadRow(self, request, context):
            return test_proxy_pb2.RowResult()

        def MutateRow(self, request, context):
            return test_proxy_pb2.MutateRowResult()

        def MutateRows(self, request, context):
            return test_proxy_pb2.MutateRowsResult()

        def BulkMutateRows(self, request, context):
            return test_proxy_pb2.MutateRowsResult()

        def CheckAndMutateRow(self, request, context):
            return test_proxy_pb2.CheckAndMutateRowResult()

        def ReadModifyWriteRow(self, request, context):
            return test_proxy_pb2.RowResult()

        def SampleRowKeys(self, request, context):
            return test_proxy_pb2.SampleRowKeysResult()


    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_proxy_pb2_grpc.add_CloudBigtableV2TestProxyServicer_to_server(
        TestProxyGrpcServer(queue_pool), server
    )
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("grpc_server_process started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    port = "50055"
    if len(sys.argv) > 1:
        port = sys.argv[1]
    # start and run both processes
    # larger pools support more concurrent requests
    response_queue_pool = [multiprocessing.Queue() for _ in range(100)]
    request_q = multiprocessing.Queue()

    # run client in forground and proxy in background
    # breakpoints can be attached to client_handler_process
    proxy = multiprocessing.Process(
        target=grpc_server_process,
        args=(
            request_q,
            response_queue_pool,
            port
        ),
    )
    proxy.start()
    client_handler.client_handler_process(request_q, response_queue_pool)

    # uncomment to run proxy in foreground instead
    # client = multiprocessing.Process(
    #     target=client_handler.client_handler_process,
    #     args=(
    #         request_q,
    #         response_queue_pool,
    #     ),
    # )
    # client.start()
    # grpc_server_process(request_q, response_queue_pool, port)
    # client.join()
