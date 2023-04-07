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

import logging
from multiprocessing import Process
import multiprocessing
import time
import inspect
from collections import namedtuple
import random
import sys


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
            return

        @delegate_to_client_handler
        def ReadRows(self, request, context, client_response=None):
            rows = [data_pb2.Row(**d) for d in client_response]
            if rows:
                status = Status(code=0)
            else:
                status = Status(code=5)
            result = test_proxy_pb2.RowsResult(row=rows, status=status)
            return result

        def ReadRow(self, request, context):
            return test_proxy_pb2.RowResult()

        def MutateRow(self, request, context):
            return test_proxy_pb2.MutateRowResult()

        def MutateRows(self, request, context):
            return test_proxy_pb2.MutateRowsResult()

        def CheckAndMutateRow(self, request, context):
            return test_proxy_pb2.CheckAndMutateRowResult()

        def ReadModifyWriteRow(self, request, context):
            return test_proxy_pb2.ReadModifyWriteRowResult()

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


def client_handler_process(request_q, queue_pool):
    import asyncio
    asyncio.run(client_handler_process_async(request_q, queue_pool))

async def client_handler_process_async(request_q, queue_pool):
    """
    Defines a process that recives Bigtable requests from a grpc_server_process,
    and runs the request using a client library instance
    """
    from google.cloud.bigtable import BigtableDataClient
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery
    import grpc
    from google.api_core import client_options as client_options_lib
    from google.cloud.environment_vars import BIGTABLE_EMULATOR
    import re
    import os

    def camel_to_snake(str):
        return re.sub(r"(?<!^)(?=[A-Z])", "_", str).lower()

    class TestProxyClientHandler:
        """
        Implements the same methods as the grpc server, but handles the client
        library side of the request.

        Requests received in TestProxyGrpcServer are converted to a dictionary,
        and supplied to the TestProxyClientHandler methods as kwargs.
        The client response is then returned back to the TestProxyGrpcServer
        """

        def __init__(
            self,
            data_target=None,
            project_id=None,
            instance_id=None,
            app_profile_id=None,
            per_operation_timeout=None,
            **kwargs,
        ):
            self.closed = False
            # use emulator
            os.environ[BIGTABLE_EMULATOR] = data_target
            self.client = BigtableDataClient()
            self.project_id = project_id
            self.instance_id = instance_id
            self.app_profile_id = app_profile_id
            self.per_operation_timeout = per_operation_timeout

        def error_safe(func):
            """
            Catch and pass errors back to the grpc_server_process
            Also check if client is closed before processing requests
            """

            def wrapper(self, *args, **kwargs):
                try:
                    if self.closed:
                        raise RuntimeError("client is closed")
                    return func(self, *args, **kwargs)
                except (Exception, NotImplementedError) as e:
                    # exceptions should be raised in grpc_server_process
                    return e

            return wrapper

        def close(self):
            self.closed = True

        @error_safe
        async def ReadRows(self, request, **kwargs):
            table_id = request["table_name"].split("/")[-1]
            app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
            table = self.client.get_table(self.instance_id, table_id, app_profile_id)
            kwargs = {"operation_timeout": self.per_operation_timeout} if self.per_operation_timeout else {}
            result_list = await table.read_rows(request, **kwargs)
            # pack results back into protobuf-parsable format
            serialized_response = [row.to_dict() for row in result_list]
            return serialized_response

    # Listen to requests from grpc server process
    print("client_handler_process started")
    client_map = {}
    while True:
        if not request_q.empty():
            json_data = request_q.get()
            json_data = {camel_to_snake(k): v for k, v in json_data.items()}
            if "request" in json_data:
                json_data["request"] = {
                    camel_to_snake(k): v for k, v in json_data["request"].items()
                }
            # print(json_data)
            fn_name = json_data.pop("proxy_request")
            out_q = queue_pool[json_data.pop("response_queue_idx")]
            client_id = json_data["client_id"]
            client = client_map.get(client_id, None)
            # handle special cases for client creation and deletion
            if fn_name == "CreateClient":
                client = TestProxyClientHandler(**json_data)
                client_map[client_id] = client
                out_q.put(True)
            elif client is None:
                out_q.put(RuntimeError("client not found"))
            elif fn_name == "CloseClient":
                client.close()
                out_q.put(True)
            elif fn_name == "RemoveClient":
                client_map.pop(json_data["client_id"], None)
                out_q.put(True)
            else:
                # run actual rpc against client
                fn = getattr(client, fn_name)
                result = await fn(**json_data)
                out_q.put(result)
        time.sleep(1e-4)


if __name__ == "__main__":
    port = "50055"
    if len(sys.argv) > 1:
        port = sys.argv[1]
    # start and run both processes
    # larger pools support more concurrent requests
    response_queue_pool = [multiprocessing.Queue() for _ in range(100)]
    request_q = multiprocessing.Queue()
    logging.basicConfig()
    proxy = Process(
        target=grpc_server_process,
        args=(
            request_q,
            response_queue_pool,
            port
        ),
    )
    proxy.start()
    client = Process(
        target=client_handler_process,
        args=(
            request_q,
            response_queue_pool,
        ),
    )
    client.start()
    proxy.join()
    client.join()
