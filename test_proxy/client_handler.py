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
This module contains the client handler process for proxy_server.py.
"""

def client_handler_process(request_q, queue_pool):
    import asyncio
    asyncio.run(client_handler_process_async(request_q, queue_pool))


async def client_handler_process_async(request_q, queue_pool):
    """
    Defines a process that recives Bigtable requests from a grpc_server_process,
    and runs the request using a client library instance
    """
    from google.cloud.bigtable import BigtableDataClient
    from google.cloud.environment_vars import BIGTABLE_EMULATOR
    import re
    import os
    import asyncio
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*Bigtable emulator.*")

    def camel_to_snake(str):
        return re.sub(r"(?<!^)(?=[A-Z])", "_", str).lower()

    def format_dict(input_obj):
        if isinstance(input_obj, list):
            return [format_dict(x) for x in input_obj]
        elif isinstance(input_obj, dict):
            return {camel_to_snake(k): format_dict(v) for k, v in input_obj.items()}
        elif isinstance(input_obj, str):
            # check for time encodings
            if re.match("^[0-9]+s$", input_obj):
                return int(input_obj[:-1])
            # check for int strings
            try:
                return int(input_obj)
            except (ValueError, TypeError):
                return input_obj
        else:
            return input_obj

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
            self.client = BigtableDataClient(project=project_id)
            self.instance_id = instance_id
            self.app_profile_id = app_profile_id
            self.per_operation_timeout = per_operation_timeout

        def error_safe(func):
            """
            Catch and pass errors back to the grpc_server_process
            Also check if client is closed before processing requests
            """

            async def wrapper(self, *args, **kwargs):
                try:
                    if self.closed:
                        raise RuntimeError("client is closed")
                    return await func(self, *args, **kwargs)
                except (Exception, NotImplementedError) as e:
                    error_msg = f"{type(e).__name__}: {e}"
                    if e.__cause__:
                        error_msg += f" {type(e.__cause__).__name__}: {e.__cause__}"
                    # exceptions should be raised in grpc_server_process
                    return {"error": error_msg}

            return wrapper

        def close(self):
            self.closed = True

        @error_safe
        async def ReadRows(self, request, **kwargs):
            table_id = request["table_name"].split("/")[-1]
            app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
            table = self.client.get_table(self.instance_id, table_id, app_profile_id)
            kwargs["operation_timeout"] = kwargs.get("operation_timeout", self.per_operation_timeout) or 20
            result_list = await table.read_rows(request, **kwargs)
            # pack results back into protobuf-parsable format
            serialized_response = [row.to_dict() for row in result_list]
            return serialized_response

    # Listen to requests from grpc server process
    print("client_handler_process started")
    client_map = {}
    background_tasks = set()
    while True:
        if not request_q.empty():
            json_data = format_dict(request_q.get())
            fn_name = json_data.pop("proxy_request")
            print(f"--- running {fn_name} with {json_data}")
            out_q = queue_pool[json_data.pop("response_queue_idx")]
            client_id = json_data.pop("client_id")
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
                client_map.pop(client_id, None)
                out_q.put(True)
                print("")
            else:
                # run actual rpc against client
                async def _run_fn(out_q, fn, **kwargs):
                    result = await fn(**kwargs)
                    out_q.put(result)
                fn = getattr(client, fn_name)
                task = asyncio.create_task(_run_fn(out_q, fn, **json_data))
                await asyncio.sleep(0)
                background_tasks.add(task)
                task.add_done_callback(background_tasks.remove)
        await asyncio.sleep(0.01)


