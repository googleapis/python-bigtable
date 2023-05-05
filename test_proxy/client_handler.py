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
import os
from contextlib import asynccontextmanager

from google.cloud.environment_vars import BIGTABLE_EMULATOR
from google.cloud.bigtable import BigtableDataClient


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
        enable_timing=False,
        enable_profiling=False,
        **kwargs,
    ):
        self.closed = False
        # use emulator
        os.environ[BIGTABLE_EMULATOR] = data_target
        self.client = BigtableDataClient(project=project_id)
        self.instance_id = instance_id
        self.app_profile_id = app_profile_id
        self.per_operation_timeout = per_operation_timeout
        # track timing and profiling if enabled
        self._enabled_timing = enable_timing
        self.total_time = 0
        self._enabled_profiling = enable_profiling
        self._profiler = None
        if self._enabled_profiling:
            import yappi
            self._profiler = yappi

    def close(self):
        self.closed = True

    @asynccontextmanager
    async def measure_call(self):
        """Tracks the core part of the library call, for profiling purposes"""
        if self._enabled_timing:
            import timeit
            starting_time = timeit.default_timer()
        if self._enabled_profiling:
            self._profiler.start()
        yield
        if self._enabled_profiling:
            self._profiler.stop()
        if self._enabled_timing:
            elapsed_time = timeit.default_timer() - starting_time
            self.total_time += elapsed_time

    @error_safe
    async def ReadRows(self, request, **kwargs):
        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        async with self.measure_call():
            result_list = await table.read_rows(request, **kwargs)
        # pack results back into protobuf-parsable format
        serialized_response = [row.to_dict() for row in result_list]
        return serialized_response
