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
    async def wrapper(self, *args, raise_on_error=False, **kwargs):
        try:
            if self.closed:
                raise RuntimeError("client is closed")
            return await func(self, *args, **kwargs)
        except (Exception, NotImplementedError) as e:
            if raise_on_error or self._raise_on_error:
                raise e
            else:
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
        raise_on_error=False,
        enable_timing=False,
        enable_profiling=False,
        **kwargs,
    ):
        self._raise_on_error = raise_on_error
        self.closed = False
        # use emulator
        if data_target is not None:
            os.environ[BIGTABLE_EMULATOR] = data_target
        self.client = BigtableDataClient(project=project_id)
        self.instance_id = instance_id
        self.app_profile_id = app_profile_id
        self.per_operation_timeout = per_operation_timeout
        # track timing and profiling if enabled
        self._enabled_timing = enable_timing
        self.total_time = 0
        self._enabled_profiling = enable_profiling

    def close(self):
        self.closed = True

    @asynccontextmanager
    async def measure_call(self):
        """Tracks the core part of the library call, for profiling purposes"""
        if self._enabled_timing:
            import timeit
            starting_time = timeit.default_timer()
        if self._enabled_profiling:
            import yappi
            yappi.start()
        yield
        if self._enabled_profiling:
            import yappi
            yappi.stop()
        if self._enabled_timing:
            elapsed_time = timeit.default_timer() - starting_time
            self.total_time += elapsed_time

    def print_profile(self, profile_rows=25, save_path:str|None=None) -> str:
        if not self._enabled_profiling:
            raise RuntimeError("Profiling is not enabled")
        import pandas as pd
        import io
        import yappi
        pd.set_option("display.max_colwidth", None)
        stats = yappi.convert2pstats(yappi.get_func_stats())
        stats.strip_dirs()
        result = io.StringIO()
        stats.stream = result
        stats.sort_stats("cumtime").print_stats()
        result = result.getvalue()
        result = "ncalls" + result.split("ncalls")[-1]
        df = pd.DataFrame([x.split(maxsplit=5) for x in result.split("\n")])
        df = df.rename(columns=df.iloc[0]).drop(df.index[0])
        profile_df = df[:profile_rows]
        if save_path:
            yappi.get_func_stats().save(save_path, type="pstat")
        return profile_df

    def reset_profile(self):
        if self._enabled_profiling:
            import yappi
            yappi.clear_stats()

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
