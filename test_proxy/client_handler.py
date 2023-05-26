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
from __future__ import annotations
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
                # exceptions should be raised in grpc_server_process
                return encode_exception(e)

    return wrapper

def encode_exception(exc):
    """
    Encode an exception or chain of exceptions to pass back to grpc_handler
    """
    from google.api_core.exceptions import GoogleAPICallError
    error_msg = f"{type(exc).__name__}: {exc}"
    result = {"error": error_msg}
    if exc.__cause__:
        result["cause"] = encode_exception(exc.__cause__)
    if hasattr(exc, "exceptions"):
        result["subexceptions"] = [encode_exception(e) for e in exc.exceptions]
    if hasattr(exc, "index"):
        result["index"] = exc.index
    if isinstance(exc, GoogleAPICallError):
        if  exc.grpc_status_code is not None:
            result["code"] = exc.grpc_status_code.value[0]
        elif exc.code is not None:
            result["code"] = int(exc.code)
        else:
            result["code"] = -1
    elif result.get("cause", {}).get("code", None):
        # look for code code in cause
        result["code"] = result["cause"]["code"]
    elif result.get("subexceptions", None):
        # look for code in subexceptions
        for subexc in result["subexceptions"]:
            if subexc.get("code", None):
                result["code"] = subexc["code"]
    return result

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

    def print_profile(self, save_path:str|None=None) -> str:
        if not self._enabled_profiling:
            raise RuntimeError("Profiling is not enabled")
        import io
        import yappi
        result = io.StringIO()
        yappi.get_func_stats().print_all(out=result)
        stats = yappi.convert2pstats(yappi.get_func_stats())
        if save_path:
            yappi.get_func_stats().save(save_path, type="pstat")
        return result.getvalue()

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

    @error_safe
    async def MutateRow(self, request, **kwargs):
        import base64
        from google.cloud.bigtable.mutations import Mutation
        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        row_key = request["row_key"]
        try:
            # conformance tests send row keys as base64 encoded strings
            row_key = base64.b64decode(row_key)
        except Exception:
            pass
        mutations = [Mutation._from_dict(d) for d in request["mutations"]]
        async with self.measure_call():
            await table.mutate_row(row_key, mutations, **kwargs)
        return "OK"

    @error_safe
    async def BulkMutateRows(self, request, **kwargs):
        from google.cloud.bigtable.mutations import RowMutationEntry
        import base64
        table_id = request["table_name"].split("/")[-1]
        app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        table = self.client.get_table(self.instance_id, table_id, app_profile_id)
        kwargs["operation_timeout"] = kwargs.get("operation_timeout", self.per_operation_timeout) or 20
        # conformance tests send row keys as base64 encoded strings
        for entry in request["entries"]:
            try:
                entry["row_key"] = base64.b64decode(entry["row_key"])
            except Exception:
                pass
        entry_list = [RowMutationEntry._from_dict(entry) for entry in request["entries"]]
        async with self.measure_call():
            await table.bulk_mutate_rows(entry_list, **kwargs)
        return "OK"
