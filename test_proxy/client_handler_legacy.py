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

from google.cloud.environment_vars import BIGTABLE_EMULATOR
from google.cloud.bigtable.deprecated.client import Client

import client_handler

class LegacyTestProxyClientHandler(client_handler.TestProxyClientHandler):


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
        self.client = Client(project=project_id)
        self.instance_id = instance_id
        self.app_profile_id = app_profile_id
        self.per_operation_timeout = per_operation_timeout

    @client_handler.error_safe
    async def ReadRows(self, request, **kwargs):
        table_id = request["table_name"].split("/")[-1]
        # app_profile_id = self.app_profile_id or request.get("app_profile_id", None)
        instance = self.client.instance(self.instance_id)
        table = instance.table(table_id)

        limit = request.get("rows_limit", None)
        start_key = request.get("rows", {}).get("row_keys", [None])[0]
        end_key = request.get("rows", {}).get("row_keys", [None])[-1]
        end_inclusive = request.get("rows", {}).get("row_ranges", [{}])[-1].get("end_key_closed", True)

        result_list = table.read_rows(start_key=start_key, end_key=end_key, limit=limit, end_inclusive=end_inclusive)
        # pack results back into protobuf-parsable format
        serialized_response = [row.to_dict() for row in result_list]
        return serialized_response
