# Copyright 2024 Google LLC
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
#
from __future__ import annotations

from typing import Any

import grpc

import google.auth.credentials

from google.cloud.bigtable.data._sync._autogen import BigtableDataClient_SyncGen
from google.cloud.bigtable.data._sync._autogen import Table_SyncGen


class BigtableDataClient(BigtableDataClient_SyncGen):
    def __init__(
        self,
        *,
        project: str | None = None,
        credentials: google.auth.credentials.Credentials | None = None,
        client_options: dict[str, Any]
        | "google.api_core.client_options.ClientOptions"
        | None = None,
        **kwargs
    ):
        # remove pool size option in sync client
        super().__init__(
            project=project, credentials=credentials, client_options=client_options, pool_size=1
        )

    def _transport_init(self, pool_size: int) -> str:
        return "grpc"

    def _prep_emulator_channel(self, host:str, pool_size: int) -> str:
        self.transport._grpc_channel = grpc.insecure_channel(target=host)

    @staticmethod
    def _client_version() -> str:
        return f"{google.cloud.bigtable.__version__}-data"

    def _start_background_channel_refresh(self) -> None:
        # TODO: implement channel refresh
        pass


class Table(Table_SyncGen):

    def _register_with_client(self):
        self.client._register_instance(self.instance_id, self)
        self._register_instance_task = None
