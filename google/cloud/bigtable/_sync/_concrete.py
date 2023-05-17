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
#
from __future__ import annotations
from grpc import Channel

from typing import Any

import google.auth.credentials
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigtable_v2.types.bigtable import PingAndWarmRequest
from google.cloud.bigtable._sync._autogen import _ReadRowsOperation_Sync
from google.cloud.bigtable._sync._autogen import Table_Sync
from google.cloud.bigtable._sync._autogen import BigtableDataClient_Sync
from google.cloud.bigtable._sync._autogen import ReadRowsIterator_Sync


class _ReadRowsOperation_Sync_Concrete(_ReadRowsOperation_Sync):
    @staticmethod
    def _prepare_stream(gapic_stream, *args, **kwargs):
        return gapic_stream, None


class Table_Sync_Concrete(Table_Sync):
    # TODO: buffer_size does not apply to sync surface
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # register table with client
        self.client._register_instance(self.instance_id, self)


class BigtableDataClient_Sync_Concrete(BigtableDataClient_Sync):
    def __init__(
        self,
        *,
        project: str | None = None,
        credentials: google.auth.credentials.Credentials | None = None,
        client_options: dict[str, Any]
        | "google.api_core.client_options.ClientOptions"
        | None = None,
    ):
        # remove pool size option in sync client
        super().__init__(
            project=project, credentials=credentials, client_options=client_options
        )

    def __init__transport__(self, *args, **kwargs):
        # use grpc transport for sync client
        return "grpc"

    def close(self):
        # TODO: Can this be auto-generated?
        self.transport.close()
        self._channel_refresh_tasks = []

    def _ping_and_warm_instances(self) -> list[GoogleAPICallError | None]:
        results: list[GoogleAPICallError | None] = []
        for instance_name in self._active_instances:
            try:
                self._gapic_client.ping_and_warm(name=instance_name)
                results.append(None)
            except GoogleAPICallError as e:
                # fail warm attempts silently
                results.append(e)
        return results

    def _register_instance(self, instance_id: str, owner: Table_Sync) -> None:
        instance_name = self._gapic_client.instance_path(self.project, instance_id)
        self._instance_owners.setdefault(instance_name, set()).add(id(owner))
        if instance_name not in self._active_instances:
            self._active_instances.add(instance_name)
            # warm channel for new instance
            self._ping_and_warm_instances()


class ReadRowsIterator_Sync_Concrete(ReadRowsIterator_Sync):
    pass
