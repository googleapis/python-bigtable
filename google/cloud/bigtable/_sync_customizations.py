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
from google.api_core.exceptions import GoogleAPICallError
from grpc import Channel

from google.cloud.bigtable._sync_autogen import _ReadRowsOperation_Sync
from google.cloud.bigtable._sync_autogen import Table_Sync
from google.cloud.bigtable._sync_autogen import BigtableDataClient_Sync
from google.cloud.bigtable._sync_autogen import ReadRowsIterator_Sync


class _ReadRowsOperation_Sync_Patched(_ReadRowsOperation_Sync):
    @staticmethod
    async def _prepare_stream(gapic_stream, *args, **kwargs):
        return gapic_stream


class Table_Sync_Patched(Table_Sync):
    pass


class BigtableDataClient_Sync_Patched(BigtableDataClient_Sync):
    def __init__transport__(self, *args, **kwargs):
        # use grpc transport for sync client
        return "grpc"

    def close(self, timeout: float = 2.0):
        # TODO: Can this be auto-generated?
        self.transport.close()
        self._channel_refresh_tasks = []

    def _ping_and_warm_instances(
        self, channel: Channel
    ) -> list[GoogleAPICallError | None]:
        results : list[GoogleAPICallError | None] = []
        for instance_name in self._active_instances:
            try:
                channel.unary_unary(
                    "/google.bigtable.v2.Bigtable/PingAndWarmChannel"
                )({"name": instance_name})
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
            self._ping_and_warm_instances(self.transport.grpc_channel)


class ReadRowsIterator_Sync_Patched(ReadRowsIterator_Sync):
    pass
