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
from __future__ import annotations


class TestPooledChannel:
    pass

    """
    @pytest.mark.asyncio
    async def test_channel_pool_rotation(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledChannel,
        )

        pool_size = 7

        with mock.patch.object(PooledChannel, "next_channel") as next_channel:
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert len(client.transport._grpc_channel._pool) == pool_size
            next_channel.reset_mock()
            with mock.patch.object(
                type(client.transport._grpc_channel._pool[0]), "unary_unary"
            ) as unary_unary:
                # calling an rpc `pool_size` times should use a different channel each time
                channel_next = None
                for i in range(pool_size):
                    channel_last = channel_next
                    channel_next = client.transport.grpc_channel._pool[i]
                    assert channel_last != channel_next
                    next_channel.return_value = channel_next
                    client.transport.ping_and_warm()
                    assert next_channel.call_count == i + 1
                    unary_unary.assert_called_once()
                    unary_unary.reset_mock()
        await client.close()

    @pytest.mark.asyncio
    async def test_channel_pool_creation(self):
        pool_size = 14
        with mock.patch(
            "google.api_core.grpc_helpers_async.create_channel"
        ) as create_channel:
            create_channel.return_value = AsyncMock()
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert create_channel.call_count == pool_size
            await client.close()
        # channels should be unique
        client = self._make_one(project="project-id", pool_size=pool_size)
        pool_list = list(client.transport._grpc_channel._pool)
        pool_set = set(client.transport._grpc_channel._pool)
        assert len(pool_list) == len(pool_set)
        await client.close()
"""
