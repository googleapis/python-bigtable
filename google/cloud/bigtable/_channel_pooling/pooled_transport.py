# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
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

from typing import (
    Awaitable,
    Callable,
)

from grpc.experimental import aio  # type: ignore

from google.cloud.bigtable_v2.services.bigtable.transports.grpc_asyncio import (
    BigtableGrpcAsyncIOTransport,
)

from .pooled_channel import PooledChannel
from .pooled_channel import StaticPoolOptions
from .dynamic_pooled_channel import DynamicPooledChannel
from .dynamic_pooled_channel import DynamicPoolOptions


def create_pooled_transport(
    channel_init_callback: Callable[[aio.Channel], Awaitable[None]],
    *args,
    options: DynamicPoolOptions | StaticPoolOptions | None = None,
    insecure=False,
    **kwargs,
):
    # initialize a temporary transport to create credentials from the input args
    # TODO: remove need for tmp_transport
    tmp_transport = BigtableGrpcAsyncIOTransport(*args, **kwargs)
    if options is None:
        options = DynamicPoolOptions()
    if isinstance(options, DynamicPoolOptions):
        cls = DynamicPooledChannel
    else:
        cls = PooledChannel
    pool = cls(
        tmp_transport._host,
        # use the credentials which are saved
        credentials=tmp_transport._credentials,
        # Set ``credentials_file`` to ``None`` here as
        # the credentials that we saved earlier should be used.
        credentials_file=None,
        scopes=tmp_transport._scopes,
        ssl_credentials=tmp_transport._ssl_channel_credentials,
        quota_project_id=kwargs.get("quota_project_id", None),
        options=[
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
        ],
        pool_options=options,
        channel_init_callback=channel_init_callback,
        insecure=insecure,
    )
    return BigtableGrpcAsyncIOTransport(*args, **kwargs, channel=pool)
