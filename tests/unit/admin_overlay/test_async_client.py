# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
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

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
except ImportError:  # pragma: NO COVER
    import mock

import functools

from google.api_core import gapic_v1
from google.api_core import retry as retries
from google.cloud.bigtable.admin_v2.services.bigtable_table_admin import transports
from google.cloud.bigtable.admin_v2.types import bigtable_table_admin
from google.cloud.bigtable.admin_v2.overlay.services.bigtable_table_admin.async_client import (
    BigtableTableAdminAsyncClient,
    DEFAULT_CLIENT_INFO,
)
from google.cloud.bigtable.admin_v2.overlay.types import async_consistency

from google.cloud.bigtable import __version__ as bigtable_version

import pytest


PARENT_NAME = "my_parent"
TABLE_NAME = "my_table"
CONSISTENCY_TOKEN = "abcdefg"


@pytest.mark.parametrize(
    "transport_class,transport_name",
    [
        (
            transports.BigtableTableAdminGrpcAsyncIOTransport,
            "grpc_asyncio",
        ),
    ],
)
def test_bigtable_table_admin_async_client_client_version(transport_class, transport_name):
    with mock.patch.object(transport_class, "__init__") as patched:
        patched.return_value = None
        BigtableTableAdminAsyncClient(transport=transport_name)

        # call_args.kwargs is not supported in Python 3.7, so find them from the tuple
        # instead. It's always the last item in the call_args tuple.
        transport_init_call_kwargs = patched.call_args[-1]
        assert transport_init_call_kwargs["client_info"] == DEFAULT_CLIENT_INFO

    assert (
        DEFAULT_CLIENT_INFO.client_library_version
        == f"{bigtable_version}-admin-overlay-async"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "request": bigtable_table_admin.CheckConsistencyRequest(
                name=TABLE_NAME,
                consistency_token=CONSISTENCY_TOKEN,
            )
        },
        {
            "request": {
                "name": TABLE_NAME,
                "consistency_token": CONSISTENCY_TOKEN,
            },
        },
        {
            "name": TABLE_NAME,
            "consistency_token": CONSISTENCY_TOKEN,
        },
        {
            "request": bigtable_table_admin.CheckConsistencyRequest(
                name=TABLE_NAME,
                consistency_token=CONSISTENCY_TOKEN,
            ),
            "retry": mock.Mock(spec=retries.Retry),
            "timeout": mock.Mock(spec=retries.Retry),
            "metadata": [("foo", "bar")],
        },
    ],
)
async def test_bigtable_table_admin_async_client_wait_for_consistency(kwargs):
    client = BigtableTableAdminAsyncClient()

    future = await client.wait_for_consistency(**kwargs)

    assert isinstance(future, async_consistency.AsyncCheckConsistencyPollingFuture)
    assert future._check_consistency_call_retry == kwargs.get("retry", gapic_v1.method.DEFAULT)

    check_consistency_call = future._check_consistency_call
    assert isinstance(check_consistency_call, functools.partial)

    assert check_consistency_call.func == client.check_consistency
    assert check_consistency_call.args == (kwargs.get("request", None),)
    assert check_consistency_call.keywords == {
        "name": kwargs.get("name", None),
        "consistency_token": kwargs.get("consistency_token", None),
        "timeout": kwargs.get("timeout", gapic_v1.method.DEFAULT),
        "metadata": kwargs.get("metadata", ()),
    }
