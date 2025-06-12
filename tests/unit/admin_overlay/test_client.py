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
    from unittest.mock import AsyncMock  # pragma: NO COVER
except ImportError:  # pragma: NO COVER
    import mock

import functools

from google.api_core import gapic_v1
from google.api_core import retry as retries
from google.cloud.bigtable.admin_v2.services.bigtable_table_admin import transports
from google.cloud.bigtable.admin_v2.types import bigtable_table_admin
from google.cloud.bigtable.admin_v2.overlay.services.bigtable_table_admin.client import (
    BigtableTableAdminClient,
    DEFAULT_CLIENT_INFO,
)
from google.cloud.bigtable.admin_v2.overlay.types import consistency, restore_table

from google.cloud.bigtable import __version__ as bigtable_version

import pytest


PARENT_NAME = "my_parent"
TABLE_NAME = "my_table"
CONSISTENCY_TOKEN = "abcdefg"


@pytest.mark.parametrize(
    "transport_class,transport_name",
    [
        (
            transports.BigtableTableAdminGrpcTransport,
            "grpc",
        ),
        (
            transports.BigtableTableAdminRestTransport,
            "rest",
        ),
    ],
)
def test_bigtable_table_admin_client_client_version(transport_class, transport_name):
    with mock.patch.object(transport_class, "__init__") as patched:
        patched.return_value = None
        BigtableTableAdminClient(transport=transport_name)
        transport_init_call = patched.call_args
        assert transport_init_call.kwargs["client_info"] == DEFAULT_CLIENT_INFO

    assert (
        DEFAULT_CLIENT_INFO.client_library_version
        == f"{bigtable_version}-admin-overlay"
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "request": bigtable_table_admin.RestoreTableRequest(
                parent=PARENT_NAME,
                table_id=TABLE_NAME,
            )
        },
        {
            "request": {
                "parent": PARENT_NAME,
                "table_id": TABLE_NAME,
            },
        },
        {
            "request": bigtable_table_admin.RestoreTableRequest(
                parent=PARENT_NAME,
                table_id=TABLE_NAME,
            ),
            "retry": mock.Mock(spec=retries.Retry),
            "timeout": mock.Mock(spec=retries.Retry),
            "metadata": [("foo", "bar")],
        },
    ],
)
def test_bigtable_table_admin_client_restore_table(kwargs):
    client = BigtableTableAdminClient()

    with mock.patch.object(restore_table, "RestoreTableOperation") as future_mock:
        with mock.patch.object(client, "_transport") as transport_mock:
            with mock.patch.object(client, "_restore_table") as restore_table_mock:
                operation_mock = mock.Mock()
                restore_table_mock.return_value = operation_mock
                client.restore_table(**kwargs)

                restore_table_mock.assert_called_once_with(
                    request=kwargs["request"],
                    retry=kwargs.get("retry", gapic_v1.method.DEFAULT),
                    timeout=kwargs.get("timeout", gapic_v1.method.DEFAULT),
                    metadata=kwargs.get("metadata", ()),
                )
                future_mock.assert_called_once_with(
                    transport_mock.operations_client, operation_mock
                )


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
def test_bigtable_table_admin_client_wait_for_consistency(kwargs):
    client = BigtableTableAdminClient()

    future = client.wait_for_consistency(**kwargs)

    assert type(future) == consistency.CheckConsistencyPollingFuture
    assert future._default_retry == kwargs.get("retry", gapic_v1.method.DEFAULT)

    check_consistency_call = future._check_consistency_call
    assert type(check_consistency_call) == functools.partial

    assert check_consistency_call.func == client.check_consistency
    assert check_consistency_call.args == (kwargs.get("request", None),)
    assert check_consistency_call.keywords == {
        "name": kwargs.get("name", None),
        "consistency_token": kwargs.get("consistency_token", None),
        "timeout": kwargs.get("timeout", gapic_v1.method.DEFAULT),
        "metadata": kwargs.get("metadata", ()),
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "request": bigtable_table_admin.GenerateConsistencyTokenRequest(
                name=TABLE_NAME,
            )
        },
        {
            "request": {"name": TABLE_NAME},
        },
        {
            "name": TABLE_NAME,
        },
        {
            "request": bigtable_table_admin.GenerateConsistencyTokenRequest(
                name=TABLE_NAME,
            ),
            "retry": mock.Mock(spec=retries.Retry),
            "timeout": mock.Mock(spec=retries.Retry),
            "metadata": [("foo", "bar")],
        },
    ],
)
def test_bigtable_table_admin_client_wait_for_replication(kwargs):
    client = BigtableTableAdminClient()

    with mock.patch.object(client, "generate_consistency_token") as generate_mock:
        generate_mock.return_value = (
            bigtable_table_admin.GenerateConsistencyTokenResponse(
                consistency_token=CONSISTENCY_TOKEN,
            )
        )
        future = client.wait_for_replication(**kwargs)

        expected_check_consistency_request = (
            bigtable_table_admin.CheckConsistencyRequest(
                name=TABLE_NAME,
                consistency_token=CONSISTENCY_TOKEN,
            )
        )

        assert type(future) == consistency.CheckConsistencyPollingFuture
        assert future._default_retry == kwargs.get("retry", gapic_v1.method.DEFAULT)

        check_consistency_call = future._check_consistency_call
        assert type(check_consistency_call) == functools.partial

        assert check_consistency_call.func == client.check_consistency
        assert check_consistency_call.args == (expected_check_consistency_request,)
        assert check_consistency_call.keywords == {
            "name": None,
            "consistency_token": None,
            "timeout": kwargs.get("timeout", gapic_v1.method.DEFAULT),
            "metadata": kwargs.get("metadata", ()),
        }
