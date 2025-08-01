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
    from unittest.mock import AsyncMock  # pragma: NO COVER  # noqa: F401
except ImportError:  # pragma: NO COVER
    import mock

from google.api_core import exceptions
from google.api_core import gapic_v1
from google.api_core import retry as retries
from google.auth.credentials import AnonymousCredentials
from google.cloud.bigtable_admin_v2.services.bigtable_table_admin import transports
from google.cloud.bigtable_admin_v2.types import bigtable_table_admin
from google.cloud.bigtable_admin_v2.overlay.services.bigtable_table_admin.async_client import (
    BigtableTableAdminAsyncClient,
    DEFAULT_CLIENT_INFO,
)
from google.cloud.bigtable_admin_v2.overlay.types import (
    async_restore_table,
    wait_for_consistency_request,
)

from google.cloud.bigtable import __version__ as bigtable_version

from test_async_consistency import (
    FALSE_CONSISTENCY_RESPONSE,
    TRUE_CONSISTENCY_RESPONSE,
)

import pytest


PARENT_NAME = "my_parent"
TABLE_NAME = "my_table"
CONSISTENCY_TOKEN = "abcdefg"


def _make_client(**kwargs):
    kwargs["credentials"] = kwargs.get("credentials", AnonymousCredentials())
    return BigtableTableAdminAsyncClient(**kwargs)


@pytest.mark.parametrize(
    "transport_class,transport_name",
    [
        (
            transports.BigtableTableAdminGrpcAsyncIOTransport,
            "grpc_asyncio",
        ),
    ],
)
def test_bigtable_table_admin_async_client_client_version(
    transport_class, transport_name
):
    with mock.patch.object(transport_class, "__init__") as patched:
        patched.return_value = None
        _make_client(transport=transport_name)

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
async def test_bigtable_table_admin_async_client_restore_table(kwargs):
    client = _make_client()

    with mock.patch.object(
        async_restore_table, "AsyncRestoreTableOperation", new_callable=mock.AsyncMock
    ) as future_mock:
        with mock.patch.object(
            client._client, "_transport", new_callable=mock.AsyncMock
        ) as transport_mock:
            with mock.patch.object(
                client, "_restore_table", new_callable=mock.AsyncMock
            ) as restore_table_mock:
                operation_mock = mock.Mock()
                restore_table_mock.return_value = operation_mock
                await client.restore_table(**kwargs)

                restore_table_mock.assert_called_once_with(
                    request=kwargs["request"],
                    retry=kwargs.get("retry", gapic_v1.method.DEFAULT),
                    timeout=kwargs.get("timeout", gapic_v1.method.DEFAULT),
                    metadata=kwargs.get("metadata", ()),
                )
                future_mock.assert_called_once_with(
                    transport_mock.operations_client, operation_mock
                )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,check_consistency_request_extras",
    [
        (
            {
                "request": wait_for_consistency_request.WaitForConsistencyRequest(
                    name=TABLE_NAME,
                )
            },
            {},
        ),
        (
            {
                "request": wait_for_consistency_request.WaitForConsistencyRequest(
                    name=TABLE_NAME,
                    standard_read_remote_writes=bigtable_table_admin.StandardReadRemoteWrites(),
                )
            },
            {
                "standard_read_remote_writes": bigtable_table_admin.StandardReadRemoteWrites(),
            },
        ),
        (
            {
                "request": wait_for_consistency_request.WaitForConsistencyRequest(
                    name=TABLE_NAME,
                    data_boost_read_local_writes=bigtable_table_admin.DataBoostReadLocalWrites(),
                )
            },
            {
                "data_boost_read_local_writes": bigtable_table_admin.DataBoostReadLocalWrites(),
            },
        ),
        (
            {
                "request": {
                    "name": TABLE_NAME,
                    "data_boost_read_local_writes": {},
                }
            },
            {
                "data_boost_read_local_writes": bigtable_table_admin.DataBoostReadLocalWrites(),
            },
        ),
        (
            {
                "name": TABLE_NAME,
            },
            {},
        ),
        (
            {
                "request": wait_for_consistency_request.WaitForConsistencyRequest(
                    name=TABLE_NAME,
                ),
                "retry": mock.Mock(spec=retries.Retry),
                "timeout": mock.Mock(spec=retries.Retry),
                "metadata": [("foo", "bar")],
            },
            {},
        ),
    ],
)
async def test_bigtable_table_admin_async_client_wait_for_consistency(
    kwargs, check_consistency_request_extras
):
    client = _make_client()
    poll_count = 3
    check_mock_side_effect = [FALSE_CONSISTENCY_RESPONSE] * (poll_count - 1)
    check_mock_side_effect.append(TRUE_CONSISTENCY_RESPONSE)

    with mock.patch.object(
        client, "generate_consistency_token", new_callable=mock.AsyncMock
    ) as generate_mock:
        with mock.patch.object(
            client, "check_consistency", new_callable=mock.AsyncMock
        ) as check_mock:
            generate_mock.return_value = (
                bigtable_table_admin.GenerateConsistencyTokenResponse(
                    consistency_token=CONSISTENCY_TOKEN,
                )
            )

            check_mock.side_effect = check_mock_side_effect
            result = await client.wait_for_consistency(**kwargs)

            assert result is True

            generate_mock.assert_awaited_once_with(
                bigtable_table_admin.GenerateConsistencyTokenRequest(
                    name=TABLE_NAME,
                ),
                retry=kwargs.get("retry", gapic_v1.method.DEFAULT),
                timeout=kwargs.get("timeout", gapic_v1.method.DEFAULT),
                metadata=kwargs.get("metadata", ()),
            )

            expected_check_consistency_request = (
                bigtable_table_admin.CheckConsistencyRequest(
                    name=TABLE_NAME,
                    consistency_token=CONSISTENCY_TOKEN,
                    **check_consistency_request_extras,
                )
            )

            check_mock.assert_awaited_with(
                expected_check_consistency_request,
                retry=kwargs.get("retry", gapic_v1.method.DEFAULT),
                timeout=kwargs.get("timeout", gapic_v1.method.DEFAULT),
                metadata=kwargs.get("metadata", ()),
            )


@pytest.mark.asyncio
async def test_bigtable_table_admin_async_client_wait_for_consistency_error_in_call():
    client = _make_client()
    request = wait_for_consistency_request.WaitForConsistencyRequest(
        name=TABLE_NAME,
    )

    with pytest.raises(exceptions.GoogleAPICallError):
        with mock.patch.object(
            client, "generate_consistency_token", new_callable=mock.AsyncMock
        ) as generate_mock:
            generate_mock.side_effect = exceptions.DeadlineExceeded(
                "Deadline Exceeded."
            )
            await client.wait_for_consistency(request)

    with pytest.raises(exceptions.GoogleAPICallError):
        with mock.patch.object(
            client, "generate_consistency_token", new_callable=mock.AsyncMock
        ) as generate_mock:
            with mock.patch.object(
                client, "check_consistency", new_callable=mock.AsyncMock
            ) as check_mock:
                generate_mock.return_value = (
                    bigtable_table_admin.GenerateConsistencyTokenResponse(
                        consistency_token=CONSISTENCY_TOKEN,
                    )
                )

                check_mock.side_effect = exceptions.DeadlineExceeded(
                    "Deadline Exceeded."
                )
                await client.wait_for_consistency(request)


@pytest.mark.asyncio
async def test_bigtable_table_admin_async_client_wait_for_consistency_user_error():
    client = _make_client()
    with pytest.raises(ValueError):
        await client.wait_for_consistency(
            {
                "name": TABLE_NAME,
            },
            name=TABLE_NAME,
        )
