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

import asyncio

import pytest

from google.cloud.bigtable_v2.types import ReadRowsResponse
from google.cloud.bigtable.read_rows_query import ReadRowsQuery

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


def _make_client(*args, **kwargs):
    from google.cloud.bigtable.client import BigtableDataClient

    return BigtableDataClient(*args, **kwargs)


def _make_chunk(*args, **kwargs):
    from google.cloud.bigtable_v2 import ReadRowsResponse

    kwargs["row_key"] = kwargs.get("row_key", b"row_key")
    kwargs["family_name"] = kwargs.get("family_name", "family_name")
    kwargs["qualifier"] = kwargs.get("qualifier", b"qualifier")
    kwargs["value"] = kwargs.get("value", b"value")
    kwargs["commit_row"] = kwargs.get("commit_row", True)

    return ReadRowsResponse.CellChunk(*args, **kwargs)


async def _make_gapic_stream(chunk_list: list[ReadRowsResponse]):
    from google.cloud.bigtable_v2 import ReadRowsResponse
    async def inner():
        for chunk in chunk_list:
            yield ReadRowsResponse(chunks=[chunk])
    return inner()


@pytest.mark.asyncio
async def test_read_rows_stream():
    client = _make_client()
    table = client.get_table("instance", "table")
    query = ReadRowsQuery()
    chunks = [_make_chunk()]
    with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
        read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(chunks)
        gen = await table.read_rows_stream(query, operation_timeout=3)
        async for row in gen:
            print(row)
    await client.close()
