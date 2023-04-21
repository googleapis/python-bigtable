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
from google.cloud.bigtable_v2.types import RequestStats
from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.exceptions import InvalidChunk

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


class TestReadRows:
    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

    def _make_stats(self):
        from google.cloud.bigtable_v2.types import RequestStats
        from google.cloud.bigtable_v2.types import FullReadStatsView
        from google.cloud.bigtable_v2.types import ReadIterationStats

        return RequestStats(
            full_read_stats_view=FullReadStatsView(
                read_iteration_stats=ReadIterationStats(
                    rows_seen_count=1,
                    rows_returned_count=2,
                    cells_seen_count=3,
                    cells_returned_count=4,
                )
            )
        )

    def _make_chunk(self, *args, **kwargs):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        kwargs["row_key"] = kwargs.get("row_key", b"row_key")
        kwargs["family_name"] = kwargs.get("family_name", "family_name")
        kwargs["qualifier"] = kwargs.get("qualifier", b"qualifier")
        kwargs["value"] = kwargs.get("value", b"value")
        kwargs["commit_row"] = kwargs.get("commit_row", True)

        return ReadRowsResponse.CellChunk(*args, **kwargs)

    async def _make_gapic_stream(
        self,
        chunk_list: list[ReadRowsResponse.CellChunk | Exception],
        request_stats: RequestStats | None = None,
        sleep_time=0,
    ):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        async def inner():
            for chunk in chunk_list:
                if sleep_time:
                    await asyncio.sleep(sleep_time)
                if isinstance(chunk, Exception):
                    raise chunk
                else:
                    yield ReadRowsResponse(chunks=[chunk])
            if request_stats:
                yield ReadRowsResponse(request_stats=request_stats)

        return inner()

    @pytest.mark.asyncio
    async def test_read_rows(self):
        client = self._make_client()
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        chunks = [
            self._make_chunk(row_key=b"test_1"),
            self._make_chunk(row_key=b"test_2"),
        ]
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                chunks
            )
            results = await table.read_rows(query, operation_timeout=3)
            assert len(results) == 2
            assert results[0].row_key == b"test_1"
            assert results[1].row_key == b"test_2"
        await client.close()

    @pytest.mark.asyncio
    async def test_read_rows_stream(self):
        client = self._make_client()
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        chunks = [
            self._make_chunk(row_key=b"test_1"),
            self._make_chunk(row_key=b"test_2"),
        ]
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                chunks
            )
            gen = await table.read_rows_stream(query, operation_timeout=3)
            results = [row async for row in gen]
            assert len(results) == 2
            assert results[0].row_key == b"test_1"
            assert results[1].row_key == b"test_2"
        await client.close()

    @pytest.mark.parametrize("include_app_profile", [True, False])
    @pytest.mark.asyncio
    async def test_read_rows_query_matches_request(self, include_app_profile):
        from google.cloud.bigtable import RowRange

        async with self._make_client() as client:
            app_profile_id = "app_profile_id" if include_app_profile else None
            table = client.get_table("instance", "table", app_profile_id=app_profile_id)
            row_keys = [b"test_1", "test_2"]
            row_ranges = RowRange("start", "end")
            filter_ = {"test": "filter"}
            limit = 99
            query = ReadRowsQuery(
                row_keys=row_keys,
                row_ranges=row_ranges,
                row_filter=filter_,
                limit=limit,
            )
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    []
                )
                results = await table.read_rows(query, operation_timeout=3)
                assert len(results) == 0
                call_request = read_rows.call_args_list[0][0][0]
                query_dict = query._to_dict()
                if include_app_profile:
                    assert set(call_request.keys()) == set(query_dict.keys()) | {
                        "table_name",
                        "app_profile_id",
                    }
                else:
                    assert set(call_request.keys()) == set(query_dict.keys()) | {
                        "table_name"
                    }
                assert call_request["rows"] == query_dict["rows"]
                assert call_request["filter"] == filter_
                assert call_request["rows_limit"] == limit
                assert call_request["table_name"] == table.table_name
                if include_app_profile:
                    assert call_request["app_profile_id"] == app_profile_id

    @pytest.mark.parametrize(
        "input_buffer_size, expected_buffer_size",
        [(-100, 0), (-1, 0), (0, 0), (1, 1), (2, 2), (100, 100), (101, 101)],
    )
    @pytest.mark.asyncio
    async def test_read_rows_buffer_size(self, input_buffer_size, expected_buffer_size):
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks
                )
                with mock.patch.object(asyncio, "Queue") as queue:
                    queue.side_effect = asyncio.CancelledError
                    try:
                        gen = await table.read_rows_stream(
                            query, operation_timeout=3, buffer_size=input_buffer_size
                        )
                        [row async for row in gen]
                    except asyncio.CancelledError:
                        pass
                    queue.assert_called_once_with(maxsize=expected_buffer_size)

    @pytest.mark.parametrize("operation_timeout", [0.001, 0.023, 0.1])
    @pytest.mark.asyncio
    async def test_read_rows_timeout(self, operation_timeout):
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks, sleep_time=1
                )
                try:
                    await table.read_rows(query, operation_timeout=operation_timeout)
                except core_exceptions.DeadlineExceeded as e:
                    assert (
                        e.message
                        == f"operation_timeout of {operation_timeout:0.1f}s exceeded"
                    )

    @pytest.mark.parametrize(
        "per_row_t, operation_t, expected_num",
        [
            (0.1, 0.01, 0),
            (0.1, 0.19, 1),
            (0.05, 0.54, 10),
            (0.05, 0.14, 2),
            (0.05, 0.24, 4),
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_per_row_timeout(
        self, per_row_t, operation_t, expected_num
    ):
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        # mocking uniform ensures there are no sleeps between retries
        with mock.patch("random.uniform", side_effect=lambda a, b: 0):
            async with self._make_client() as client:
                table = client.get_table("instance", "table")
                query = ReadRowsQuery()
                chunks = [self._make_chunk(row_key=b"test_1")]
                with mock.patch.object(
                    table.client._gapic_client, "read_rows"
                ) as read_rows:
                    read_rows.side_effect = (
                        lambda *args, **kwargs: self._make_gapic_stream(
                            chunks, sleep_time=5
                        )
                    )
                    try:
                        await table.read_rows(
                            query,
                            per_row_timeout=per_row_t,
                            operation_timeout=operation_t,
                        )
                    except core_exceptions.DeadlineExceeded as deadline_exc:
                        retry_exc = deadline_exc.__cause__
                        if expected_num == 0:
                            assert retry_exc is None
                        else:
                            assert type(retry_exc) == RetryExceptionGroup
                            assert f"{expected_num} failed attempts" in str(retry_exc)
                            assert len(retry_exc.exceptions) == expected_num
                            for sub_exc in retry_exc.exceptions:
                                assert (
                                    sub_exc.message
                                    == f"per_row_timeout of {per_row_t:0.1f}s exceeded"
                                )

    @pytest.mark.parametrize(
        "per_request_t, operation_t, expected_num",
        [
            (0.05, 0.09, 1),
            (0.05, 0.54, 10),
            (0.05, 0.14, 2),
            (0.05, 0.24, 4),
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_per_request_timeout(
        self, per_request_t, operation_t, expected_num
    ):
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        # mocking uniform ensures there are no sleeps between retries
        with mock.patch("random.uniform", side_effect=lambda a, b: 0):
            async with self._make_client() as client:
                table = client.get_table("instance", "table")
                query = ReadRowsQuery()
                chunks = [core_exceptions.DeadlineExceeded("mock deadline")]
                with mock.patch.object(
                    table.client._gapic_client, "read_rows"
                ) as read_rows:
                    read_rows.side_effect = (
                        lambda *args, **kwargs: self._make_gapic_stream(
                            chunks, sleep_time=per_request_t
                        )
                    )
                    try:
                        await table.read_rows(
                            query,
                            operation_timeout=operation_t,
                            per_request_timeout=per_request_t,
                        )
                    except core_exceptions.DeadlineExceeded as e:
                        retry_exc = e.__cause__
                        if expected_num == 0:
                            assert retry_exc is None
                        else:
                            assert type(retry_exc) == RetryExceptionGroup
                            assert f"{expected_num} failed attempts" in str(retry_exc)
                            assert len(retry_exc.exceptions) == expected_num
                            for sub_exc in retry_exc.exceptions:
                                assert sub_exc.message == "mock deadline"
                    assert read_rows.call_count == expected_num + 1
                    called_kwargs = read_rows.call_args[1]
                    assert called_kwargs["timeout"] == per_request_t

    @pytest.mark.asyncio
    async def test_read_rows_idle_timeout(self):
        from google.cloud.bigtable.client import ReadRowsIterator
        from google.cloud.bigtable_v2.services.bigtable.async_client import (
            BigtableAsyncClient,
        )
        from google.cloud.bigtable.exceptions import IdleTimeout
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        chunks = [
            self._make_chunk(row_key=b"test_1"),
            self._make_chunk(row_key=b"test_2"),
        ]
        with mock.patch.object(BigtableAsyncClient, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                chunks
            )
            with mock.patch.object(
                ReadRowsIterator, "_start_idle_timer"
            ) as start_idle_timer:
                client = self._make_client()
                table = client.get_table("instance", "table")
                query = ReadRowsQuery()
                gen = await table.read_rows_stream(query)
            # should start idle timer on creation
            start_idle_timer.assert_called_once()
        with mock.patch.object(_ReadRowsOperation, "aclose", AsyncMock()) as aclose:
            # start idle timer with our own value
            await gen._start_idle_timer(0.1)
            # should timeout after being abandoned
            await gen.__anext__()
            await asyncio.sleep(0.2)
            # generator should be expired
            assert not gen.active
            assert type(gen._merger_or_error) == IdleTimeout
            assert gen._idle_timeout_task is None
            await client.close()
            with pytest.raises(IdleTimeout) as e:
                await gen.__anext__()

            expected_msg = (
                "Timed out waiting for next Row to be consumed. (idle_timeout=0.1s)"
            )
            assert e.value.message == expected_msg
            aclose.assert_called_once()
            aclose.assert_awaited()

    @pytest.mark.parametrize(
        "exc_type",
        [
            core_exceptions.Aborted,
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_retryable_error(self, exc_type):
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            expected_error = exc_type("mock error")
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    [expected_error]
                )
                try:
                    await table.read_rows(query, operation_timeout=0.1)
                except core_exceptions.DeadlineExceeded as e:
                    retry_exc = e.__cause__
                    root_cause = retry_exc.exceptions[0]
                    assert type(root_cause) == exc_type
                    assert root_cause == expected_error

    @pytest.mark.parametrize(
        "exc_type",
        [
            core_exceptions.Cancelled,
            core_exceptions.PreconditionFailed,
            core_exceptions.NotFound,
            core_exceptions.PermissionDenied,
            core_exceptions.Conflict,
            core_exceptions.InternalServerError,
            core_exceptions.TooManyRequests,
            core_exceptions.ResourceExhausted,
            InvalidChunk,
        ],
    )
    @pytest.mark.asyncio
    async def test_read_rows_non_retryable_error(self, exc_type):
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            expected_error = exc_type("mock error")
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    [expected_error]
                )
                try:
                    await table.read_rows(query, operation_timeout=0.1)
                except exc_type as e:
                    assert e == expected_error

    @pytest.mark.asyncio
    async def test_read_rows_request_stats(self):
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            stats = self._make_stats()
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks, request_stats=stats
                )
                gen = await table.read_rows_stream(query)
                [row async for row in gen]
                assert gen.request_stats == stats

    @pytest.mark.asyncio
    async def test_read_rows_request_stats_missing(self):
        async with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks, request_stats=None
                )
                gen = await table.read_rows_stream(query)
                [row async for row in gen]
                assert gen.request_stats is None

    @pytest.mark.asyncio
    async def test_read_rows_revise_request(self):
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        with mock.patch.object(
            _ReadRowsOperation, "_revise_request_rowset"
        ) as revise_rowset:
            with mock.patch.object(_ReadRowsOperation, "aclose"):
                revise_rowset.side_effect = [
                    "modified",
                    core_exceptions.Cancelled("mock error"),
                ]
                async with self._make_client() as client:
                    table = client.get_table("instance", "table")
                    row_keys = [b"test_1", b"test_2", b"test_3"]
                    query = ReadRowsQuery(row_keys=row_keys)
                    chunks = [
                        self._make_chunk(row_key=b"test_1"),
                        core_exceptions.Aborted("mock retryable error"),
                    ]
                    with mock.patch.object(
                        table.client._gapic_client, "read_rows"
                    ) as read_rows:
                        read_rows.side_effect = (
                            lambda *args, **kwargs: self._make_gapic_stream(
                                chunks, request_stats=None
                            )
                        )
                        try:
                            await table.read_rows(query)
                        except core_exceptions.Cancelled:
                            revise_rowset.assert_called()
                            first_call_kwargs = revise_rowset.call_args_list[0].kwargs
                            assert (
                                first_call_kwargs["row_set"] == query._to_dict()["rows"]
                            )
                            assert first_call_kwargs["last_seen_row_key"] == b"test_1"
                            second_call_kwargs = revise_rowset.call_args_list[1].kwargs
                            assert second_call_kwargs["row_set"] == "modified"
                            assert second_call_kwargs["last_seen_row_key"] == b"test_1"

    @pytest.mark.asyncio
    async def test_read_rows_default_timeouts(self):
        """
        Ensure that the default timeouts are set on the read rows operation when not overridden
        """
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        operation_timeout = 8
        per_row_timeout = 2
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            async with self._make_client() as client:
                async with client.get_table(
                    "instance",
                    "table",
                    default_operation_timeout=operation_timeout,
                    default_per_row_timeout=per_row_timeout,
                    default_per_request_timeout=per_request_timeout,
                ) as table:
                    try:
                        await table.read_rows(ReadRowsQuery())
                    except RuntimeError:
                        pass
                    kwargs = mock_op.call_args_list[0].kwargs
                    assert kwargs["operation_timeout"] == operation_timeout
                    assert kwargs["per_row_timeout"] == per_row_timeout
                    assert kwargs["per_request_timeout"] == per_request_timeout

    @pytest.mark.asyncio
    async def test_read_rows_default_timeout_override(self):
        """
        When timeouts are passed, they overwrite default values
        """
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        operation_timeout = 8
        per_row_timeout = 2
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            async with self._make_client() as client:
                async with client.get_table(
                    "instance",
                    "table",
                    default_operation_timeout=99,
                    default_per_row_timeout=98,
                    default_per_request_timeout=97,
                ) as table:
                    try:
                        await table.read_rows(
                            ReadRowsQuery(),
                            operation_timeout=operation_timeout,
                            per_row_timeout=per_row_timeout,
                            per_request_timeout=per_request_timeout,
                        )
                    except RuntimeError:
                        pass
                    kwargs = mock_op.call_args_list[0].kwargs
                    assert kwargs["operation_timeout"] == operation_timeout
                    assert kwargs["per_row_timeout"] == per_row_timeout
                    assert kwargs["per_request_timeout"] == per_request_timeout
