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

import pytest

from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore # noqa F401

TEST_FAMILY = "family_name"
TEST_QUALIFIER = b"qualifier"
TEST_TIMESTAMP = 123456789
TEST_LABELS = ["label1", "label2"]


class TestReadRowsOperation:
    """
    Tests helper functions in the ReadRowsOperation class
    in-depth merging logic in merge_row_response_stream and _read_rows_retryable_attempt
    is tested in test_read_rows_acceptance test_client_read_rows, and conformance tests
    """

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync

        return _ReadRowsOperationAsync

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        from google.cloud.bigtable.data import ReadRowsQuery

        row_limit = 91
        query = ReadRowsQuery(limit=row_limit)
        client = mock.Mock()
        client.read_rows = mock.Mock()
        client.read_rows.return_value = None
        table = mock.Mock()
        table._client = client
        table.table_name = "test_table"
        table.app_profile_id = "test_profile"
        expected_operation_timeout = 42
        expected_request_timeout = 44
        time_gen_mock = mock.Mock()
        with mock.patch(
            "google.cloud.bigtable.data._async._read_rows._attempt_timeout_generator",
            time_gen_mock,
        ):
            instance = self._make_one(
                query,
                table,
                operation_timeout=expected_operation_timeout,
                attempt_timeout=expected_request_timeout,
            )
        assert time_gen_mock.call_count == 1
        time_gen_mock.assert_called_once_with(
            expected_request_timeout, expected_operation_timeout
        )
        assert instance._last_yielded_row_key is None
        assert instance._remaining_count == row_limit
        assert instance.operation_timeout == expected_operation_timeout
        assert client.read_rows.call_count == 0
        assert instance._metadata == [
            (
                "x-goog-request-params",
                "table_name=test_table&app_profile_id=test_profile",
            )
        ]
        assert instance.request.table_name == table.table_name
        assert instance.request.app_profile_id == table.app_profile_id
        assert instance.request.rows_limit == row_limit

    @pytest.mark.parametrize(
        "in_keys,last_key,expected",
        [
            (["b", "c", "d"], "a", ["b", "c", "d"]),
            (["a", "b", "c"], "b", ["c"]),
            (["a", "b", "c"], "c", []),
            (["a", "b", "c"], "d", []),
            (["d", "c", "b", "a"], "b", ["d", "c"]),
        ],
    )
    def test_revise_request_rowset_keys(self, in_keys, last_key, expected):
        from google.cloud.bigtable_v2.types import RowSet as RowSetPB
        from google.cloud.bigtable_v2.types import RowRange as RowRangePB

        in_keys = [key.encode("utf-8") for key in in_keys]
        expected = [key.encode("utf-8") for key in expected]
        last_key = last_key.encode("utf-8")

        sample_range = RowRangePB(start_key_open=last_key)
        row_set = RowSetPB(row_keys=in_keys, row_ranges=[sample_range])
        revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
        assert revised.row_keys == expected
        assert revised.row_ranges == [sample_range]

    @pytest.mark.parametrize(
        "in_ranges,last_key,expected",
        [
            (
                [{"start_key_open": "b", "end_key_closed": "d"}],
                "a",
                [{"start_key_open": "b", "end_key_closed": "d"}],
            ),
            (
                [{"start_key_closed": "b", "end_key_closed": "d"}],
                "a",
                [{"start_key_closed": "b", "end_key_closed": "d"}],
            ),
            (
                [{"start_key_open": "a", "end_key_closed": "d"}],
                "b",
                [{"start_key_open": "b", "end_key_closed": "d"}],
            ),
            (
                [{"start_key_closed": "a", "end_key_open": "d"}],
                "b",
                [{"start_key_open": "b", "end_key_open": "d"}],
            ),
            (
                [{"start_key_closed": "b", "end_key_closed": "d"}],
                "b",
                [{"start_key_open": "b", "end_key_closed": "d"}],
            ),
            ([{"start_key_closed": "b", "end_key_closed": "d"}], "d", []),
            ([{"start_key_closed": "b", "end_key_open": "d"}], "d", []),
            ([{"start_key_closed": "b", "end_key_closed": "d"}], "e", []),
            ([{"start_key_closed": "b"}], "z", [{"start_key_open": "z"}]),
            ([{"start_key_closed": "b"}], "a", [{"start_key_closed": "b"}]),
            (
                [{"end_key_closed": "z"}],
                "a",
                [{"start_key_open": "a", "end_key_closed": "z"}],
            ),
            (
                [{"end_key_open": "z"}],
                "a",
                [{"start_key_open": "a", "end_key_open": "z"}],
            ),
        ],
    )
    def test_revise_request_rowset_ranges(self, in_ranges, last_key, expected):
        from google.cloud.bigtable_v2.types import RowSet as RowSetPB
        from google.cloud.bigtable_v2.types import RowRange as RowRangePB

        # convert to protobuf
        next_key = (last_key + "a").encode("utf-8")
        last_key = last_key.encode("utf-8")
        in_ranges = [
            RowRangePB(**{k: v.encode("utf-8") for k, v in r.items()})
            for r in in_ranges
        ]
        expected = [
            RowRangePB(**{k: v.encode("utf-8") for k, v in r.items()}) for r in expected
        ]

        row_set = RowSetPB(row_ranges=in_ranges, row_keys=[next_key])
        revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
        assert revised.row_keys == [next_key]
        assert revised.row_ranges == expected

    @pytest.mark.parametrize("last_key", ["a", "b", "c"])
    def test_revise_request_full_table(self, last_key):
        from google.cloud.bigtable_v2.types import RowSet as RowSetPB
        from google.cloud.bigtable_v2.types import RowRange as RowRangePB

        # convert to protobuf
        last_key = last_key.encode("utf-8")
        row_set = RowSetPB()
        for selected_set in [row_set, None]:
            revised = self._get_target_class()._revise_request_rowset(
                selected_set, last_key
            )
            assert revised.row_keys == []
            assert len(revised.row_ranges) == 1
            assert revised.row_ranges[0] == RowRangePB(start_key_open=last_key)

    def test_revise_to_empty_rowset(self):
        """revising to an empty rowset should raise error"""
        from google.cloud.bigtable.data.exceptions import _RowSetComplete
        from google.cloud.bigtable_v2.types import RowSet as RowSetPB
        from google.cloud.bigtable_v2.types import RowRange as RowRangePB

        row_keys = [b"a", b"b", b"c"]
        row_range = RowRangePB(end_key_open=b"c")
        row_set = RowSetPB(row_keys=row_keys, row_ranges=[row_range])
        with pytest.raises(_RowSetComplete):
            self._get_target_class()._revise_request_rowset(row_set, b"d")

    @pytest.mark.parametrize(
        "start_limit,emit_num,expected_limit",
        [
            (10, 0, 10),
            (10, 1, 9),
            (10, 10, 0),
            (None, 10, None),
            (None, 0, None),
            (4, 2, 2),
        ],
    )
    @pytest.mark.asyncio
    async def test_revise_limit(self, start_limit, emit_num, expected_limit):
        """
        revise_limit should revise the request's limit field
        - if limit is 0 (unlimited), it should never be revised
        - if start_limit-emit_num == 0, the request should end early
        - if the number emitted exceeds the new limit, an exception should
          should be raised (tested in test_revise_limit_over_limit)
        """
        from google.cloud.bigtable.data import ReadRowsQuery

        async def mock_stream():
            for i in range(emit_num):
                yield i

        query = ReadRowsQuery(limit=start_limit)
        table = mock.Mock()
        table.table_name = "table_name"
        table.app_profile_id = "app_profile_id"
        instance = self._make_one(query, table, 10, 10)
        assert instance._remaining_count == start_limit
        with mock.patch.object(instance, "read_rows_attempt") as mock_attempt:
            mock_attempt.return_value = mock_stream()
            # read emit_num rows
            async for val in instance.start_operation():
                pass
        assert instance._remaining_count == expected_limit

    @pytest.mark.parametrize("start_limit,emit_num", [(5, 10), (3, 9), (1, 10)])
    @pytest.mark.asyncio
    async def test_revise_limit_over_limit(self, start_limit, emit_num):
        """
        Should raise runtime error if we get in state where emit_num > start_num
        (unless start_num == 0, which represents unlimited)
        """
        from google.cloud.bigtable.data import ReadRowsQuery

        async def mock_stream():
            for i in range(emit_num):
                yield i

        query = ReadRowsQuery(limit=start_limit)
        table = mock.Mock()
        table.table_name = "table_name"
        table.app_profile_id = "app_profile_id"
        instance = self._make_one(query, table, 10, 10)
        assert instance._remaining_count == start_limit
        with mock.patch.object(instance, "read_rows_attempt") as mock_attempt:
            mock_attempt.return_value = mock_stream()
            with pytest.raises(RuntimeError) as e:
                # read emit_num rows
                async for val in instance.start_operation():
                    pass
            assert "emit count exceeds row limit" in str(e.value)

    @pytest.mark.asyncio
    async def test_aclose(self):
        """
        should be able to close a stream safely with aclose.
        Closed generators should raise StopAsyncIteration on next yield
        """

        async def mock_stream():
            while True:
                yield 1

        instance = self._make_one(mock.Mock(), mock.Mock(), 1, 1)
        with mock.patch.object(instance, "read_rows_attempt") as mock_attempt:
            wrapped_gen = mock_stream()
            mock_attempt.return_value = wrapped_gen
            gen = instance.start_operation()
            # read one row
            await gen.__anext__()
            await gen.aclose()
            with pytest.raises(StopAsyncIteration):
                await gen.__anext__()
            # try calling a second time
            await gen.aclose()
            # ensure close was propagated to wrapped generator
            with pytest.raises(StopAsyncIteration):
                await wrapped_gen.__anext__()

    @pytest.mark.asyncio
    async def test_retryable_ignore_repeated_rows(self):
        """
        Duplicate rows should cause an invalid chunk error
        """
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync
        from google.cloud.bigtable.data.exceptions import InvalidChunk
        from google.cloud.bigtable_v2.types import ReadRowsResponse

        row_key = b"duplicate"

        async def mock_awaitable_stream():
            async def mock_stream():
                while True:
                    yield ReadRowsResponse(
                        chunks=[
                            ReadRowsResponse.CellChunk(row_key=row_key, commit_row=True)
                        ]
                    )
                    yield ReadRowsResponse(
                        chunks=[
                            ReadRowsResponse.CellChunk(row_key=row_key, commit_row=True)
                        ]
                    )

            return mock_stream()

        instance = mock.Mock()
        instance._last_yielded_row_key = None
        stream = _ReadRowsOperationAsync.chunk_stream(instance, mock_awaitable_stream())
        await stream.__anext__()
        with pytest.raises(InvalidChunk) as exc:
            await stream.__anext__()
        assert "row keys should be strictly increasing" in str(exc.value)


class MockStream(_ReadRowsOperationAsync):
    """
    Mock a _ReadRowsOperationAsync stream for testing
    """

    def __init__(self, items=None, errors=None, operation_timeout=None):
        self.transient_errors = errors
        self.operation_timeout = operation_timeout
        self.next_idx = 0
        if items is None:
            items = list(range(10))
        self.items = items

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.next_idx >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.next_idx]
        self.next_idx += 1
        if isinstance(item, Exception):
            raise item
        return item

    async def aclose(self):
        pass


# class TestReadRowsAsyncIterator:
#     async def mock_stream(self, size=10):
#         for i in range(size):
#             yield i

#     def _make_one(self, *args, **kwargs):
#         from google.cloud.bigtable.data._async._read_rows import ReadRowsAsyncIterator

#         stream = MockStream(*args, **kwargs)
#         return ReadRowsAsyncIterator(stream)

#     def test_ctor(self):
#         with mock.patch("time.monotonic", return_value=0):
#             iterator = self._make_one()
#             assert iterator._last_interaction_time == 0
#             assert iterator._idle_timeout_task is None
#             assert iterator.active is True

#     def test___aiter__(self):
#         iterator = self._make_one()
#         assert iterator.__aiter__() is iterator

#     @pytest.mark.skipif(
#         sys.version_info < (3, 8), reason="mock coroutine requires python3.8 or higher"
#     )
#     @pytest.mark.asyncio
#     async def test__start_idle_timer(self):
#         """Should start timer coroutine"""
#         iterator = self._make_one()
#         expected_timeout = 10
#         with mock.patch("time.monotonic", return_value=1):
#             with mock.patch.object(iterator, "_idle_timeout_coroutine") as mock_coro:
#                 await iterator._start_idle_timer(expected_timeout)
#                 assert mock_coro.call_count == 1
#                 assert mock_coro.call_args[0] == (expected_timeout,)
#         assert iterator._last_interaction_time == 1
#         assert iterator._idle_timeout_task is not None

#     @pytest.mark.skipif(
#         sys.version_info < (3, 8), reason="mock coroutine requires python3.8 or higher"
#     )
#     @pytest.mark.asyncio
#     async def test__start_idle_timer_duplicate(self):
#         """Multiple calls should replace task"""
#         iterator = self._make_one()
#         with mock.patch.object(iterator, "_idle_timeout_coroutine") as mock_coro:
#             await iterator._start_idle_timer(1)
#             first_task = iterator._idle_timeout_task
#             await iterator._start_idle_timer(2)
#             second_task = iterator._idle_timeout_task
#             assert mock_coro.call_count == 2

#             assert first_task is not None
#             assert first_task != second_task
#             # old tasks hould be cancelled
#             with pytest.raises(asyncio.CancelledError):
#                 await first_task
#             # new task should not be cancelled
#             await second_task

#     @pytest.mark.asyncio
#     async def test__idle_timeout_coroutine(self):
#         from google.cloud.bigtable.data.exceptions import IdleTimeout

#         iterator = self._make_one()
#         await iterator._idle_timeout_coroutine(0.05)
#         await asyncio.sleep(0.1)
#         assert iterator.active is False
#         with pytest.raises(IdleTimeout):
#             await iterator.__anext__()

#     @pytest.mark.asyncio
#     async def test__idle_timeout_coroutine_extensions(self):
#         """touching the generator should reset the idle timer"""
#         iterator = self._make_one(items=list(range(100)))
#         await iterator._start_idle_timer(0.05)
#         for i in range(10):
#             # will not expire as long as it is in use
#             assert iterator.active is True
#             await iterator.__anext__()
#             await asyncio.sleep(0.03)
#         # now let it expire
#         await asyncio.sleep(0.5)
#         assert iterator.active is False

#     @pytest.mark.asyncio
#     async def test___anext__(self):
#         num_rows = 10
#         iterator = self._make_one(items=list(range(num_rows)))
#         for i in range(num_rows):
#             assert await iterator.__anext__() == i
#         with pytest.raises(StopAsyncIteration):
#             await iterator.__anext__()

#     @pytest.mark.asyncio
#     async def test___anext__with_deadline_error(self):
#         """
#         RetryErrors mean a deadline has been hit.
#         Should be wrapped in a DeadlineExceeded exception
#         """
#         from google.api_core import exceptions as core_exceptions

#         items = [1, core_exceptions.RetryError("retry error", None)]
#         expected_timeout = 99
#         iterator = self._make_one(items=items, operation_timeout=expected_timeout)
#         assert await iterator.__anext__() == 1
#         with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
#             await iterator.__anext__()
#         assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
#             exc.value
#         )
#         assert exc.value.__cause__ is None

#     @pytest.mark.asyncio
#     async def test___anext__with_deadline_error_with_cause(self):
#         """
#         Transient errors should be exposed as an error group
#         """
#         from google.api_core import exceptions as core_exceptions
#         from google.cloud.bigtable.data.exceptions import RetryExceptionGroup

#         items = [1, core_exceptions.RetryError("retry error", None)]
#         expected_timeout = 99
#         errors = [RuntimeError("error1"), ValueError("error2")]
#         iterator = self._make_one(
#             items=items, operation_timeout=expected_timeout, errors=errors
#         )
#         assert await iterator.__anext__() == 1
#         with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
#             await iterator.__anext__()
#         assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
#             exc.value
#         )
#         error_group = exc.value.__cause__
#         assert isinstance(error_group, RetryExceptionGroup)
#         assert len(error_group.exceptions) == 2
#         assert error_group.exceptions[0] is errors[0]
#         assert error_group.exceptions[1] is errors[1]
#         assert "2 failed attempts" in str(error_group)

#     @pytest.mark.asyncio
#     async def test___anext__with_error(self):
#         """
#         Other errors should be raised as-is
#         """
#         from google.api_core import exceptions as core_exceptions

#         items = [1, core_exceptions.InternalServerError("mock error")]
#         iterator = self._make_one(items=items)
#         assert await iterator.__anext__() == 1
#         with pytest.raises(core_exceptions.InternalServerError) as exc:
#             await iterator.__anext__()
#         assert exc.value is items[1]
#         assert iterator.active is False
#         # next call should raise same error
#         with pytest.raises(core_exceptions.InternalServerError) as exc:
#             await iterator.__anext__()

#     @pytest.mark.asyncio
#     async def test__finish_with_error(self):
#         iterator = self._make_one()
#         await iterator._start_idle_timer(10)
#         timeout_task = iterator._idle_timeout_task
#         assert await iterator.__anext__() == 0
#         assert iterator.active is True
#         err = ZeroDivisionError("mock error")
#         await iterator._finish_with_error(err)
#         assert iterator.active is False
#         assert iterator._error is err
#         assert iterator._idle_timeout_task is None
#         with pytest.raises(ZeroDivisionError) as exc:
#             await iterator.__anext__()
#             assert exc.value is err
#         # timeout task should be cancelled
#         with pytest.raises(asyncio.CancelledError):
#             await timeout_task

#     @pytest.mark.asyncio
#     async def test_aclose(self):
#         iterator = self._make_one()
#         await iterator._start_idle_timer(10)
#         timeout_task = iterator._idle_timeout_task
#         assert await iterator.__anext__() == 0
#         assert iterator.active is True
#         await iterator.aclose()
#         assert iterator.active is False
#         assert isinstance(iterator._error, StopAsyncIteration)
#         assert iterator._idle_timeout_task is None
#         with pytest.raises(StopAsyncIteration) as e:
#             await iterator.__anext__()
#             assert "closed" in str(e.value)
#         # timeout task should be cancelled
#         with pytest.raises(asyncio.CancelledError):
#             await timeout_task
