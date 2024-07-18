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
    @pytest.mark.parametrize("with_range", [True, False])
    def test_revise_request_rowset_keys_with_range(
        self, in_keys, last_key, expected, with_range
    ):
        from google.cloud.bigtable_v2.types import RowSet as RowSetPB
        from google.cloud.bigtable_v2.types import RowRange as RowRangePB
        from google.cloud.bigtable.data.exceptions import _RowSetComplete

        in_keys = [key.encode("utf-8") for key in in_keys]
        expected = [key.encode("utf-8") for key in expected]
        last_key = last_key.encode("utf-8")

        if with_range:
            sample_range = [RowRangePB(start_key_open=last_key)]
        else:
            sample_range = []
        row_set = RowSetPB(row_keys=in_keys, row_ranges=sample_range)
        if not with_range and expected == []:
            # expect exception if we are revising to an empty rowset
            with pytest.raises(_RowSetComplete):
                self._get_target_class()._revise_request_rowset(row_set, last_key)
        else:
            revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
            assert revised.row_keys == expected
            assert revised.row_ranges == sample_range

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
    @pytest.mark.parametrize("with_key", [True, False])
    def test_revise_request_rowset_ranges(
        self, in_ranges, last_key, expected, with_key
    ):
        from google.cloud.bigtable_v2.types import RowSet as RowSetPB
        from google.cloud.bigtable_v2.types import RowRange as RowRangePB
        from google.cloud.bigtable.data.exceptions import _RowSetComplete

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

        if with_key:
            row_keys = [next_key]
        else:
            row_keys = []

        row_set = RowSetPB(row_ranges=in_ranges, row_keys=row_keys)
        if not with_key and expected == []:
            # expect exception if we are revising to an empty rowset
            with pytest.raises(_RowSetComplete):
                self._get_target_class()._revise_request_rowset(row_set, last_key)
        else:
            revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
            assert revised.row_keys == row_keys
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
        from google.cloud.bigtable_v2.types import ReadRowsResponse

        async def awaitable_stream():
            async def mock_stream():
                for i in range(emit_num):
                    yield ReadRowsResponse(
                        chunks=[
                            ReadRowsResponse.CellChunk(
                                row_key=str(i).encode(),
                                family_name="b",
                                qualifier=b"c",
                                value=b"d",
                                commit_row=True,
                            )
                        ]
                    )

            return mock_stream()

        query = ReadRowsQuery(limit=start_limit)
        table = mock.Mock()
        table.table_name = "table_name"
        table.app_profile_id = "app_profile_id"
        instance = self._make_one(query, table, 10, 10)
        assert instance._remaining_count == start_limit
        # read emit_num rows
        async for val in instance.chunk_stream(awaitable_stream()):
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
        from google.cloud.bigtable_v2.types import ReadRowsResponse
        from google.cloud.bigtable.data.exceptions import InvalidChunk

        async def awaitable_stream():
            async def mock_stream():
                for i in range(emit_num):
                    yield ReadRowsResponse(
                        chunks=[
                            ReadRowsResponse.CellChunk(
                                row_key=str(i).encode(),
                                family_name="b",
                                qualifier=b"c",
                                value=b"d",
                                commit_row=True,
                            )
                        ]
                    )

            return mock_stream()

        query = ReadRowsQuery(limit=start_limit)
        table = mock.Mock()
        table.table_name = "table_name"
        table.app_profile_id = "app_profile_id"
        instance = self._make_one(query, table, 10, 10)
        assert instance._remaining_count == start_limit
        with pytest.raises(InvalidChunk) as e:
            # read emit_num rows
            async for val in instance.chunk_stream(awaitable_stream()):
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

        with mock.patch.object(
            _ReadRowsOperationAsync, "_read_rows_attempt"
        ) as mock_attempt:
            instance = self._make_one(mock.Mock(), mock.Mock(), 1, 1)
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
        instance._remaining_count = None
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
