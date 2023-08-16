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
import sys
import asyncio

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

    def test_ctor_defaults(self):
        request = {}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        client.read_rows.return_value = None
        default_operation_timeout = 600
        time_gen_mock = mock.Mock()
        with mock.patch(
            "google.cloud.bigtable.data._async._read_rows._attempt_timeout_generator",
            time_gen_mock,
        ):
            instance = self._make_one(request, client)
        assert time_gen_mock.call_count == 1
        time_gen_mock.assert_called_once_with(None, default_operation_timeout)
        assert instance.transient_errors == []
        assert instance._last_emitted_row_key is None
        assert instance._emit_count == 0
        assert instance.operation_timeout == default_operation_timeout
        retryable_fn = instance._partial_retryable
        assert retryable_fn.func == instance._read_rows_retryable_attempt
        assert retryable_fn.args[0] == client.read_rows
        assert retryable_fn.args[1] == time_gen_mock.return_value
        assert retryable_fn.args[2] == 0
        assert client.read_rows.call_count == 0

    def test_ctor(self):
        row_limit = 91
        request = {"rows_limit": row_limit}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        client.read_rows.return_value = None
        expected_operation_timeout = 42
        expected_request_timeout = 44
        time_gen_mock = mock.Mock()
        with mock.patch(
            "google.cloud.bigtable.data._async._read_rows._attempt_timeout_generator",
            time_gen_mock,
        ):
            instance = self._make_one(
                request,
                client,
                operation_timeout=expected_operation_timeout,
                attempt_timeout=expected_request_timeout,
            )
        assert time_gen_mock.call_count == 1
        time_gen_mock.assert_called_once_with(
            expected_request_timeout, expected_operation_timeout
        )
        assert instance.transient_errors == []
        assert instance._last_emitted_row_key is None
        assert instance._emit_count == 0
        assert instance.operation_timeout == expected_operation_timeout
        retryable_fn = instance._partial_retryable
        assert retryable_fn.func == instance._read_rows_retryable_attempt
        assert retryable_fn.args[0] == client.read_rows
        assert retryable_fn.args[1] == time_gen_mock.return_value
        assert retryable_fn.args[2] == row_limit
        assert client.read_rows.call_count == 0

    def test___aiter__(self):
        request = {}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        instance = self._make_one(request, client)
        assert instance.__aiter__() is instance

    @pytest.mark.asyncio
    async def test_transient_error_capture(self):
        from google.api_core import exceptions as core_exceptions

        client = mock.Mock()
        client.read_rows = mock.Mock()
        test_exc = core_exceptions.Aborted("test")
        test_exc2 = core_exceptions.DeadlineExceeded("test")
        client.read_rows.side_effect = [test_exc, test_exc2]
        instance = self._make_one({}, client)
        with pytest.raises(RuntimeError):
            await instance.__anext__()
        assert len(instance.transient_errors) == 2
        assert instance.transient_errors[0] == test_exc
        assert instance.transient_errors[1] == test_exc2

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
        sample_range = {"start_key_open": last_key}
        row_set = {"row_keys": in_keys, "row_ranges": [sample_range]}
        revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
        assert revised["row_keys"] == expected
        assert revised["row_ranges"] == [sample_range]

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
        next_key = last_key + "a"
        row_set = {"row_keys": [next_key], "row_ranges": in_ranges}
        revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
        assert revised["row_keys"] == [next_key]
        assert revised["row_ranges"] == expected

    @pytest.mark.parametrize("last_key", ["a", "b", "c"])
    def test_revise_request_full_table(self, last_key):
        row_set = {"row_keys": [], "row_ranges": []}
        for selected_set in [row_set, None]:
            revised = self._get_target_class()._revise_request_rowset(
                selected_set, last_key
            )
            assert revised["row_keys"] == []
            assert len(revised["row_ranges"]) == 1
            assert revised["row_ranges"][0]["start_key_open"] == last_key

    def test_revise_to_empty_rowset(self):
        """revising to an empty rowset should raise error"""
        from google.cloud.bigtable.data.exceptions import _RowSetComplete

        row_keys = ["a", "b", "c"]
        row_set = {"row_keys": row_keys, "row_ranges": [{"end_key_open": "c"}]}
        with pytest.raises(_RowSetComplete):
            self._get_target_class()._revise_request_rowset(row_set, "d")

    @pytest.mark.parametrize(
        "start_limit,emit_num,expected_limit",
        [
            (10, 0, 10),
            (10, 1, 9),
            (10, 10, 0),
            (0, 10, 0),
            (0, 0, 0),
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
        import itertools

        request = {"rows_limit": start_limit}
        instance = self._make_one(request, mock.Mock())
        instance._emit_count = emit_num
        instance._last_emitted_row_key = "a"
        gapic_mock = mock.Mock()
        gapic_mock.side_effect = [GeneratorExit("stop_fn")]
        mock_timeout_gen = itertools.repeat(5)

        attempt = instance._read_rows_retryable_attempt(
            gapic_mock, mock_timeout_gen, start_limit
        )
        if start_limit != 0 and expected_limit == 0:
            # if we emitted the expected number of rows, we should receive a StopAsyncIteration
            with pytest.raises(StopAsyncIteration):
                await attempt.__anext__()
        else:
            with pytest.raises(GeneratorExit):
                await attempt.__anext__()
            assert request["rows_limit"] == expected_limit

    @pytest.mark.parametrize("start_limit,emit_num", [(5, 10), (3, 9), (1, 10)])
    @pytest.mark.asyncio
    async def test_revise_limit_over_limit(self, start_limit, emit_num):
        """
        Should raise runtime error if we get in state where emit_num > start_num
        (unless start_num == 0, which represents unlimited)
        """
        import itertools

        request = {"rows_limit": start_limit}
        instance = self._make_one(request, mock.Mock())
        instance._emit_count = emit_num
        instance._last_emitted_row_key = "a"
        mock_timeout_gen = itertools.repeat(5)
        attempt = instance._read_rows_retryable_attempt(
            mock.Mock(), mock_timeout_gen, start_limit
        )
        with pytest.raises(RuntimeError) as e:
            await attempt.__anext__()
        assert "emit count exceeds row limit" in str(e.value)

    @pytest.mark.asyncio
    async def test_aclose(self):
        import asyncio

        instance = self._make_one({}, mock.Mock())
        await instance.aclose()
        assert instance._stream is None
        assert instance._last_emitted_row_key is None
        with pytest.raises(asyncio.InvalidStateError):
            await instance.__anext__()
        # try calling a second time
        await instance.aclose()

    @pytest.mark.parametrize("limit", [1, 3, 10])
    @pytest.mark.asyncio
    async def test_retryable_attempt_hit_limit(self, limit):
        """
        Stream should end after hitting the limit
        """
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        import itertools

        instance = self._make_one({}, mock.Mock())

        async def mock_gapic(*args, **kwargs):
            # continuously return a single row
            async def gen():
                for i in range(limit * 2):
                    chunk = ReadRowsResponse.CellChunk(
                        row_key=str(i).encode(),
                        family_name="family_name",
                        qualifier=b"qualifier",
                        commit_row=True,
                    )
                    yield ReadRowsResponse(chunks=[chunk])

            return gen()

        mock_timeout_gen = itertools.repeat(5)
        gen = instance._read_rows_retryable_attempt(mock_gapic, mock_timeout_gen, limit)
        # should yield values up to the limit
        for i in range(limit):
            await gen.__anext__()
        # next value should be StopAsyncIteration
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.asyncio
    async def test_retryable_ignore_repeated_rows(self):
        """
        Duplicate rows should cause an invalid chunk error
        """
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync
        from google.cloud.bigtable.data.row import Row
        from google.cloud.bigtable.data.exceptions import InvalidChunk

        async def mock_stream():
            while True:
                yield Row(b"dup_key", cells=[])
                yield Row(b"dup_key", cells=[])

        with mock.patch.object(
            _ReadRowsOperationAsync, "merge_row_response_stream"
        ) as mock_stream_fn:
            mock_stream_fn.return_value = mock_stream()
            instance = self._make_one({}, mock.AsyncMock())
            first_row = await instance.__anext__()
            assert first_row.row_key == b"dup_key"
            with pytest.raises(InvalidChunk) as exc:
                await instance.__anext__()
            assert "Last emitted row key out of order" in str(exc.value)

    @pytest.mark.asyncio
    async def test_retryable_ignore_last_scanned_rows(self):
        """
        Last scanned rows should not be emitted
        """
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync
        from google.cloud.bigtable.data.row import Row, _LastScannedRow

        async def mock_stream():
            while True:
                yield Row(b"key1", cells=[])
                yield _LastScannedRow(b"key2_ignored")
                yield Row(b"key3", cells=[])

        with mock.patch.object(
            _ReadRowsOperationAsync, "merge_row_response_stream"
        ) as mock_stream_fn:
            mock_stream_fn.return_value = mock_stream()
            instance = self._make_one({}, mock.AsyncMock())
            first_row = await instance.__anext__()
            assert first_row.row_key == b"key1"
            second_row = await instance.__anext__()
            assert second_row.row_key == b"key3"

    @pytest.mark.asyncio
    async def test_retryable_cancel_on_close(self):
        """Underlying gapic call should be cancelled when stream is closed"""
        from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync
        from google.cloud.bigtable.data.row import Row

        async def mock_stream():
            while True:
                yield Row(b"key1", cells=[])

        with mock.patch.object(
            _ReadRowsOperationAsync, "merge_row_response_stream"
        ) as mock_stream_fn:
            mock_stream_fn.return_value = mock_stream()
            mock_gapic = mock.AsyncMock()
            mock_call = await mock_gapic.read_rows()
            instance = self._make_one({}, mock_gapic)
            await instance.__anext__()
            assert mock_call.cancel.call_count == 0
            await instance.aclose()
            assert mock_call.cancel.call_count == 1


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


class TestReadRowsAsyncIterator:
    async def mock_stream(self, size=10):
        for i in range(size):
            yield i

    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable.data._async._read_rows import ReadRowsAsyncIterator

        stream = MockStream(*args, **kwargs)
        return ReadRowsAsyncIterator(stream)

    def test_ctor(self):
        with mock.patch("time.monotonic", return_value=0):
            iterator = self._make_one()
            assert iterator._last_interaction_time == 0
            assert iterator._idle_timeout_task is None
            assert iterator.active is True

    def test___aiter__(self):
        iterator = self._make_one()
        assert iterator.__aiter__() is iterator

    @pytest.mark.skipif(
        sys.version_info < (3, 8), reason="mock coroutine requires python3.8 or higher"
    )
    @pytest.mark.asyncio
    async def test__start_idle_timer(self):
        """Should start timer coroutine"""
        iterator = self._make_one()
        expected_timeout = 10
        with mock.patch("time.monotonic", return_value=1):
            with mock.patch.object(iterator, "_idle_timeout_coroutine") as mock_coro:
                await iterator._start_idle_timer(expected_timeout)
                assert mock_coro.call_count == 1
                assert mock_coro.call_args[0] == (expected_timeout,)
        assert iterator._last_interaction_time == 1
        assert iterator._idle_timeout_task is not None

    @pytest.mark.skipif(
        sys.version_info < (3, 8), reason="mock coroutine requires python3.8 or higher"
    )
    @pytest.mark.asyncio
    async def test__start_idle_timer_duplicate(self):
        """Multiple calls should replace task"""
        iterator = self._make_one()
        with mock.patch.object(iterator, "_idle_timeout_coroutine") as mock_coro:
            await iterator._start_idle_timer(1)
            first_task = iterator._idle_timeout_task
            await iterator._start_idle_timer(2)
            second_task = iterator._idle_timeout_task
            assert mock_coro.call_count == 2

            assert first_task is not None
            assert first_task != second_task
            # old tasks hould be cancelled
            with pytest.raises(asyncio.CancelledError):
                await first_task
            # new task should not be cancelled
            await second_task

    @pytest.mark.asyncio
    async def test__idle_timeout_coroutine(self):
        from google.cloud.bigtable.data.exceptions import IdleTimeout

        iterator = self._make_one()
        await iterator._idle_timeout_coroutine(0.05)
        await asyncio.sleep(0.1)
        assert iterator.active is False
        with pytest.raises(IdleTimeout):
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test__idle_timeout_coroutine_extensions(self):
        """touching the generator should reset the idle timer"""
        iterator = self._make_one(items=list(range(100)))
        await iterator._start_idle_timer(0.05)
        for i in range(10):
            # will not expire as long as it is in use
            assert iterator.active is True
            await iterator.__anext__()
            await asyncio.sleep(0.03)
        # now let it expire
        await asyncio.sleep(0.5)
        assert iterator.active is False

    @pytest.mark.asyncio
    async def test___anext__(self):
        num_rows = 10
        iterator = self._make_one(items=list(range(num_rows)))
        for i in range(num_rows):
            assert await iterator.__anext__() == i
        with pytest.raises(StopAsyncIteration):
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test___anext__with_deadline_error(self):
        """
        RetryErrors mean a deadline has been hit.
        Should be wrapped in a DeadlineExceeded exception
        """
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        iterator = self._make_one(items=items, operation_timeout=expected_timeout)
        assert await iterator.__anext__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            await iterator.__anext__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        assert exc.value.__cause__ is None

    @pytest.mark.asyncio
    async def test___anext__with_deadline_error_with_cause(self):
        """
        Transient errors should be exposed as an error group
        """
        from google.api_core import exceptions as core_exceptions
        from google.cloud.bigtable.data.exceptions import RetryExceptionGroup

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        errors = [RuntimeError("error1"), ValueError("error2")]
        iterator = self._make_one(
            items=items, operation_timeout=expected_timeout, errors=errors
        )
        assert await iterator.__anext__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            await iterator.__anext__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        error_group = exc.value.__cause__
        assert isinstance(error_group, RetryExceptionGroup)
        assert len(error_group.exceptions) == 2
        assert error_group.exceptions[0] is errors[0]
        assert error_group.exceptions[1] is errors[1]
        assert "2 failed attempts" in str(error_group)

    @pytest.mark.asyncio
    async def test___anext__with_error(self):
        """
        Other errors should be raised as-is
        """
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.InternalServerError("mock error")]
        iterator = self._make_one(items=items)
        assert await iterator.__anext__() == 1
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            await iterator.__anext__()
        assert exc.value is items[1]
        assert iterator.active is False
        # next call should raise same error
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            await iterator.__anext__()

    @pytest.mark.asyncio
    async def test__finish_with_error(self):
        iterator = self._make_one()
        await iterator._start_idle_timer(10)
        timeout_task = iterator._idle_timeout_task
        assert await iterator.__anext__() == 0
        assert iterator.active is True
        err = ZeroDivisionError("mock error")
        await iterator._finish_with_error(err)
        assert iterator.active is False
        assert iterator._error is err
        assert iterator._idle_timeout_task is None
        with pytest.raises(ZeroDivisionError) as exc:
            await iterator.__anext__()
            assert exc.value is err
        # timeout task should be cancelled
        with pytest.raises(asyncio.CancelledError):
            await timeout_task

    @pytest.mark.asyncio
    async def test_aclose(self):
        iterator = self._make_one()
        await iterator._start_idle_timer(10)
        timeout_task = iterator._idle_timeout_task
        assert await iterator.__anext__() == 0
        assert iterator.active is True
        await iterator.aclose()
        assert iterator.active is False
        assert isinstance(iterator._error, StopAsyncIteration)
        assert iterator._idle_timeout_task is None
        with pytest.raises(StopAsyncIteration) as e:
            await iterator.__anext__()
            assert "closed" in str(e.value)
        # timeout task should be cancelled
        with pytest.raises(asyncio.CancelledError):
            await timeout_task
