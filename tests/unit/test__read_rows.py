import unittest
from unittest import mock
import pytest

from google.cloud.bigtable.exceptions import InvalidChunk

TEST_FAMILY = "family_name"
TEST_QUALIFIER = b"column_qualifier"
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
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        return _ReadRowsOperation

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor_defaults(self):
        request = {}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        client.read_rows.return_value = None
        instance = self._make_one(request, client)
        assert instance.transient_errors == []
        assert instance._last_seen_row_key is None
        assert instance._emit_count == 0
        retryable_fn = instance._partial_retryable
        assert retryable_fn.func == instance._read_rows_retryable_attempt
        assert retryable_fn.args[0] == client.read_rows
        assert retryable_fn.args[1] == 0
        assert retryable_fn.args[2] is None
        assert retryable_fn.args[3] is None
        assert retryable_fn.args[4] == 0
        assert client.read_rows.call_count == 0

    def test_ctor(self):
        row_limit = 91
        request = {"rows_limit": row_limit}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        client.read_rows.return_value = None
        expected_buffer_size = 21
        expected_operation_timeout = 42
        expected_row_timeout = 43
        expected_request_timeout = 44
        instance = self._make_one(
            request,
            client,
            buffer_size=expected_buffer_size,
            operation_timeout=expected_operation_timeout,
            per_row_timeout=expected_row_timeout,
            per_request_timeout=expected_request_timeout,
        )
        assert instance.transient_errors == []
        assert instance._last_seen_row_key is None
        assert instance._emit_count == 0
        assert instance.operation_timeout == expected_operation_timeout
        retryable_fn = instance._partial_retryable
        assert retryable_fn.func == instance._read_rows_retryable_attempt
        assert retryable_fn.args[0] == client.read_rows
        assert retryable_fn.args[1] == expected_buffer_size
        assert retryable_fn.args[2] == expected_row_timeout
        assert retryable_fn.args[3] == expected_request_timeout
        assert retryable_fn.args[4] == row_limit
        assert client.read_rows.call_count == 0

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
        # ensure that the _revise_to_empty_set method
        # does not return a full table scan
        row_keys = ["a", "b", "c"]
        row_set = {"row_keys": row_keys, "row_ranges": [{"end_key_open": "c"}]}
        revised = self._get_target_class()._revise_request_rowset(row_set, "d")
        assert revised == row_set
        assert len(revised["row_keys"]) == 3
        assert revised["row_keys"] == row_keys

    @pytest.mark.parametrize(
        "start_limit,emit_num,expected_limit",
        [
            (10, 0, 10),
            (10, 1, 9),
            (10, 10, 0),
            (0, 10, 0),
            (0, 0, 0),
            (4, 2, 2),
            (3, 9, 0),
        ],
    )
    @pytest.mark.asyncio
    async def test_revise_limit(self, start_limit, emit_num, expected_limit):
        request = {"rows_limit": start_limit}
        instance = self._make_one(request, mock.Mock())
        instance._emit_count = emit_num
        instance._last_seen_row_key = "a"
        gapic_mock = mock.Mock()
        gapic_mock.side_effect = [RuntimeError("stop_fn")]
        attempt = instance._read_rows_retryable_attempt(
            gapic_mock, 0, None, None, start_limit
        )
        if start_limit != 0 and expected_limit == 0:
            # if we emitted the expected number of rows, we should receive a StopAsyncIteration
            with pytest.raises(StopAsyncIteration):
                await attempt.__anext__()
        else:
            with pytest.raises(RuntimeError):
                await attempt.__anext__()
            assert request["rows_limit"] == expected_limit

    @pytest.mark.asyncio
    async def test__generator_to_buffer(self):
        import asyncio

        async def test_generator(n):
            for i in range(n):
                yield i

        out_buffer = asyncio.Queue()
        await self._get_target_class()._generator_to_buffer(
            out_buffer, test_generator(10)
        )
        assert out_buffer.qsize() == 11
        for i in range(10):
            assert out_buffer.get_nowait() == i
        assert out_buffer.get_nowait() == StopAsyncIteration
        assert out_buffer.empty()

    @pytest.mark.asyncio
    async def test__generator_to_buffer_with_error(self):
        import asyncio

        async def test_generator(n, error_at=2):
            for i in range(n):
                if i == error_at:
                    raise ValueError("test error")
                else:
                    yield i

        out_buffer = asyncio.Queue()
        await self._get_target_class()._generator_to_buffer(
            out_buffer, test_generator(10, error_at=4)
        )
        assert out_buffer.qsize() == 5
        for i in range(4):
            assert out_buffer.get_nowait() == i
        assert isinstance(out_buffer.get_nowait(), ValueError)
        assert out_buffer.empty()

    @pytest.mark.asyncio
    async def test__buffer_to_generator(self):
        import asyncio

        buffer = asyncio.Queue()
        for i in range(10):
            buffer.put_nowait(i)
        buffer.put_nowait(StopAsyncIteration)
        gen = self._get_target_class()._buffer_to_generator(buffer)
        for i in range(10):
            assert await gen.__anext__() == i
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.asyncio
    async def test__buffer_to_generator_with_error(self):
        import asyncio

        buffer = asyncio.Queue()
        for i in range(4):
            buffer.put_nowait(i)
        test_error = ValueError("test error")
        buffer.put_nowait(test_error)
        gen = self._get_target_class()._buffer_to_generator(buffer)
        for i in range(4):
            assert await gen.__anext__() == i
        with pytest.raises(ValueError) as e:
            await gen.__anext__()
        assert e.value == test_error

    @pytest.mark.asyncio
    async def test_generator_to_buffer_to_generator(self):
        import asyncio

        async def test_generator():
            for i in range(10):
                yield i

        buffer = asyncio.Queue()
        await self._get_target_class()._generator_to_buffer(buffer, test_generator())
        out_gen = self._get_target_class()._buffer_to_generator(buffer)

        out_expected = [i async for i in test_generator()]
        out_actual = [i async for i in out_gen]
        assert out_expected == out_actual

    @pytest.mark.asyncio
    async def test_aclose(self):
        import asyncio

        instance = self._make_one({}, mock.Mock())
        await instance.aclose()
        assert instance._stream is None
        assert instance._last_seen_row_key is None
        with pytest.raises(asyncio.InvalidStateError):
            await instance.__anext__()
        # try calling a second time
        await instance.aclose()


class TestStateMachine(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable._read_rows import _StateMachine

        return _StateMachine

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        # ensure that the _StateMachine constructor
        # sets the initial state
        pass


class TestState(unittest.TestCase):
    pass


class TestRowBuilder(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable._read_rows import _RowBuilder

        return _RowBuilder

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        with mock.patch.object(self._get_target_class(), "reset") as reset:
            self._make_one()
            reset.assert_called_once()
        row_builder = self._make_one()
        self.assertIsNone(row_builder.current_key)
        self.assertIsNone(row_builder.working_cell)
        self.assertIsNone(row_builder.working_value)
        self.assertEqual(row_builder.completed_cells, [])

    def test_start_row(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        self.assertEqual(row_builder.current_key, b"row_key")
        row_builder.start_row(b"row_key2")
        self.assertEqual(row_builder.current_key, b"row_key2")

    def test_start_cell(self):
        # test with no family
        with self.assertRaises(InvalidChunk) as e:
            self._make_one().start_cell("", TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
            self.assertEqual(str(e.exception), "Missing family for a new cell")
        # test with no row
        with self.assertRaises(InvalidChunk) as e:
            row_builder = self._make_one()
            row_builder.start_cell(
                TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS
            )
            self.assertEqual(str(e.exception), "start_cell called without a row")
        # test with valid row
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        self.assertEqual(row_builder.working_cell.family, TEST_FAMILY)
        self.assertEqual(row_builder.working_cell.column_qualifier, TEST_QUALIFIER)
        self.assertEqual(row_builder.working_cell.timestamp_micros, TEST_TIMESTAMP)
        self.assertEqual(row_builder.working_cell.labels, TEST_LABELS)
        self.assertEqual(row_builder.working_value, b"")

    def test_cell_value(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        with self.assertRaises(InvalidChunk):
            # start_cell must be called before cell_value
            row_builder.cell_value(b"cell_value")
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        row_builder.cell_value(b"cell_value")
        self.assertEqual(row_builder.working_value, b"cell_value")
        # should be able to continuously append to the working value
        row_builder.cell_value(b"appended")
        self.assertEqual(row_builder.working_value, b"cell_valueappended")

    def test_finish_cell(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        row_builder.finish_cell()
        self.assertEqual(len(row_builder.completed_cells), 1)
        self.assertEqual(row_builder.completed_cells[0].family, TEST_FAMILY)
        self.assertEqual(
            row_builder.completed_cells[0].column_qualifier, TEST_QUALIFIER
        )
        self.assertEqual(
            row_builder.completed_cells[0].timestamp_micros, TEST_TIMESTAMP
        )
        self.assertEqual(row_builder.completed_cells[0].labels, TEST_LABELS)
        self.assertEqual(row_builder.completed_cells[0].value, b"")
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)
        # add additional cell with value
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        row_builder.cell_value(b"cell_value")
        row_builder.cell_value(b"appended")
        row_builder.finish_cell()
        self.assertEqual(len(row_builder.completed_cells), 2)
        self.assertEqual(row_builder.completed_cells[1].family, TEST_FAMILY)
        self.assertEqual(
            row_builder.completed_cells[1].column_qualifier, TEST_QUALIFIER
        )
        self.assertEqual(
            row_builder.completed_cells[1].timestamp_micros, TEST_TIMESTAMP
        )
        self.assertEqual(row_builder.completed_cells[1].labels, TEST_LABELS)
        self.assertEqual(row_builder.completed_cells[1].value, b"cell_valueappended")
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)

    def test_finish_cell_no_cell(self):
        with self.assertRaises(InvalidChunk) as e:
            self._make_one().finish_cell()
            self.assertEqual(str(e.exception), "finish_cell called before start_cell")
        with self.assertRaises(InvalidChunk) as e:
            row_builder = self._make_one()
            row_builder.start_row(b"row_key")
            row_builder.finish_cell()
            self.assertEqual(str(e.exception), "finish_cell called before start_cell")

    def test_finish_row(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        for i in range(3):
            row_builder.start_cell(str(i), TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
            row_builder.cell_value(b"cell_value: ")
            row_builder.cell_value(str(i).encode("utf-8"))
            row_builder.finish_cell()
            self.assertEqual(len(row_builder.completed_cells), i + 1)
        output = row_builder.finish_row()
        self.assertEqual(row_builder.current_key, None)
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)
        self.assertEqual(len(row_builder.completed_cells), 0)

        self.assertEqual(output.row_key, b"row_key")
        self.assertEqual(len(output), 3)
        for i in range(3):
            self.assertEqual(output[i].family, str(i))
            self.assertEqual(output[i].column_qualifier, TEST_QUALIFIER)
            self.assertEqual(output[i].timestamp_micros, TEST_TIMESTAMP)
            self.assertEqual(output[i].labels, TEST_LABELS)
            self.assertEqual(output[i].value, b"cell_value: " + str(i).encode("utf-8"))

    def finish_row_no_row(self):
        with self.assertRaises(InvalidChunk) as e:
            self._make_one().finish_row()
            self.assertEqual(str(e.exception), "No row in progress")

    def test_reset(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        for i in range(3):
            row_builder.start_cell(str(i), TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
            row_builder.cell_value(b"cell_value: ")
            row_builder.cell_value(str(i).encode("utf-8"))
            row_builder.finish_cell()
            self.assertEqual(len(row_builder.completed_cells), i + 1)
        row_builder.reset()
        self.assertEqual(row_builder.current_key, None)
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)
        self.assertEqual(len(row_builder.completed_cells), 0)
