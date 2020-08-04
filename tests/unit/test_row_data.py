# Copyright 2016 Google LLC
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


import unittest
import mock

from google.api_core.exceptions import DeadlineExceeded
from ._testing import _make_credentials
from google.cloud.bigtable.row_set import RowRange
from google.cloud.bigtable_v2.proto import data_pb2 as data_v2_pb2

from tests.unit.test_base_row_data import (
    MultiCallableStub,
    ChannelStub,
    TestPartialRowsDataConstants,
    _Client,
    _flatten_cells,
    _MockCancellableIterator,
    _MockFailureIterator_1,
    _PartialCellData,
    _ReadRowsResponseV2,
    _generate_cell_chunks,
    _parse_readrows_acceptance_tests,
    _ReadRowsResponseCellChunkPB,
    _make_cell,
    _ReadRowsRequestPB,
    _read_rows_retry_exception,
)


class TestPartialRowsData(unittest.TestCase, TestPartialRowsDataConstants):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row_data import PartialRowsData

        return PartialRowsData

    @staticmethod
    def _get_target_client_class():
        from google.cloud.bigtable.client import Client

        return Client

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_constructor(self):
        from google.cloud.bigtable.row_data import DEFAULT_RETRY_READ_ROWS

        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        partial_rows_data = self._make_one(client._data_stub.ReadRows, request)
        self.assertIs(partial_rows_data.request, request)
        self.assertEqual(partial_rows_data.rows, {})
        self.assertEqual(partial_rows_data.retry, DEFAULT_RETRY_READ_ROWS)

    def test_constructor_with_retry(self):
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = retry = object()
        partial_rows_data = self._make_one(client._data_stub.ReadRows, request, retry)
        self.assertIs(partial_rows_data.request, request)
        self.assertEqual(partial_rows_data.rows, {})
        self.assertEqual(partial_rows_data.retry, retry)

    def test___eq__(self):
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        partial_rows_data1 = self._make_one(client._data_stub.ReadRows, request)
        partial_rows_data2 = self._make_one(client._data_stub.ReadRows, request)
        self.assertEqual(partial_rows_data1.rows, partial_rows_data2.rows)

    def test___eq__type_differ(self):
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        partial_rows_data1 = self._make_one(client._data_stub.ReadRows, request)
        partial_rows_data2 = object()
        self.assertNotEqual(partial_rows_data1, partial_rows_data2)

    def test___ne__same_value(self):
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        partial_rows_data1 = self._make_one(client._data_stub.ReadRows, request)
        partial_rows_data2 = self._make_one(client._data_stub.ReadRows, request)
        comparison_val = partial_rows_data1 != partial_rows_data2
        self.assertTrue(comparison_val)

    def test___ne__(self):
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        partial_rows_data1 = self._make_one(client._data_stub.ReadRows, request)
        partial_rows_data2 = self._make_one(client._data_stub.ReadRows, request)
        self.assertNotEqual(partial_rows_data1, partial_rows_data2)

    def test_rows_getter(self):
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        partial_rows_data = self._make_one(client._data_stub.ReadRows, request)
        partial_rows_data.rows = value = object()
        self.assertIs(partial_rows_data.rows, value)

    def _make_client(self, *args, **kwargs):
        return self._get_target_client_class()(*args, **kwargs)

    def test_state_start(self):
        client = _Client()
        iterator = _MockCancellableIterator()
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [iterator]
        request = object()
        yrd = self._make_one(client._data_stub.ReadRows, request)
        self.assertEqual(yrd.state, yrd.NEW_ROW)

    def test_state_new_row_w_row(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client

        chunk = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )
        chunks = [chunk]

        response = _ReadRowsResponseV2(chunks)
        iterator = _MockCancellableIterator(response)
        channel = ChannelStub(responses=[iterator])
        data_api = bigtable_client.BigtableClient(channel=channel)
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        request = object()

        yrd = self._make_one(client._table_data_client.transport.read_rows, request)

        yrd._response_iterator = iterator
        rows = [row for row in yrd]

        result = rows[0]
        self.assertEqual(result.row_key, self.ROW_KEY)
        self.assertEqual(yrd._counter, 1)
        self.assertEqual(yrd.state, yrd.NEW_ROW)

    def test_multiple_chunks(self):
        from google.cloud.bigtable_v2.gapic import bigtable_client

        chunk1 = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=False,
        )
        chunk2 = _ReadRowsResponseCellChunkPB(
            qualifier=self.QUALIFIER + b"1",
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )
        chunks = [chunk1, chunk2]

        response = _ReadRowsResponseV2(chunks)
        iterator = _MockCancellableIterator(response)
        channel = ChannelStub(responses=[iterator])
        data_api = bigtable_client.BigtableClient(channel=channel)
        credentials = _make_credentials()
        client = self._make_client(
            project="project-id", credentials=credentials, admin=True
        )
        client._table_data_client = data_api
        request = object()

        yrd = self._make_one(client._table_data_client.transport.read_rows, request)

        yrd._response_iterator = iterator
        rows = [row for row in yrd]
        result = rows[0]
        self.assertEqual(result.row_key, self.ROW_KEY)
        self.assertEqual(yrd._counter, 1)
        self.assertEqual(yrd.state, yrd.NEW_ROW)

    def test_cancel(self):
        client = _Client()
        response_iterator = _MockCancellableIterator()
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [response_iterator]
        request = object()
        yield_rows_data = self._make_one(client._data_stub.ReadRows, request)
        self.assertEqual(response_iterator.cancel_calls, 0)
        yield_rows_data.cancel()
        self.assertEqual(response_iterator.cancel_calls, 1)
        self.assertEqual(list(yield_rows_data), [])

    # 'consume_next' tested via 'TestPartialRowsData_JSON_acceptance_tests'

    def test__copy_from_previous_unset(self):
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        yrd = self._make_one(client._data_stub.ReadRows, request)
        cell = _PartialCellData()
        yrd._copy_from_previous(cell)
        self.assertEqual(cell.row_key, b"")
        self.assertEqual(cell.family_name, u"")
        self.assertIsNone(cell.qualifier)
        self.assertEqual(cell.timestamp_micros, 0)
        self.assertEqual(cell.labels, [])

    def test__copy_from_previous_blank(self):
        ROW_KEY = "RK"
        FAMILY_NAME = u"A"
        QUALIFIER = b"C"
        TIMESTAMP_MICROS = 100
        LABELS = ["L1", "L2"]
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        yrd = self._make_one(client._data_stub.ReadRows, request)
        cell = _PartialCellData(
            row_key=ROW_KEY,
            family_name=FAMILY_NAME,
            qualifier=QUALIFIER,
            timestamp_micros=TIMESTAMP_MICROS,
            labels=LABELS,
        )
        yrd._previous_cell = _PartialCellData()
        yrd._copy_from_previous(cell)
        self.assertEqual(cell.row_key, ROW_KEY)
        self.assertEqual(cell.family_name, FAMILY_NAME)
        self.assertEqual(cell.qualifier, QUALIFIER)
        self.assertEqual(cell.timestamp_micros, TIMESTAMP_MICROS)
        self.assertEqual(cell.labels, LABELS)

    def test__copy_from_previous_filled(self):
        ROW_KEY = "RK"
        FAMILY_NAME = u"A"
        QUALIFIER = b"C"
        TIMESTAMP_MICROS = 100
        LABELS = ["L1", "L2"]
        client = _Client()
        client._data_stub = mock.MagicMock()
        request = object()
        yrd = self._make_one(client._data_stub.ReadRows, request)
        yrd._previous_cell = _PartialCellData(
            row_key=ROW_KEY,
            family_name=FAMILY_NAME,
            qualifier=QUALIFIER,
            timestamp_micros=TIMESTAMP_MICROS,
            labels=LABELS,
        )
        cell = _PartialCellData()
        yrd._copy_from_previous(cell)
        self.assertEqual(cell.row_key, ROW_KEY)
        self.assertEqual(cell.family_name, FAMILY_NAME)
        self.assertEqual(cell.qualifier, QUALIFIER)
        self.assertEqual(cell.timestamp_micros, 0)
        self.assertEqual(cell.labels, [])

    def test_valid_last_scanned_row_key_on_start(self):
        client = _Client()
        response = _ReadRowsResponseV2(chunks=(), last_scanned_row_key="2.AFTER")
        iterator = _MockCancellableIterator(response)
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [iterator]
        request = object()
        yrd = self._make_one(client._data_stub.ReadRows, request)
        yrd.last_scanned_row_key = "1.BEFORE"
        self._consume_all(yrd)
        self.assertEqual(yrd.last_scanned_row_key, "2.AFTER")

    def test_invalid_empty_chunk(self):
        from google.cloud.bigtable.base_row_data import InvalidChunk

        client = _Client()
        chunks = _generate_cell_chunks([""])
        response = _ReadRowsResponseV2(chunks)
        iterator = _MockCancellableIterator(response)
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [iterator]
        request = object()
        yrd = self._make_one(client._data_stub.ReadRows, request)
        with self.assertRaises(InvalidChunk):
            self._consume_all(yrd)

    def test_state_cell_in_progress(self):
        LABELS = ["L1", "L2"]

        request = object()
        read_rows = mock.MagicMock()
        yrd = self._make_one(read_rows, request)

        chunk = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            labels=LABELS,
        )
        yrd._update_cell(chunk)

        more_cell_data = _ReadRowsResponseCellChunkPB(value=self.VALUE)
        yrd._update_cell(more_cell_data)

        self.assertEqual(yrd._cell.row_key, self.ROW_KEY)
        self.assertEqual(yrd._cell.family_name, self.FAMILY_NAME)
        self.assertEqual(yrd._cell.qualifier, self.QUALIFIER)
        self.assertEqual(yrd._cell.timestamp_micros, self.TIMESTAMP_MICROS)
        self.assertEqual(yrd._cell.labels, LABELS)
        self.assertEqual(yrd._cell.value, self.VALUE + self.VALUE)

    def test_yield_rows_data(self):
        client = _Client()

        chunk = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )
        chunks = [chunk]

        response = _ReadRowsResponseV2(chunks)
        iterator = _MockCancellableIterator(response)
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [iterator]

        request = object()

        yrd = self._make_one(client._data_stub.ReadRows, request)

        result = self._consume_all(yrd)[0]

        self.assertEqual(result, self.ROW_KEY)

    def test_yield_retry_rows_data(self):
        from google.api_core import retry

        client = _Client()

        retry_read_rows = retry.Retry(predicate=_read_rows_retry_exception)

        chunk = _ReadRowsResponseCellChunkPB(
            row_key=self.ROW_KEY,
            family_name=self.FAMILY_NAME,
            qualifier=self.QUALIFIER,
            timestamp_micros=self.TIMESTAMP_MICROS,
            value=self.VALUE,
            commit_row=True,
        )
        chunks = [chunk]

        response = _ReadRowsResponseV2(chunks)
        failure_iterator = _MockFailureIterator_1()
        iterator = _MockCancellableIterator(response)
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [failure_iterator, iterator]

        request = object()

        yrd = self._make_one(client._data_stub.ReadRows, request, retry_read_rows)

        result = self._consume_all(yrd)[0]

        self.assertEqual(result, self.ROW_KEY)

    def _consume_all(self, yrd):
        return [row.row_key for row in yrd]


class TestPartialRowsData_JSON_acceptance_tests(unittest.TestCase):

    _json_tests = None

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row_data import PartialRowsData

        return PartialRowsData

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def _load_json_test(self, test_name):
        import os

        if self.__class__._json_tests is None:
            dirname = os.path.dirname(__file__)
            filename = os.path.join(dirname, "read-rows-acceptance-test.json")
            raw = _parse_readrows_acceptance_tests(filename)
            tests = self.__class__._json_tests = {}
            for (name, chunks, results) in raw:
                tests[name] = chunks, results
        return self.__class__._json_tests[test_name]

    # JSON Error cases:  invalid chunks

    def _fail_during_consume(self, testcase_name):
        from google.cloud.bigtable.base_row_data import InvalidChunk

        client = _Client()
        chunks, results = self._load_json_test(testcase_name)
        response = _ReadRowsResponseV2(chunks)
        iterator = _MockCancellableIterator(response)
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [iterator]
        request = object()
        prd = self._make_one(client._data_stub.ReadRows, request)
        with self.assertRaises(InvalidChunk):
            prd.consume_all()
        expected_result = self._sort_flattend_cells(
            [result for result in results if not result["error"]]
        )
        flattened = self._sort_flattend_cells(_flatten_cells(prd))
        self.assertEqual(flattened, expected_result)

    def test_invalid_no_cell_key_before_commit(self):
        self._fail_during_consume("invalid - no cell key before commit")

    def test_invalid_no_cell_key_before_value(self):
        self._fail_during_consume("invalid - no cell key before value")

    def test_invalid_new_col_family_wo_qualifier(self):
        self._fail_during_consume("invalid - new col family must specify qualifier")

    def test_invalid_no_commit_between_rows(self):
        self._fail_during_consume("invalid - no commit between rows")

    def test_invalid_no_commit_after_first_row(self):
        self._fail_during_consume("invalid - no commit after first row")

    def test_invalid_duplicate_row_key(self):
        self._fail_during_consume("invalid - duplicate row key")

    def test_invalid_new_row_missing_row_key(self):
        self._fail_during_consume("invalid - new row missing row key")

    def test_invalid_bare_reset(self):
        self._fail_during_consume("invalid - bare reset")

    def test_invalid_bad_reset_no_commit(self):
        self._fail_during_consume("invalid - bad reset, no commit")

    def test_invalid_missing_key_after_reset(self):
        self._fail_during_consume("invalid - missing key after reset")

    def test_invalid_reset_with_chunk(self):
        self._fail_during_consume("invalid - reset with chunk")

    def test_invalid_commit_with_chunk(self):
        self._fail_during_consume("invalid - commit with chunk")

    # JSON Error cases:  incomplete final row

    def _sort_flattend_cells(self, flattened):
        import operator

        key_func = operator.itemgetter("rk", "fm", "qual")
        return sorted(flattened, key=key_func)

    def _incomplete_final_row(self, testcase_name):
        client = _Client()
        chunks, results = self._load_json_test(testcase_name)
        response = _ReadRowsResponseV2(chunks)
        iterator = _MockCancellableIterator(response)
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [iterator]
        request = object()
        prd = self._make_one(client._data_stub.ReadRows, request)
        with self.assertRaises(ValueError):
            prd.consume_all()
        self.assertEqual(prd.state, prd.ROW_IN_PROGRESS)
        expected_result = self._sort_flattend_cells(
            [result for result in results if not result["error"]]
        )
        flattened = self._sort_flattend_cells(_flatten_cells(prd))
        self.assertEqual(flattened, expected_result)

    def test_invalid_no_commit(self):
        self._incomplete_final_row("invalid - no commit")

    def test_invalid_last_row_missing_commit(self):
        self._incomplete_final_row("invalid - last row missing commit")

    # Non-error cases

    _marker = object()

    def _match_results(self, testcase_name, expected_result=_marker):
        client = _Client()
        chunks, results = self._load_json_test(testcase_name)
        response = _ReadRowsResponseV2(chunks)
        iterator = _MockCancellableIterator(response)
        client._data_stub = mock.MagicMock()
        client._data_stub.ReadRows.side_effect = [iterator]
        request = object()
        prd = self._make_one(client._data_stub.ReadRows, request)
        prd.consume_all()
        flattened = self._sort_flattend_cells(_flatten_cells(prd))
        if expected_result is self._marker:
            expected_result = self._sort_flattend_cells(results)
        self.assertEqual(flattened, expected_result)

    def test_bare_commit_implies_ts_zero(self):
        self._match_results("bare commit implies ts=0")

    def test_simple_row_with_timestamp(self):
        self._match_results("simple row with timestamp")

    def test_missing_timestamp_implies_ts_zero(self):
        self._match_results("missing timestamp, implied ts=0")

    def test_empty_cell_value(self):
        self._match_results("empty cell value")

    def test_two_unsplit_cells(self):
        self._match_results("two unsplit cells")

    def test_two_qualifiers(self):
        self._match_results("two qualifiers")

    def test_two_families(self):
        self._match_results("two families")

    def test_with_labels(self):
        self._match_results("with labels")

    def test_split_cell_bare_commit(self):
        self._match_results("split cell, bare commit")

    def test_split_cell(self):
        self._match_results("split cell")

    def test_split_four_ways(self):
        self._match_results("split four ways")

    def test_two_split_cells(self):
        self._match_results("two split cells")

    def test_multi_qualifier_splits(self):
        self._match_results("multi-qualifier splits")

    def test_multi_qualifier_multi_split(self):
        self._match_results("multi-qualifier multi-split")

    def test_multi_family_split(self):
        self._match_results("multi-family split")

    def test_two_rows(self):
        self._match_results("two rows")

    def test_two_rows_implicit_timestamp(self):
        self._match_results("two rows implicit timestamp")

    def test_two_rows_empty_value(self):
        self._match_results("two rows empty value")

    def test_two_rows_one_with_multiple_cells(self):
        self._match_results("two rows, one with multiple cells")

    def test_two_rows_multiple_cells_multiple_families(self):
        self._match_results("two rows, multiple cells, multiple families")

    def test_two_rows_multiple_cells(self):
        self._match_results("two rows, multiple cells")

    def test_two_rows_four_cells_two_labels(self):
        self._match_results("two rows, four cells, 2 labels")

    def test_two_rows_with_splits_same_timestamp(self):
        self._match_results("two rows with splits, same timestamp")

    def test_no_data_after_reset(self):
        # JSON testcase has `"results": null`
        self._match_results("no data after reset", expected_result=[])

    def test_simple_reset(self):
        self._match_results("simple reset")

    def test_reset_to_new_val(self):
        self._match_results("reset to new val")

    def test_reset_to_new_qual(self):
        self._match_results("reset to new qual")

    def test_reset_with_splits(self):
        self._match_results("reset with splits")

    def test_two_resets(self):
        self._match_results("two resets")

    def test_reset_to_new_row(self):
        self._match_results("reset to new row")

    def test_reset_in_between_chunks(self):
        self._match_results("reset in between chunks")

    def test_empty_cell_chunk(self):
        self._match_results("empty cell chunk")

    def test_empty_second_qualifier(self):
        self._match_results("empty second qualifier")
