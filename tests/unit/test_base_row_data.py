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


class MultiCallableStub(object):
    """Stub for the grpc.UnaryUnaryMultiCallable interface."""

    def __init__(self, method, channel_stub):
        self.method = method
        self.channel_stub = channel_stub

    def __call__(self, request, timeout=None, metadata=None, credentials=None):
        self.channel_stub.requests.append((self.method, request))

        return self.channel_stub.responses.pop()


class ChannelStub(object):
    """Stub for the grpc.Channel interface."""

    def __init__(self, responses=[]):
        self.responses = responses
        self.requests = []

    def unary_unary(self, method, request_serializer=None, response_deserializer=None):
        return MultiCallableStub(method, self)

    def unary_stream(self, method, request_serializer=None, response_deserializer=None):
        return MultiCallableStub(method, self)


class TestCell(unittest.TestCase):
    timestamp_micros = 18738724000  # Make sure millis granularity

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.base_row_data import Cell

        return Cell

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def _from_pb_test_helper(self, labels=None):
        import datetime
        from google.cloud._helpers import _EPOCH
        from google.cloud.bigtable_v2.proto import data_pb2 as data_v2_pb2

        timestamp_micros = TestCell.timestamp_micros
        timestamp = _EPOCH + datetime.timedelta(microseconds=timestamp_micros)
        value = b"value-bytes"

        if labels is None:
            cell_pb = data_v2_pb2.Cell(value=value, timestamp_micros=timestamp_micros)
            cell_expected = self._make_one(value, timestamp_micros)
        else:
            cell_pb = data_v2_pb2.Cell(
                value=value, timestamp_micros=timestamp_micros, labels=labels
            )
            cell_expected = self._make_one(value, timestamp_micros, labels=labels)

        klass = self._get_target_class()
        result = klass.from_pb(cell_pb)
        self.assertEqual(result, cell_expected)
        self.assertEqual(result.timestamp, timestamp)

    def test_from_pb(self):
        self._from_pb_test_helper()

    def test_from_pb_with_labels(self):
        labels = [u"label1", u"label2"]
        self._from_pb_test_helper(labels)

    def test_constructor(self):
        value = object()
        cell = self._make_one(value, TestCell.timestamp_micros)
        self.assertEqual(cell.value, value)

    def test___eq__(self):
        value = object()
        cell1 = self._make_one(value, TestCell.timestamp_micros)
        cell2 = self._make_one(value, TestCell.timestamp_micros)
        self.assertEqual(cell1, cell2)

    def test___eq__type_differ(self):
        cell1 = self._make_one(None, None)
        cell2 = object()
        self.assertNotEqual(cell1, cell2)

    def test___ne__same_value(self):
        value = object()
        cell1 = self._make_one(value, TestCell.timestamp_micros)
        cell2 = self._make_one(value, TestCell.timestamp_micros)
        comparison_val = cell1 != cell2
        self.assertFalse(comparison_val)

    def test___ne__(self):
        value1 = "value1"
        value2 = "value2"
        cell1 = self._make_one(value1, TestCell.timestamp_micros)
        cell2 = self._make_one(value2, TestCell.timestamp_micros)
        self.assertNotEqual(cell1, cell2)


class TestPartialRowData(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.base_row_data import PartialRowData

        return PartialRowData

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_constructor(self):
        row_key = object()
        partial_row_data = self._make_one(row_key)
        self.assertIs(partial_row_data._row_key, row_key)
        self.assertEqual(partial_row_data._cells, {})

    def test___eq__(self):
        row_key = object()
        partial_row_data1 = self._make_one(row_key)
        partial_row_data2 = self._make_one(row_key)
        self.assertEqual(partial_row_data1, partial_row_data2)

    def test___eq__type_differ(self):
        partial_row_data1 = self._make_one(None)
        partial_row_data2 = object()
        self.assertNotEqual(partial_row_data1, partial_row_data2)

    def test___ne__same_value(self):
        row_key = object()
        partial_row_data1 = self._make_one(row_key)
        partial_row_data2 = self._make_one(row_key)
        comparison_val = partial_row_data1 != partial_row_data2
        self.assertFalse(comparison_val)

    def test___ne__(self):
        row_key1 = object()
        partial_row_data1 = self._make_one(row_key1)
        row_key2 = object()
        partial_row_data2 = self._make_one(row_key2)
        self.assertNotEqual(partial_row_data1, partial_row_data2)

    def test___ne__cells(self):
        row_key = object()
        partial_row_data1 = self._make_one(row_key)
        partial_row_data1._cells = object()
        partial_row_data2 = self._make_one(row_key)
        self.assertNotEqual(partial_row_data1, partial_row_data2)

    def test_to_dict(self):
        cell1 = object()
        cell2 = object()
        cell3 = object()

        family_name1 = u"name1"
        family_name2 = u"name2"
        qual1 = b"col1"
        qual2 = b"col2"
        qual3 = b"col3"

        partial_row_data = self._make_one(None)
        partial_row_data._cells = {
            family_name1: {qual1: cell1, qual2: cell2},
            family_name2: {qual3: cell3},
        }

        result = partial_row_data.to_dict()
        expected_result = {
            b"name1:col1": cell1,
            b"name1:col2": cell2,
            b"name2:col3": cell3,
        }
        self.assertEqual(result, expected_result)

    def test_cell_value(self):
        family_name = u"name1"
        qualifier = b"col1"
        cell = _make_cell(b"value-bytes")

        partial_row_data = self._make_one(None)
        partial_row_data._cells = {family_name: {qualifier: [cell]}}

        result = partial_row_data.cell_value(family_name, qualifier)
        self.assertEqual(result, cell.value)

    def test_cell_value_invalid_index(self):
        family_name = u"name1"
        qualifier = b"col1"
        cell = _make_cell(b"")

        partial_row_data = self._make_one(None)
        partial_row_data._cells = {family_name: {qualifier: [cell]}}

        with self.assertRaises(IndexError):
            partial_row_data.cell_value(family_name, qualifier, index=None)

    def test_cell_value_invalid_column_family_key(self):
        family_name = u"name1"
        qualifier = b"col1"

        partial_row_data = self._make_one(None)

        with self.assertRaises(KeyError):
            partial_row_data.cell_value(family_name, qualifier)

    def test_cell_value_invalid_column_key(self):
        family_name = u"name1"
        qualifier = b"col1"

        partial_row_data = self._make_one(None)
        partial_row_data._cells = {family_name: {}}

        with self.assertRaises(KeyError):
            partial_row_data.cell_value(family_name, qualifier)

    def test_cell_values(self):
        family_name = u"name1"
        qualifier = b"col1"
        cell = _make_cell(b"value-bytes")

        partial_row_data = self._make_one(None)
        partial_row_data._cells = {family_name: {qualifier: [cell]}}

        values = []
        for value, timestamp_micros in partial_row_data.cell_values(
            family_name, qualifier
        ):
            values.append(value)

        self.assertEqual(values[0], cell.value)

    def test_cell_values_with_max_count(self):
        family_name = u"name1"
        qualifier = b"col1"
        cell_1 = _make_cell(b"value-bytes-1")
        cell_2 = _make_cell(b"value-bytes-2")

        partial_row_data = self._make_one(None)
        partial_row_data._cells = {family_name: {qualifier: [cell_1, cell_2]}}

        values = []
        for value, timestamp_micros in partial_row_data.cell_values(
            family_name, qualifier, max_count=1
        ):
            values.append(value)

        self.assertEqual(1, len(values))
        self.assertEqual(values[0], cell_1.value)

    def test_cells_property(self):
        partial_row_data = self._make_one(None)
        cells = {1: 2}
        partial_row_data._cells = cells
        self.assertEqual(partial_row_data.cells, cells)

    def test_row_key_getter(self):
        row_key = object()
        partial_row_data = self._make_one(row_key)
        self.assertIs(partial_row_data.row_key, row_key)


class _Client(object):

    data_stub = None


class Test_retry_read_rows_exception(unittest.TestCase):
    @staticmethod
    def _call_fut(exc):
        from google.cloud.bigtable.base_row_data import _retry_read_rows_exception

        return _retry_read_rows_exception(exc)

    @staticmethod
    def _make_grpc_call_error(exception):
        from grpc import Call
        from grpc import RpcError

        class TestingException(Call, RpcError):
            def __init__(self, exception):
                self.exception = exception

            def code(self):
                return self.exception.grpc_status_code

            def details(self):
                return "Testing"

        return TestingException(exception)

    def test_w_miss(self):
        from google.api_core.exceptions import Conflict

        exception = Conflict("testing")
        self.assertFalse(self._call_fut(exception))

    def test_w_service_unavailable(self):
        from google.api_core.exceptions import ServiceUnavailable

        exception = ServiceUnavailable("testing")
        self.assertTrue(self._call_fut(exception))

    def test_w_deadline_exceeded(self):
        from google.api_core.exceptions import DeadlineExceeded

        exception = DeadlineExceeded("testing")
        self.assertTrue(self._call_fut(exception))

    def test_w_miss_wrapped_in_grpc(self):
        from google.api_core.exceptions import Conflict

        wrapped = Conflict("testing")
        exception = self._make_grpc_call_error(wrapped)
        self.assertFalse(self._call_fut(exception))

    def test_w_service_unavailable_wrapped_in_grpc(self):
        from google.api_core.exceptions import ServiceUnavailable

        wrapped = ServiceUnavailable("testing")
        exception = self._make_grpc_call_error(wrapped)
        self.assertTrue(self._call_fut(exception))

    def test_w_deadline_exceeded_wrapped_in_grpc(self):
        from google.api_core.exceptions import DeadlineExceeded

        wrapped = DeadlineExceeded("testing")
        exception = self._make_grpc_call_error(wrapped)
        self.assertTrue(self._call_fut(exception))


class TestPartialRowsDataConstants:
    ROW_KEY = b"row-key"
    FAMILY_NAME = u"family"
    QUALIFIER = b"qualifier"
    TIMESTAMP_MICROS = 100
    VALUE = b"value"


class Test_ReadRowsRequestManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.table_name = "table_name"
        cls.row_range1 = RowRange(b"row_key21", b"row_key29")
        cls.row_range2 = RowRange(b"row_key31", b"row_key39")
        cls.row_range3 = RowRange(b"row_key41", b"row_key49")

        cls.request = _ReadRowsRequestPB(table_name=cls.table_name)
        cls.request.rows.row_ranges.add(**cls.row_range1.get_range_kwargs())
        cls.request.rows.row_ranges.add(**cls.row_range2.get_range_kwargs())
        cls.request.rows.row_ranges.add(**cls.row_range3.get_range_kwargs())

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.base_row_data import _ReadRowsRequestManager

        return _ReadRowsRequestManager

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_constructor(self):
        request = mock.Mock()
        last_scanned_key = "last_key"
        rows_read_so_far = 10

        request_manager = self._make_one(request, last_scanned_key, rows_read_so_far)
        self.assertEqual(request, request_manager.message)
        self.assertEqual(last_scanned_key, request_manager.last_scanned_key)
        self.assertEqual(rows_read_so_far, request_manager.rows_read_so_far)

    def test__filter_row_key(self):
        table_name = "table_name"
        request = _ReadRowsRequestPB(table_name=table_name)
        request.rows.row_keys.extend(
            [b"row_key1", b"row_key2", b"row_key3", b"row_key4"]
        )

        last_scanned_key = b"row_key2"
        request_manager = self._make_one(request, last_scanned_key, 2)
        row_keys = request_manager._filter_rows_keys()

        expected_row_keys = [b"row_key3", b"row_key4"]
        self.assertEqual(expected_row_keys, row_keys)

    def test__filter_row_ranges_all_ranges_added_back(self):
        last_scanned_key = b"row_key14"
        request_manager = self._make_one(self.request, last_scanned_key, 2)
        row_ranges = request_manager._filter_row_ranges()

        exp_row_range1 = data_v2_pb2.RowRange(
            start_key_closed=b"row_key21", end_key_open=b"row_key29"
        )
        exp_row_range2 = data_v2_pb2.RowRange(
            start_key_closed=b"row_key31", end_key_open=b"row_key39"
        )
        exp_row_range3 = data_v2_pb2.RowRange(
            start_key_closed=b"row_key41", end_key_open=b"row_key49"
        )
        exp_row_ranges = [exp_row_range1, exp_row_range2, exp_row_range3]

        self.assertEqual(exp_row_ranges, row_ranges)

    def test__filter_row_ranges_all_ranges_already_read(self):
        last_scanned_key = b"row_key54"
        request_manager = self._make_one(self.request, last_scanned_key, 2)
        row_ranges = request_manager._filter_row_ranges()

        self.assertEqual(row_ranges, [])

    def test__filter_row_ranges_all_ranges_already_read_open_closed(self):
        last_scanned_key = b"row_key54"

        row_range1 = RowRange(b"row_key21", b"row_key29", False, True)
        row_range2 = RowRange(b"row_key31", b"row_key39")
        row_range3 = RowRange(b"row_key41", b"row_key49", False, True)

        request = _ReadRowsRequestPB(table_name=self.table_name)
        request.rows.row_ranges.add(**row_range1.get_range_kwargs())
        request.rows.row_ranges.add(**row_range2.get_range_kwargs())
        request.rows.row_ranges.add(**row_range3.get_range_kwargs())

        request_manager = self._make_one(request, last_scanned_key, 2)
        request_manager.new_message = _ReadRowsRequestPB(table_name=self.table_name)
        row_ranges = request_manager._filter_row_ranges()

        self.assertEqual(row_ranges, [])

    def test__filter_row_ranges_some_ranges_already_read(self):
        last_scanned_key = b"row_key22"
        request_manager = self._make_one(self.request, last_scanned_key, 2)
        request_manager.new_message = _ReadRowsRequestPB(table_name=self.table_name)
        row_ranges = request_manager._filter_row_ranges()

        exp_row_range1 = data_v2_pb2.RowRange(
            start_key_open=b"row_key22", end_key_open=b"row_key29"
        )
        exp_row_range2 = data_v2_pb2.RowRange(
            start_key_closed=b"row_key31", end_key_open=b"row_key39"
        )
        exp_row_range3 = data_v2_pb2.RowRange(
            start_key_closed=b"row_key41", end_key_open=b"row_key49"
        )
        exp_row_ranges = [exp_row_range1, exp_row_range2, exp_row_range3]

        self.assertEqual(exp_row_ranges, row_ranges)

    def test_build_updated_request(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter

        row_filter = RowSampleFilter(0.33)
        last_scanned_key = b"row_key25"
        request = _ReadRowsRequestPB(
            filter=row_filter.to_pb(), rows_limit=8, table_name=self.table_name
        )
        request.rows.row_ranges.add(**self.row_range1.get_range_kwargs())

        request_manager = self._make_one(request, last_scanned_key, 2)

        result = request_manager.build_updated_request()

        expected_result = _ReadRowsRequestPB(
            table_name=self.table_name, filter=row_filter.to_pb(), rows_limit=6
        )
        expected_result.rows.row_ranges.add(
            start_key_open=last_scanned_key, end_key_open=self.row_range1.end_key
        )

        self.assertEqual(expected_result, result)

    def test_build_updated_request_full_table(self):
        last_scanned_key = b"row_key14"

        request = _ReadRowsRequestPB(table_name=self.table_name)
        request_manager = self._make_one(request, last_scanned_key, 2)

        result = request_manager.build_updated_request()
        expected_result = _ReadRowsRequestPB(table_name=self.table_name, filter={})
        expected_result.rows.row_ranges.add(start_key_open=last_scanned_key)
        self.assertEqual(expected_result, result)

    def test_build_updated_request_no_start_key(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter

        row_filter = RowSampleFilter(0.33)
        last_scanned_key = b"row_key25"
        request = _ReadRowsRequestPB(
            filter=row_filter.to_pb(), rows_limit=8, table_name=self.table_name
        )
        request.rows.row_ranges.add(end_key_open=b"row_key29")

        request_manager = self._make_one(request, last_scanned_key, 2)

        result = request_manager.build_updated_request()

        expected_result = _ReadRowsRequestPB(
            table_name=self.table_name, filter=row_filter.to_pb(), rows_limit=6
        )
        expected_result.rows.row_ranges.add(
            start_key_open=last_scanned_key, end_key_open=b"row_key29"
        )

        self.assertEqual(expected_result, result)

    def test_build_updated_request_no_end_key(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter

        row_filter = RowSampleFilter(0.33)
        last_scanned_key = b"row_key25"
        request = _ReadRowsRequestPB(
            filter=row_filter.to_pb(), rows_limit=8, table_name=self.table_name
        )
        request.rows.row_ranges.add(start_key_closed=b"row_key20")

        request_manager = self._make_one(request, last_scanned_key, 2)

        result = request_manager.build_updated_request()

        expected_result = _ReadRowsRequestPB(
            table_name=self.table_name, filter=row_filter.to_pb(), rows_limit=6
        )
        expected_result.rows.row_ranges.add(start_key_open=last_scanned_key)

        self.assertEqual(expected_result, result)

    def test_build_updated_request_rows(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter

        row_filter = RowSampleFilter(0.33)
        last_scanned_key = b"row_key4"
        request = _ReadRowsRequestPB(
            filter=row_filter.to_pb(), rows_limit=5, table_name=self.table_name
        )
        request.rows.row_keys.extend(
            [
                b"row_key1",
                b"row_key2",
                b"row_key4",
                b"row_key5",
                b"row_key7",
                b"row_key9",
            ]
        )

        request_manager = self._make_one(request, last_scanned_key, 3)

        result = request_manager.build_updated_request()

        expected_result = _ReadRowsRequestPB(
            table_name=self.table_name, filter=row_filter.to_pb(), rows_limit=2
        )
        expected_result.rows.row_keys.extend([b"row_key5", b"row_key7", b"row_key9"])

        self.assertEqual(expected_result, result)

    def test_build_updated_request_rows_limit(self):
        last_scanned_key = b"row_key14"

        request = _ReadRowsRequestPB(table_name=self.table_name, rows_limit=10)
        request_manager = self._make_one(request, last_scanned_key, 2)

        result = request_manager.build_updated_request()
        expected_result = _ReadRowsRequestPB(
            table_name=self.table_name, filter={}, rows_limit=8
        )
        expected_result.rows.row_ranges.add(start_key_open=last_scanned_key)
        self.assertEqual(expected_result, result)

    def test__key_already_read(self):
        last_scanned_key = b"row_key14"
        request = _ReadRowsRequestPB(table_name=self.table_name)
        request_manager = self._make_one(request, last_scanned_key, 2)

        self.assertTrue(request_manager._key_already_read(b"row_key11"))
        self.assertFalse(request_manager._key_already_read(b"row_key16"))


def _flatten_cells(prd):
    # Match results format from JSON testcases.
    # Doesn't handle error cases.
    from google.cloud._helpers import _bytes_to_unicode
    from google.cloud._helpers import _microseconds_from_datetime

    for row_key, row in prd.rows.items():
        for family_name, family in row.cells.items():
            for qualifier, column in family.items():
                for cell in column:
                    yield {
                        u"rk": _bytes_to_unicode(row_key),
                        u"fm": family_name,
                        u"qual": _bytes_to_unicode(qualifier),
                        u"ts": _microseconds_from_datetime(cell.timestamp),
                        u"value": _bytes_to_unicode(cell.value),
                        u"label": u" ".join(cell.labels),
                        u"error": False,
                    }


class _MockCancellableIterator(object):

    cancel_calls = 0

    def __init__(self, *values):
        self.iter_values = iter(values)

    def cancel(self):
        self.cancel_calls += 1

    def next(self):
        return next(self.iter_values)

    __next__ = next


class _MockFailureIterator_1(object):
    def next(self):
        raise DeadlineExceeded("Failed to read from server")

    __next__ = next


class _PartialCellData(object):

    row_key = b""
    family_name = u""
    qualifier = None
    timestamp_micros = 0

    def __init__(self, **kw):
        self.labels = kw.pop("labels", [])
        self.__dict__.update(kw)


class _ReadRowsResponseV2(object):
    def __init__(self, chunks, last_scanned_row_key=""):
        self.chunks = chunks
        self.last_scanned_row_key = last_scanned_row_key


def _generate_cell_chunks(chunk_text_pbs):
    from google.protobuf.text_format import Merge
    from google.cloud.bigtable_v2.proto.bigtable_pb2 import ReadRowsResponse

    chunks = []

    for chunk_text_pb in chunk_text_pbs:
        chunk = ReadRowsResponse.CellChunk()
        chunks.append(Merge(chunk_text_pb, chunk))

    return chunks


def _parse_readrows_acceptance_tests(filename):
    """Parse acceptance tests from JSON

    See
    https://github.com/googleapis/python-bigtable/blob/master/\
    tests/unit/read-rows-acceptance-test.json
    """
    import json

    with open(filename) as json_file:
        test_json = json.load(json_file)

    for test in test_json["tests"]:
        name = test["name"]
        chunks = _generate_cell_chunks(test["chunks"])
        results = test["results"]
        yield name, chunks, results


def _ReadRowsResponseCellChunkPB(*args, **kw):
    from google.cloud.bigtable_v2.proto import bigtable_pb2 as messages_v2_pb2

    family_name = kw.pop("family_name", None)
    qualifier = kw.pop("qualifier", None)
    message = messages_v2_pb2.ReadRowsResponse.CellChunk(*args, **kw)

    if family_name:
        message.family_name.value = family_name
    if qualifier:
        message.qualifier.value = qualifier

    return message


def _make_cell(value):
    from google.cloud.bigtable import base_row_data

    return base_row_data.Cell(value, TestCell.timestamp_micros)


def _ReadRowsRequestPB(*args, **kw):
    from google.cloud.bigtable_v2.proto import bigtable_pb2 as messages_v2_pb2

    return messages_v2_pb2.ReadRowsRequest(*args, **kw)


def _read_rows_retry_exception(exc):
    return isinstance(exc, DeadlineExceeded)
