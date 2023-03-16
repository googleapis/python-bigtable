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

import unittest

TEST_ROWS = [
    "row_key_1",
    b"row_key_2",
]


class TestReadRowsQuery(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery

        return ReadRowsQuery

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor_defaults(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        self.assertEqual(query.row_ranges, [])
        self.assertEqual(query.filter, None)
        self.assertEqual(query.limit, None)

    def test_ctor_explicit(self):
        from google.cloud.bigtable.row_filters import RowFilterChain

        filter_ = RowFilterChain()
        query = self._make_one(["row_key_1", "row_key_2"], limit=10, row_filter=filter_)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn("row_key_1".encode(), query.row_keys)
        self.assertIn("row_key_2".encode(), query.row_keys)
        self.assertEqual(query.row_ranges, [])
        self.assertEqual(query.filter, filter_)
        self.assertEqual(query.limit, 10)

    def test_ctor_invalid_limit(self):
        with self.assertRaises(ValueError):
            self._make_one(limit=-1)

    def test_set_filter(self):
        from google.cloud.bigtable.row_filters import RowFilterChain

        filter1 = RowFilterChain()
        query = self._make_one()
        self.assertEqual(query.filter, None)
        result = query.set_filter(filter1)
        self.assertEqual(query.filter, filter1)
        self.assertEqual(result, query)
        filter2 = RowFilterChain()
        result = query.set_filter(filter2)
        self.assertEqual(query.filter, filter2)
        result = query.set_filter(None)
        self.assertEqual(query.filter, None)
        self.assertEqual(result, query)
        query.filter = RowFilterChain()
        self.assertEqual(query.filter, RowFilterChain())
        with self.assertRaises(ValueError):
            query.filter = 1

    def test_set_filter_dict(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsRequest

        filter1 = RowSampleFilter(0.5)
        filter1_dict = filter1.to_dict()
        query = self._make_one()
        self.assertEqual(query.filter, None)
        result = query.set_filter(filter1_dict)
        self.assertEqual(query.filter, filter1_dict)
        self.assertEqual(result, query)
        output = query.to_dict()
        self.assertEqual(output["filter"], filter1_dict)
        proto_output = ReadRowsRequest(**output)
        self.assertEqual(proto_output.filter, filter1.to_pb())

        query.filter = None
        self.assertEqual(query.filter, None)

    def test_set_limit(self):
        query = self._make_one()
        self.assertEqual(query.limit, None)
        result = query.set_limit(10)
        self.assertEqual(query.limit, 10)
        self.assertEqual(result, query)
        query.limit = 9
        self.assertEqual(query.limit, 9)
        result = query.set_limit(0)
        self.assertEqual(query.limit, 0)
        self.assertEqual(result, query)
        with self.assertRaises(ValueError):
            query.set_limit(-1)
        with self.assertRaises(ValueError):
            query.limit = -100

    def test_add_rows_str(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_str = "test_row"
        result = query.add_rows(input_str)
        self.assertEqual(len(query.row_keys), 1)
        self.assertIn(input_str.encode(), query.row_keys)
        self.assertEqual(result, query)
        input_str2 = "test_row2"
        result = query.add_rows(input_str2)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(input_str.encode(), query.row_keys)
        self.assertIn(input_str2.encode(), query.row_keys)
        self.assertEqual(result, query)

    def test_add_rows_bytes(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_bytes = b"test_row"
        result = query.add_rows(input_bytes)
        self.assertEqual(len(query.row_keys), 1)
        self.assertIn(input_bytes, query.row_keys)
        self.assertEqual(result, query)
        input_bytes2 = b"test_row2"
        result = query.add_rows(input_bytes2)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(input_bytes, query.row_keys)
        self.assertIn(input_bytes2, query.row_keys)
        self.assertEqual(result, query)

    def test_add_rows_batch(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_batch = ["test_row", b"test_row2", "test_row3"]
        result = query.add_rows(input_batch)
        self.assertEqual(len(query.row_keys), 3)
        self.assertIn(b"test_row", query.row_keys)
        self.assertIn(b"test_row2", query.row_keys)
        self.assertIn(b"test_row3", query.row_keys)
        self.assertEqual(result, query)
        # test adding another batch
        query.add_rows(["test_row4", b"test_row5"])
        self.assertEqual(len(query.row_keys), 5)
        self.assertIn(input_batch[0].encode(), query.row_keys)
        self.assertIn(input_batch[1], query.row_keys)
        self.assertIn(input_batch[2].encode(), query.row_keys)
        self.assertIn(b"test_row4", query.row_keys)
        self.assertIn(b"test_row5", query.row_keys)

    def test_add_rows_invalid(self):
        query = self._make_one()
        with self.assertRaises(ValueError):
            query.add_rows(1)
        with self.assertRaises(ValueError):
            query.add_rows(["s", 0])

    def test_duplicate_rows(self):
        # should only hold one of each input key
        key_1 = b"test_row"
        key_2 = b"test_row2"
        query = self._make_one(row_keys=[key_1, key_1, key_2])
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(key_1, query.row_keys)
        self.assertIn(key_2, query.row_keys)
        key_3 = "test_row3"
        query.add_rows([key_3 for _ in range(10)])
        self.assertEqual(len(query.row_keys), 3)

    def test_add_range(self):
        # test with start and end keys
        query = self._make_one()
        self.assertEqual(query.row_ranges, [])
        result = query.add_range("test_row", "test_row2")
        self.assertEqual(len(query.row_ranges), 1)
        self.assertEqual(query.row_ranges[0][0].key, "test_row".encode())
        self.assertEqual(query.row_ranges[0][1].key, "test_row2".encode())
        self.assertEqual(query.row_ranges[0][0].is_inclusive, True)
        self.assertEqual(query.row_ranges[0][1].is_inclusive, False)
        self.assertEqual(result, query)
        # test with start key only
        result = query.add_range("test_row3")
        self.assertEqual(len(query.row_ranges), 2)
        self.assertEqual(query.row_ranges[1][0].key, "test_row3".encode())
        self.assertEqual(query.row_ranges[1][1], None)
        self.assertEqual(result, query)
        # test with end key only
        result = query.add_range(start_key=None, end_key="test_row5")
        self.assertEqual(len(query.row_ranges), 3)
        self.assertEqual(query.row_ranges[2][0], None)
        self.assertEqual(query.row_ranges[2][1].key, "test_row5".encode())
        self.assertEqual(query.row_ranges[2][1].is_inclusive, False)
        # test with start and end keys and inclusive flags
        result = query.add_range(b"test_row6", b"test_row7", False, True)
        self.assertEqual(len(query.row_ranges), 4)
        self.assertEqual(query.row_ranges[3][0].key, b"test_row6")
        self.assertEqual(query.row_ranges[3][1].key, b"test_row7")
        self.assertEqual(query.row_ranges[3][0].is_inclusive, False)
        self.assertEqual(query.row_ranges[3][1].is_inclusive, True)
        # test with nothing passed
        result = query.add_range()
        self.assertEqual(len(query.row_ranges), 5)
        self.assertEqual(query.row_ranges[4][0], None)
        self.assertEqual(query.row_ranges[4][1], None)
        # test with inclusive flags only
        with self.assertRaises(ValueError):
            query.add_range(start_is_inclusive=True, end_is_inclusive=True)
        with self.assertRaises(ValueError):
            query.add_range(start_is_inclusive=False, end_is_inclusive=False)
        with self.assertRaises(ValueError):
            query.add_range(start_is_inclusive=False)
        with self.assertRaises(ValueError):
            query.add_range(end_is_inclusive=True)

    def test_to_dict_rows_default(self):
        # dictionary should be in rowset proto format
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsRequest

        query = self._make_one()
        output = query.to_dict()
        self.assertTrue(isinstance(output, dict))
        self.assertEqual(len(output.keys()), 1)
        expected = {"rows": {"row_keys": [], "row_ranges": []}}
        self.assertEqual(output, expected)

        request_proto = ReadRowsRequest(**output)
        self.assertEqual(request_proto.rows.row_keys, [])
        self.assertEqual(request_proto.rows.row_ranges, [])
        self.assertFalse(request_proto.filter)
        self.assertEqual(request_proto.rows_limit, 0)

    def test_to_dict_rows_populated(self):
        # dictionary should be in rowset proto format
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsRequest
        from google.cloud.bigtable.row_filters import PassAllFilter

        row_filter = PassAllFilter(False)
        query = self._make_one(limit=100, row_filter=row_filter)
        query.add_range("test_row", "test_row2")
        query.add_range("test_row3")
        query.add_range(start_key=None, end_key="test_row5")
        query.add_range(b"test_row6", b"test_row7", False, True)
        query.add_range()
        query.add_rows(["test_row", b"test_row2", "test_row3"])
        query.add_rows(["test_row3", b"test_row4"])
        output = query.to_dict()
        self.assertTrue(isinstance(output, dict))
        request_proto = ReadRowsRequest(**output)
        rowset_proto = request_proto.rows
        # check rows
        self.assertEqual(len(rowset_proto.row_keys), 4)
        self.assertEqual(rowset_proto.row_keys[0], b"test_row")
        self.assertEqual(rowset_proto.row_keys[1], b"test_row2")
        self.assertEqual(rowset_proto.row_keys[2], b"test_row3")
        self.assertEqual(rowset_proto.row_keys[3], b"test_row4")
        # check ranges
        self.assertEqual(len(rowset_proto.row_ranges), 5)
        self.assertEqual(rowset_proto.row_ranges[0].start_key_closed, b"test_row")
        self.assertEqual(rowset_proto.row_ranges[0].end_key_open, b"test_row2")
        self.assertEqual(rowset_proto.row_ranges[1].start_key_closed, b"test_row3")
        self.assertEqual(rowset_proto.row_ranges[1].end_key_open, b"")
        self.assertEqual(rowset_proto.row_ranges[2].start_key_closed, b"")
        self.assertEqual(rowset_proto.row_ranges[2].end_key_open, b"test_row5")
        self.assertEqual(rowset_proto.row_ranges[3].start_key_open, b"test_row6")
        self.assertEqual(rowset_proto.row_ranges[3].end_key_closed, b"test_row7")
        self.assertEqual(rowset_proto.row_ranges[4].start_key_closed, b"")
        self.assertEqual(rowset_proto.row_ranges[4].end_key_open, b"")
        # check limit
        self.assertEqual(request_proto.rows_limit, 100)
        # check filter
        filter_proto = request_proto.filter
        self.assertEqual(filter_proto, row_filter.to_pb())

    def test_shard(self):
        pass
