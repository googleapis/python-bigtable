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
import pytest

TEST_ROWS = [
    "row_key_1",
    b"row_key_2",
]


class TestRowRange(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.read_rows_query import RowRange

        return RowRange

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor_start_end(self):
        row_range = self._make_one("test_row", "test_row2")
        self.assertEqual(row_range.start.key, "test_row".encode())
        self.assertEqual(row_range.end.key, "test_row2".encode())
        self.assertEqual(row_range.start.is_inclusive, True)
        self.assertEqual(row_range.end.is_inclusive, False)

    def test_ctor_start_only(self):
        row_range = self._make_one("test_row3")
        self.assertEqual(row_range.start.key, "test_row3".encode())
        self.assertEqual(row_range.start.is_inclusive, True)
        self.assertEqual(row_range.end, None)

    def test_ctor_end_only(self):
        row_range = self._make_one(end_key="test_row4")
        self.assertEqual(row_range.end.key, "test_row4".encode())
        self.assertEqual(row_range.end.is_inclusive, False)
        self.assertEqual(row_range.start, None)

    def test_ctor_inclusive_flags(self):
        row_range = self._make_one("test_row5", "test_row6", False, True)
        self.assertEqual(row_range.start.key, "test_row5".encode())
        self.assertEqual(row_range.end.key, "test_row6".encode())
        self.assertEqual(row_range.start.is_inclusive, False)
        self.assertEqual(row_range.end.is_inclusive, True)

    def test_ctor_defaults(self):
        row_range = self._make_one()
        self.assertEqual(row_range.start, None)
        self.assertEqual(row_range.end, None)

    def test_ctor_flags_only(self):
        with self.assertRaises(ValueError) as exc:
            self._make_one(start_is_inclusive=True, end_is_inclusive=True)
        self.assertEqual(
            exc.exception.args,
            ("start_is_inclusive must be set with start_key",),
        )
        with self.assertRaises(ValueError) as exc:
            self._make_one(start_is_inclusive=False, end_is_inclusive=False)
        self.assertEqual(
            exc.exception.args,
            ("start_is_inclusive must be set with start_key",),
        )
        with self.assertRaises(ValueError) as exc:
            self._make_one(start_is_inclusive=False)
        self.assertEqual(
            exc.exception.args,
            ("start_is_inclusive must be set with start_key",),
        )
        with self.assertRaises(ValueError) as exc:
            self._make_one(end_is_inclusive=True)
        self.assertEqual(
            exc.exception.args, ("end_is_inclusive must be set with end_key",)
        )

    def test_ctor_invalid_keys(self):
        # test with invalid keys
        with self.assertRaises(ValueError) as exc:
            self._make_one(1, "2")
        self.assertEqual(exc.exception.args, ("start_key must be a string or bytes",))
        with self.assertRaises(ValueError) as exc:
            self._make_one("1", 2)
        self.assertEqual(exc.exception.args, ("end_key must be a string or bytes",))

    def test__to_dict_defaults(self):
        row_range = self._make_one("test_row", "test_row2")
        expected = {
            "start_key_closed": b"test_row",
            "end_key_open": b"test_row2",
        }
        self.assertEqual(row_range._to_dict(), expected)

    def test__to_dict_inclusive_flags(self):
        row_range = self._make_one("test_row", "test_row2", False, True)
        expected = {
            "start_key_open": b"test_row",
            "end_key_closed": b"test_row2",
        }
        self.assertEqual(row_range._to_dict(), expected)


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
        with self.assertRaises(ValueError) as exc:
            self._make_one(limit=-1)
        self.assertEqual(exc.exception.args, ("limit must be >= 0",))

    def test_set_filter(self):
        from google.cloud.bigtable.row_filters import RowFilterChain

        filter1 = RowFilterChain()
        query = self._make_one()
        self.assertEqual(query.filter, None)
        query.filter = filter1
        self.assertEqual(query.filter, filter1)
        filter2 = RowFilterChain()
        query.filter = filter2
        self.assertEqual(query.filter, filter2)
        query.filter = None
        self.assertEqual(query.filter, None)
        query.filter = RowFilterChain()
        self.assertEqual(query.filter, RowFilterChain())
        with self.assertRaises(ValueError) as exc:
            query.filter = 1
        self.assertEqual(
            exc.exception.args, ("row_filter must be a RowFilter or dict",)
        )

    def test_set_filter_dict(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsRequest

        filter1 = RowSampleFilter(0.5)
        filter1_dict = filter1.to_dict()
        query = self._make_one()
        self.assertEqual(query.filter, None)
        query.filter = filter1_dict
        self.assertEqual(query.filter, filter1_dict)
        output = query._to_dict()
        self.assertEqual(output["filter"], filter1_dict)
        proto_output = ReadRowsRequest(**output)
        self.assertEqual(proto_output.filter, filter1._to_pb())

        query.filter = None
        self.assertEqual(query.filter, None)

    def test_set_limit(self):
        query = self._make_one()
        self.assertEqual(query.limit, None)
        query.limit = 10
        self.assertEqual(query.limit, 10)
        query.limit = 9
        self.assertEqual(query.limit, 9)
        query.limit = 0
        self.assertEqual(query.limit, 0)
        with self.assertRaises(ValueError) as exc:
            query.limit = -1
        self.assertEqual(exc.exception.args, ("limit must be >= 0",))
        with self.assertRaises(ValueError) as exc:
            query.limit = -100
        self.assertEqual(exc.exception.args, ("limit must be >= 0",))

    def test_add_key_str(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_str = "test_row"
        query.add_key(input_str)
        self.assertEqual(len(query.row_keys), 1)
        self.assertIn(input_str.encode(), query.row_keys)
        input_str2 = "test_row2"
        query.add_key(input_str2)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(input_str.encode(), query.row_keys)
        self.assertIn(input_str2.encode(), query.row_keys)

    def test_add_key_bytes(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_bytes = b"test_row"
        query.add_key(input_bytes)
        self.assertEqual(len(query.row_keys), 1)
        self.assertIn(input_bytes, query.row_keys)
        input_bytes2 = b"test_row2"
        query.add_key(input_bytes2)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(input_bytes, query.row_keys)
        self.assertIn(input_bytes2, query.row_keys)

    def test_add_rows_batch(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_batch = ["test_row", b"test_row2", "test_row3"]
        for k in input_batch:
            query.add_key(k)
        self.assertEqual(len(query.row_keys), 3)
        self.assertIn(b"test_row", query.row_keys)
        self.assertIn(b"test_row2", query.row_keys)
        self.assertIn(b"test_row3", query.row_keys)
        # test adding another batch
        for k in ["test_row4", b"test_row5"]:
            query.add_key(k)
        self.assertEqual(len(query.row_keys), 5)
        self.assertIn(input_batch[0].encode(), query.row_keys)
        self.assertIn(input_batch[1], query.row_keys)
        self.assertIn(input_batch[2].encode(), query.row_keys)
        self.assertIn(b"test_row4", query.row_keys)
        self.assertIn(b"test_row5", query.row_keys)

    def test_add_key_invalid(self):
        query = self._make_one()
        with self.assertRaises(ValueError) as exc:
            query.add_key(1)
        self.assertEqual(exc.exception.args, ("row_key must be string or bytes",))
        with self.assertRaises(ValueError) as exc:
            query.add_key(["s"])
        self.assertEqual(exc.exception.args, ("row_key must be string or bytes",))

    def test_duplicate_rows(self):
        # should only hold one of each input key
        key_1 = b"test_row"
        key_2 = b"test_row2"
        query = self._make_one(row_keys=[key_1, key_1, key_2])
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(key_1, query.row_keys)
        self.assertIn(key_2, query.row_keys)
        key_3 = "test_row3"
        for i in range(10):
            query.add_key(key_3)
        self.assertEqual(len(query.row_keys), 3)

    def test_add_range(self):
        from google.cloud.bigtable.read_rows_query import RowRange

        query = self._make_one()
        self.assertEqual(query.row_ranges, [])
        input_range = RowRange(start_key=b"test_row")
        query.add_range(input_range)
        self.assertEqual(len(query.row_ranges), 1)
        self.assertEqual(query.row_ranges[0], input_range)
        input_range2 = RowRange(start_key=b"test_row2")
        query.add_range(input_range2)
        self.assertEqual(len(query.row_ranges), 2)
        self.assertEqual(query.row_ranges[0], input_range)
        self.assertEqual(query.row_ranges[1], input_range2)

    def test_add_range_dict(self):
        query = self._make_one()
        self.assertEqual(query.row_ranges, [])
        input_range = {"start_key_closed": b"test_row"}
        query.add_range(input_range)
        self.assertEqual(len(query.row_ranges), 1)
        self.assertEqual(query.row_ranges[0], input_range)

    def test_to_dict_rows_default(self):
        # dictionary should be in rowset proto format
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsRequest

        query = self._make_one()
        output = query._to_dict()
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
        from google.cloud.bigtable.read_rows_query import RowRange

        row_filter = PassAllFilter(False)
        query = self._make_one(limit=100, row_filter=row_filter)
        query.add_range(RowRange("test_row", "test_row2"))
        query.add_range(RowRange("test_row3"))
        query.add_range(RowRange(start_key=None, end_key="test_row5"))
        query.add_range(RowRange(b"test_row6", b"test_row7", False, True))
        query.add_range(RowRange())
        query.add_key("test_row")
        query.add_key(b"test_row2")
        query.add_key("test_row3")
        query.add_key(b"test_row3")
        query.add_key(b"test_row4")
        output = query._to_dict()
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
        self.assertEqual(filter_proto, row_filter._to_pb())

def _parse_query_string(query_string):
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery, RowRange
    query = ReadRowsQuery()
    segments = query_string.split(",")
    for segment in segments:
        if "-" in segment:
            start, end = segment.split("-")
            s_open, e_open = True, True
            if start == "":
                start = None
                s_open = None
            else:
                if start[0] == "(":
                    s_open = False
                start = start[1:]
            if end == "":
                end = None
                e_open = None
            else:
                if end[-1] == ")":
                    e_open = False
                end = end[:-1]
            query.add_range(RowRange(start, end, s_open, e_open))
        else:
            query.add_key(segment)
    return query

@pytest.mark.parametrize(
    "query_string,shard_points", [
        ("a,[p-q)", []),
        ("0_key,[1_range_start-2_range_end)", ["3_split"]),
        ("-1_range_end)", ["5_split"]),
        ("0_key,[1_range_start-2_range_end)", ["2_range_end"]),
        ("9_row_key,(5_range_start-7_range_end)", ["3_split"]),
        ("(5_range_start-", ["3_split"]),
        ("3_split,[3_split-5_split)", ["3_split", "5_split"]),
        ("[3_split-", ["3_split"]),
    ]
)
def test_shard_no_split(query_string, shard_points):
    # these queries should not be sharded
    initial_query = _parse_query_string(query_string)
    row_samples = [(point.encode(), None) for point in shard_points]
    sharded_queries = initial_query.shard(row_samples)
    assert len(sharded_queries) == 1
    assert initial_query == sharded_queries[0]

def test_shard_full_table_scan():
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery, RowRange
    full_scan_query = ReadRowsQuery(row_ranges=[RowRange()])
    split_points = [('a', None)]
    sharded_queries = full_scan_query.shard(split_points)
    assert len(sharded_queries) == 2
    assert sharded_queries[0] == _parse_query_string("-a)")
    assert sharded_queries[1] == _parse_query_string("[a-")
