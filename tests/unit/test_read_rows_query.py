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

import pytest

TEST_ROWS = [
    "row_key_1",
    b"row_key_2",
]


class TestRowRange:
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.read_rows_query import RowRange

        return RowRange

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor_start_end(self):
        row_range = self._make_one("test_row", "test_row2")
        assert row_range.start.key == "test_row".encode()
        assert row_range.end.key == "test_row2".encode()
        assert row_range.start.is_inclusive is True
        assert row_range.end.is_inclusive is False

    def test_ctor_start_only(self):
        row_range = self._make_one("test_row3")
        assert row_range.start.key == "test_row3".encode()
        assert row_range.start.is_inclusive is True
        assert row_range.end is None

    def test_ctor_end_only(self):
        row_range = self._make_one(end_key="test_row4")
        assert row_range.end.key == "test_row4".encode()
        assert row_range.end.is_inclusive is False
        assert row_range.start is None

    def test_ctor_inclusive_flags(self):
        row_range = self._make_one("test_row5", "test_row6", False, True)
        assert row_range.start.key == "test_row5".encode()
        assert row_range.end.key == "test_row6".encode()
        assert row_range.start.is_inclusive is False
        assert row_range.end.is_inclusive is True

    def test_ctor_defaults(self):
        row_range = self._make_one()
        assert row_range.start is None
        assert row_range.end is None

    def test_ctor_flags_only(self):
        with pytest.raises(ValueError) as exc:
            self._make_one(start_is_inclusive=True, end_is_inclusive=True)
        assert str(exc.value) == "start_is_inclusive must be set with start_key"
        with pytest.raises(ValueError) as exc:
            self._make_one(start_is_inclusive=False, end_is_inclusive=False)
        assert str(exc.value) == "start_is_inclusive must be set with start_key"
        with pytest.raises(ValueError) as exc:
            self._make_one(start_is_inclusive=False)
        assert str(exc.value) == "start_is_inclusive must be set with start_key"
        with pytest.raises(ValueError) as exc:
            self._make_one(end_is_inclusive=True)
        assert str(exc.value) == "end_is_inclusive must be set with end_key"

    def test_ctor_invalid_keys(self):
        # test with invalid keys
        with pytest.raises(ValueError) as exc:
            self._make_one(1, "2")
        assert str(exc.value) == "start_key must be a string or bytes"
        with pytest.raises(ValueError) as exc:
            self._make_one("1", 2)
        assert str(exc.value) == "end_key must be a string or bytes"

    def test__to_dict_defaults(self):
        row_range = self._make_one("test_row", "test_row2")
        expected = {
            "start_key_closed": b"test_row",
            "end_key_open": b"test_row2",
        }
        assert row_range._to_dict() == expected

    def test__to_dict_inclusive_flags(self):
        row_range = self._make_one("test_row", "test_row2", False, True)
        expected = {
            "start_key_open": b"test_row",
            "end_key_closed": b"test_row2",
        }
        assert row_range._to_dict() == expected

    @pytest.mark.parametrize(
        "input_dict,expected_start,expected_end,start_is_inclusive,end_is_inclusive",
        [
            (
                {"start_key_closed": "test_row", "end_key_open": "test_row2"},
                b"test_row",
                b"test_row2",
                True,
                False,
            ),
            (
                {"start_key_closed": b"test_row", "end_key_open": b"test_row2"},
                b"test_row",
                b"test_row2",
                True,
                False,
            ),
            (
                {"start_key_open": "test_row", "end_key_closed": "test_row2"},
                b"test_row",
                b"test_row2",
                False,
                True,
            ),
            ({"start_key_open": b"a"}, b"a", None, False, None),
            ({"end_key_closed": b"b"}, None, b"b", None, True),
            ({"start_key_closed": "a"}, b"a", None, True, None),
            ({"end_key_open": b"b"}, None, b"b", None, False),
            ({}, None, None, None, None),
        ],
    )
    def test__from_dict(
        self,
        input_dict,
        expected_start,
        expected_end,
        start_is_inclusive,
        end_is_inclusive,
    ):
        from google.cloud.bigtable.read_rows_query import RowRange

        row_range = RowRange._from_dict(input_dict)
        assert row_range._to_dict().keys() == input_dict.keys()
        found_start = row_range.start
        found_end = row_range.end
        if expected_start is None:
            assert found_start is None
            assert start_is_inclusive is None
        else:
            assert found_start.key == expected_start
            assert found_start.is_inclusive == start_is_inclusive
        if expected_end is None:
            assert found_end is None
            assert end_is_inclusive is None
        else:
            assert found_end.key == expected_end
            assert found_end.is_inclusive == end_is_inclusive

    @pytest.mark.parametrize(
        "dict_repr",
        [
            {"start_key_closed": "test_row", "end_key_open": "test_row2"},
            {"start_key_closed": b"test_row", "end_key_open": b"test_row2"},
            {"start_key_open": "test_row", "end_key_closed": "test_row2"},
            {"start_key_open": b"a"},
            {"end_key_closed": b"b"},
            {"start_key_closed": "a"},
            {"end_key_open": b"b"},
            {},
        ],
    )
    def test__from_points(self, dict_repr):
        from google.cloud.bigtable.read_rows_query import RowRange

        row_range_from_dict = RowRange._from_dict(dict_repr)
        row_range_from_points = RowRange._from_points(
            row_range_from_dict.start, row_range_from_dict.end
        )
        assert row_range_from_points._to_dict() == row_range_from_dict._to_dict()

    @pytest.mark.parametrize(
        "first_dict,second_dict,should_match",
        [
            (
                {"start_key_closed": "a", "end_key_open": "b"},
                {"start_key_closed": "a", "end_key_open": "b"},
                True,
            ),
            (
                {"start_key_closed": "a", "end_key_open": "b"},
                {"start_key_closed": "a", "end_key_open": "c"},
                False,
            ),
            (
                {"start_key_closed": "a", "end_key_open": "b"},
                {"start_key_closed": "a", "end_key_closed": "b"},
                False,
            ),
            (
                {"start_key_closed": b"a", "end_key_open": b"b"},
                {"start_key_closed": "a", "end_key_open": "b"},
                True,
            ),
            ({}, {}, True),
            ({"start_key_closed": "a"}, {}, False),
            ({"start_key_closed": "a"}, {"start_key_closed": "a"}, True),
            ({"start_key_closed": "a"}, {"start_key_open": "a"}, False),
        ],
    )
    def test___hash__(self, first_dict, second_dict, should_match):
        from google.cloud.bigtable.read_rows_query import RowRange

        row_range1 = RowRange._from_dict(first_dict)
        row_range2 = RowRange._from_dict(second_dict)
        assert (hash(row_range1) == hash(row_range2)) == should_match


class TestReadRowsQuery:
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery

        return ReadRowsQuery

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor_defaults(self):
        query = self._make_one()
        assert query.row_keys == set()
        assert query.row_ranges == set()
        assert query.filter is None
        assert query.limit is None

    def test_ctor_explicit(self):
        from google.cloud.bigtable.row_filters import RowFilterChain
        from google.cloud.bigtable.read_rows_query import RowRange

        filter_ = RowFilterChain()
        query = self._make_one(
            ["row_key_1", "row_key_2"],
            row_ranges=[RowRange("row_key_3", "row_key_4")],
            limit=10,
            row_filter=filter_,
        )
        assert len(query.row_keys) == 2
        assert "row_key_1".encode() in query.row_keys
        assert "row_key_2".encode() in query.row_keys
        assert len(query.row_ranges) == 1
        assert RowRange("row_key_3", "row_key_4") in query.row_ranges
        assert query.filter == filter_
        assert query.limit == 10

    def test_ctor_invalid_limit(self):
        with pytest.raises(ValueError) as exc:
            self._make_one(limit=-1)
        assert str(exc.value) == "limit must be >= 0"

    def test_set_filter(self):
        from google.cloud.bigtable.row_filters import RowFilterChain

        filter1 = RowFilterChain()
        query = self._make_one()
        assert query.filter is None
        query.filter = filter1
        assert query.filter == filter1
        filter2 = RowFilterChain()
        query.filter = filter2
        assert query.filter == filter2
        query.filter = None
        assert query.filter is None
        query.filter = RowFilterChain()
        assert query.filter == RowFilterChain()
        with pytest.raises(ValueError) as exc:
            query.filter = 1
        assert str(exc.value) == "row_filter must be a RowFilter or dict"

    def test_set_filter_dict(self):
        from google.cloud.bigtable.row_filters import RowSampleFilter
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsRequest

        filter1 = RowSampleFilter(0.5)
        filter1_dict = filter1.to_dict()
        query = self._make_one()
        assert query.filter is None
        query.filter = filter1_dict
        assert query.filter == filter1_dict
        output = query._to_dict()
        assert output["filter"] == filter1_dict
        proto_output = ReadRowsRequest(**output)
        assert proto_output.filter == filter1._to_pb()

        query.filter = None
        assert query.filter is None

    def test_set_limit(self):
        query = self._make_one()
        assert query.limit is None
        query.limit = 10
        assert query.limit == 10
        query.limit = 9
        assert query.limit == 9
        query.limit = 0
        assert query.limit == 0
        with pytest.raises(ValueError) as exc:
            query.limit = -1
        assert str(exc.value) == "limit must be >= 0"
        with pytest.raises(ValueError) as exc:
            query.limit = -100
        assert str(exc.value) == "limit must be >= 0"

    def test_add_key_str(self):
        query = self._make_one()
        assert query.row_keys == set()
        input_str = "test_row"
        query.add_key(input_str)
        assert len(query.row_keys) == 1
        assert input_str.encode() in query.row_keys
        input_str2 = "test_row2"
        query.add_key(input_str2)
        assert len(query.row_keys) == 2
        assert input_str.encode() in query.row_keys
        assert input_str2.encode() in query.row_keys

    def test_add_key_bytes(self):
        query = self._make_one()
        assert query.row_keys == set()
        input_bytes = b"test_row"
        query.add_key(input_bytes)
        assert len(query.row_keys) == 1
        assert input_bytes in query.row_keys
        input_bytes2 = b"test_row2"
        query.add_key(input_bytes2)
        assert len(query.row_keys) == 2
        assert input_bytes in query.row_keys
        assert input_bytes2 in query.row_keys

    def test_add_rows_batch(self):
        query = self._make_one()
        assert query.row_keys == set()
        input_batch = ["test_row", b"test_row2", "test_row3"]
        for k in input_batch:
            query.add_key(k)
        assert len(query.row_keys) == 3
        assert b"test_row" in query.row_keys
        assert b"test_row2" in query.row_keys
        assert b"test_row3" in query.row_keys
        # test adding another batch
        for k in ["test_row4", b"test_row5"]:
            query.add_key(k)
        assert len(query.row_keys) == 5
        assert input_batch[0].encode() in query.row_keys
        assert input_batch[1] in query.row_keys
        assert input_batch[2].encode() in query.row_keys
        assert b"test_row4" in query.row_keys
        assert b"test_row5" in query.row_keys

    def test_add_key_invalid(self):
        query = self._make_one()
        with pytest.raises(ValueError) as exc:
            query.add_key(1)
        assert str(exc.value) == "row_key must be string or bytes"
        with pytest.raises(ValueError) as exc:
            query.add_key(["s"])
        assert str(exc.value) == "row_key must be string or bytes"

    def test_duplicate_rows(self):
        # should only hold one of each input key
        key_1 = b"test_row"
        key_2 = b"test_row2"
        query = self._make_one(row_keys=[key_1, key_1, key_2])
        assert len(query.row_keys) == 2
        assert key_1 in query.row_keys
        assert key_2 in query.row_keys
        key_3 = "test_row3"
        for i in range(10):
            query.add_key(key_3)
        assert len(query.row_keys) == 3

    def test_add_range(self):
        from google.cloud.bigtable.read_rows_query import RowRange

        query = self._make_one()
        assert query.row_ranges == set()
        input_range = RowRange(start_key=b"test_row")
        query.add_range(input_range)
        assert len(query.row_ranges) == 1
        assert input_range in query.row_ranges
        input_range2 = RowRange(start_key=b"test_row2")
        query.add_range(input_range2)
        assert len(query.row_ranges) == 2
        assert input_range in query.row_ranges
        assert input_range2 in query.row_ranges
        query.add_range(input_range2)
        assert len(query.row_ranges) == 2

    def test_add_range_dict(self):
        from google.cloud.bigtable.read_rows_query import RowRange

        query = self._make_one()
        assert query.row_ranges == set()
        input_range = {"start_key_closed": b"test_row"}
        query.add_range(input_range)
        assert len(query.row_ranges) == 1
        range_obj = RowRange._from_dict(input_range)
        assert range_obj in query.row_ranges

    def test_to_dict_rows_default(self):
        # dictionary should be in rowset proto format
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsRequest

        query = self._make_one()
        output = query._to_dict()
        assert isinstance(output, dict)
        assert len(output.keys()) == 1
        expected = {"rows": {"row_keys": [], "row_ranges": []}}
        assert output == expected

        request_proto = ReadRowsRequest(**output)
        assert request_proto.rows.row_keys == []
        assert request_proto.rows.row_ranges == []
        assert not request_proto.filter
        assert request_proto.rows_limit == 0

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
        query.add_range({})
        query.add_key("test_row")
        query.add_key(b"test_row2")
        query.add_key("test_row3")
        query.add_key(b"test_row3")
        query.add_key(b"test_row4")
        output = query._to_dict()
        assert isinstance(output, dict)
        request_proto = ReadRowsRequest(**output)
        rowset_proto = request_proto.rows
        # check rows
        assert len(rowset_proto.row_keys) == 4
        assert rowset_proto.row_keys[0] == b"test_row"
        assert rowset_proto.row_keys[1] == b"test_row2"
        assert rowset_proto.row_keys[2] == b"test_row3"
        assert rowset_proto.row_keys[3] == b"test_row4"
        # check ranges
        assert len(rowset_proto.row_ranges) == 5
        assert {
            "start_key_closed": b"test_row",
            "end_key_open": b"test_row2",
        } in output["rows"]["row_ranges"]
        assert {"start_key_closed": b"test_row3"} in output["rows"]["row_ranges"]
        assert {"end_key_open": b"test_row5"} in output["rows"]["row_ranges"]
        assert {
            "start_key_open": b"test_row6",
            "end_key_closed": b"test_row7",
        } in output["rows"]["row_ranges"]
        assert {} in output["rows"]["row_ranges"]
        # check limit
        assert request_proto.rows_limit == 100
        # check filter
        filter_proto = request_proto.filter
        assert filter_proto == row_filter._to_pb()

    def _parse_query_string(self, query_string):
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
        "query_string,shard_points",
        [
            ("a,[p-q)", []),
            ("0_key,[1_range_start-2_range_end)", ["3_split"]),
            ("-1_range_end)", ["5_split"]),
            ("0_key,[1_range_start-2_range_end)", ["2_range_end"]),
            ("9_row_key,(5_range_start-7_range_end)", ["3_split"]),
            ("(5_range_start-", ["3_split"]),
            ("3_split,[3_split-5_split)", ["3_split", "5_split"]),
            ("[3_split-", ["3_split"]),
        ],
    )
    def test_shard_no_split(self, query_string, shard_points):
        # these queries should not be sharded
        initial_query = self._parse_query_string(query_string)
        row_samples = [(point.encode(), None) for point in shard_points]
        sharded_queries = initial_query.shard(row_samples)
        assert len(sharded_queries) == 1
        assert initial_query == sharded_queries[0]

    def test_shard_full_table_scan(self):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery, RowRange

        full_scan_query = ReadRowsQuery(row_ranges=[RowRange()])
        split_points = [(b"a", None)]
        sharded_queries = full_scan_query.shard(split_points)
        assert len(sharded_queries) == 2
        assert sharded_queries[0] == self._parse_query_string("-a)")
        assert sharded_queries[1] == self._parse_query_string("[a-")

    def test_shard_multiple_keys(self):
        initial_query = self._parse_query_string("1_beforeSplit,2_onSplit,3_afterSplit")
        split_points = [(b"2_onSplit", None)]
        sharded_queries = initial_query.shard(split_points)
        assert len(sharded_queries) == 2
        assert sharded_queries[0] == self._parse_query_string("1_beforeSplit")
        assert sharded_queries[1] == self._parse_query_string("2_onSplit,3_afterSplit")

    def test_shard_keys_empty_left(self):
        initial_query = self._parse_query_string("5_test,8_test")
        split_points = [(b"0_split", None), (b"6_split", None)]
        sharded_queries = initial_query.shard(split_points)
        assert len(sharded_queries) == 2
        assert sharded_queries[0] == self._parse_query_string("5_test")
        assert sharded_queries[1] == self._parse_query_string("8_test")

    def test_shard_keys_empty_right(self):
        initial_query = self._parse_query_string("0_test,2_test")
        split_points = [(b"1_split", None), (b"5_split", None)]
        sharded_queries = initial_query.shard(split_points)
        assert len(sharded_queries) == 2
        assert sharded_queries[0] == self._parse_query_string("0_test")
        assert sharded_queries[1] == self._parse_query_string("2_test")

    def test_shard_mixed_split(self):
        initial_query = self._parse_query_string("0,a,c,-a],-b],(c-e],(d-f],(m-")
        split_points = [(s.encode(), None) for s in ["a", "d", "j", "o"]]
        sharded_queries = initial_query.shard(split_points)
        assert len(sharded_queries) == 5
        assert sharded_queries[0] == self._parse_query_string("0,-a)")
        assert sharded_queries[1] == self._parse_query_string("a,c,[a-a],[a-b],(c-d)")
        assert sharded_queries[2] == self._parse_query_string("[d-e],(d-f]")
        assert sharded_queries[3] == self._parse_query_string("(m-o)")
        assert sharded_queries[4] == self._parse_query_string("[o-")

    def test_shard_unsorted_request(self):
        initial_query = self._parse_query_string(
            "7_row_key_1,2_row_key_2,[8_range_1_start-9_range_1_end),[3_range_2_start-4_range_2_end)"
        )
        split_points = [(b"5-split", None)]
        sharded_queries = initial_query.shard(split_points)
        assert len(sharded_queries) == 2
        assert sharded_queries[0] == self._parse_query_string(
            "2_row_key_2,[3_range_2_start-4_range_2_end)"
        )
        assert sharded_queries[1] == self._parse_query_string(
            "7_row_key_1,[8_range_1_start-9_range_1_end)"
        )

    @pytest.mark.parametrize(
        "first_args,second_args,expected",
        [
            ((), (), True),
            ((), ("a",), False),
            (("a",), (), False),
            (("a",), ("a",), True),
            (("a",), (b"a",), True),
            (("a",), ("b",), False),
            (("a",), ("a", ["b"]), False),
            (("a", ["b"]), ("a", ["b"]), True),
            (("a", ["b"]), ("a", ["b", "c"]), False),
            (("a", ["b", "c"]), ("a", [b"b", "c"]), True),
            (("a", ["b", "c"], 1), ("a", ["b", b"c"], 1), True),
            (("a", ["b"], 1), ("a", ["b"], 2), False),
            (("a", ["b"], 1, {"a": "b"}), ("a", ["b"], 1, {"a": "b"}), True),
            (("a", ["b"], 1, {"a": "b"}), ("a", ["b"], 1), False),
        ],
    )
    def test___eq__(self, first_args, second_args, expected):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        from google.cloud.bigtable.read_rows_query import RowRange

        # replace row_range placeholders with a RowRange object
        if len(first_args) > 1:
            first_args = list(first_args)
            first_args[1] = [RowRange(c) for c in first_args[1]]
        if len(second_args) > 1:
            second_args = list(second_args)
            second_args[1] = [RowRange(c) for c in second_args[1]]
        first = ReadRowsQuery(*first_args)
        second = ReadRowsQuery(*second_args)
        assert (first == second) == expected

    def test___repr__(self):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery

        instance = self._make_one(row_keys=["a", "b"], row_filter={}, limit=10)
        # should be able to recreate the instance from the repr
        repr_str = repr(instance)
        recreated = eval(repr_str)
        assert isinstance(recreated, ReadRowsQuery)
        assert recreated == instance
