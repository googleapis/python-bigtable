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
#
from __future__ import annotations
from typing import TYPE_CHECKING, Any
from bisect import bisect_left
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from google.cloud.bigtable.row_filters import RowFilter

if TYPE_CHECKING:
    from google.cloud.bigtable import RowKeySamples


@dataclass
class _RangePoint:
    """Model class for a point in a row range"""

    key: bytes
    is_inclusive: bool

    def __hash__(self) -> int:
        return hash((self.key, self.is_inclusive))


@dataclass
class RowRange:
    start: _RangePoint | None
    end: _RangePoint | None

    def __init__(
        self,
        start_key: str | bytes | None = None,
        end_key: str | bytes | None = None,
        start_is_inclusive: bool | None = None,
        end_is_inclusive: bool | None = None,
    ):
        # check for invalid combinations of arguments
        if start_is_inclusive is None:
            start_is_inclusive = True
        elif start_key is None:
            raise ValueError("start_is_inclusive must be set with start_key")
        if end_is_inclusive is None:
            end_is_inclusive = False
        elif end_key is None:
            raise ValueError("end_is_inclusive must be set with end_key")
        # ensure that start_key and end_key are bytes
        if isinstance(start_key, str):
            start_key = start_key.encode()
        elif start_key is not None and not isinstance(start_key, bytes):
            raise ValueError("start_key must be a string or bytes")
        if isinstance(end_key, str):
            end_key = end_key.encode()
        elif end_key is not None and not isinstance(end_key, bytes):
            raise ValueError("end_key must be a string or bytes")

        self.start = (
            _RangePoint(start_key, start_is_inclusive)
            if start_key is not None
            else None
        )
        self.end = (
            _RangePoint(end_key, end_is_inclusive) if end_key is not None else None
        )

    def _to_dict(self) -> dict[str, bytes]:
        """Converts this object to a dictionary"""
        output = {}
        if self.start is not None:
            key = "start_key_closed" if self.start.is_inclusive else "start_key_open"
            output[key] = self.start.key
        if self.end is not None:
            key = "end_key_closed" if self.end.is_inclusive else "end_key_open"
            output[key] = self.end.key
        return output

    def __hash__(self) -> int:
        return hash((self.start, self.end))

    @classmethod
    def _from_dict(cls, data: dict[str, bytes]) -> RowRange:
        """Creates a RowRange from a dictionary"""
        start_key = data.get("start_key_closed", data.get("start_key_open"))
        end_key = data.get("end_key_closed", data.get("end_key_open"))
        start_is_inclusive = "start_key_closed" in data if start_key else None
        end_is_inclusive = "end_key_closed" in data if end_key else None
        return cls(
            start_key,
            end_key,
            start_is_inclusive,
            end_is_inclusive,
        )

    @classmethod
    def _from_points(
        cls, start: _RangePoint | None, end: _RangePoint | None
    ) -> RowRange:
        """Creates a RowRange from two RangePoints"""
        kwargs: dict[str, Any] = {}
        if start is not None:
            kwargs["start_key"] = start.key
            kwargs["start_is_inclusive"] = start.is_inclusive
        if end is not None:
            kwargs["end_key"] = end.key
            kwargs["end_is_inclusive"] = end.is_inclusive
        return cls(**kwargs)

    def __bool__(self) -> bool:
        """
        Empty RowRanges (representing a full table scan) are falsy, because
        they can be substituted with None. Non-empty RowRanges are truthy.
        """
        return self.start is not None or self.end is not None


class ReadRowsQuery:
    """
    Class to encapsulate details of a read row request
    """

    def __init__(
        self,
        row_keys: list[str | bytes] | str | bytes | None = None,
        row_ranges: list[RowRange] | RowRange | None = None,
        limit: int | None = None,
        row_filter: RowFilter | dict[str, Any] | None = None,
    ):
        """
        Create a new ReadRowsQuery

        Args:
          - row_keys: row keys to include in the query
                a query can contain multiple keys, but ranges should be preferred
          - row_ranges: ranges of rows to include in the query
          - limit: the maximum number of rows to return. None or 0 means no limit
                default: None (no limit)
          - row_filter: a RowFilter to apply to the query
        """
        self.row_keys: set[bytes] = set()
        self.row_ranges: set[RowRange] = set()
        if row_ranges is not None:
            if isinstance(row_ranges, RowRange):
                row_ranges = [row_ranges]
            for r in row_ranges:
                self.add_range(r)
        if row_keys is not None:
            if isinstance(row_keys, (str, bytes)):
                row_keys = [row_keys]
            for k in row_keys:
                self.add_key(k)
        self.limit: int | None = limit
        self.filter: RowFilter | dict[str, Any] | None = row_filter

    @property
    def limit(self) -> int | None:
        return self._limit

    @limit.setter
    def limit(self, new_limit: int | None):
        """
        Set the maximum number of rows to return by this query.

        None or 0 means no limit

        Args:
          - new_limit: the new limit to apply to this query
        Returns:
          - a reference to this query for chaining
        Raises:
          - ValueError if new_limit is < 0
        """
        if new_limit is not None and new_limit < 0:
            raise ValueError("limit must be >= 0")
        self._limit = new_limit

    @property
    def filter(self) -> RowFilter | dict[str, Any] | None:
        return self._filter

    @filter.setter
    def filter(self, row_filter: RowFilter | dict[str, Any] | None):
        """
        Set a RowFilter to apply to this query

        Args:
          - row_filter: a RowFilter to apply to this query
              Can be a RowFilter object or a dict representation
        Returns:
          - a reference to this query for chaining
        """
        if not (
            isinstance(row_filter, dict)
            or isinstance(row_filter, RowFilter)
            or row_filter is None
        ):
            raise ValueError("row_filter must be a RowFilter or dict")
        self._filter = row_filter

    def add_key(self, row_key: str | bytes):
        """
        Add a row key to this query

        A query can contain multiple keys, but ranges should be preferred

        Args:
          - row_key: a key to add to this query
        Returns:
          - a reference to this query for chaining
        Raises:
          - ValueError if an input is not a string or bytes
        """
        if isinstance(row_key, str):
            row_key = row_key.encode()
        elif not isinstance(row_key, bytes):
            raise ValueError("row_key must be string or bytes")
        self.row_keys.add(row_key)

    def add_range(
        self,
        row_range: RowRange | dict[str, bytes],
    ):
        """
        Add a range of row keys to this query.

        Args:
          - row_range: a range of row keys to add to this query
              Can be a RowRange object or a dict representation in
              RowRange proto format
        """
        if not (isinstance(row_range, dict) or isinstance(row_range, RowRange)):
            raise ValueError("row_range must be a RowRange or dict")
        if isinstance(row_range, dict):
            row_range = RowRange._from_dict(row_range)
        self.row_ranges.add(row_range)

    def shard(self, shard_keys: RowKeySamples) -> list[ReadRowsQuery]:
        """
        Split this query into multiple queries that can be evenly distributed
        across nodes and run in parallel

        Returns:
            - a list of queries that represent a sharded version of the original
              query (if possible)
        Raises:
            - AttributeError if the query contains a limit
        """
        if self.limit is not None:
            raise AttributeError("Cannot shard query with a limit")
        if len(self.row_keys) == 0 and len(self.row_ranges) == 0:
            # empty query represents full scan
            # ensure that we have at least one key or range
            full_scan_query = ReadRowsQuery(
                row_ranges=RowRange(), row_filter=self.filter
            )
            return full_scan_query.shard(shard_keys)

        sharded_queries: dict[int, ReadRowsQuery] = defaultdict(
            lambda: ReadRowsQuery(row_filter=self.filter)
        )
        # the split_points divde our key space into segments
        # each split_point defines last key that belongs to a segment
        # our goal is to break up the query into subqueries that each operate in a single segment
        split_points = [sample[0] for sample in shard_keys if sample[0]]

        # 1. handle row_keys
        # use binary search to find the segment that each key belongs to
        for this_key in list(self.row_keys):
            # bisect_left: in case of exact match, pick left side (i.e. keys are inclusive ends)
            segment_index = bisect_left(split_points, this_key)
            sharded_queries[segment_index].add_key(this_key)

        # 2. handle row_ranges
        for this_range in self.row_ranges:
            # 2a. find the index of the segment the start key belongs to
            if this_range.start is None:
                # if range is open on the left, include first segment
                start_segment = 0
            else:
                # use binary search to find the segment the start key belongs to
                # bisect method determines how we break ties when the start key matches a split point
                # if inclusive, bisect_left to the left segment, otherwise bisect_right
                bisect = bisect_left if this_range.start.is_inclusive else bisect_right
                start_segment = bisect(split_points, this_range.start.key)

            # 2b. find the index of the segment the end key belongs to
            if this_range.end is None:
                # if range is open on the right, include final segment
                end_segment = len(split_points)
            else:
                # use binary search to find the segment the end key belongs to.
                # optimization: remove keys up to start_segment from searched list,
                # then add start_segment back to get the correct index
                cropped_pts = split_points[start_segment:]
                end_segment = (
                    bisect_left(cropped_pts, this_range.end.key) + start_segment
                )

            # 2c. create new range definitions for each segment this_range spans
            if start_segment == end_segment:
                # this_range is contained in a single segment.
                # Add this_range to that segment's query only
                sharded_queries[start_segment].add_range(this_range)
            else:
                # this_range spans multiple segments. Create a new range for each segment's query
                # 2c.1. add new range for first segment this_range spans
                # first range spans from start_key to the split_point representing the last key in the segment
                last_key_in_first_segment = split_points[start_segment]
                start_range = RowRange._from_points(
                    start=this_range.start,
                    end=_RangePoint(last_key_in_first_segment, is_inclusive=True),
                )
                sharded_queries[start_segment].add_range(start_range)
                # 2c.2. add new range for last segment this_range spans
                # we start the final range using the end key from of the previous segment, with is_inclusive=False
                previous_segment = end_segment - 1
                last_key_before_segment = split_points[previous_segment]
                end_range = RowRange._from_points(
                    start=_RangePoint(last_key_before_segment, is_inclusive=False),
                    end=this_range.end,
                )
                sharded_queries[end_segment].add_range(end_range)
                # 2c.3. add new spanning range to all segments other than the first and last
                for this_segment in range(start_segment + 1, end_segment):
                    prev_segment = this_segment - 1
                    prev_end_key = split_points[prev_segment]
                    this_end_key = split_points[prev_segment + 1]
                    new_range = RowRange(
                        start_key=prev_end_key,
                        start_is_inclusive=False,
                        end_key=this_end_key,
                        end_is_inclusive=True,
                    )
                    sharded_queries[this_segment].add_range(new_range)
        # 3. return list of queries in order of segment
        # pull populated segments out of sharded_queries dict
        keys = sorted(list(sharded_queries.keys()))
        # return list of queries
        return [sharded_queries[k] for k in keys]

    def _to_dict(self) -> dict[str, Any]:
        """
        Convert this query into a dictionary that can be used to construct a
        ReadRowsRequest protobuf
        """
        row_ranges = []
        for r in self.row_ranges:
            dict_range = r._to_dict() if isinstance(r, RowRange) else r
            row_ranges.append(dict_range)
        row_keys = list(self.row_keys)
        row_keys.sort()
        row_set = {"row_keys": row_keys, "row_ranges": row_ranges}
        final_dict: dict[str, Any] = {
            "rows": row_set,
        }
        dict_filter = (
            self.filter.to_dict() if isinstance(self.filter, RowFilter) else self.filter
        )
        if dict_filter:
            final_dict["filter"] = dict_filter
        if self.limit is not None:
            final_dict["rows_limit"] = self.limit
        return final_dict

    def __eq__(self, other):
        """
        RowRanges are equal if they have the same row keys, row ranges,
        filter and limit, or if they both represent a full scan with the
        same filter and limit
        """
        if not isinstance(other, ReadRowsQuery):
            return False
        # empty queries are equal
        if len(self.row_keys) == 0 and len(other.row_keys) == 0:
            this_range_empty = len(self.row_ranges) == 0 or all(
                [bool(r) is False for r in self.row_ranges]
            )
            other_range_empty = len(other.row_ranges) == 0 or all(
                [bool(r) is False for r in other.row_ranges]
            )
            if this_range_empty and other_range_empty:
                return self.filter == other.filter and self.limit == other.limit
        return (
            self.row_keys == other.row_keys
            and self.row_ranges == other.row_ranges
            and self.filter == other.filter
            and self.limit == other.limit
        )

    def __repr__(self):
        return f"ReadRowsQuery(row_keys={list(self.row_keys)}, row_ranges={list(self.row_ranges)}, row_filter={self.filter}, limit={self.limit})"
