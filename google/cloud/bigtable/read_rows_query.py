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
import bisect
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
        across nodes and be run in parallel

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

        split_points = [sample[0] for sample in shard_keys if sample[0]]
        sharded_queries: dict[int, ReadRowsQuery] = defaultdict(
            lambda: ReadRowsQuery(row_filter=self.filter)
        )

        # use binary search to find split point segments for each row key in original query
        for this_key in list(self.row_keys):
            index = bisect.bisect_right(split_points, this_key)
            sharded_queries[index].add_key(this_key)

        # use binary search to find start and end segments for each row range in original query
        # if range spans multiple segments, split it into multiple ranges
        for this_range in self.row_ranges:
            # start index always bisects right, since points define the left side of the range
            start_index = (
                bisect.bisect_right(split_points, this_range.start.key)
                if this_range.start is not None
                else 0
            )
            # end index can bisect left or right, depending on whether the range is inclusive
            if this_range.end is None:
                end_index = len(split_points)
            elif this_range.end.is_inclusive:
                end_index = bisect.bisect_right(split_points, this_range.end.key)
            else:
                end_index = bisect.bisect_left(split_points, this_range.end.key)
            # create new ranges for each segment
            if start_index == end_index:
                # range is contained in a single segment
                sharded_queries[start_index].add_range(this_range)
            else:
                # range spans multiple segments
                # create start and end ranges
                start_range = RowRange._from_points(
                    this_range.start, _RangePoint(split_points[start_index], False)
                )
                sharded_queries[start_index].add_range(start_range)
                end_range = RowRange._from_points(
                    _RangePoint(split_points[end_index - 1], True), this_range.end
                )
                sharded_queries[end_index].add_range(end_range)
                # put the middle of the range in all segments in between
                for i in range(start_index + 1, end_index):
                    mid_range = RowRange(
                        split_points[i], split_points[i + 1], True, False
                    )
                    sharded_queries[i].add_range(mid_range)
        # return a list of queries, sorted by segment index
        keys = list(sharded_queries.keys())
        keys.sort()
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
