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
from .row_response import row_key
from dataclasses import dataclass
from google.cloud.bigtable.row_filters import RowFilter

if TYPE_CHECKING:
    from google.cloud.bigtable import RowKeySamples


@dataclass
class _RangePoint:
    # model class for a point in a row range
    key: row_key
    is_inclusive: bool


class ReadRowsQuery:
    """
    Class to encapsulate details of a read row request
    """

    def __init__(
        self,
        row_keys: list[str | bytes] | str | bytes | None = None,
        limit: int | None = None,
        row_filter: RowFilter | dict[str, Any] | None = None,
    ):
        """
        Create a new ReadRowsQuery

        Args:
          - row_keys: a list of row keys to include in the query
          - limit: the maximum number of rows to return. None or 0 means no limit
                default: None (no limit)
          - row_filter: a RowFilter to apply to the query
        """
        self.row_keys: set[bytes] = set()
        self.row_ranges: list[tuple[_RangePoint | None, _RangePoint | None]] = []
        if row_keys:
            self.add_rows(row_keys)
        self.limit: int | None = limit
        self.filter: RowFilter | dict[str, Any] = row_filter

    def set_limit(self, new_limit: int | None):
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
        return self

    def set_filter(
        self, row_filter: RowFilter | dict[str, Any] | None
    ) -> ReadRowsQuery:
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
            raise ValueError(
                "row_filter must be a RowFilter or corresponding dict representation"
            )
        self._filter = row_filter
        return self

    def add_rows(self, row_keys: list[str | bytes] | str | bytes) -> ReadRowsQuery:
        """
        Add a list of row keys to this query

        Args:
          - row_keys: a list of row keys to add to this query
        Returns:
          - a reference to this query for chaining
        Raises:
          - ValueError if an input is not a string or bytes
        """
        if not isinstance(row_keys, list):
            row_keys = [row_keys]
        update_set = set()
        for k in row_keys:
            if isinstance(k, str):
                k = k.encode()
            elif not isinstance(k, bytes):
                raise ValueError("row_keys must be strings or bytes")
            update_set.add(k)
        self.row_keys.update(update_set)
        return self

    def add_range(
        self,
        start_key: str | bytes | None = None,
        end_key: str | bytes | None = None,
        start_is_inclusive: bool | None = None,
        end_is_inclusive: bool | None = None,
    ) -> ReadRowsQuery:
        """
        Add a range of row keys to this query.

        Args:
          - start_key: the start of the range
              if None, start_key is interpreted as the empty string, inclusive
          - end_key: the end of the range
              if None, end_key is interpreted as the infinite row key, exclusive
          - start_is_inclusive: if True, the start key is included in the range
              defaults to True if None. Must not be included if start_key is None
          - end_is_inclusive: if True, the end key is included in the range
              defaults to False if None. Must not be included if end_key is None
        """
        # check for invalid combinations of arguments
        if start_is_inclusive is None:
            start_is_inclusive = True
        elif start_key is None:
            raise ValueError(
                "start_is_inclusive must not be included if start_key is None"
            )
        if end_is_inclusive is None:
            end_is_inclusive = False
        elif end_key is None:
            raise ValueError("end_is_inclusive must not be included if end_key is None")
        # ensure that start_key and end_key are bytes
        if isinstance(start_key, str):
            start_key = start_key.encode()
        elif start_key is not None and not isinstance(start_key, bytes):
            raise ValueError("start_key must be a string or bytes")
        if isinstance(end_key, str):
            end_key = end_key.encode()
        elif end_key is not None and not isinstance(end_key, bytes):
            raise ValueError("end_key must be a string or bytes")

        start_pt = (
            _RangePoint(start_key, start_is_inclusive)
            if start_key is not None
            else None
        )
        end_pt = _RangePoint(end_key, end_is_inclusive) if end_key is not None else None
        self.row_ranges.append((start_pt, end_pt))
        return self

    def shard(self, shard_keys: "RowKeySamples" | None = None) -> list[ReadRowsQuery]:
        """
        Split this query into multiple queries that can be evenly distributed
        across nodes and be run in parallel

        Returns:
            - a list of queries that represent a sharded version of the original
              query (if possible)
        """
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        """
        Convert this query into a dictionary that can be used to construct a
        ReadRowsRequest protobuf
        """
        ranges = []
        for start, end in self.row_ranges:
            new_range = {}
            if start is not None:
                key = "start_key_closed" if start.is_inclusive else "start_key_open"
                new_range[key] = start.key
            if end is not None:
                key = "end_key_closed" if end.is_inclusive else "end_key_open"
                new_range[key] = end.key
            ranges.append(new_range)
        row_keys = list(self.row_keys)
        row_keys.sort()
        row_set = {"row_keys": row_keys, "row_ranges": ranges}
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

    # Support limit and filter as properties

    @property
    def limit(self) -> int | None:
        """
        Getter implementation for limit property
        """
        return self._limit

    @limit.setter
    def limit(self, new_limit: int | None):
        """
        Setter implementation for limit property
        """
        self.set_limit(new_limit)

    @property
    def filter(self):
        """
        Getter implemntation for filter property
        """
        return self._filter

    @filter.setter
    def filter(self, row_filter: RowFilter | dict[str, Any] | None):
        """
        Setter implementation for filter property
        """
        self.set_filter(row_filter)
