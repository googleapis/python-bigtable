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

from collections import OrderedDict
from collections import Sequence
from typing_extensions import TypeAlias

# Type aliases used internally for readability.
row_key: TypeAlias = bytes
family_id: TypeAlias = str
qualifier: TypeAlias = bytes
row_value: TypeAlias = bytes


class RowResponse(Sequence):
    """
    Model class for row data returned from server

    Does not represent all data contained in the row, only data returned by a
    query.
    Expected to be read-only to users, and written by backend

    Can be indexed:
    cells = row["family", "qualifier"]
    """

    def __init__(self, key: row_key, cells: list[CellResponse]):
        self.row_key = key
        self.cells: OrderedDict[
            family_id, OrderedDict[qualifier, list[CellResponse]]
        ] = OrderedDict()
        """Expected to be used internally only"""
        pass

    def get_cells(
        self, family: str | None, qualifer: str | bytes | None
    ) -> list[CellResponse]:
        """
        Returns cells sorted in Bigtable native order:
            - Family lexicographically ascending
            - Qualifier lexicographically ascending
            - Timestamp in reverse chronological order

        If family or qualifier not passed, will include all

        Syntactic sugar: cells = row["family", "qualifier"]
        """
        raise NotImplementedError

    def get_index(self) -> dict[family_id, list[qualifier]]:
        """
        Returns a list of family and qualifiers for the object
        """
        raise NotImplementedError

    def __str__(self):
        """
        Human-readable string representation

        (family, qualifier)   cells
        (ABC, XYZ)            [b"123", b"456" ...(+5)]
        (DEF, XYZ)            [b"123"]
        (GHI, XYZ)            [b"123", b"456" ...(+2)]
        """
        raise NotImplementedError


class CellResponse:
    """
    Model class for cell data

    Does not represent all data contained in the cell, only data returned by a
    query.
    Expected to be read-only to users, and written by backend
    """

    def __init__(
        self,
        value: row_value,
        row: row_key,
        family: family_id,
        column_qualifier: qualifier,
        labels: list[str] | None = None,
        timestamp: int | None = None,
    ):
        self.value = value
        self.row_key = row
        self.family = family
        self.column_qualifier = column_qualifier
        self.labels = labels
        self.timestamp = timestamp

    def decode_value(self, encoding="UTF-8", errors=None) -> str:
        """decode bytes to string"""
        return self.value.decode(encoding, errors)

    def __int__(self) -> int:
        """
        Allows casting cell to int
        Interprets value as a 64-bit big-endian signed integer, as expected by
        ReadModifyWrite increment rule
        """
        return int.from_bytes(self.value, byteorder="big", signed=True)

    def __str__(self) -> str:
        """
        Allows casting cell to str
        Prints encoded byte string, same as printing value directly.
        """
        return str(self.value)

    """For Bigtable native ordering"""

    def __lt__(self, other) -> bool:
        raise NotImplementedError

    def __eq__(self, other) -> bool:
        raise NotImplementedError
