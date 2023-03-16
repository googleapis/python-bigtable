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
from typing import Sequence, Generator, Mapping, overload, Union, List, Tuple, Any
from functools import total_ordering

# Type aliases used internally for readability.
row_key = bytes
family_id = str
qualifier = bytes
row_value = bytes


class RowResponse(
    Sequence["CellResponse"],
    Mapping[
        Union[family_id, Tuple[family_id, Union[qualifier, str]]], List["CellResponse"]
    ],
):
    """
    Model class for row data returned from server

    Does not represent all data contained in the row, only data returned by a
    query.
    Expected to be read-only to users, and written by backend

    Can be indexed:
    cells = row["family", "qualifier"]
    """

    def __init__(
        self,
        key: row_key,
        cells: list[CellResponse]
        | dict[tuple[family_id, qualifier], list[dict[str, Any]]],
    ):
        """Expected to be used internally only"""
        self.row_key = key
        self._cells_map: dict[
            family_id, dict[qualifier, list[CellResponse]]
        ] = OrderedDict()
        self._cells_list: list[CellResponse] = []
        if isinstance(cells, dict):
            # handle dict input
            tmp_list = []
            for (family, qualifier), cell_list in cells.items():
                for cell_dict in cell_list:
                    cell_obj = CellResponse._from_dict(
                        key, family, qualifier, cell_dict
                    )
                    tmp_list.append(cell_obj)
            cells = tmp_list
        # add cells to internal stores using Bigtable native ordering
        for cell in sorted(cells):
            if cell.row_key != self.row_key:
                raise ValueError(
                    f"CellResponse row_key ({cell.row_key!r}) does not match RowResponse key ({self.row_key!r})"
                )
            if cell.family not in self._cells_map:
                self._cells_map[cell.family] = OrderedDict()
            if cell.column_qualifier not in self._cells_map[cell.family]:
                self._cells_map[cell.family][cell.column_qualifier] = []
            self._cells_map[cell.family][cell.column_qualifier].append(cell)
            self._cells_list.append(cell)

    def get_cells(
        self, family: str | None = None, qualifier: str | bytes | None = None
    ) -> list[CellResponse]:
        """
        Returns cells sorted in Bigtable native order:
            - Family lexicographically ascending
            - Qualifier ascending
            - Timestamp in reverse chronological order

        If family or qualifier not passed, will include all

        Can also be accessed through indexing:
          cells = row["family", "qualifier"]
          cells = row["family"]
        """
        if family is None:
            if qualifier is not None:
                # get_cells(None, "qualifier") is not allowed
                raise ValueError("Qualifier passed without family")
            else:
                # return all cells on get_cells()
                return self._cells_list
        if qualifier is None:
            # return all cells in family on get_cells(family)
            return list(self._get_all_from_family(family))
        if isinstance(qualifier, str):
            qualifier = qualifier.encode("utf-8")
        # return cells in family and qualifier on get_cells(family, qualifier)
        if family not in self._cells_map:
            raise ValueError(f"Family '{family}' not found in row '{self.row_key!r}'")
        if qualifier not in self._cells_map[family]:
            raise ValueError(
                f"Qualifier '{qualifier!r}' not found in family '{family}' in row '{self.row_key!r}'"
            )
        return self._cells_map[family][qualifier]

    def _get_all_from_family(
        self, family: family_id
    ) -> Generator[CellResponse, None, None]:
        """
        Returns all cells in the row
        """
        if family not in self._cells_map:
            raise ValueError(f"Family '{family}' not found in row '{self.row_key!r}'")
        qualifier_dict = self._cells_map.get(family, {})
        for cell_batch in qualifier_dict.values():
            for cell in cell_batch:
                yield cell

    def __str__(self) -> str:
        """
        Human-readable string representation

        {
          (family='fam', qualifier=b'col'): [b'value', (+1 more),],
          (family='fam', qualifier=b'col2'): [b'other'],
        }
        """
        output = ["{"]
        for key in self.keys():
            if len(self[key]) == 0:
                output.append(f"  {key}: []")
            elif len(self[key]) == 1:
                output.append(
                    f"  (family='{key[0]}', qualifier={key[1]}): [{self[key][0]}],"
                )
            else:
                output.append(
                    f"  (family='{key[0]}', qualifier={key[1]}): [{self[key][0]}, (+{len(self[key])-1} more)],"
                )
        output.append("}")
        return "\n".join(output)

    def __repr__(self):
        cell_str_buffer = ["{"]
        for key, cell_list in self.items():
            repr_list = [cell.to_dict(use_nanoseconds=True) for cell in cell_list]
            cell_str_buffer.append(f"  ('{key[0]}', {key[1]}): {repr_list},")
        cell_str_buffer.append("}")
        cell_str = "\n".join(cell_str_buffer)
        output = f"RowResponse(key={self.row_key!r}, cells={cell_str})"
        return output

    def to_dict(self) -> dict[str, Any]:
        """
        Returns a dictionary representation of the cell in the Bigtable Row
        proto format

        https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2#row
        """
        families_list: list[dict[str, Any]] = []
        for family in self._cells_map:
            column_list: list[dict[str, Any]] = []
            for qualifier in self._cells_map[family]:
                cells_list: list[dict[str, Any]] = []
                for cell in self._cells_map[family][qualifier]:
                    cells_list.append(cell.to_dict())
                column_list.append({"qualifier": qualifier, "cells": cells_list})
            families_list.append({"name": family, "columns": column_list})
        return {"key": self.row_key, "families": families_list}

    # Sequence and Mapping methods
    def __iter__(self):
        # iterate as a sequence; yield all cells
        for cell in self._cells_list:
            yield cell

    def __contains__(self, item):
        if isinstance(item, family_id):
            # check if family key is in RowResponse
            return item in self._cells_map
        elif (
            isinstance(item, tuple)
            and isinstance(item[0], family_id)
            and isinstance(item[1], (qualifier, str))
        ):
            # check if (family, qualifier) pair is in RowResponse
            qualifer = item[1] if isinstance(item[1], bytes) else item[1].encode()
            return item[0] in self._cells_map and qualifer in self._cells_map[item[0]]
        # check if CellResponse is in RowResponse
        return item in self._cells_list

    @overload
    def __getitem__(
        self,
        index: family_id | tuple[family_id, qualifier | str],
    ) -> List[CellResponse]:
        # overload signature for type checking
        pass

    @overload
    def __getitem__(self, index: int, /) -> CellResponse:
        # overload signature for type checking
        pass

    @overload
    def __getitem__(self, index: slice) -> list[CellResponse]:
        # overload signature for type checking
        pass

    def __getitem__(self, index):
        if isinstance(index, family_id):
            return self.get_cells(family=index)
        elif (
            isinstance(index, tuple)
            and isinstance(index[0], family_id)
            and isinstance(index[1], (qualifier, str))
        ):
            return self.get_cells(family=index[0], qualifier=index[1])
        elif isinstance(index, int) or isinstance(index, slice):
            # index is int or slice
            return self._cells_list[index]
        else:
            raise TypeError(
                "Index must be family_id, (family_id, qualifier), int, or slice"
            )

    def __len__(self):
        return len(self._cells_list)

    def keys(self):
        key_list = []
        for family in self._cells_map:
            for qualifier in self._cells_map[family]:
                key_list.append((family, qualifier))
        return key_list

    def values(self):
        return self._cells_list

    def items(self):
        for key in self.keys():
            yield key, self[key]

    def __eq__(self, other):
        # for performance reasons, check row metadata
        # before checking individual cells
        if not isinstance(other, RowResponse):
            return False
        if self.row_key != other.row_key:
            return False
        if len(self._cells_list) != len(other._cells_list):
            return False
        keys, other_keys = self.keys(), other.keys()
        if keys != other_keys:
            return False
        for key in keys:
            if len(self[key]) != len(other[key]):
                return False
        # compare individual cell lists
        if self._cells_list != other._cells_list:
            return False
        return True


@total_ordering
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
        column_qualifier: qualifier | str,
        timestamp_ns: int,
        labels: list[str] | None = None,
    ):
        """
        CellResponse constructor

        CellResponse objects are not intended to be constructed by users.
        They are returned by the Bigtable backend.
        """
        self.value = value
        self.row_key = row
        self.family = family
        if isinstance(column_qualifier, str):
            column_qualifier = column_qualifier.encode()
        self.column_qualifier = column_qualifier
        self.timestamp_ns = timestamp_ns
        self.labels = labels if labels is not None else []

    @staticmethod
    def _from_dict(
        row_key: bytes, family: str, qualifier: bytes, cell_dict: dict[str, Any]
    ) -> CellResponse:
        """
        Helper function to create CellResponse from a dictionary

        CellResponse objects are not intended to be constructed by users.
        They are returned by the Bigtable backend.
        """
        # Bigtable backend will use microseconds for timestamps,
        # but the Python library prefers nanoseconds where possible
        timestamp = cell_dict.get(
            "timestamp_ns", cell_dict.get("timestamp_micros", -1) * 1000
        )
        if timestamp < 0:
            raise ValueError("invalid timestamp")
        cell_obj = CellResponse(
            cell_dict["value"],
            row_key,
            family,
            qualifier,
            timestamp,
            cell_dict.get("labels", None),
        )
        return cell_obj

    def __int__(self) -> int:
        """
        Allows casting cell to int
        Interprets value as a 64-bit big-endian signed integer, as expected by
        ReadModifyWrite increment rule
        """
        return int.from_bytes(self.value, byteorder="big", signed=True)

    def to_dict(self, use_nanoseconds=False) -> dict[str, Any]:
        """
        Returns a dictionary representation of the cell in the Bigtable Cell
        proto format

        https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2#cell
        """
        cell_dict: dict[str, Any] = {
            "value": self.value,
        }
        if use_nanoseconds:
            cell_dict["timestamp_ns"] = self.timestamp_ns
        else:
            cell_dict["timestamp_micros"] = self.timestamp_ns // 1000
        if self.labels:
            cell_dict["labels"] = self.labels
        return cell_dict

    def __str__(self) -> str:
        """
        Allows casting cell to str
        Prints encoded byte string, same as printing value directly.
        """
        return str(self.value)

    def __repr__(self):
        return f"CellResponse(value={self.value!r}, row={self.row_key!r}, family='{self.family}', column_qualifier={self.column_qualifier!r}, timestamp_ns={self.timestamp_ns}, labels={self.labels})"

    """For Bigtable native ordering"""

    def __lt__(self, other) -> bool:
        if not isinstance(other, CellResponse):
            return NotImplemented
        this_ordering = (
            self.family,
            self.column_qualifier,
            -self.timestamp_ns,
            self.value,
            self.labels,
        )
        other_ordering = (
            other.family,
            other.column_qualifier,
            -other.timestamp_ns,
            other.value,
            other.labels,
        )
        return this_ordering < other_ordering

    def __eq__(self, other) -> bool:
        if not isinstance(other, CellResponse):
            return NotImplemented
        return (
            self.row_key == other.row_key
            and self.family == other.family
            and self.column_qualifier == other.column_qualifier
            and self.value == other.value
            and self.timestamp_ns == other.timestamp_ns
            and len(self.labels) == len(other.labels)
            and all([label in other.labels for label in self.labels])
        )

    def __ne__(self, other) -> bool:
        return not self == other

    def __hash__(self):
        return hash(
            (
                self.row_key,
                self.family,
                self.column_qualifier,
                self.value,
                self.timestamp_ns,
                tuple(self.labels),
            )
        )
