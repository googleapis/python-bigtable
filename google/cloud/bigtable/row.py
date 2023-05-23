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
from typing import Sequence, Generator, overload, Any
from functools import total_ordering

from google.cloud.bigtable_v2.types import Row as RowPB

# Type aliases used internally for readability.
row_key = bytes
family_id = str
qualifier = bytes
row_value = bytes


class Row(Sequence["Cell"]):
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
        cells: list[Cell],
    ):
        """
        Initializes a Row object

        Row objects are not intended to be created by users.
        They are returned by the Bigtable backend.
        """
        self.row_key = key
        self._cells_map: dict[family_id, dict[qualifier, list[Cell]]] = OrderedDict()
        self._cells_list: list[Cell] = []
        # add cells to internal stores using Bigtable native ordering
        for cell in cells:
            if cell.family not in self._cells_map:
                self._cells_map[cell.family] = OrderedDict()
            if cell.column_qualifier not in self._cells_map[cell.family]:
                self._cells_map[cell.family][cell.column_qualifier] = []
            self._cells_map[cell.family][cell.column_qualifier].append(cell)
            self._cells_list.append(cell)

    @classmethod
    def _from_pb(cls, row_pb: RowPB) -> Row:
        """
        Creates a row from a protobuf representation

        Row objects are not intended to be created by users.
        They are returned by the Bigtable backend.
        """
        row_key: bytes = row_pb.key
        cell_list: list[Cell] = []
        for family in row_pb.families:
            for column in family.columns:
                for cell in column.cells:
                    new_cell = Cell(
                        value=cell.value,
                        row=row_key,
                        family=family.name,
                        column_qualifier=column.qualifier,
                        timestamp_micros=cell.timestamp_micros,
                        labels=list(cell.labels) if cell.labels else None,
                    )
                    cell_list.append(new_cell)
        return cls(row_key, cells=cell_list)

    def get_cells(
        self, family: str | None = None, qualifier: str | bytes | None = None
    ) -> list[Cell]:
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

    def _get_all_from_family(self, family: family_id) -> Generator[Cell, None, None]:
        """
        Returns all cells in the row for the family_id
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
        for family, qualifier in self.get_column_components():
            cell_list = self[family, qualifier]
            line = [f"  (family={family!r}, qualifier={qualifier!r}): "]
            if len(cell_list) == 0:
                line.append("[],")
            elif len(cell_list) == 1:
                line.append(f"[{cell_list[0]}],")
            else:
                line.append(f"[{cell_list[0]}, (+{len(cell_list)-1} more)],")
            output.append("".join(line))
        output.append("}")
        return "\n".join(output)

    def __repr__(self):
        cell_str_buffer = ["{"]
        for family, qualifier in self.get_column_components():
            cell_list = self[family, qualifier]
            repr_list = [cell.to_dict() for cell in cell_list]
            cell_str_buffer.append(f"  ('{family}', {qualifier}): {repr_list},")
        cell_str_buffer.append("}")
        cell_str = "\n".join(cell_str_buffer)
        output = f"Row(key={self.row_key!r}, cells={cell_str})"
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
        """
        Allow iterating over all cells in the row
        """
        # iterate as a sequence; yield all cells
        for cell in self._cells_list:
            yield cell

    def __contains__(self, item):
        """
        Implements `in` operator

        Works for both cells in the internal list, and `family` or
        `(family, qualifier)` pairs associated with the cells
        """
        if isinstance(item, family_id):
            # check if family key is in Row
            return item in self._cells_map
        elif (
            isinstance(item, tuple)
            and isinstance(item[0], family_id)
            and isinstance(item[1], (qualifier, str))
        ):
            # check if (family, qualifier) pair is in Row
            qualifer = item[1] if isinstance(item[1], bytes) else item[1].encode()
            return item[0] in self._cells_map and qualifer in self._cells_map[item[0]]
        # check if Cell is in Row
        return item in self._cells_list

    @overload
    def __getitem__(
        self,
        index: family_id | tuple[family_id, qualifier | str],
    ) -> list[Cell]:
        # overload signature for type checking
        pass

    @overload
    def __getitem__(self, index: int) -> Cell:
        # overload signature for type checking
        pass

    @overload
    def __getitem__(self, index: slice) -> list[Cell]:
        # overload signature for type checking
        pass

    def __getitem__(self, index):
        """
        Implements [] indexing

        Supports indexing by family, (family, qualifier) pair,
        numerical index, and index slicing
        """
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
        """
        Implements `len()` operator
        """
        return len(self._cells_list)

    def get_column_components(self):
        """
        Returns a list of (family, qualifier) pairs associated with the cells

        Pairs can be used for indexing
        """
        key_list = []
        for family in self._cells_map:
            for qualifier in self._cells_map[family]:
                key_list.append((family, qualifier))
        return key_list

    def __eq__(self, other):
        """
        Implements `==` operator
        """
        # for performance reasons, check row metadata
        # before checking individual cells
        if not isinstance(other, Row):
            return False
        if self.row_key != other.row_key:
            return False
        if len(self._cells_list) != len(other._cells_list):
            return False
        components = self.get_column_components()
        other_components = other.get_column_components()
        if len(components) != len(other_components):
            return False
        if components != other_components:
            return False
        for family, qualifier in components:
            if len(self[family, qualifier]) != len(other[family, qualifier]):
                return False
        # compare individual cell lists
        if self._cells_list != other._cells_list:
            return False
        return True

    def __ne__(self, other) -> bool:
        """
        Implements `!=` operator
        """
        return not self == other


@total_ordering
class Cell:
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
        timestamp_micros: int,
        labels: list[str] | None = None,
    ):
        """
        Cell constructor

        Cell objects are not intended to be constructed by users.
        They are returned by the Bigtable backend.
        """
        self.value = value
        self.row_key = row
        self.family = family
        if isinstance(column_qualifier, str):
            column_qualifier = column_qualifier.encode()
        self.column_qualifier = column_qualifier
        self.timestamp_micros = timestamp_micros
        self.labels = labels if labels is not None else []

    def __int__(self) -> int:
        """
        Allows casting cell to int
        Interprets value as a 64-bit big-endian signed integer, as expected by
        ReadModifyWrite increment rule
        """
        return int.from_bytes(self.value, byteorder="big", signed=True)

    def to_dict(self) -> dict[str, Any]:
        """
        Returns a dictionary representation of the cell in the Bigtable Cell
        proto format

        https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2#cell
        """
        cell_dict: dict[str, Any] = {
            "value": self.value,
        }
        cell_dict["timestamp_micros"] = self.timestamp_micros
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
        """
        Returns a string representation of the cell
        """
        return f"Cell(value={self.value!r}, row={self.row_key!r}, family='{self.family}', column_qualifier={self.column_qualifier!r}, timestamp_micros={self.timestamp_micros}, labels={self.labels})"

    """For Bigtable native ordering"""

    def __lt__(self, other) -> bool:
        """
        Implements `<` operator
        """
        if not isinstance(other, Cell):
            return NotImplemented
        this_ordering = (
            self.family,
            self.column_qualifier,
            -self.timestamp_micros,
            self.value,
            self.labels,
        )
        other_ordering = (
            other.family,
            other.column_qualifier,
            -other.timestamp_micros,
            other.value,
            other.labels,
        )
        return this_ordering < other_ordering

    def __eq__(self, other) -> bool:
        """
        Implements `==` operator
        """
        if not isinstance(other, Cell):
            return NotImplemented
        return (
            self.row_key == other.row_key
            and self.family == other.family
            and self.column_qualifier == other.column_qualifier
            and self.value == other.value
            and self.timestamp_micros == other.timestamp_micros
            and len(self.labels) == len(other.labels)
            and all([label in other.labels for label in self.labels])
        )

    def __ne__(self, other) -> bool:
        """
        Implements `!=` operator
        """
        return not self == other

    def __hash__(self):
        """
        Implements `hash()` function to fingerprint cell
        """
        return hash(
            (
                self.row_key,
                self.family,
                self.column_qualifier,
                self.value,
                self.timestamp_micros,
                tuple(self.labels),
            )
        )
