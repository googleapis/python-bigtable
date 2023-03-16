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

import time

TEST_VALUE = b"1234"
TEST_ROW_KEY = b"row"
TEST_FAMILY_ID = "cf1"
TEST_QUALIFIER = b"col"
TEST_TIMESTAMP = time.time_ns()
TEST_LABELS = ["label1", "label2"]


class TestRowResponse(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row_response import RowResponse

        return RowResponse

    def _make_one(self, *args, **kwargs):
        if len(args) == 0:
            args = (TEST_ROW_KEY, [self._make_cell()])
        return self._get_target_class()(*args, **kwargs)

    def _make_cell(
        self,
        value=TEST_VALUE,
        row_key=TEST_ROW_KEY,
        family_id=TEST_FAMILY_ID,
        qualifier=TEST_QUALIFIER,
        timestamp=TEST_TIMESTAMP,
        labels=TEST_LABELS,
    ):
        from google.cloud.bigtable.row_response import CellResponse

        return CellResponse(value, row_key, family_id, qualifier, timestamp, labels)

    def test_ctor(self):
        cells = [self._make_cell(), self._make_cell()]
        row_response = self._make_one(TEST_ROW_KEY, cells)
        self.assertEqual(list(row_response), cells)
        self.assertEqual(row_response.row_key, TEST_ROW_KEY)

    def test_ctor_dict(self):
        cells = {
            (TEST_FAMILY_ID, TEST_QUALIFIER): [
                self._make_cell().to_dict(),
                self._make_cell().to_dict(use_nanoseconds=True),
            ]
        }
        row_response = self._make_one(TEST_ROW_KEY, cells)
        self.assertEqual(row_response.row_key, TEST_ROW_KEY)
        self.assertEqual(len(row_response), 2)
        for i in range(2):
            self.assertEqual(row_response[i].value, TEST_VALUE)
            self.assertEqual(row_response[i].row_key, TEST_ROW_KEY)
            self.assertEqual(row_response[i].family, TEST_FAMILY_ID)
            self.assertEqual(row_response[i].column_qualifier, TEST_QUALIFIER)
            self.assertEqual(row_response[i].labels, TEST_LABELS)
        self.assertEqual(row_response[0].timestamp_ns, TEST_TIMESTAMP)
        # second cell was initialized with use_nanoseconds=False, so it doesn't have full precision
        self.assertEqual(row_response[1].timestamp_ns, TEST_TIMESTAMP // 1000 * 1000)

    def test_ctor_bad_cell(self):
        cells = [self._make_cell(), self._make_cell()]
        cells[1].row_key = b"other"
        with self.assertRaises(ValueError):
            self._make_one(TEST_ROW_KEY, cells)

    def test_cell_order(self):
        # cells should be ordered on init
        cell1 = self._make_cell(value=b"1")
        cell2 = self._make_cell(value=b"2")
        resp = self._make_one(TEST_ROW_KEY, [cell2, cell1])
        output = list(resp)
        self.assertEqual(output, [cell1, cell2])

    def test_get_cells(self):
        cell_list = []
        for family_id in ["1", "2"]:
            for qualifier in [b"a", b"b"]:
                cell = self._make_cell(family_id=family_id, qualifier=qualifier)
                cell_list.append(cell)
        # test getting all cells
        row_response = self._make_one(TEST_ROW_KEY, cell_list)
        self.assertEqual(row_response.get_cells(), cell_list)
        # test getting cells in a family
        output = row_response.get_cells(family="1")
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0].family, "1")
        self.assertEqual(output[1].family, "1")
        self.assertEqual(output[0], cell_list[0])
        # test getting cells in a family/qualifier
        # should accept bytes or str for qualifier
        for q in [b"a", "a"]:
            output = row_response.get_cells(family="1", qualifier=q)
            self.assertEqual(len(output), 1)
            self.assertEqual(output[0].family, "1")
            self.assertEqual(output[0].column_qualifier, b"a")
            self.assertEqual(output[0], cell_list[0])
        # calling with just qualifier should raise an error
        with self.assertRaises(ValueError):
            row_response.get_cells(qualifier=b"a")
        # test calling with bad family or qualifier
        with self.assertRaises(ValueError):
            row_response.get_cells(family="3", qualifier=b"a")
        with self.assertRaises(ValueError):
            row_response.get_cells(family="3")
        with self.assertRaises(ValueError):
            row_response.get_cells(family="1", qualifier=b"c")

    def test__repr__(self):
        from google.cloud.bigtable.row_response import CellResponse
        from google.cloud.bigtable.row_response import RowResponse

        cell_str = (
            "{'value': b'1234', 'timestamp_ns': %d, 'labels': ['label1', 'label2']}"
            % (TEST_TIMESTAMP)
        )
        expected_prefix = "RowResponse(key=b'row', cells="
        row = self._make_one(TEST_ROW_KEY, [self._make_cell()])
        self.assertIn(expected_prefix, repr(row))
        self.assertIn(cell_str, repr(row))
        expected_full = (
            "RowResponse(key=b'row', cells={\n  ('cf1', b'col'): [{'value': b'1234', 'timestamp_ns': %d, 'labels': ['label1', 'label2']}],\n})"
            % (TEST_TIMESTAMP)
        )
        self.assertEqual(expected_full, repr(row))
        # should be able to construct instance from __repr__
        result = eval(repr(row))
        self.assertEqual(result, row)
        self.assertIsInstance(result, RowResponse)
        self.assertIsInstance(result[0], CellResponse)
        # try with multiple cells
        row = self._make_one(TEST_ROW_KEY, [self._make_cell(), self._make_cell()])
        self.assertIn(expected_prefix, repr(row))
        self.assertIn(cell_str, repr(row))
        # should be able to construct instance from __repr__
        result = eval(repr(row))
        self.assertEqual(result, row)
        self.assertIsInstance(result, RowResponse)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], CellResponse)
        self.assertIsInstance(result[1], CellResponse)

    def test___str__(self):
        cells = {
            ("3", TEST_QUALIFIER): [
                self._make_cell().to_dict(),
                self._make_cell().to_dict(),
                self._make_cell().to_dict(),
            ]
        }
        cells[("1", TEST_QUALIFIER)] = [self._make_cell().to_dict()]

        row_response = self._make_one(TEST_ROW_KEY, cells)
        expected = (
            "{\n"
            + "  (family='1', qualifier=b'col'): [b'1234'],\n"
            + "  (family='3', qualifier=b'col'): [b'1234', (+2 more)],\n"
            + "}"
        )
        self.assertEqual(expected, str(row_response))

    def test_to_dict(self):
        from google.cloud.bigtable_v2.types import Row

        cell1 = self._make_cell()
        cell2 = self._make_cell()
        cell2.value = b"other"
        row = self._make_one(TEST_ROW_KEY, [cell1, cell2])
        row_dict = row.to_dict()
        expected_dict = {
            "key": TEST_ROW_KEY,
            "families": [
                {
                    "name": TEST_FAMILY_ID,
                    "columns": [
                        {
                            "qualifier": TEST_QUALIFIER,
                            "cells": [
                                {
                                    "value": TEST_VALUE,
                                    "timestamp_micros": TEST_TIMESTAMP // 1000,
                                    "labels": TEST_LABELS,
                                },
                                {
                                    "value": b"other",
                                    "timestamp_micros": TEST_TIMESTAMP // 1000,
                                    "labels": TEST_LABELS,
                                },
                            ],
                        }
                    ],
                },
            ],
        }
        self.assertEqual(len(row_dict), len(expected_dict))
        for key, value in expected_dict.items():
            self.assertEqual(row_dict[key], value)
        # should be able to construct a Cell proto from the dict
        row_proto = Row(**row_dict)
        self.assertEqual(row_proto.key, TEST_ROW_KEY)
        self.assertEqual(len(row_proto.families), 1)
        family = row_proto.families[0]
        self.assertEqual(family.name, TEST_FAMILY_ID)
        self.assertEqual(len(family.columns), 1)
        column = family.columns[0]
        self.assertEqual(column.qualifier, TEST_QUALIFIER)
        self.assertEqual(len(column.cells), 2)
        self.assertEqual(column.cells[0].value, TEST_VALUE)
        self.assertEqual(column.cells[0].timestamp_micros, TEST_TIMESTAMP // 1000)
        self.assertEqual(column.cells[0].labels, TEST_LABELS)
        self.assertEqual(column.cells[1].value, cell2.value)
        self.assertEqual(column.cells[1].timestamp_micros, TEST_TIMESTAMP // 1000)
        self.assertEqual(column.cells[1].labels, TEST_LABELS)

    def test_iteration(self):
        from types import GeneratorType
        from google.cloud.bigtable.row_response import CellResponse

        # should be able to iterate over the RowResponse as a list
        cell3 = self._make_cell(value=b"3")
        cell1 = self._make_cell(value=b"1")
        cell2 = self._make_cell(value=b"2")
        row_response = self._make_one(TEST_ROW_KEY, [cell3, cell1, cell2])
        self.assertEqual(len(row_response), 3)
        # should create generator object
        self.assertIsInstance(iter(row_response), GeneratorType)
        result_list = list(row_response)
        self.assertEqual(len(result_list), 3)
        # should be able to iterate over all cells
        idx = 0
        for cell in row_response:
            self.assertIsInstance(cell, CellResponse)
            self.assertEqual(cell.value, result_list[idx].value)
            self.assertEqual(cell.value, str(idx + 1).encode())
            idx += 1

    def test_contains_cell(self):
        cell3 = self._make_cell(value=b"3")
        cell1 = self._make_cell(value=b"1")
        cell2 = self._make_cell(value=b"2")
        cell4 = self._make_cell(value=b"4")
        row_response = self._make_one(TEST_ROW_KEY, [cell3, cell1, cell2])
        self.assertIn(cell1, row_response)
        self.assertIn(cell2, row_response)
        self.assertNotIn(cell4, row_response)
        cell3_copy = self._make_cell(value=b"3")
        self.assertIn(cell3_copy, row_response)

    def test_contains_family_id(self):
        new_family_id = "new_family_id"
        cell = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell2 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            new_family_id,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        row_response = self._make_one(TEST_ROW_KEY, [cell, cell2])
        self.assertIn(TEST_FAMILY_ID, row_response)
        self.assertIn("new_family_id", row_response)
        self.assertIn(new_family_id, row_response)
        self.assertNotIn("not_a_family_id", row_response)
        self.assertNotIn(None, row_response)

    def test_contains_family_qualifier_tuple(self):
        new_family_id = "new_family_id"
        new_qualifier = b"new_qualifier"
        cell = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell2 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            new_family_id,
            new_qualifier,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        row_response = self._make_one(TEST_ROW_KEY, [cell, cell2])
        self.assertIn((TEST_FAMILY_ID, TEST_QUALIFIER), row_response)
        self.assertIn(("new_family_id", "new_qualifier"), row_response)
        self.assertIn(("new_family_id", b"new_qualifier"), row_response)
        self.assertIn((new_family_id, new_qualifier), row_response)

        self.assertNotIn(("not_a_family_id", TEST_QUALIFIER), row_response)
        self.assertNotIn((TEST_FAMILY_ID, "not_a_qualifier"), row_response)
        self.assertNotIn((TEST_FAMILY_ID, new_qualifier), row_response)
        self.assertNotIn(("not_a_family_id", "not_a_qualifier"), row_response)
        self.assertNotIn((None, None), row_response)
        self.assertNotIn(None, row_response)

    def test_int_indexing(self):
        # should be able to index into underlying list with an index number directly
        cell_list = [self._make_cell(value=str(i).encode()) for i in range(10)]
        sorted(cell_list)
        row_response = self._make_one(TEST_ROW_KEY, cell_list)
        self.assertEqual(len(row_response), 10)
        for i in range(10):
            self.assertEqual(row_response[i].value, str(i).encode())
            # backwards indexing should work
            self.assertEqual(row_response[-i - 1].value, str(9 - i).encode())
        with self.assertRaises(IndexError):
            row_response[10]
        with self.assertRaises(IndexError):
            row_response[-11]

    def test_slice_indexing(self):
        # should be able to index with a range of indices
        cell_list = [self._make_cell(value=str(i).encode()) for i in range(10)]
        sorted(cell_list)
        row_response = self._make_one(TEST_ROW_KEY, cell_list)
        self.assertEqual(len(row_response), 10)
        self.assertEqual(len(row_response[0:10]), 10)
        self.assertEqual(row_response[0:10], cell_list)
        self.assertEqual(len(row_response[0:]), 10)
        self.assertEqual(row_response[0:], cell_list)
        self.assertEqual(len(row_response[:10]), 10)
        self.assertEqual(row_response[:10], cell_list)
        self.assertEqual(len(row_response[0:10:1]), 10)
        self.assertEqual(row_response[0:10:1], cell_list)
        self.assertEqual(len(row_response[0:10:2]), 5)
        self.assertEqual(row_response[0:10:2], [cell_list[i] for i in range(0, 10, 2)])
        self.assertEqual(len(row_response[0:10:3]), 4)
        self.assertEqual(row_response[0:10:3], [cell_list[i] for i in range(0, 10, 3)])
        self.assertEqual(len(row_response[10:0:-1]), 9)
        self.assertEqual(len(row_response[10:0:-2]), 5)
        self.assertEqual(row_response[10:0:-3], cell_list[10:0:-3])
        self.assertEqual(len(row_response[0:100]), 10)

    def test_family_indexing(self):
        # should be able to retrieve cells in a family
        new_family_id = "new_family_id"
        cell = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell2 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell3 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            new_family_id,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        row_response = self._make_one(TEST_ROW_KEY, [cell, cell2, cell3])

        self.assertEqual(len(row_response[TEST_FAMILY_ID]), 2)
        self.assertEqual(row_response[TEST_FAMILY_ID][0], cell)
        self.assertEqual(row_response[TEST_FAMILY_ID][1], cell2)
        self.assertEqual(len(row_response[new_family_id]), 1)
        self.assertEqual(row_response[new_family_id][0], cell3)
        with self.assertRaises(ValueError):
            row_response["not_a_family_id"]
        with self.assertRaises(TypeError):
            row_response[None]
        with self.assertRaises(TypeError):
            row_response[b"new_family_id"]

    def test_family_qualifier_indexing(self):
        # should be able to retrieve cells in a family/qualifier tuplw
        new_family_id = "new_family_id"
        new_qualifier = b"new_qualifier"
        cell = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell2 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell3 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            new_family_id,
            new_qualifier,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        row_response = self._make_one(TEST_ROW_KEY, [cell, cell2, cell3])

        self.assertEqual(len(row_response[TEST_FAMILY_ID, TEST_QUALIFIER]), 2)
        self.assertEqual(row_response[TEST_FAMILY_ID, TEST_QUALIFIER][0], cell)
        self.assertEqual(row_response[TEST_FAMILY_ID, TEST_QUALIFIER][1], cell2)
        self.assertEqual(len(row_response[new_family_id, new_qualifier]), 1)
        self.assertEqual(row_response[new_family_id, new_qualifier][0], cell3)
        self.assertEqual(len(row_response["new_family_id", "new_qualifier"]), 1)
        self.assertEqual(len(row_response["new_family_id", b"new_qualifier"]), 1)
        with self.assertRaises(ValueError):
            row_response[new_family_id, "not_a_qualifier"]
        with self.assertRaises(ValueError):
            row_response["not_a_family_id", new_qualifier]
        with self.assertRaises(TypeError):
            row_response[None, None]
        with self.assertRaises(TypeError):
            row_response[b"new_family_id", b"new_qualifier"]

    def test_keys(self):
        # should be able to retrieve (family,qualifier) tuples as keys
        new_family_id = "new_family_id"
        new_qualifier = b"new_qualifier"
        cell = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell2 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        cell3 = self._make_cell(
            TEST_VALUE,
            TEST_ROW_KEY,
            new_family_id,
            new_qualifier,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        row_response = self._make_one(TEST_ROW_KEY, [cell, cell2, cell3])

        self.assertEqual(len(row_response.keys()), 2)
        self.assertEqual(
            row_response.keys(),
            [(TEST_FAMILY_ID, TEST_QUALIFIER), (new_family_id, new_qualifier)],
        )

        row_response = self._make_one(TEST_ROW_KEY, [])
        self.assertEqual(len(row_response.keys()), 0)
        self.assertEqual(row_response.keys(), [])

        row_response = self._make_one(TEST_ROW_KEY, [cell])
        self.assertEqual(len(row_response.keys()), 1)
        self.assertEqual(row_response.keys(), [(TEST_FAMILY_ID, TEST_QUALIFIER)])

    def test_values(self):
        # values should return the list of all cells
        cell_list = [self._make_cell(qualifier=str(i).encode()) for i in range(10)]
        row_response = self._make_one(TEST_ROW_KEY, cell_list)
        sorted(cell_list)

        self.assertEqual(len(row_response.values()), 10)
        self.assertEqual(row_response.values(), cell_list)

    def test_items(self):
        cell_list = [self._make_cell() for i in range(10)]
        sorted(cell_list)
        row_response = self._make_one(TEST_ROW_KEY, cell_list)

        self.assertEqual(len(list(row_response.items())), 1)
        self.assertEqual(
            list(row_response.items())[0][0], (TEST_FAMILY_ID, TEST_QUALIFIER)
        )
        self.assertEqual(list(row_response.items())[0][1], cell_list)

        row_response = self._make_one(TEST_ROW_KEY, [])
        self.assertEqual(len(list(row_response.items())), 0)

        cell_list = [self._make_cell(qualifier=str(i).encode()) for i in range(10)]
        row_response = self._make_one(TEST_ROW_KEY, cell_list)
        sorted(cell_list)
        self.assertEqual(len(list(row_response.items())), 10)
        keys = [t[0] for t in row_response.items()]
        cells = [t[1] for t in row_response.items()]
        for i in range(10):
            self.assertEqual(keys[i], (TEST_FAMILY_ID, str(i).encode()))
            self.assertEqual(len(cells[i]), 1)
            self.assertEqual(cells[i][0], cell_list[i])

    def test_index_of(self):
        # given a cell, should find index in underlying list
        cell_list = [self._make_cell(value=str(i).encode()) for i in range(10)]
        sorted(cell_list)
        row_response = self._make_one(TEST_ROW_KEY, cell_list)

        self.assertEqual(row_response.index(cell_list[0]), 0)
        self.assertEqual(row_response.index(cell_list[5]), 5)
        self.assertEqual(row_response.index(cell_list[9]), 9)
        with self.assertRaises(ValueError):
            row_response.index(self._make_cell())
        with self.assertRaises(ValueError):
            row_response.index(None)


class TestCellResponse(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row_response import CellResponse

        return CellResponse

    def _make_one(self, *args, **kwargs):
        if len(args) == 0:
            args = (
                TEST_VALUE,
                TEST_ROW_KEY,
                TEST_FAMILY_ID,
                TEST_QUALIFIER,
                TEST_TIMESTAMP,
                TEST_LABELS,
            )
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        cell = self._make_one(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        self.assertEqual(cell.value, TEST_VALUE)
        self.assertEqual(cell.row_key, TEST_ROW_KEY)
        self.assertEqual(cell.family, TEST_FAMILY_ID)
        self.assertEqual(cell.column_qualifier, TEST_QUALIFIER)
        self.assertEqual(cell.timestamp_ns, TEST_TIMESTAMP)
        self.assertEqual(cell.labels, TEST_LABELS)

    def test_to_dict(self):
        from google.cloud.bigtable_v2.types import Cell

        cell = self._make_one()
        cell_dict = cell.to_dict()
        expected_dict = {
            "value": TEST_VALUE,
            "timestamp_micros": TEST_TIMESTAMP // 1000,
            "labels": TEST_LABELS,
        }
        self.assertEqual(len(cell_dict), len(expected_dict))
        for key, value in expected_dict.items():
            self.assertEqual(cell_dict[key], value)
        # should be able to construct a Cell proto from the dict
        cell_proto = Cell(**cell_dict)
        self.assertEqual(cell_proto.value, TEST_VALUE)
        self.assertEqual(cell_proto.timestamp_micros, TEST_TIMESTAMP // 1000)
        self.assertEqual(cell_proto.labels, TEST_LABELS)

    def test_to_dict_nanos_timestamp(self):
        cell = self._make_one()
        cell_dict = cell.to_dict(use_nanoseconds=True)
        expected_dict = {
            "value": TEST_VALUE,
            "timestamp_ns": TEST_TIMESTAMP,
            "labels": TEST_LABELS,
        }
        self.assertEqual(len(cell_dict), len(expected_dict))
        for key, value in expected_dict.items():
            self.assertEqual(cell_dict[key], value)

    def test_to_dict_no_labels(self):
        from google.cloud.bigtable_v2.types import Cell

        cell_no_labels = self._make_one(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            None,
        )
        cell_dict = cell_no_labels.to_dict()
        expected_dict = {
            "value": TEST_VALUE,
            "timestamp_micros": TEST_TIMESTAMP // 1000,
        }
        self.assertEqual(len(cell_dict), len(expected_dict))
        for key, value in expected_dict.items():
            self.assertEqual(cell_dict[key], value)
        # should be able to construct a Cell proto from the dict
        cell_proto = Cell(**cell_dict)
        self.assertEqual(cell_proto.value, TEST_VALUE)
        self.assertEqual(cell_proto.timestamp_micros, TEST_TIMESTAMP // 1000)
        self.assertEqual(cell_proto.labels, [])

    def test_int_value(self):
        test_int = 1234
        bytes_value = test_int.to_bytes(4, "big", signed=True)
        cell = self._make_one(
            bytes_value,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        self.assertEqual(int(cell), test_int)
        # ensure string formatting works
        formatted = "%d" % cell
        self.assertEqual(formatted, str(test_int))
        self.assertEqual(int(formatted), test_int)

    def test_int_value_negative(self):
        test_int = -99999
        bytes_value = test_int.to_bytes(4, "big", signed=True)
        cell = self._make_one(
            bytes_value,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        self.assertEqual(int(cell), test_int)
        # ensure string formatting works
        formatted = "%d" % cell
        self.assertEqual(formatted, str(test_int))
        self.assertEqual(int(formatted), test_int)

    def test___str__(self):
        test_value = b"helloworld"
        cell = self._make_one(
            test_value,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        self.assertEqual(str(cell), "b'helloworld'")
        self.assertEqual(str(cell), str(test_value))

    def test___repr__(self):
        from google.cloud.bigtable.row_response import CellResponse  # type: ignore # noqa: F401

        cell = self._make_one()
        expected = (
            "CellResponse(value=b'1234', row=b'row', "
            + "family='cf1', column_qualifier=b'col', "
            + f"timestamp_ns={TEST_TIMESTAMP}, labels=['label1', 'label2'])"
        )
        self.assertEqual(repr(cell), expected)
        # should be able to construct instance from __repr__
        result = eval(repr(cell))
        self.assertEqual(result, cell)

    def test___repr___no_labels(self):
        from google.cloud.bigtable.row_response import CellResponse  # type: ignore # noqa: F401

        cell_no_labels = self._make_one(
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            None,
        )
        expected = (
            "CellResponse(value=b'1234', row=b'row', "
            + "family='cf1', column_qualifier=b'col', "
            + f"timestamp_ns={TEST_TIMESTAMP}, labels=[])"
        )
        self.assertEqual(repr(cell_no_labels), expected)
        # should be able to construct instance from __repr__
        result = eval(repr(cell_no_labels))
        self.assertEqual(result, cell_no_labels)

    def test_equality(self):
        cell1 = self._make_one()
        cell2 = self._make_one()
        self.assertEqual(cell1, cell2)
        self.assertTrue(cell1 == cell2)
        args = (
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        for i in range(0, len(args)):
            # try changing each argument
            modified_cell = self._make_one(*args[:i], args[i] + args[i], *args[i + 1 :])
            self.assertNotEqual(cell1, modified_cell)
            self.assertFalse(cell1 == modified_cell)
            self.assertTrue(cell1 != modified_cell)

    def test_hash(self):
        # class should be hashable
        cell1 = self._make_one()
        d = {cell1: 1}
        cell2 = self._make_one()
        self.assertEqual(d[cell2], 1)

        args = (
            TEST_VALUE,
            TEST_ROW_KEY,
            TEST_FAMILY_ID,
            TEST_QUALIFIER,
            TEST_TIMESTAMP,
            TEST_LABELS,
        )
        for i in range(0, len(args)):
            # try changing each argument
            modified_cell = self._make_one(*args[:i], args[i] + args[i], *args[i + 1 :])
            with self.assertRaises(KeyError):
                d[modified_cell]

    def test_ordering(self):
        # create cell list in order from lowest to highest
        higher_cells = []
        i = 0
        # families; alphebetical order
        for family in ["z", "y", "x"]:
            # qualifiers; lowest byte value first
            for qualifier in [b"z", b"y", b"x"]:
                # timestamps; newest first
                for timestamp in [
                    TEST_TIMESTAMP,
                    TEST_TIMESTAMP + 1,
                    TEST_TIMESTAMP + 2,
                ]:
                    cell = self._make_one(
                        TEST_VALUE,
                        TEST_ROW_KEY,
                        family,
                        qualifier,
                        timestamp,
                        TEST_LABELS,
                    )
                    # cell should be the highest priority encountered so far
                    self.assertEqual(i, len(higher_cells))
                    i += 1
                    for other in higher_cells:
                        self.assertLess(cell, other)
                    higher_cells.append(cell)
        # final order should be reverse of sorted order
        expected_order = higher_cells
        expected_order.reverse()
        self.assertEqual(expected_order, sorted(higher_cells))
