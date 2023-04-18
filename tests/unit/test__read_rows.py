import unittest
from unittest import mock

from google.cloud.bigtable.exceptions import InvalidChunk

TEST_FAMILY = "family_name"
TEST_QUALIFIER = b"column_qualifier"
TEST_TIMESTAMP = 123456789
TEST_LABELS = ["label1", "label2"]


class TestReadRowsOperation(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable._read_rows import _ReadRowsOperation

        return _ReadRowsOperation

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_revise_to_empty_rowset(self):
        # ensure that the _revise_to_empty_set method
        # does not return a full table scan
        pass


class TestStateMachine(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable._read_rows import _StateMachine

        return _StateMachine

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)


class TestState(unittest.TestCase):
    pass


class TestRowBuilder(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable._read_rows import _RowBuilder

        return _RowBuilder

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        with mock.patch.object(self._get_target_class(), "reset") as reset:
            self._make_one()
            reset.assert_called_once()
        row_builder = self._make_one()
        self.assertIsNone(row_builder.current_key)
        self.assertIsNone(row_builder.working_cell)
        self.assertIsNone(row_builder.working_value)
        self.assertEqual(row_builder.completed_cells, [])

    def test_start_row(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        self.assertEqual(row_builder.current_key, b"row_key")
        row_builder.start_row(b"row_key2")
        self.assertEqual(row_builder.current_key, b"row_key2")

    def test_start_cell(self):
        # test with no family
        with self.assertRaises(InvalidChunk) as e:
            self._make_one().start_cell("", TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
            self.assertEqual(str(e.exception), "Missing family for a new cell")
        # test with no row
        with self.assertRaises(InvalidChunk) as e:
            row_builder = self._make_one()
            row_builder.start_cell(
                TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS
            )
            self.assertEqual(str(e.exception), "start_cell called without a row")
        # test with valid row
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        self.assertEqual(row_builder.working_cell.family, TEST_FAMILY)
        self.assertEqual(row_builder.working_cell.column_qualifier, TEST_QUALIFIER)
        self.assertEqual(row_builder.working_cell.timestamp_micros, TEST_TIMESTAMP)
        self.assertEqual(row_builder.working_cell.labels, TEST_LABELS)
        self.assertEqual(row_builder.working_value, b"")

    def test_cell_value(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        with self.assertRaises(InvalidChunk) as e:
            # start_cell must be called before cell_value
            row_builder.cell_value(b"cell_value")
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        row_builder.cell_value(b"cell_value")
        self.assertEqual(row_builder.working_value, b"cell_value")
        # should be able to continuously append to the working value
        row_builder.cell_value(b"appended")
        self.assertEqual(row_builder.working_value, b"cell_valueappended")

    def test_finish_cell(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        row_builder.finish_cell()
        self.assertEqual(len(row_builder.completed_cells), 1)
        self.assertEqual(row_builder.completed_cells[0].family, TEST_FAMILY)
        self.assertEqual(row_builder.completed_cells[0].column_qualifier, TEST_QUALIFIER)
        self.assertEqual(row_builder.completed_cells[0].timestamp_micros, TEST_TIMESTAMP)
        self.assertEqual(row_builder.completed_cells[0].labels, TEST_LABELS)
        self.assertEqual(row_builder.completed_cells[0].value, b"")
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)
        # add additional cell with value
        row_builder.start_cell(TEST_FAMILY, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        row_builder.cell_value(b"cell_value")
        row_builder.cell_value(b"appended")
        row_builder.finish_cell()
        self.assertEqual(len(row_builder.completed_cells), 2)
        self.assertEqual(row_builder.completed_cells[1].family, TEST_FAMILY)
        self.assertEqual(row_builder.completed_cells[1].column_qualifier, TEST_QUALIFIER)
        self.assertEqual(row_builder.completed_cells[1].timestamp_micros, TEST_TIMESTAMP)
        self.assertEqual(row_builder.completed_cells[1].labels, TEST_LABELS)
        self.assertEqual(row_builder.completed_cells[1].value, b"cell_valueappended")
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)

    def test_finish_cell_no_cell(self):
        with self.assertRaises(InvalidChunk) as e:
            self._make_one().finish_cell()
            self.assertEqual(str(e.exception), "finish_cell called before start_cell")
        with self.assertRaises(InvalidChunk) as e:
            row_builder = self._make_one()
            row_builder.start_row(b"row_key")
            row_builder.finish_cell()
            self.assertEqual(str(e.exception), "finish_cell called before start_cell")

    def test_finish_row(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        for i in range(3):
            row_builder.start_cell(str(i), TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
            row_builder.cell_value(b"cell_value: ")
            row_builder.cell_value(str(i).encode("utf-8"))
            row_builder.finish_cell()
            self.assertEqual(len(row_builder.completed_cells), i + 1)
        output = row_builder.finish_row()
        self.assertEqual(row_builder.current_key, None)
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)
        self.assertEqual(len(row_builder.completed_cells), 0)

        self.assertEqual(output.row_key, b"row_key")
        self.assertEqual(len(output), 3)
        for i in range(3):
            self.assertEqual(output[i].family, str(i))
            self.assertEqual(output[i].column_qualifier, TEST_QUALIFIER)
            self.assertEqual(output[i].timestamp_micros, TEST_TIMESTAMP)
            self.assertEqual(output[i].labels, TEST_LABELS)
            self.assertEqual(output[i].value, b"cell_value: " + str(i).encode("utf-8"))


    def finish_row_no_row(self):
        with self.assertRaises(InvalidChunk) as e:
            self._make_one().finish_row()
            self.assertEqual(str(e.exception), "No row in progress")

    def test_reset(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        for i in range(3):
            row_builder.start_cell(str(i), TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
            row_builder.cell_value(b"cell_value: ")
            row_builder.cell_value(str(i).encode("utf-8"))
            row_builder.finish_cell()
            self.assertEqual(len(row_builder.completed_cells), i + 1)
        row_builder.reset()
        self.assertEqual(row_builder.current_key, None)
        self.assertEqual(row_builder.working_cell, None)
        self.assertEqual(row_builder.working_value, None)
        self.assertEqual(len(row_builder.completed_cells), 0)
