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
        pass

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

    def test_cell_value_no_cell(self):
        pass

    def test_cell_value(self):
        pass

    def test_finish_cell(self):
        pass

    def test_finish_cell_no_cell(self):
        pass

    def test_finish_row(self):
        pass

    def finish_row_no_row(self):
        pass

    def test_reset(self):
        pass
