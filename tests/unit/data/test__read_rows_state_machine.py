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
import pytest

from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data._read_rows_state_machine import AWAITING_NEW_ROW
from google.cloud.bigtable.data._read_rows_state_machine import AWAITING_NEW_CELL
from google.cloud.bigtable.data._read_rows_state_machine import AWAITING_CELL_VALUE

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore # noqa F401

TEST_FAMILY = "family_name"
TEST_QUALIFIER = b"qualifier"
TEST_TIMESTAMP = 123456789
TEST_LABELS = ["label1", "label2"]


class TestStateMachine(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.data._read_rows_state_machine import _StateMachine

        return _StateMachine

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        from google.cloud.bigtable.data._read_rows_state_machine import _RowBuilder

        instance = self._make_one()
        assert instance.last_seen_row_key is None
        assert instance.current_state == AWAITING_NEW_ROW
        assert instance.current_family is None
        assert instance.current_qualifier is None
        assert isinstance(instance.adapter, _RowBuilder)
        assert instance.adapter.current_key is None
        assert instance.adapter.working_cell is None
        assert instance.adapter.working_value is None
        assert instance.adapter.completed_cells == []

    def test_is_terminal_state(self):

        instance = self._make_one()
        assert instance.is_terminal_state() is True
        instance.current_state = AWAITING_NEW_ROW
        assert instance.is_terminal_state() is True
        instance.current_state = AWAITING_NEW_CELL
        assert instance.is_terminal_state() is False
        instance.current_state = AWAITING_CELL_VALUE
        assert instance.is_terminal_state() is False

    def test__reset_row(self):
        instance = self._make_one()
        instance.current_state = mock.Mock()
        instance.current_family = "family"
        instance.current_qualifier = "qualifier"
        instance.adapter = mock.Mock()
        instance._reset_row()
        assert instance.current_state == AWAITING_NEW_ROW
        assert instance.current_family is None
        assert instance.current_qualifier is None
        assert instance.adapter.reset.call_count == 1

    def test_handle_last_scanned_row_wrong_state(self):
        from google.cloud.bigtable.data.exceptions import InvalidChunk

        instance = self._make_one()
        instance.current_state = AWAITING_NEW_CELL
        with pytest.raises(InvalidChunk) as e:
            instance.handle_last_scanned_row("row_key")
        assert e.value.args[0] == "Last scanned row key received in invalid state"
        instance.current_state = AWAITING_CELL_VALUE
        with pytest.raises(InvalidChunk) as e:
            instance.handle_last_scanned_row("row_key")
        assert e.value.args[0] == "Last scanned row key received in invalid state"

    def test_handle_last_scanned_row_out_of_order(self):
        from google.cloud.bigtable.data.exceptions import InvalidChunk

        instance = self._make_one()
        instance.last_seen_row_key = b"b"
        with pytest.raises(InvalidChunk) as e:
            instance.handle_last_scanned_row(b"a")
        assert e.value.args[0] == "Last scanned row key is out of order"
        with pytest.raises(InvalidChunk) as e:
            instance.handle_last_scanned_row(b"b")
        assert e.value.args[0] == "Last scanned row key is out of order"

    def test_handle_last_scanned_row(self):
        from google.cloud.bigtable.data.row import _LastScannedRow

        instance = self._make_one()
        instance.adapter = mock.Mock()
        instance.last_seen_row_key = b"a"
        output_row = instance.handle_last_scanned_row(b"b")
        assert instance.last_seen_row_key == b"b"
        assert isinstance(output_row, _LastScannedRow)
        assert output_row.row_key == b"b"
        assert instance.current_state == AWAITING_NEW_ROW
        assert instance.current_family is None
        assert instance.current_qualifier is None
        assert instance.adapter.reset.call_count == 1

    def test__handle_complete_row(self):
        from google.cloud.bigtable.data.row import Row

        instance = self._make_one()
        instance.current_state = mock.Mock()
        instance.current_family = "family"
        instance.current_qualifier = "qualifier"
        instance.adapter = mock.Mock()
        instance._handle_complete_row(Row(b"row_key", {}))
        assert instance.last_seen_row_key == b"row_key"
        assert instance.current_state == AWAITING_NEW_ROW
        assert instance.current_family is None
        assert instance.current_qualifier is None
        assert instance.adapter.reset.call_count == 1

    def test__handle_reset_chunk_errors(self):
        from google.cloud.bigtable.data.exceptions import InvalidChunk
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = self._make_one()
        with pytest.raises(InvalidChunk) as e:
            instance._handle_reset_chunk(mock.Mock())
        instance.current_state = mock.Mock()
        assert e.value.args[0] == "Reset chunk received when not processing row"
        with pytest.raises(InvalidChunk) as e:
            instance._handle_reset_chunk(
                ReadRowsResponse.CellChunk(row_key=b"row_key")._pb
            )
        assert e.value.args[0] == "Reset chunk has a row key"
        with pytest.raises(InvalidChunk) as e:
            instance._handle_reset_chunk(
                ReadRowsResponse.CellChunk(family_name="family")._pb
            )
        assert e.value.args[0] == "Reset chunk has a family name"
        with pytest.raises(InvalidChunk) as e:
            instance._handle_reset_chunk(
                ReadRowsResponse.CellChunk(qualifier=b"qualifier")._pb
            )
        assert e.value.args[0] == "Reset chunk has a qualifier"
        with pytest.raises(InvalidChunk) as e:
            instance._handle_reset_chunk(
                ReadRowsResponse.CellChunk(timestamp_micros=1)._pb
            )
        assert e.value.args[0] == "Reset chunk has a timestamp"
        with pytest.raises(InvalidChunk) as e:
            instance._handle_reset_chunk(ReadRowsResponse.CellChunk(value=b"value")._pb)
        assert e.value.args[0] == "Reset chunk has a value"
        with pytest.raises(InvalidChunk) as e:
            instance._handle_reset_chunk(
                ReadRowsResponse.CellChunk(labels=["label"])._pb
            )
        assert e.value.args[0] == "Reset chunk has labels"

    def test_handle_chunk_out_of_order(self):
        from google.cloud.bigtable.data.exceptions import InvalidChunk
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = self._make_one()
        instance.last_seen_row_key = b"b"
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(row_key=b"a")._pb
            instance.handle_chunk(chunk)
        assert "increasing" in e.value.args[0]
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(row_key=b"b")._pb
            instance.handle_chunk(chunk)
        assert "increasing" in e.value.args[0]

    def test_handle_chunk_reset(self):
        """Should call _handle_reset_chunk when a chunk with reset_row is encountered"""
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = self._make_one()
        with mock.patch.object(type(instance), "_handle_reset_chunk") as mock_reset:
            chunk = ReadRowsResponse.CellChunk(reset_row=True)._pb
            output = instance.handle_chunk(chunk)
            assert output is None
            assert mock_reset.call_count == 1

    @pytest.mark.parametrize("state", [AWAITING_NEW_ROW, AWAITING_CELL_VALUE])
    def handle_chunk_with_commit_wrong_state(self, state):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = self._make_one()
        with mock.patch.object(
            type(instance.current_state), "handle_chunk"
        ) as mock_state_handle:
            mock_state_handle.return_value = state(mock.Mock())
            with pytest.raises(InvalidChunk) as e:
                chunk = ReadRowsResponse.CellChunk(commit_row=True)._pb
                instance.handle_chunk(mock.Mock(), chunk)
            assert instance.current_state == state
            assert e.value.args[0] == "Commit chunk received with in invalid state"

    def test_handle_chunk_with_commit(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data.row import Row

        instance = self._make_one()
        with mock.patch.object(type(instance), "_reset_row") as mock_reset:
            chunk = ReadRowsResponse.CellChunk(
                row_key=b"row_key", family_name="f", qualifier=b"q", commit_row=True
            )._pb
            output = instance.handle_chunk(chunk)
            assert isinstance(output, Row)
            assert output.row_key == b"row_key"
            assert output[0].family == "f"
            assert output[0].qualifier == b"q"
            assert instance.last_seen_row_key == b"row_key"
        assert mock_reset.call_count == 1

    def test_handle_chunk_with_commit_empty_strings(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data.row import Row

        instance = self._make_one()
        with mock.patch.object(type(instance), "_reset_row") as mock_reset:
            chunk = ReadRowsResponse.CellChunk(
                row_key=b"row_key", family_name="", qualifier=b"", commit_row=True
            )._pb
            output = instance.handle_chunk(chunk)
            assert isinstance(output, Row)
            assert output.row_key == b"row_key"
            assert output[0].family == ""
            assert output[0].qualifier == b""
            assert instance.last_seen_row_key == b"row_key"
        assert mock_reset.call_count == 1

    def handle_chunk_incomplete(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = self._make_one()
        chunk = ReadRowsResponse.CellChunk(
            row_key=b"row_key", family_name="f", qualifier=b"q", commit_row=False
        )._pb
        output = instance.handle_chunk(chunk)
        assert output is None
        assert isinstance(instance.current_state, AWAITING_CELL_VALUE)
        assert instance.current_family == "f"
        assert instance.current_qualifier == b"q"


class TestState(unittest.TestCase):
    def test_AWAITING_NEW_ROW_empty_key(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = AWAITING_NEW_ROW
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(row_key=b"")._pb
            instance.handle_chunk(mock.Mock(), chunk)
        assert "missing a row key" in e.value.args[0]
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk()._pb
            instance.handle_chunk(mock.Mock(), chunk)
        assert "missing a row key" in e.value.args[0]

    def test_AWAITING_NEW_ROW(self):
        """
        AWAITING_NEW_ROW should start a RowBuilder row, then
        delegate the call to AWAITING_NEW_CELL
        """
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = AWAITING_NEW_ROW
        state_machine = mock.Mock()
        with mock.patch.object(AWAITING_NEW_CELL, "handle_chunk") as mock_delegate:
            chunk = ReadRowsResponse.CellChunk(row_key=b"row_key")._pb
            instance.handle_chunk(state_machine, chunk)
            assert state_machine.adapter.start_row.call_count == 1
            assert state_machine.adapter.start_row.call_args[0][0] == b"row_key"
        mock_delegate.assert_called_once_with(state_machine, chunk)

    def test_AWAITING_NEW_CELL_family_without_qualifier(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        state_machine.current_qualifier = b"q"
        instance = AWAITING_NEW_CELL
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(family_name="fam")._pb
            instance.handle_chunk(state_machine, chunk)
        assert "New family must specify qualifier" in e.value.args[0]

    def test_AWAITING_NEW_CELL_qualifier_without_family(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_NEW_CELL
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(qualifier=b"q")._pb
            instance.handle_chunk(state_machine, chunk)
        assert "Family not found" in e.value.args[0]

    def test_AWAITING_NEW_CELL_no_row_state(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_NEW_CELL
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk()._pb
            instance.handle_chunk(state_machine, chunk)
        assert "Missing family for new cell" in e.value.args[0]
        state_machine.current_family = "fam"
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk()._pb
            instance.handle_chunk(state_machine, chunk)
        assert "Missing qualifier for new cell" in e.value.args[0]

    def test_AWAITING_NEW_CELL_invalid_row_key(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_NEW_CELL
        state_machine.adapter.current_key = b"abc"
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(row_key=b"123")._pb
            instance.handle_chunk(state_machine, chunk)
        assert "Row key changed mid row" in e.value.args[0]

    def test_AWAITING_NEW_CELL_success_no_split(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        state_machine.adapter = mock.Mock()
        instance = AWAITING_NEW_CELL
        row_key = b"row_key"
        family = "fam"
        qualifier = b"q"
        labels = ["label"]
        timestamp = 123
        value = b"value"
        chunk = ReadRowsResponse.CellChunk(
            row_key=row_key,
            family_name=family,
            qualifier=qualifier,
            timestamp_micros=timestamp,
            value=value,
            labels=labels,
        )._pb
        state_machine.adapter.current_key = row_key
        new_state = instance.handle_chunk(state_machine, chunk)
        assert state_machine.adapter.start_cell.call_count == 1
        kwargs = state_machine.adapter.start_cell.call_args[1]
        assert kwargs["family"] == family
        assert kwargs["qualifier"] == qualifier
        assert kwargs["timestamp_micros"] == timestamp
        assert kwargs["labels"] == labels
        assert state_machine.adapter.cell_value.call_count == 1
        assert state_machine.adapter.cell_value.call_args[0][0] == value
        assert state_machine.adapter.finish_cell.call_count == 1
        assert new_state == AWAITING_NEW_CELL

    def test_AWAITING_NEW_CELL_success_with_split(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        state_machine.adapter = mock.Mock()
        instance = AWAITING_NEW_CELL
        row_key = b"row_key"
        family = "fam"
        qualifier = b"q"
        labels = ["label"]
        timestamp = 123
        value = b"value"
        chunk = ReadRowsResponse.CellChunk(
            value_size=1,
            row_key=row_key,
            family_name=family,
            qualifier=qualifier,
            timestamp_micros=timestamp,
            value=value,
            labels=labels,
        )._pb
        state_machine.adapter.current_key = row_key
        new_state = instance.handle_chunk(state_machine, chunk)
        assert state_machine.adapter.start_cell.call_count == 1
        kwargs = state_machine.adapter.start_cell.call_args[1]
        assert kwargs["family"] == family
        assert kwargs["qualifier"] == qualifier
        assert kwargs["timestamp_micros"] == timestamp
        assert kwargs["labels"] == labels
        assert state_machine.adapter.cell_value.call_count == 1
        assert state_machine.adapter.cell_value.call_args[0][0] == value
        assert state_machine.adapter.finish_cell.call_count == 0
        assert new_state == AWAITING_CELL_VALUE

    def test_AWAITING_CELL_VALUE_w_row_key(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_CELL_VALUE
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(row_key=b"123")._pb
            instance.handle_chunk(state_machine, chunk)
        assert "In progress cell had a row key" in e.value.args[0]

    def test_AWAITING_CELL_VALUE_w_family(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_CELL_VALUE
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(family_name="")._pb
            instance.handle_chunk(state_machine, chunk)
        assert "In progress cell had a family name" in e.value.args[0]

    def test_AWAITING_CELL_VALUE_w_qualifier(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_CELL_VALUE
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(qualifier=b"")._pb
            instance.handle_chunk(state_machine, chunk)
        assert "In progress cell had a qualifier" in e.value.args[0]

    def test_AWAITING_CELL_VALUE_w_timestamp(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_CELL_VALUE
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(timestamp_micros=123)._pb
            instance.handle_chunk(state_machine, chunk)
        assert "In progress cell had a timestamp" in e.value.args[0]

    def test_AWAITING_CELL_VALUE_w_labels(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        instance = AWAITING_CELL_VALUE
        with pytest.raises(InvalidChunk) as e:
            chunk = ReadRowsResponse.CellChunk(labels=[""])._pb
            instance.handle_chunk(state_machine, chunk)
        assert "In progress cell had labels" in e.value.args[0]

    def test_AWAITING_CELL_VALUE_continuation(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        state_machine.adapter = mock.Mock()
        instance = AWAITING_CELL_VALUE
        value = b"value"
        chunk = ReadRowsResponse.CellChunk(value=value, value_size=1)._pb
        new_state = instance.handle_chunk(state_machine, chunk)
        assert state_machine.adapter.cell_value.call_count == 1
        assert state_machine.adapter.cell_value.call_args[0][0] == value
        assert state_machine.adapter.finish_cell.call_count == 0
        assert new_state == AWAITING_CELL_VALUE

    def test_AWAITING_CELL_VALUE_final_chunk(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._async._read_rows import _StateMachine

        state_machine = _StateMachine()
        state_machine.adapter = mock.Mock()
        instance = AWAITING_CELL_VALUE
        value = b"value"
        chunk = ReadRowsResponse.CellChunk(value=value, value_size=0)._pb
        new_state = instance.handle_chunk(state_machine, chunk)
        assert state_machine.adapter.cell_value.call_count == 1
        assert state_machine.adapter.cell_value.call_args[0][0] == value
        assert state_machine.adapter.finish_cell.call_count == 1
        assert new_state == AWAITING_NEW_CELL


class TestRowBuilder(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.data._read_rows_state_machine import _RowBuilder

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
        self.assertEqual(row_builder.working_cell.qualifier, TEST_QUALIFIER)
        self.assertEqual(row_builder.working_cell.timestamp_micros, TEST_TIMESTAMP)
        self.assertEqual(row_builder.working_cell.labels, TEST_LABELS)
        self.assertEqual(row_builder.working_value, b"")

    def test_cell_value(self):
        row_builder = self._make_one()
        row_builder.start_row(b"row_key")
        with self.assertRaises(InvalidChunk):
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
        self.assertEqual(row_builder.completed_cells[0].qualifier, TEST_QUALIFIER)
        self.assertEqual(
            row_builder.completed_cells[0].timestamp_micros, TEST_TIMESTAMP
        )
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
        self.assertEqual(row_builder.completed_cells[1].qualifier, TEST_QUALIFIER)
        self.assertEqual(
            row_builder.completed_cells[1].timestamp_micros, TEST_TIMESTAMP
        )
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
            self.assertEqual(output[i].qualifier, TEST_QUALIFIER)
            self.assertEqual(output[i].timestamp_micros, TEST_TIMESTAMP)
            self.assertEqual(output[i].labels, TEST_LABELS)
            self.assertEqual(output[i].value, b"cell_value: " + str(i).encode("utf-8"))

    def test_finish_row_no_row(self):
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


class TestChunkHasField:
    def test__chunk_has_field_empty(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._read_rows_state_machine import _chunk_has_field

        chunk = ReadRowsResponse.CellChunk()._pb
        assert not _chunk_has_field(chunk, "family_name")
        assert not _chunk_has_field(chunk, "qualifier")

    def test__chunk_has_field_populated_empty_strings(self):
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
        from google.cloud.bigtable.data._read_rows_state_machine import _chunk_has_field

        chunk = ReadRowsResponse.CellChunk(qualifier=b"", family_name="")._pb
        assert _chunk_has_field(chunk, "family_name")
        assert _chunk_has_field(chunk, "qualifier")



