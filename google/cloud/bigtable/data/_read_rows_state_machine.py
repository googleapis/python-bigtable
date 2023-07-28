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

from typing import Type

from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
from google.cloud.bigtable.data.row import Row, Cell, _LastScannedRow
from google.cloud.bigtable.data.exceptions import InvalidChunk

"""
This module provides classes for the read_rows state machine:

- ReadRowsOperation is the highest level class, providing an interface for asynchronous
  merging end-to-end
- StateMachine is used internally to track the state of the merge, including
  the current row key and the keys of the rows that have been processed.
  It processes a stream of chunks, and will raise InvalidChunk if it reaches
  an invalid state.
- State classes track the current state of the StateMachine, and define what
  to do on the next chunk.
- RowBuilder is used by the StateMachine to build a Row object.
"""


class _StateMachine:
    """
    State Machine converts chunks into Rows

    Chunks are added to the state machine via handle_chunk, which
    transitions the state machine through the various states.

    When a row is complete, it will be returned from handle_chunk,
    and the state machine will reset to AWAITING_NEW_ROW

    If an unexpected chunk is received for the current state,
    the state machine will raise an InvalidChunk exception

    The server may send a heartbeat message indicating that it has
    processed a particular row, to facilitate retries. This will be passed
    to the state machine via handle_last_scanned_row, which emit a
    _LastScannedRow marker to the stream.
    """

    __slots__ = (
        "current_state",
        "last_seen_row_key",
        "adapter",
    )

    def __init__(self):
        # represents either the last row emitted, or the last_scanned_key sent from backend
        # all future rows should have keys > last_seen_row_key
        self.last_seen_row_key: bytes | None = None
        self.adapter = _RowBuilder()
        self._reset_row()

    def _reset_row(self) -> None:
        """
        Drops the current row and transitions to AWAITING_NEW_ROW to start a fresh one
        """
        self.current_state: Type[_State] = AWAITING_NEW_ROW
        self.adapter.reset()

    def is_terminal_state(self) -> bool:
        """
        Returns true if the state machine is in a terminal state (AWAITING_NEW_ROW)

        At the end of the read_rows stream, if the state machine is not in a terminal
        state, an exception should be raised
        """
        return self.current_state == AWAITING_NEW_ROW

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> Row:
        """
        Called by ReadRowsOperation to notify the state machine of a scan heartbeat

        Returns an empty row with the last_scanned_row_key
        """
        if self.last_seen_row_key and self.last_seen_row_key >= last_scanned_row_key:
            raise InvalidChunk("Last scanned row key is out of order")
        if not self.current_state == AWAITING_NEW_ROW:
            raise InvalidChunk("Last scanned row key received in invalid state")
        scan_marker = _LastScannedRow(last_scanned_row_key)
        self._handle_complete_row(scan_marker)
        return scan_marker

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> Row | None:
        """
        Called by ReadRowsOperation to process a new chunk

        Returns a Row if the chunk completes a row, otherwise returns None
        """
        if chunk.reset_row:
            # reset row if requested
            self._handle_reset_chunk(chunk)
            return None

        # process the chunk and update the state
        self.current_state = self.current_state.handle_chunk(self, chunk)
        if chunk.commit_row:
            # check if row is complete, and return it if so
            if not self.current_state == AWAITING_NEW_CELL:
                raise InvalidChunk("Commit chunk received in invalid state")
            complete_row = self.adapter.finish_row()
            self._handle_complete_row(complete_row)
            return complete_row
        else:
            # row is not complete, return None
            return None

    def _handle_complete_row(self, complete_row: Row) -> None:
        """
        Complete row, update seen keys, and move back to AWAITING_NEW_ROW

        Called by StateMachine when a commit_row flag is set on a chunk,
        or when a scan heartbeat is received
        """
        self.last_seen_row_key = complete_row.row_key
        self._reset_row()

    def _handle_reset_chunk(self, chunk: ReadRowsResponse.CellChunk):
        """
        Drop all buffers and reset the row in progress

        Called by StateMachine when a reset_row flag is set on a chunk
        """
        self._reset_row()


class _State:
    """
    Represents a state the state machine can be in

    Each state is responsible for handling the next chunk, and then
    transitioning to the next state
    """

    @staticmethod
    def handle_chunk(
        owner: _StateMachine, chunk: ReadRowsResponse.CellChunk
    ) -> Type["_State"]:
        raise NotImplementedError


class AWAITING_NEW_ROW(_State):
    """
    Default state
    Awaiting a chunk to start a new row
    Exit states:
      - AWAITING_NEW_CELL: when a chunk with a row_key is received
    """

    @staticmethod
    def handle_chunk(
        owner: _StateMachine, chunk: ReadRowsResponse.CellChunk
    ) -> Type["_State"]:
        owner.adapter.start_row(chunk.row_key)
        # the first chunk signals both the start of a new row and the start of a new cell, so
        # force the chunk processing in the AWAITING_CELL_VALUE.
        return AWAITING_NEW_CELL.handle_chunk(owner, chunk)


class AWAITING_NEW_CELL(_State):
    """
    Represents a cell boundary witin a row

    Exit states:
    - AWAITING_NEW_CELL: when the incoming cell is complete and ready for another
    - AWAITING_CELL_VALUE: when the value is split across multiple chunks
    """

    @staticmethod
    def handle_chunk(
        owner: _StateMachine, chunk: ReadRowsResponse.CellChunk
    ) -> Type["_State"]:
        is_split = chunk.value_size > 0
        owner.adapter.start_cell(chunk)
        # transition to new state
        if is_split:
            return AWAITING_CELL_VALUE
        else:
            # cell is complete
            owner.adapter.finish_cell()
            return AWAITING_NEW_CELL


class AWAITING_CELL_VALUE(_State):
    """
    State that represents a split cell's continuation

    Exit states:
    - AWAITING_NEW_CELL: when the cell is complete
    - AWAITING_CELL_VALUE: when additional value chunks are required
    """

    @staticmethod
    def handle_chunk(
        owner: _StateMachine, chunk: ReadRowsResponse.CellChunk
    ) -> Type["_State"]:
        is_last = chunk.value_size == 0
        owner.adapter.cell_value(chunk)
        # transition to new state
        if not is_last:
            return AWAITING_CELL_VALUE
        else:
            # cell is complete
            owner.adapter.finish_cell()
            return AWAITING_NEW_CELL


class _RowBuilder:
    """
    called by state machine to build rows
    State machine makes the following guarantees:
        Exactly 1 `start_row` for each row.
        Exactly 1 `start_cell` for each cell.
        At least 1 `cell_value` for each cell.
        Exactly 1 `finish_cell` for each cell.
        Exactly 1 `finish_row` for each row.
    `reset` can be called at any point and can be invoked multiple times in
    a row.
    """

    __slots__ = "current_key", "working_cell", "completed_cells"

    def __init__(self):
        # initialize state
        self.completed_cells : list[Cell] = []
        self.current_key: bytes | None = None
        self.working_cell: Cell | None = None

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        self.current_key = None
        self.working_cell = None
        self.completed_cells = []

    def start_row(self, key: bytes) -> None:
        """Called to start a new row. This will be called once per row"""
        self.current_key = key

    def start_cell(
        self,
        chunk: ReadRowsResponse.CellChunk,
    ) -> None:
        """called to start a new cell in a row."""
        if self.current_key is None:
            raise InvalidChunk("start_cell called without a row")
        self.working_cell = Cell(self.current_key, self.working_cell, chunk)

    def cell_value(self, chunk: ReadRowsResponse.CellChunk) -> None:
        """called multiple times per cell to concatenate the cell value"""
        self.working_cell._add_chunk(chunk)

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        if self.working_cell is None:
            raise InvalidChunk("finish_cell called before start_cell")
        self.completed_cells.append(self.working_cell)
        self.working_cell = None

    def finish_row(self) -> Row:
        """called once per row to signal that all cells have been processed (unless reset)"""
        if self.current_key is None:
            raise InvalidChunk("No row in progress")
        new_row = Row(self.current_key, self.completed_cells)
        self.reset()
        return new_row
