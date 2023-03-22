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

from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
from google.cloud.bigtable.row_response import RowResponse, CellResponse
import asyncio

from typing import (
    cast,
    List,
    Dict,
    Set,
    Any,
    AsyncIterable,
    AsyncGenerator,
    Tuple,
)


class InvalidChunk(RuntimeError):
    """Exception raised to invalid chunk data from back-end."""


class RowMerger:
    def __init__(self, max_queue_size: int|None = None):
        if max_queue_size is None:
            max_queue_size = -1
        self.state_machine: StateMachine = StateMachine()
        self.cache: asyncio.Queue[RowResponse] = asyncio.Queue(max_queue_size)

    async def merge_row_stream(
        self, request_generator: AsyncIterable[ReadRowsResponse]
    ) -> AsyncGenerator[RowResponse, None]:
        """
        Consume chunks from a ReadRowsResponse stream into a set of Rows
        """
        async for row_response in request_generator:
            # ensure that the response is a ReadRowsResponse
            if not isinstance(row_response, ReadRowsResponse):
                row_response = ReadRowsResponse(row_response)
            last_scanned = row_response.last_scanned_row_key
            # if the server sends a scan heartbeat, notify the state machine.
            if last_scanned:
                self.state_machine.handle_last_scanned_row(last_scanned)
                if self.state_machine.has_complete_row():
                    yield self.state_machine.consume_row()
            # process new chunks through the state machine.
            for chunk in row_response.chunks:
                self.state_machine.handle_chunk(chunk)
                if self.state_machine.has_complete_row():
                    yield self.state_machine.consume_row()
        if self.state_machine.is_row_in_progress():
            # read rows is complete, but there's still data in the merger
            raise RuntimeError("read_rows completed with partial state remaining")

    async def _generator_to_cache(self, input_generator: AsyncIterable[Any]) -> None:
        async for item in input_generator:
            await self.cache.put(item)

    async def merge_row_stream_with_cache(self, request_generator: AsyncIterable[ReadRowsResponse]) -> None:
        stream_task = asyncio.create_task(self._generator_to_cache(self.merge_row_stream(request_generator)))
         # read from state machine and push into cache
        while not stream_task.done() or not self.cache.empty():
            if not self.cache.empty():
                yield await self.cache.get()
            else:
                # wait for either the stream to finish, or a new item to enter the cache
                get_from_cache = asyncio.create_task(self.cache.get())
                await asyncio.wait(
                    [stream_task, get_from_cache], return_when=asyncio.FIRST_COMPLETED
                )
                if get_from_cache.done():
                    yield get_from_cache.result()
        # stream and cache are complete. if there's an exception, raise it
        if stream_task.exception():
            raise cast(Exception, stream_task.exception())



class StateMachine:
    def __init__(self):
        self.completed_row_keys: Set[bytes] = set({})
        self.adapter: "RowBuilder" = RowBuilder()
        self.reset()

    def reset(self) -> None:
        self.current_state: State = AWAITING_NEW_ROW(self)
        self.last_cell_data: Dict[str, Any] = {}
        # represents either the last row emitted, or the last_scanned_key sent from backend
        # all future rows should have keys > last_seen_row_key
        self.last_seen_row_key: bytes | None = None
        # self.expected_cell_size:int = 0
        # self.remaining_cell_bytes:int = 0
        self.complete_row: RowResponse | None = None
        # self.num_cells_in_row:int = 0
        self.adapter.reset()

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> None:
        if self.last_seen_row_key and self.last_seen_row_key >= last_scanned_row_key:
            raise InvalidChunk("Last scanned row key is out of order")
        self.last_scanned_row_key = last_scanned_row_key
        self.current_state = self.current_state.handle_last_scanned_row(
            last_scanned_row_key
        )

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> None:
        if chunk.row_key in self.completed_row_keys:
            raise InvalidChunk(f"duplicate row key: {chunk.row_key.decode()}")
        self.current_state = self.current_state.handle_chunk(chunk)

    def has_complete_row(self) -> bool:
        return (
            isinstance(self.current_state, AWAITING_ROW_CONSUME)
            and self.complete_row is not None
        )

    def consume_row(self) -> RowResponse:
        """
        Returns the last completed row and transitions to a new row
        """
        if not self.has_complete_row() or self.complete_row is None:
            raise RuntimeError("No row to consume")
        row = self.complete_row
        self.reset()
        self.completed_row_keys.add(row.row_key)
        return row

    def is_row_in_progress(self) -> bool:
        return not isinstance(self.current_state, AWAITING_NEW_ROW)

    def handle_commit_row(self) -> "State":
        """
        Called when a row is complete.
        Wait in AWAITING_ROW_CONSUME state for the RowMerger to consume it
        """
        self.complete_row = self.adapter.finish_row()
        self.last_seen_row_key = self.complete_row.row_key
        return AWAITING_ROW_CONSUME(self)

    def handle_reset_chunk(
        self, chunk: ReadRowsResponse.CellChunk
    ) -> "AWAITING_NEW_ROW":
        """
        When a reset chunk comes in, drop all buffers and reset to AWAITING_NEW_ROW state
        """
        # ensure reset chunk matches expectations
        if isinstance(self.current_state, AWAITING_NEW_ROW):
            raise InvalidChunk("Bare reset")
        if chunk.row_key:
            raise InvalidChunk("Reset chunk has a row key")
        if "family_name" in chunk:
            raise InvalidChunk("Reset chunk has family_name")
        if "qualifier" in chunk:
            raise InvalidChunk("Reset chunk has qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("Reset chunk has a timestamp")
        if chunk.labels:
            raise InvalidChunk("Reset chunk has labels")
        if chunk.value:
            raise InvalidChunk("Reset chunk has a value")
        self.reset()
        if not isinstance(self.current_state, AWAITING_NEW_ROW):
            raise RuntimeError("Failed to reset state machine")
        return self.current_state


class State:
    def __init__(self, owner: "StateMachine"):
        self._owner = owner

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> "State":
        raise NotImplementedError

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        raise NotImplementedError


class AWAITING_NEW_ROW(State):
    """
    Default state
    Awaiting a chunk to start a new row
    Exit states: any (depending on chunk)
    """

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> "State":
        self._owner.complete_row = self._owner.adapter.create_scan_marker_row(
            last_scanned_row_key
        )
        return AWAITING_ROW_CONSUME(self._owner)

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        if not chunk.row_key:
            raise InvalidChunk("New row is missing a row key")
        if (
            self._owner.last_seen_row_key
            and self._owner.last_seen_row_key >= chunk.row_key
        ):
            raise InvalidChunk("Out of order row keys")
        self._owner.adapter.start_row(chunk.row_key)
        # the first chunk signals both the start of a new row and the start of a new cell, so
        # force the chunk processing in the AWAITING_CELL_VALUE.
        return AWAITING_NEW_CELL(self._owner).handle_chunk(chunk)


class AWAITING_NEW_CELL(State):
    """
    Represents a cell boundary witin a row
    Exit states: any (depending on chunk)
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        if chunk.reset_row:
            return self._owner.handle_reset_chunk(chunk)
        chunk_size = len(chunk.value)
        is_split = chunk.value_size > 0
        expected_cell_size = chunk.value_size if is_split else chunk_size
        # track latest cell data. New chunks won't send repeated data
        if chunk.family_name:
            self._owner.last_cell_data["family"] = chunk.family_name
            if not chunk.qualifier:
                raise InvalidChunk("new column family must specify qualifier")
        if chunk.qualifier:
            self._owner.last_cell_data["qualifier"] = chunk.qualifier
            if not self._owner.last_cell_data.get("family", False):
                raise InvalidChunk("family not found")
        self._owner.last_cell_data["labels"] = chunk.labels
        self._owner.last_cell_data["timestamp"] = chunk.timestamp_micros

        # ensure that all chunks after the first one either are missing a row
        # key or the row is the same
        if (
            self._owner.adapter.row_in_progress()
            and chunk.row_key
            and chunk.row_key != self._owner.adapter.current_key
        ):
            raise InvalidChunk("row key changed mid row")

        self._owner.adapter.start_cell(
            **self._owner.last_cell_data,
            size=expected_cell_size,
        )
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if is_split:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            if chunk.commit_row:
                # row is also complete
                return self._owner.handle_commit_row()
            else:
                # wait for more cells for this row
                return AWAITING_NEW_CELL(self._owner)


class AWAITING_CELL_VALUE(State):
    """
    State that represents a split cell's continuation
    Exit states: any (depending on chunk)
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        # ensure reset chunk matches expectations
        if chunk.row_key:
            raise InvalidChunk("found row key mid cell")
        if "family_name" in chunk:
            raise InvalidChunk("In progress cell had a family name")
        if "qualifier" in chunk:
            raise InvalidChunk("In progress cell had a qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("In progress cell had a timestamp")
        if chunk.labels:
            raise InvalidChunk("In progress cell had labels")
        # check for reset row
        if chunk.reset_row:
            return self._owner.handle_reset_chunk(chunk)
        is_last = chunk.value_size == 0
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if not is_last:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            if chunk.commit_row:
                # row is also complete
                return self._owner.handle_commit_row()
            else:
                # wait for more cells for this row
                return AWAITING_NEW_CELL(self._owner)


class AWAITING_ROW_CONSUME(State):
    """
    Represents a completed row. Prevents new rows being read until it is consumed
    Exit states: AWAITING_NEW_ROW
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        raise RuntimeError("Skipping completed row")


class RowBuilder:
    """
    called by state machine to build rows
    State machine makes the following guarantees:
        Exactly 1 `start_row` for each row.
        Exactly 1 `start_cell` for each cell.
        At least 1 `cell_value` for each cell.
        Exactly 1 `finish_cell` for each cell.
        Exactly 1 `finish_row` for each row.
    `create_scan_marker_row` can be called one or more times between `finish_row` and
    `start_row`. `reset` can be called at any point and can be invoked multiple times in
    a row.
    """

    def __init__(self):
        self.reset()

    def row_in_progress(self) -> bool:
        return self.current_key is not None

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        self.current_key: bytes | None = None
        self.working_cell: Tuple[CellResponse, bytearray] | None = None
        self.completed_cells: List[CellResponse] = []

    def create_scan_marker_row(self, key: bytes) -> RowResponse:
        """creates a special row to mark server progress before any data is received"""
        return RowResponse(key, [])

    def start_row(self, key: bytes) -> None:
        """Called to start a new row. This will be called once per row"""
        self.current_key = key

    def start_cell(
        self,
        family: str,
        qualifier: bytes,
        timestamp: int,
        labels: List[str],
        size: int,
    ) -> None:
        """called to start a new cell in a row."""
        if not family:
            raise InvalidChunk("missing family for a new cell")
        if qualifier is None:
            raise InvalidChunk("missing qualifier for a new cell")
        if self.current_key is None:
            raise InvalidChunk("no row in progress")
        working_value = bytearray(size)
        self.working_cell = (
            CellResponse(b"", self.current_key, family, qualifier, labels, timestamp),
            working_value,
        )

    def cell_value(self, value: bytes) -> None:
        """called multiple times per cell to concatenate the cell value"""
        if self.working_cell is None:
            raise InvalidChunk("cell value received before start_cell")
        self.working_cell[1].extend(value)

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        if self.working_cell is None:
            raise InvalidChunk("cell value received before start_cell")
        complete_cell, complete_value = self.working_cell
        if not complete_value:
            raise InvalidChunk("cell value was never set")
        complete_cell.value = bytes(complete_value)
        self.completed_cells.append(complete_cell)
        self.working_cell = None

    def finish_row(self) -> RowResponse:
        """called once per row to signal that all cells have been processed (unless reset)"""
        if self.current_key is None:
            raise InvalidChunk("no row in progress")
        new_row = RowResponse(self.current_key, self.completed_cells)
        self.reset()
        return new_row
