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

from abc import ABC, abstractmethod

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
    """
    RowMerger takes in a stream of ReadRows chunks
    and processes them into a stream of RowResponses.

    RowMerger can wrap the stream directly, or use a cache to decouple
    the producer from the consumer

    RowMerger uses a StateMachine instance to handle the chunk parsing
    logic
    """
    def __init__(self):
        self.state_machine: StateMachine = StateMachine()

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
                yield self.state_machine.handle_last_scanned_row(last_scanned)
            # process new chunks through the state machine.
            for chunk in row_response.chunks:
                complete_row = self.state_machine.handle_chunk(chunk)
                if complete_row is not None:
                    yield complete_row
        if not self.state_machine.is_terminal_state():
            # read rows is complete, but there's still data in the merger
            raise RuntimeError("read_rows completed with partial state remaining")

    async def _generator_to_cache(
        self, cache: asyncio.Queue[Any], input_generator: AsyncIterable[Any]
    ) -> None:
        async for item in input_generator:
            await cache.put(item)

    async def merge_row_stream_with_cache(
        self,
        request_generator: AsyncIterable[ReadRowsResponse],
        max_cache_size: int | None = None,
    ) -> None:
        if max_cache_size is None:
            max_cache_size = -1
        cache: asyncio.Queue[RowResponse] = asyncio.Queue(max_cache_size)

        stream_task = asyncio.create_task(
            self._generator_to_cache(cache, self.merge_row_stream(request_generator))
        )
        # read from state machine and push into cache
        while not stream_task.done() or not cache.empty():
            if not cache.empty():
                yield await cache.get()
            else:
                # wait for either the stream to finish, or a new item to enter the cache
                get_from_cache = asyncio.create_task(cache.get())
                await asyncio.wait(
                    [stream_task, get_from_cache], return_when=asyncio.FIRST_COMPLETED
                )
                if get_from_cache.done():
                    yield get_from_cache.result()
        # stream and cache are complete. if there's an exception, raise it
        if stream_task.exception():
            raise cast(Exception, stream_task.exception())


class StateMachine:
    """
    State Machine converts chunks into RowResponses

    Chunks are added to the state machine via handle_chunk, which
    transitions the state machine through the various states.

    When a row is complete, it will be returned from handle_chunk,
    and the state machine will reset to AWAITING_NEW_ROW

    If an unexpected chunk is received for the current state,
    the state machine will raise an InvalidChunk exception
    """
    def __init__(self):
        self.completed_row_keys: Set[bytes] = set({})
        # represents either the last row emitted, or the last_scanned_key sent from backend
        # all future rows should have keys > last_seen_row_key
        self.last_seen_row_key: bytes | None = None
        self.adapter: "RowBuilder" = RowBuilder()
        self._reset_row()

    def _reset_row(self) -> None:
        """
        Drops the current row and transitions to AWAITING_NEW_ROW to start a fresh one
        """
        self.current_state: State = AWAITING_NEW_ROW(self)
        self.current_family : bytes | None = None
        self.current_qualifier : bytes | None = None
        # self.expected_cell_size:int = 0
        # self.remaining_cell_bytes:int = 0
        # self.num_cells_in_row:int = 0
        self.adapter.reset()

    def is_terminal_state(self) -> bool:
        """
        Returns true if the state machine is in a terminal state (AWAITING_NEW_ROW)

        At the end of the read_rows stream, if the state machine is not in a terminal
        state, an exception should be raised
        """
        return isinstance(self.current_state, AWAITING_NEW_ROW)

    def handle_last_scanned_row(self, last_scanned_row_key: bytes) -> RowResponse:
        """
        Called by RowMerger to notify the state machine of a scan heartbeat

        Returns an empty row with the last_scanned_row_key
        """
        if self.last_seen_row_key and self.last_seen_row_key >= last_scanned_row_key:
            raise InvalidChunk("Last scanned row key is out of order")
        if not isinstance(self.current_state, AWAITING_NEW_ROW):
            raise InvalidChunk("Last scanned row key received in invalid state")
        scan_marker = RowResponse(last_scanned_row_key, [])
        self._handle_complete_row(scan_marker)
        return scan_marker

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> RowResponse | None:
        """
        Called by RowMerger to process a new chunk

        Returns a RowResponse if the chunk completes a row, otherwise returns None
        """
        if chunk.row_key in self.completed_row_keys:
            raise InvalidChunk(f"duplicate row key: {chunk.row_key.decode()}")
        if self.last_seen_row_key and self.last_seen_row_key >= chunk.row_key:
            raise InvalidChunk("Out of order row keys")
        if chunk.reset_row:
            # reset row if requested
            self._handle_reset_row(chunk)
        else:
            # otherwise, process the chunk and update the state
            self.current_state = self.current_state.handle_chunk(chunk)
        if chunk.commit_row:
            # check if row is complete, and return it if so
            if not isinstance(self.current_state, AWAITING_NEW_CELL):
                raise InvalidChunk("commit row attempted without finishing cell")
            complete_row = self.adapter.finish_row()
            self._handle_complete_row(complete_row)
            return complete_row
        else:
            # row is not complete, return None
            return None

    def _handle_complete_row(self, complete_row:RowResponse) -> None:
        """
        Complete row, update seen keys, and move back to AWAITING_NEW_ROW

        Called by StateMachine when a commit_row flag is set on a chunk,
        or when a scan heartbeat is received
        """
        self.last_seen_row_key = complete_row.row_key
        self.completed_row_keys.add(complete_row.row_key)
        self._reset_row()

    def _handle_reset_chunk(self, chunk: ReadRowsResponse.CellChunk):
        """
        Drop all buffers and reset the row in progress

        Called by StateMachine when a reset_row flag is set on a chunk
        """
        # ensure reset chunk matches expectations
        if isinstance(self.current_state, AWAITING_NEW_ROW):
            raise InvalidChunk("reset chunk received when not processing row")
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
        self._reset_row()
        if not isinstance(self.current_state, AWAITING_NEW_ROW):
            raise RuntimeError("Failed to reset state machine")


class State(ABC):
    """
    Represents a state the state machine can be in

    Each state is responsible for handling the next chunk, and then
    transitioning to the next state
    """
    def __init__(self, owner: StateMachine):
        self.owner = owner

    @abstractmethod
    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        pass


class AWAITING_NEW_ROW(State):
    """
    Default state
    Awaiting a chunk to start a new row
    Exit states:
      - AWAITING_NEW_CELL: when a chunk with a row_key is received
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        if not chunk.row_key:
            raise InvalidChunk("New row is missing a row key")
        self._owner.adapter.start_row(chunk.row_key)
        # the first chunk signals both the start of a new row and the start of a new cell, so
        # force the chunk processing in the AWAITING_CELL_VALUE.
        return AWAITING_NEW_CELL(self._owner).handle_chunk(chunk)


class AWAITING_NEW_CELL(State):
    """
    Represents a cell boundary witin a row

    Exit states:
    - AWAITING_NEW_CELL: when the incoming cell is complete and ready for another
    - AWAITING_CELL_VALUE: when the value is split across multiple chunks
    """

    def handle_chunk(self, chunk: ReadRowsResponse.CellChunk) -> "State":
        chunk_size = len(chunk.value)
        is_split = chunk.value_size > 0
        expected_cell_size = chunk.value_size if is_split else chunk_size
        # track latest cell data. New chunks won't send repeated data
        if chunk.family_name:
            self._owner.current_family = chunk.family_name
            if not chunk.qualifier:
                raise InvalidChunk("new column family must specify qualifier")
        if chunk.qualifier:
            self._owner.current_qualifier = chunk.qualifier
            if self._owner.current_family is None:
                raise InvalidChunk("family not found")

        # ensure that all chunks after the first one are either missing a row
        # key or the row is the same
        if (
            chunk.row_key is not None
            and chunk.row_key != self._owner.adapter.current_key
        ):
            raise InvalidChunk("row key changed mid row")

        self._owner.adapter.start_cell(
            family=self._owner.current_family,
            qualifier=self._owner.current_qualifier,
            labels=chunk.labels,
            timestamp=chunk.timestamp_micros, 
            size=expected_cell_size,
        )
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if is_split:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            return AWAITING_NEW_CELL(self._owner)


class AWAITING_CELL_VALUE(State):
    """
    State that represents a split cell's continuation

    Exit states:
    - AWAITING_NEW_CELL: when the cell is complete
    - AWAITING_CELL_VALUE: when additional value chunks are required
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
        is_last = chunk.value_size == 0
        self._owner.adapter.cell_value(chunk.value)
        # transition to new state
        if not is_last:
            return AWAITING_CELL_VALUE(self._owner)
        else:
            # cell is complete
            self._owner.adapter.finish_cell()
            return AWAITING_NEW_CELL(self._owner)


class RowBuilder:
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

    def __init__(self):
        # initialize state
        self.reset()

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        self.current_key: bytes | None = None
        self.working_cell: CellResponse | None = None
        self.working_value: bytearray | None = None
        self.completed_cells: List[CellResponse] = []

    def start_row(self, key: bytes) -> None:
        """Called to start a new row. This will be called once per row"""
        if (
            self.current_key is not None
            or self.working_cell is not None
            or self.working_value is not None
            or self.completed_cells
        ):
            raise InvalidChunk("start_row called without finishing previous row")
        self.current_key = key

    def start_cell(
        self,
        family: str,
        qualifier: bytes,
        timestamp_micros: int,
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
        self.working_value = bytearray(size)
        timestamp_nanos = timestamp_micros * 1000
        self.working_cell = CellResponse(
            b"", self.current_key, family, qualifier, labels, timestamp_nanos
        )

    def cell_value(self, value: bytes) -> None:
        """called multiple times per cell to concatenate the cell value"""
        if self.working_value is None:
            raise InvalidChunk("cell value received before start_cell")
        self.working_value.extend(value)

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        if self.working_cell is None or self.working_value is None:
            raise InvalidChunk("cell value received before start_cell")
        self.working_cell.value = bytes(self.working_value)
        self.completed_cells.append(self.working_cell)
        self.working_cell = None
        self.working_value = None

    def finish_row(self) -> RowResponse:
        """called once per row to signal that all cells have been processed (unless reset)"""
        if self.current_key is None:
            raise InvalidChunk("no row in progress")
        new_row = RowResponse(self.current_key, self.completed_cells)
        self.reset()
        return new_row
