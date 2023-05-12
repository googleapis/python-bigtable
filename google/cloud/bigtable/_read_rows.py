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
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.types import RequestStats
from google.cloud.bigtable.row import Row, Cell, _LastScannedRow
from google.cloud.bigtable.exceptions import InvalidChunk
import asyncio
from functools import partial
from google.api_core import retry_async as retries
from google.api_core import exceptions as core_exceptions


from typing import (
    List,
    Any,
    AsyncIterable,
    AsyncIterator,
    AsyncGenerator,
    Callable,
    Awaitable,
    Type,
)

"""
This module provides a set of classes for merging ReadRowsResponse chunks
into Row objects.

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


class _ReadRowsOperation(AsyncIterable[Row]):
    """
    ReadRowsOperation handles the logic of merging chunks from a ReadRowsResponse stream
    into a stream of Row objects.

    ReadRowsOperation.merge_row_response_stream takes in a stream of ReadRowsResponse
    and turns them into a stream of Row objects using an internal
    StateMachine.

    ReadRowsOperation(request, client) handles row merging logic end-to-end, including
    performing retries on stream errors.
    """

    def __init__(
        self,
        request: dict[str, Any],
        client: BigtableAsyncClient,
        *,
        buffer_size: int = 0,
        operation_timeout: float | None = None,
        per_request_timeout: float | None = None,
    ):
        """
        Args:
          - request: the request dict to send to the Bigtable API
          - client: the Bigtable client to use to make the request
          - buffer_size: the size of the buffer to use for caching rows from the network
          - operation_timeout: the timeout to use for the entire operation, in seconds
          - per_request_timeout: the timeout to use when waiting for each individual grpc request, in seconds
        """
        self._last_seen_row_key: bytes | None = None
        self._emit_count = 0
        buffer_size = max(buffer_size, 0)
        self._request = request
        self.operation_timeout = operation_timeout
        row_limit = request.get("rows_limit", 0)
        # lock in paramters for retryable wrapper
        self._partial_retryable = partial(
            self._read_rows_retryable_attempt,
            client.read_rows,
            buffer_size,
            per_request_timeout,
            row_limit,
        )
        predicate = retries.if_exception_type(
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
            core_exceptions.Aborted,
        )

        def on_error_fn(exc):
            if predicate(exc):
                self.transient_errors.append(exc)

        retry = retries.AsyncRetry(
            predicate=predicate,
            timeout=self.operation_timeout,
            initial=0.01,
            multiplier=2,
            maximum=60,
            on_error=on_error_fn,
            is_stream=True,
        )
        self._stream: AsyncGenerator[Row | RequestStats, None] | None = retry(
            self._partial_retryable
        )()
        # contains the list of errors that were retried
        self.transient_errors: List[Exception] = []

    def __aiter__(self) -> AsyncIterator[Row | RequestStats]:
        """Implements the AsyncIterable interface"""
        return self

    async def __anext__(self) -> Row | RequestStats:
        """Implements the AsyncIterator interface"""
        if self._stream is not None:
            return await self._stream.__anext__()
        else:
            raise asyncio.InvalidStateError("stream is closed")

    async def aclose(self):
        """Close the stream and release resources"""
        if self._stream is not None:
            await self._stream.aclose()
        self._stream = None
        self._last_seen_row_key = None

    @staticmethod
    async def _generator_to_buffer(
        buffer: asyncio.Queue[Any], input_generator: AsyncIterable[Any]
    ) -> None:
        """
        Helper function to push items from an async generator into a buffer
        """
        try:
            async for item in input_generator:
                await buffer.put(item)
                await asyncio.sleep(0)
            await buffer.put(StopAsyncIteration)
        except Exception as e:
            await buffer.put(e)

    @staticmethod
    async def _buffer_to_generator(
        buffer: asyncio.Queue[Any],
    ) -> AsyncGenerator[Any, None]:
        """
        Helper function to yield items from a buffer as an async generator
        """
        while True:
            item = await buffer.get()
            if item is StopAsyncIteration:
                return
            if isinstance(item, Exception):
                raise item
            yield item
            await asyncio.sleep(0)

    async def _read_rows_retryable_attempt(
        self,
        gapic_fn: Callable[..., Awaitable[AsyncIterable[ReadRowsResponse]]],
        buffer_size: int,
        per_request_timeout: float | None,
        total_row_limit: int,
    ) -> AsyncGenerator[Row | RequestStats, None]:
        """
        Retryable wrapper for merge_rows. This function is called each time
        a retry is attempted.

        Some fresh state is created on each retry:
          - grpc network stream
          - buffer for the stream
          - state machine to hold merge chunks received from stream
        Some state is shared between retries:
          - last_seen_row_key is used to ensure that
            duplicate rows are not emitted
          - request is stored and (optionally) modified on each retry
        """
        if self._last_seen_row_key is not None:
            # if this is a retry, try to trim down the request to avoid ones we've already processed
            self._request["rows"] = _ReadRowsOperation._revise_request_rowset(
                row_set=self._request.get("rows", None),
                last_seen_row_key=self._last_seen_row_key,
            )
            # revise next request's row limit based on number emitted
            if total_row_limit:
                new_limit = total_row_limit - self._emit_count
                if new_limit <= 0:
                    return
                else:
                    self._request["rows_limit"] = new_limit
        params_str = f'table_name={self._request.get("table_name", "")}'
        if self._request.get("app_profile_id", None):
            params_str = (
                f'{params_str},app_profile_id={self._request.get("app_profile_id", "")}'
            )
        new_gapic_stream = await gapic_fn(
            self._request,
            timeout=per_request_timeout,
            metadata=[("x-goog-request-params", params_str)],
        )
        buffer: asyncio.Queue[Row | RequestStats | Exception] = asyncio.Queue(
            maxsize=buffer_size
        )
        buffer_task = asyncio.create_task(
            self._generator_to_buffer(buffer, new_gapic_stream)
        )
        buffered_stream = self._buffer_to_generator(buffer)
        state_machine = _StateMachine()
        try:
            stream = _ReadRowsOperation.merge_row_response_stream(
                buffered_stream, state_machine
            )
            # run until we get a timeout or the stream is exhausted
            while True:
                new_item = await stream.__anext__()
                # ignore rows that have already been emitted
                if isinstance(new_item, Row) and (
                    self._last_seen_row_key is None
                    or new_item.row_key > self._last_seen_row_key
                ):
                    self._last_seen_row_key = new_item.row_key
                    # don't yeild _LastScannedRow markers; they
                    # should only update last_seen_row_key
                    if not isinstance(new_item, _LastScannedRow):
                        yield new_item
                        self._emit_count += 1
                        if total_row_limit and self._emit_count >= total_row_limit:
                            return
                elif isinstance(new_item, RequestStats):
                    yield new_item
        except StopAsyncIteration:
            # end of stream
            return
        finally:
            buffer_task.cancel()

    @staticmethod
    def _revise_request_rowset(
        row_set: dict[str, Any] | None,
        last_seen_row_key: bytes,
    ) -> dict[str, Any]:
        """
        Revise the rows in the request to avoid ones we've already processed.

        Args:
          - row_set: the row set from the request
          - last_seen_row_key: the last row key encountered
        """
        # if user is doing a whole table scan, start a new one with the last seen key
        if row_set is None or (
            len(row_set.get("row_ranges", [])) == 0
            and len(row_set.get("row_keys", [])) == 0
        ):
            last_seen = last_seen_row_key
            return {
                "row_keys": [],
                "row_ranges": [{"start_key_open": last_seen}],
            }
        else:
            # remove seen keys from user-specific key list
            row_keys: list[bytes] = row_set.get("row_keys", [])
            adjusted_keys = []
            for key in row_keys:
                if key > last_seen_row_key:
                    adjusted_keys.append(key)
            # adjust ranges to ignore keys before last seen
            row_ranges: list[dict[str, Any]] = row_set.get("row_ranges", [])
            adjusted_ranges = []
            for row_range in row_ranges:
                end_key = row_range.get("end_key_closed", None) or row_range.get(
                    "end_key_open", None
                )
                if end_key is None or end_key > last_seen_row_key:
                    # end range is after last seen key
                    new_range = row_range.copy()
                    start_key = row_range.get(
                        "start_key_closed", None
                    ) or row_range.get("start_key_open", None)
                    if start_key is None or start_key <= last_seen_row_key:
                        # replace start key with last seen
                        new_range["start_key_open"] = last_seen_row_key
                        new_range.pop("start_key_closed", None)
                    adjusted_ranges.append(new_range)
            # if our modifications result in an empty row_set, return the
            # original row_set. This will avoid an unwanted full table scan
            if len(adjusted_keys) == 0 and len(adjusted_ranges) == 0:
                return row_set
            return {"row_keys": adjusted_keys, "row_ranges": adjusted_ranges}

    @staticmethod
    async def merge_row_response_stream(
        request_generator: AsyncIterable[ReadRowsResponse], state_machine: _StateMachine
    ) -> AsyncGenerator[Row | RequestStats, None]:
        """
        Consume chunks from a ReadRowsResponse stream into a set of Rows

        Args:
          - request_generator: AsyncIterable of ReadRowsResponse objects. Typically
                this is a stream of chunks from the Bigtable API
        Returns:
            - AsyncGenerator of Rows
        Raises:
            - InvalidChunk: if the chunk stream is invalid
        """
        async for row_response in request_generator:
            # unwrap protoplus object for increased performance
            response_pb = row_response._pb
            last_scanned = response_pb.last_scanned_row_key
            # if the server sends a scan heartbeat, notify the state machine.
            if last_scanned:
                yield state_machine.handle_last_scanned_row(last_scanned)
            # process new chunks through the state machine.
            for chunk in response_pb.chunks:
                complete_row = state_machine.handle_chunk(chunk)
                if complete_row is not None:
                    yield complete_row
            # yield request stats if present
            if row_response.request_stats:
                yield row_response.request_stats
        if not state_machine.is_terminal_state():
            # read rows is complete, but there's still data in the merger
            raise InvalidChunk("read_rows completed with partial state remaining")


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
        "current_family",
        "current_qualifier",
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
        self.current_family: str | None = None
        self.current_qualifier: bytes | None = None
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
        if (
            self.last_seen_row_key
            and chunk.row_key
            and self.last_seen_row_key >= chunk.row_key
        ):
            raise InvalidChunk("row keys should be strictly increasing")
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
        # ensure reset chunk matches expectations
        if self.current_state == AWAITING_NEW_ROW:
            raise InvalidChunk("Reset chunk received when not processing row")
        if chunk.row_key:
            raise InvalidChunk("Reset chunk has a row key")
        if _chunk_has_field(chunk, "family_name"):
            raise InvalidChunk("Reset chunk has a family name")
        if _chunk_has_field(chunk, "qualifier"):
            raise InvalidChunk("Reset chunk has a qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("Reset chunk has a timestamp")
        if chunk.labels:
            raise InvalidChunk("Reset chunk has labels")
        if chunk.value:
            raise InvalidChunk("Reset chunk has a value")
        self._reset_row()


class _State:
    """
    Represents a state the state machine can be in

    Each state is responsible for handling the next chunk, and then
    transitioning to the next state
    """

    @staticmethod
    def handle_chunk(owner:_StateMachine, chunk: ReadRowsResponse.CellChunk) -> "_State":
        raise NotImplementedError


class AWAITING_NEW_ROW(_State):
    """
    Default state
    Awaiting a chunk to start a new row
    Exit states:
      - AWAITING_NEW_CELL: when a chunk with a row_key is received
    """

    @staticmethod
    def handle_chunk(owner:_StateMachine, chunk: ReadRowsResponse.CellChunk) -> Type["_State"]:
        if not chunk.row_key:
            raise InvalidChunk("New row is missing a row key")
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
    def handle_chunk(owner:_StateMachine, chunk: ReadRowsResponse.CellChunk) -> Type["_State"]:
        is_split = chunk.value_size > 0
        # track latest cell data. New chunks won't send repeated data
        has_family = _chunk_has_field(chunk, "family_name")
        has_qualifier = _chunk_has_field(chunk, "qualifier")
        if has_family:
            owner.current_family = chunk.family_name.value
            if not has_qualifier:
                raise InvalidChunk("New family must specify qualifier")
        if has_qualifier:
            owner.current_qualifier = chunk.qualifier.value
            if owner.current_family is None:
                raise InvalidChunk("Family not found")

        # ensure that all chunks after the first one are either missing a row
        # key or the row is the same
        if chunk.row_key and chunk.row_key != owner.adapter.current_key:
            raise InvalidChunk("Row key changed mid row")

        if owner.current_family is None:
            raise InvalidChunk("Missing family for new cell")
        if owner.current_qualifier is None:
            raise InvalidChunk("Missing qualifier for new cell")

        owner.adapter.start_cell(
            family=owner.current_family,
            qualifier=owner.current_qualifier,
            labels=list(chunk.labels),
            timestamp_micros=chunk.timestamp_micros,
        )
        owner.adapter.cell_value(chunk.value)
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
    def handle_chunk(owner:_StateMachine, chunk: ReadRowsResponse.CellChunk) -> Type["_State"]:
        # ensure reset chunk matches expectations
        if chunk.row_key:
            raise InvalidChunk("In progress cell had a row key")
        if _chunk_has_field(chunk, "family_name"):
            raise InvalidChunk("In progress cell had a family name")
        if _chunk_has_field(chunk, "qualifier"):
            raise InvalidChunk("In progress cell had a qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("In progress cell had a timestamp")
        if chunk.labels:
            raise InvalidChunk("In progress cell had labels")
        is_last = chunk.value_size == 0
        owner.adapter.cell_value(chunk.value)
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

    __slots__ = "current_key", "working_cell", "working_value", "completed_cells"

    def __init__(self):
        # initialize state
        self.reset()

    def reset(self) -> None:
        """called when the current in progress row should be dropped"""
        self.current_key: bytes | None = None
        self.working_cell: Cell | None = None
        self.working_value: bytearray | None = None
        self.completed_cells: List[Cell] = []

    def start_row(self, key: bytes) -> None:
        """Called to start a new row. This will be called once per row"""
        self.current_key = key

    def start_cell(
        self,
        family: str,
        qualifier: bytes,
        timestamp_micros: int,
        labels: List[str],
    ) -> None:
        """called to start a new cell in a row."""
        if self.current_key is None:
            raise InvalidChunk("start_cell called without a row")
        self.working_value = bytearray()
        self.working_cell = Cell(
            b"", self.current_key, family, qualifier, timestamp_micros, labels
        )

    def cell_value(self, value: bytes) -> None:
        """called multiple times per cell to concatenate the cell value"""
        if self.working_value is None:
            raise InvalidChunk("Cell value received before start_cell")
        self.working_value.extend(value)

    def finish_cell(self) -> None:
        """called once per cell to signal the end of the value (unless reset)"""
        if self.working_cell is None or self.working_value is None:
            raise InvalidChunk("finish_cell called before start_cell")
        self.working_cell.value = bytes(self.working_value)
        self.completed_cells.append(self.working_cell)
        self.working_cell = None
        self.working_value = None

    def finish_row(self) -> Row:
        """called once per row to signal that all cells have been processed (unless reset)"""
        if self.current_key is None:
            raise InvalidChunk("No row in progress")
        new_row = Row(self.current_key, self.completed_cells)
        self.reset()
        return new_row


def _chunk_has_field(chunk: ReadRowsResponse.CellChunk, field: str) -> bool:
    """
    Returns true if the field is set on the chunk

    Required to disambiguate between empty strings and unset values
    """
    try:
        return chunk.HasField(field)
    except ValueError:
        return False
