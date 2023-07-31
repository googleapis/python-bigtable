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

from typing import (
    List,
    Any,
    AsyncIterable,
    AsyncIterator,
    AsyncGenerator,
    Iterator,
    Callable,
    Awaitable,
)
import sys
import time
import asyncio
from functools import partial
from grpc.aio import RpcContext

from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable.data.row import Row, _LastScannedRow, Cell
from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data.exceptions import _RowSetComplete
from google.cloud.bigtable.data.exceptions import IdleTimeout
from google.api_core import retry_async as retries
from google.api_core.retry_streaming_async import AsyncRetryableGenerator
from google.api_core.retry import exponential_sleep_generator
from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.data._helpers import _make_metadata
from google.cloud.bigtable.data._helpers import _attempt_timeout_generator
from google.cloud.bigtable.data._helpers import _convert_retry_deadline


class _ReadRowsOperationAsync(AsyncIterable[Row]):
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
        operation_timeout: float = 600.0,
        attempt_timeout: float | None = None,
    ):
        """
        Args:
          - request: the request dict to send to the Bigtable API
          - client: the Bigtable client to use to make the request
          - operation_timeout: the timeout to use for the entire operation, in seconds
          - attempt_timeout: the timeout to use when waiting for each individual grpc request, in seconds
                If not specified, defaults to operation_timeout
        """
        self._last_emitted_row_key: bytes | None = None
        self._emit_count = 0
        self._request = request
        self.operation_timeout = operation_timeout
        # use generator to lower per-attempt timeout as we approach operation_timeout deadline
        attempt_timeout_gen = _attempt_timeout_generator(
            attempt_timeout, operation_timeout
        )
        row_limit = request.get("rows_limit", 0)
        # lock in paramters for retryable wrapper
        retryable_stream = partial(
            self._read_rows_retryable_attempt,
            client.read_rows,
            attempt_timeout_gen,
            row_limit,
        )
        retryable_as_list = partial(
            self._read_rows_retryable_attempt_as_list,
            client.read_rows,
            attempt_timeout_gen,
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

        self._stream: AsyncGenerator[Row, None] | None = AsyncRetryableGenerator(
            retryable_stream,
            predicate,
            exponential_sleep_generator(
                0.01, 60, multiplier=2
            ),
            self.operation_timeout,
            on_error_fn,
        )
        # self._as_list_fn = retries.retry_target(
        #     retryable_as_list,
        #     predicate,
        #     exponential_sleep_generator(
        #         0.01, 60, multiplier=2
        #     ),
        #     self.operation_timeout,
        #     on_error_fn,
        # )
        self._as_list_fn = retryable_as_list
        # contains the list of errors that were retried
        self.transient_errors: List[Exception] = []

    def __aiter__(self) -> AsyncIterator[Row]:
        """Implements the AsyncIterable interface"""
        return self

    async def __anext__(self) -> Row:
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
        self._emitted_seen_row_key = None

    async def _read_rows_retryable_attempt_as_list(
        self,
        gapic_fn: Callable[..., Awaitable[AsyncIterable[ReadRowsResponse]]],
        timeout_generator: Iterator[float],
        total_row_limit: int,
    ) -> List[Row]:
        output_list = [row async for row in self._read_rows_retryable_attempt(gapic_fn, timeout_generator, total_row_limit)]
        return output_list

    async def _read_rows_retryable_attempt(
        self,
        gapic_fn: Callable[..., Awaitable[AsyncIterable[ReadRowsResponse]]],
        timeout_generator: Iterator[float],
        total_row_limit: int,
    ) -> AsyncGenerator[Row, None]:
        """
        Retryable wrapper for merge_rows. This function is called each time
        a retry is attempted.

        Some fresh state is created on each retry:
          - grpc network stream
          - state machine to hold merge chunks received from stream
        Some state is shared between retries:
          - _last_emitted_row_key is used to ensure that
            duplicate rows are not emitted
          - request is stored and (potentially) modified on each retry
        """
        if self._last_emitted_row_key is not None:
            # if this is a retry, try to trim down the request to avoid ones we've already processed
            try:
                self._request["rows"] = _ReadRowsOperationAsync._revise_request_rowset(
                    row_set=self._request.get("rows", None),
                    last_seen_row_key=self._last_emitted_row_key,
                )
            except _RowSetComplete:
                # if there are no rows left to process, we're done
                # This is not expected to happen often, but could occur if
                # a retry is triggered quickly after the last row is emitted
                return
            # revise next request's row limit based on number emitted
            if total_row_limit:
                new_limit = total_row_limit - self._emit_count
                if new_limit == 0:
                    # we have hit the row limit, so we're done
                    return
                elif new_limit < 0:
                    raise RuntimeError("unexpected state: emit count exceeds row limit")
                else:
                    self._request["rows_limit"] = new_limit
        metadata = _make_metadata(
            self._request.get("table_name", None),
            self._request.get("app_profile_id", None),
        )
        new_gapic_stream: RpcContext = await gapic_fn(
            self._request,
            timeout=next(timeout_generator),
            metadata=metadata,
        )
        try:
            stream = _ReadRowsOperationAsync.merge_row_response_stream(
                new_gapic_stream
            )
            # run until we get a timeout or the stream is exhausted
            async for new_item in stream:
                # if (
                #     self._last_emitted_row_key is not None
                #     and new_item.row_key <= self._last_emitted_row_key
                # ):
                #     raise InvalidChunk("Last emitted row key out of order")
                # don't yeild _LastScannedRow markers; they
                # should only update last_seen_row_key
                if not isinstance(new_item, _LastScannedRow):
                    yield new_item
                    self._emit_count += 1
                self._last_emitted_row_key = new_item.row_key
                if total_row_limit and self._emit_count >= total_row_limit:
                    return
        except (Exception, GeneratorExit) as exc:
            # ensure grpc stream is closed
            new_gapic_stream.cancel()
            raise exc

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
        Raises:
          - _RowSetComplete: if there are no rows left to process after the revision
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
        # remove seen keys from user-specific key list
        row_keys: list[bytes] = row_set.get("row_keys", [])
        adjusted_keys = [k for k in row_keys if k > last_seen_row_key]
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
                start_key = row_range.get("start_key_closed", None) or row_range.get(
                    "start_key_open", None
                )
                if start_key is None or start_key <= last_seen_row_key:
                    # replace start key with last seen
                    new_range["start_key_open"] = last_seen_row_key
                    new_range.pop("start_key_closed", None)
                adjusted_ranges.append(new_range)
        if len(adjusted_keys) == 0 and len(adjusted_ranges) == 0:
            # if the query is empty after revision, raise an exception
            # this will avoid an unwanted full table scan
            raise _RowSetComplete()
        return {"row_keys": adjusted_keys, "row_ranges": adjusted_ranges}

    @staticmethod
    async def merge_row_response_stream(
        response_generator: AsyncIterable[ReadRowsResponse],
    ) -> AsyncGenerator[Row, None]:
        """
        Consume chunks from a ReadRowsResponse stream into a set of Rows

        Args:
          - response_generator: AsyncIterable of ReadRowsResponse objects. Typically
                this is a stream of chunks from the Bigtable API
        Returns:
            - AsyncGenerator of Rows
        Raises:
            - InvalidChunk: if the chunk stream is invalid
        """
        row = None
        chunk_list = []
        async for row_response in response_generator:
            # unwrap protoplus object for increased performance
            response_pb = row_response._pb
            if len(response_pb.chunks) == 0:
                # check for last scanned key
                last_scanned = response_pb.last_scanned_row_key
                # if the server sends a scan heartbeat, notify the state machine.
                if last_scanned:
                    yield _LastScannedRow(last_scanned)
            else:
                # process new chunks through the state machine.
                for chunk in response_pb.chunks:
                    chunk_list.append(chunk)
                    if chunk.commit_row:
                        yield Row._from_chunks(chunk_list)
                        chunk_list.clear()

        if row is not None:
            # read rows is complete, but there's still data in the merger
            raise InvalidChunk("read_rows completed with partial state remaining")


class ReadRowsAsyncIterator(AsyncIterable[Row]):
    """
    Async iterator for ReadRows responses.

    Supports the AsyncIterator protocol for use in async for loops,
    along with:
      - `aclose` for closing the underlying stream
      - `active` for checking if the iterator is still active
      - an internal idle timer for closing the stream after a period of inactivity
    """

    def __init__(self, merger: _ReadRowsOperationAsync):
        self._merger: _ReadRowsOperationAsync = merger
        self._error: Exception | None = None
        self._last_interaction_time = time.monotonic()
        self._idle_timeout_task: asyncio.Task[None] | None = None
        # wrap merger with a wrapper that properly formats exceptions
        self._next_fn = _convert_retry_deadline(
            self._merger.__anext__,
            self._merger.operation_timeout,
            self._merger.transient_errors,
            is_async=True,
        )

    async def _start_idle_timer(self, idle_timeout: float):
        """
        Start a coroutine that will cancel a stream if no interaction
        with the iterator occurs for the specified number of seconds.

        Subsequent access to the iterator will raise an IdleTimeout exception.

        Args:
          - idle_timeout: number of seconds of inactivity before cancelling the stream
        """
        self._last_interaction_time = time.monotonic()
        if self._idle_timeout_task is not None:
            self._idle_timeout_task.cancel()
        self._idle_timeout_task = asyncio.create_task(
            self._idle_timeout_coroutine(idle_timeout)
        )
        if sys.version_info >= (3, 8):
            self._idle_timeout_task.name = f"{self.__class__.__name__}.idle_timeout"

    @property
    def active(self):
        """
        Returns True if the iterator is still active and has not been closed
        """
        return self._error is None

    async def _idle_timeout_coroutine(self, idle_timeout: float):
        """
        Coroutine that will cancel a stream if no interaction with the iterator
        in the last `idle_timeout` seconds.
        """
        while self.active:
            next_timeout = self._last_interaction_time + idle_timeout
            await asyncio.sleep(next_timeout - time.monotonic())
            if (
                self._last_interaction_time + idle_timeout < time.monotonic()
                and self.active
            ):
                # idle timeout has expired
                await self._finish_with_error(
                    IdleTimeout(
                        (
                            "Timed out waiting for next Row to be consumed. "
                            f"(idle_timeout={idle_timeout:0.1f}s)"
                        )
                    )
                )

    def __aiter__(self):
        """Implement the async iterator protocol."""
        return self

    async def __anext__(self) -> Row:
        """
        Implement the async iterator potocol.

        Return the next item in the stream if active, or
        raise an exception if the stream has been closed.
        """
        if self._error is not None:
            raise self._error
        try:
            self._last_interaction_time = time.monotonic()
            return await self._next_fn()
        except Exception as e:
            await self._finish_with_error(e)
            raise e

    async def _finish_with_error(self, e: Exception):
        """
        Helper function to close the stream and clean up resources
        after an error has occurred.
        """
        if self.active:
            await self._merger.aclose()
            self._error = e
        if self._idle_timeout_task is not None:
            self._idle_timeout_task.cancel()
            self._idle_timeout_task = None

    async def aclose(self):
        """
        Support closing the stream with an explicit call to aclose()
        """
        await self._finish_with_error(
            StopAsyncIteration(f"{self.__class__.__name__} closed")
        )
