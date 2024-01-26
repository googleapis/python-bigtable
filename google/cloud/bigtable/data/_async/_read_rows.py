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
    TYPE_CHECKING,
    AsyncGenerator,
    Awaitable,
    Sequence,
)
import time

from google.cloud.bigtable_v2.types import ReadRowsRequest as ReadRowsRequestPB
from google.cloud.bigtable_v2.types import ReadRowsResponse as ReadRowsResponsePB
from google.cloud.bigtable_v2.types import RowSet as RowSetPB
from google.cloud.bigtable_v2.types import RowRange as RowRangePB

from google.cloud.bigtable.data.row import Row, Cell
from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data.exceptions import _RowSetComplete
from google.cloud.bigtable.data._helpers import _attempt_timeout_generator
from google.cloud.bigtable.data._helpers import _make_metadata
from google.cloud.bigtable.data._helpers import backoff_generator

from google.api_core.grpc_helpers_async import GrpcAsyncStream
from google.cloud.bigtable.data._helpers import _retry_exception_factory

from google.api_core import retry as retries

if TYPE_CHECKING:
    from google.cloud.bigtable.data._async.client import TableAsync
    from google.cloud.bigtable.data._metrics import ActiveOperationMetric


class _ResetRow(Exception):
    def __init__(self, chunk):
        self.chunk = chunk


class _ReadRowsOperationAsync:
    """
    ReadRowsOperation handles the logic of merging chunks from a ReadRowsResponse stream
    into a stream of Row objects.

    ReadRowsOperation.merge_row_response_stream takes in a stream of ReadRowsResponse
    and turns them into a stream of Row objects using an internal
    StateMachine.

    ReadRowsOperation(request, client) handles row merging logic end-to-end, including
    performing retries on stream errors.
    """

    __slots__ = (
        "attempt_timeout_gen",
        "operation_timeout",
        "request",
        "table",
        "_predicate",
        "_metadata",
        "_last_yielded_row_key",
        "_remaining_count",
        "_operation_metrics",
    )

    def __init__(
        self,
        query: ReadRowsQuery,
        table: "TableAsync",
        operation_timeout: float,
        attempt_timeout: float,
        metrics: ActiveOperationMetric,
        retryable_exceptions: Sequence[type[Exception]] = (),
    ):
        self.attempt_timeout_gen = _attempt_timeout_generator(
            attempt_timeout, operation_timeout
        )
        self.operation_timeout = operation_timeout
        if isinstance(query, dict):
            self.request = ReadRowsRequestPB(
                **query,
                table_name=table.table_name,
                app_profile_id=table.app_profile_id,
            )
        else:
            self.request = query._to_pb(table)
        self.table = table
        self._predicate = retries.if_exception_type(*retryable_exceptions)
        self._metadata = _make_metadata(
            table.table_name,
            table.app_profile_id,
        )
        self._last_yielded_row_key: bytes | None = None
        self._remaining_count: int | None = self.request.rows_limit or None
        self._operation_metrics = metrics

    def start_operation(self) -> AsyncGenerator[Row, None]:
        """
        Start the read_rows operation, retrying on retryable errors.
        """
        self._operation_metrics.start()

        sleep_generator = backoff_generator()
        self._operation_metrics.backoff_generator = sleep_generator

        return retries.retry_target_stream_async(
            self._read_rows_attempt,
            self._operation_metrics.build_wrapped_predicate(self._predicate),
            sleep_generator,
            self.operation_timeout,
            exception_factory=_retry_exception_factory,
        )

    def _read_rows_attempt(self) -> AsyncGenerator[Row, None]:
        """
        Attempt a single read_rows rpc call.
        This function is intended to be wrapped by retry logic,
        which will call this function until it succeeds or
        a non-retryable error is raised.
        """
        # register metric start
        self._operation_metrics.start_attempt()
        # revise request keys and ranges between attempts
        if self._last_yielded_row_key is not None:
            # if this is a retry, try to trim down the request to avoid ones we've already processed
            try:
                self.request.rows = self._revise_request_rowset(
                    row_set=self.request.rows,
                    last_seen_row_key=self._last_yielded_row_key,
                )
            except _RowSetComplete:
                # if we've already seen all the rows, we're done
                return self.merge_rows(None, self._operation_metrics)
        # revise the limit based on number of rows already yielded
        if self._remaining_count is not None:
            self.request.rows_limit = self._remaining_count
            if self._remaining_count == 0:
                return self.merge_rows(None, self._operation_metrics)
        # create and return a new row merger
        gapic_stream = self.table.client._gapic_client.read_rows(
            self.request,
            timeout=next(self.attempt_timeout_gen),
            metadata=self._metadata,
            retry=None,
        )
        chunked_stream = self.chunk_stream(gapic_stream)
        return self.merge_rows(chunked_stream, self._operation_metrics)

    async def chunk_stream(
        self, stream: Awaitable[GrpcAsyncStream[ReadRowsResponsePB]]
    ) -> AsyncGenerator[ReadRowsResponsePB.CellChunk, None]:
        """
        process chunks out of raw read_rows stream
        """
        call = await stream
        try:
            async for resp in call:
                # extract proto from proto-plus wrapper
                resp = resp._pb

                # handle last_scanned_row_key packets, sent when server
                # has scanned past the end of the row range
                if resp.last_scanned_row_key:
                    if (
                        self._last_yielded_row_key is not None
                        and resp.last_scanned_row_key <= self._last_yielded_row_key
                    ):
                        raise InvalidChunk("last scanned out of order")
                    self._last_yielded_row_key = resp.last_scanned_row_key

                current_key = None
                # process each chunk in the response
                for c in resp.chunks:
                    if current_key is None:
                        current_key = c.row_key
                        if current_key is None:
                            raise InvalidChunk("first chunk is missing a row key")
                        elif (
                            self._last_yielded_row_key
                            and current_key <= self._last_yielded_row_key
                        ):
                            raise InvalidChunk("row keys should be strictly increasing")

                    yield c

                    if c.reset_row:
                        current_key = None
                    elif c.commit_row:
                        # update row state after each commit
                        self._last_yielded_row_key = current_key
                        if self._remaining_count is not None:
                            self._remaining_count -= 1
                            if self._remaining_count < 0:
                                raise InvalidChunk("emit count exceeds row limit")
                        current_key = None
        finally:
            # ensure stream is closed
            call.cancel()
            # send trailing metadata to metrics
            metadata = await call.trailing_metadata() + await call.initial_metadata()
            self._operation_metrics.add_response_metadata(metadata)

    @staticmethod
    async def merge_rows(
        chunks: AsyncGenerator[ReadRowsResponsePB.CellChunk, None] | None,
        operation: ActiveOperationMetric,
    ):
        """
        Merge chunks into rows
        """
        if chunks is None:
            operation.end_with_success()
            return
        it = chunks.__aiter__()
        is_first_row = True
        # For each row
        while True:
            try:
                c = await it.__anext__()
            except StopAsyncIteration:
                # stream complete
                operation.end_with_success()
                return
            row_key = c.row_key

            if not row_key:
                raise InvalidChunk("first row chunk is missing key")

            cells = []

            # shared per cell storage
            family: str | None = None
            qualifier: bytes | None = None

            try:
                # for each cell
                while True:
                    if c.reset_row:
                        raise _ResetRow(c)
                    k = c.row_key
                    f = c.family_name.value
                    q = c.qualifier.value if c.HasField("qualifier") else None
                    if k and k != row_key:
                        raise InvalidChunk("unexpected new row key")
                    if f:
                        family = f
                        if q is not None:
                            qualifier = q
                        else:
                            raise InvalidChunk("new family without qualifier")
                    elif family is None:
                        raise InvalidChunk("missing family")
                    elif q is not None:
                        if family is None:
                            raise InvalidChunk("new qualifier without family")
                        qualifier = q
                    elif qualifier is None:
                        raise InvalidChunk("missing qualifier")

                    ts = c.timestamp_micros
                    labels = c.labels if c.labels else []
                    value = c.value

                    # merge split cells
                    if c.value_size > 0:
                        buffer = [value]
                        while c.value_size > 0:
                            # throws when premature end
                            c = await it.__anext__()

                            t = c.timestamp_micros
                            cl = c.labels
                            k = c.row_key
                            if (
                                c.HasField("family_name")
                                and c.family_name.value != family
                            ):
                                raise InvalidChunk("family changed mid cell")
                            if (
                                c.HasField("qualifier")
                                and c.qualifier.value != qualifier
                            ):
                                raise InvalidChunk("qualifier changed mid cell")
                            if t and t != ts:
                                raise InvalidChunk("timestamp changed mid cell")
                            if cl and cl != labels:
                                raise InvalidChunk("labels changed mid cell")
                            if k and k != row_key:
                                raise InvalidChunk("row key changed mid cell")

                            if c.reset_row:
                                raise _ResetRow(c)
                            buffer.append(c.value)
                        value = b"".join(buffer)
                    cells.append(
                        Cell(value, row_key, family, qualifier, ts, list(labels))
                    )
                    if c.commit_row:
                        if is_first_row:
                            # record first row latency in metrics
                            is_first_row = False
                            operation.attempt_first_response()
                        block_time = time.monotonic()
                        yield Row(row_key, cells)
                        # most metric operations use setters, but this one updates
                        # the value directly to avoid extra overhead
                        operation.active_attempt.application_blocking_time += (  # type: ignore
                            time.monotonic() - block_time
                        )
                        break
                    c = await it.__anext__()
            except _ResetRow as e:
                c = e.chunk
                if (
                    c.row_key
                    or c.HasField("family_name")
                    or c.HasField("qualifier")
                    or c.timestamp_micros
                    or c.labels
                    or c.value
                ):
                    raise InvalidChunk("reset row with data")
                continue
            except StopAsyncIteration:
                raise InvalidChunk("premature end of stream")

    @staticmethod
    def _revise_request_rowset(
        row_set: RowSetPB,
        last_seen_row_key: bytes,
    ) -> RowSetPB:
        """
        Revise the rows in the request to avoid ones we've already processed.

        Args:
          - row_set: the row set from the request
          - last_seen_row_key: the last row key encountered
        Raises:
          - _RowSetComplete: if there are no rows left to process after the revision
        """
        # if user is doing a whole table scan, start a new one with the last seen key
        if row_set is None or (not row_set.row_ranges and row_set.row_keys is not None):
            last_seen = last_seen_row_key
            return RowSetPB(row_ranges=[RowRangePB(start_key_open=last_seen)])
        # remove seen keys from user-specific key list
        adjusted_keys: list[bytes] = [
            k for k in row_set.row_keys if k > last_seen_row_key
        ]
        # adjust ranges to ignore keys before last seen
        adjusted_ranges: list[RowRangePB] = []
        for row_range in row_set.row_ranges:
            end_key = row_range.end_key_closed or row_range.end_key_open or None
            if end_key is None or end_key > last_seen_row_key:
                # end range is after last seen key
                new_range = RowRangePB(row_range)
                start_key = row_range.start_key_closed or row_range.start_key_open
                if start_key is None or start_key <= last_seen_row_key:
                    # replace start key with last seen
                    new_range.start_key_open = last_seen_row_key
                adjusted_ranges.append(new_range)
        if len(adjusted_keys) == 0 and len(adjusted_ranges) == 0:
            # if the query is empty after revision, raise an exception
            # this will avoid an unwanted full table scan
            raise _RowSetComplete()
        return RowSetPB(row_keys=adjusted_keys, row_ranges=adjusted_ranges)
