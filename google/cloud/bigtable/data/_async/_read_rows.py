
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
    Any,
)

from google.cloud.bigtable_v2.types import ReadRowsRequest as ReadRowsRequestPB
from google.cloud.bigtable_v2.types import RowSet as RowSetPB
from google.cloud.bigtable_v2.types import RowRange as RowRangePB

from google.cloud.bigtable.data.row import Row, Cell
from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data._async.client import TableAsync
from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data.exceptions import _RowSetComplete
from google.cloud.bigtable.data.exceptions import RetryExceptionGroup
from google.cloud.bigtable.data._helpers import _attempt_timeout_generator
from google.cloud.bigtable.data._helpers import _convert_retry_deadline
from google.cloud.bigtable.data._helpers import _make_metadata

from google.api_core import retry_async as retries
from google.api_core.retry_streaming_async import AsyncRetryableGenerator
from google.api_core.retry import exponential_sleep_generator
from google.api_core import exceptions as core_exceptions

class _ResetRow(Exception):
    pass


class _ReadRowsOperationAsync():
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
        query : ReadRowsQuery,
        table : TableAsync,
        operation_timeout: float,
        attempt_timeout: float,
    ):
        self.attempt_timeout_gen = _attempt_timeout_generator(
            attempt_timeout, operation_timeout
        )
        self.operation_timeout = operation_timeout
        query_dict = query._to_dict() if not isinstance(query, dict) else query
        self.request = ReadRowsRequestPB(**query_dict, table_name=table.table_name)
        if table.app_profile_id:
            self.request.app_profile_id = table.app_profile_id
        self.table = table
        self._last_yielded_row_key : bytes | None = None
        self._remaining_count = self.request.rows_limit or None
        self._metadata = _make_metadata(
            table.table_name,
            table.app_profile_id,
        )

    async def make_retry_stream(self):
        stream, transient_errors = self._make_stream_helper(AsyncRetryableGenerator, self.read_rows_attempt)
        try:
            async for row in stream:
                yield row
        except core_exceptions.RetryError:
            self._raise_retry_error(transient_errors)

    async def make_retry_stream_as_list(self):
        retry_fn, transient_errors = self._make_stream_helper(retries.retry_target, self.read_rows_attempt_as_list)
        try:
            return await retry_fn
        except core_exceptions.RetryError:
            self._raise_retry_error(transient_errors)

    def _raise_retry_error(self, transient_errors):
        timeout_value = self.operation_timeout
        timeout_str = f" of {timeout_value:.1f}s" if timeout_value is not None else ""
        error_str = f"operation_timeout{timeout_str} exceeded"
        new_exc = core_exceptions.DeadlineExceeded(
            error_str,
        )
        source_exc = None
        if transient_errors:
            source_exc = RetryExceptionGroup(transient_errors)
        new_exc.__cause__ = source_exc
        raise new_exc from source_exc

    def _make_stream_helper(self, retry_wrapper, attempt_fn):
        transient_errors = []
        predicate = retries.if_exception_type(
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
            core_exceptions.Aborted,
        )
        def on_error_fn(exc):
            if predicate(exc):
                transient_errors.append(exc)
        retry_fn = retry_wrapper(
            attempt_fn,
            predicate,
            exponential_sleep_generator(
                0.01, 60, multiplier=2
            ),
            self.operation_timeout,
            on_error_fn,
        )
        return retry_fn, transient_errors

    async def read_rows_attempt(self):
        if self._last_yielded_row_key is not None:
            # if this is a retry, try to trim down the request to avoid ones we've already processed
            try:
                self.request.rows = self._revise_request_rowset(
                    row_set=self.request.rows,
                    last_seen_row_key=self._last_yielded_row_key,
                )
            except _RowSetComplete:
                return
        if self._remaining_count is not None:
            self.request.rows_limit = self._remaining_count
        s = await self.table.client._gapic_client.read_rows(self.request, timeout=next(self.attempt_timeout_gen), metadata=self._metadata)
        s = self.chunk_stream(s)
        return self.merge_rows(s)

    async def read_rows_attempt_as_list(self):
        stream = await self.read_rows_attempt()
        return [row async for row in stream]

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
        if row_set is None or (
            not row_set.row_ranges and not row_set.row_keys is None
        ):
            last_seen = last_seen_row_key
            return RowSetPB(row_ranges=[RowRangePB(start_key_open=last_seen)])
        # remove seen keys from user-specific key list
        adjusted_keys : list[bytes] = [k for k in row_set.row_keys if k > last_seen_row_key]
        # adjust ranges to ignore keys before last seen
        adjusted_ranges : list[RowRangePB] = []
        for row_range in row_set.row_ranges:
            end_key = row_range.end_key_closed or row_range.end_key_open
            if end_key is None or end_key > last_seen_row_key:
                # end range is after last seen key
                new_range = row_range.copy()
                start_key = row_range.start_key_closed or row_range.start_key_open
                if start_key is None or start_key <= last_seen_row_key:
                    # replace start key with last seen
                    new_range.start_key_open = last_seen_row_key
                    new_range.start_key_closed = None
                adjusted_ranges.append(new_range)
        if len(adjusted_keys) == 0 and len(adjusted_ranges) == 0:
            # if the query is empty after revision, raise an exception
            # this will avoid an unwanted full table scan
            raise _RowSetComplete()
        return RowSetPB(row_keys=adjusted_keys, row_ranges=adjusted_ranges)

    @staticmethod
    async def chunk_stream(stream):
        prev_key : bytes | None = None

        async for resp in stream:
            resp = resp._pb

            if resp.last_scanned_row_key:
                if prev_key is not None and resp.last_scanned_row_key >= prev_key:
                    raise InvalidChunk("last scanned out of order")
                prev_key = resp.last_scanned_row_key

            current_key = None

            for c in resp.chunks:
                if current_key is None:
                    current_key = c.row_key
                    if current_key is None:
                        raise InvalidChunk("first chunk is missing a row key")
                    elif prev_key and current_key <= prev_key:
                        raise InvalidChunk("out of order row key")

                yield c

                if c.reset_row:
                    current_key = None
                elif c.commit_row:
                    prev_key = current_key

    async def merge_rows(self, chunks):
        it = chunks.__aiter__()

        # For each row
        try:
            while True:
                c = await it.__anext__()

                # EOS
                if c is None:
                    return

                if c.reset_row:
                    continue

                row_key = c.row_key

                if not row_key:
                    raise InvalidChunk("first row chunk is missing key")

                # Cells
                cells : list[Cell] = []

                # shared per cell storage
                family = c.family_name
                qualifier = c.qualifier

                try:
                    # for each cell
                    while True:
                        f = c.family_name
                        q = c.qualifier
                        if f:
                            family = f
                            qualifier = q
                        if q:
                            qualifier = q

                        ts = c.timestamp_micros
                        labels : list[str] | None = list(c.labels) if c.labels else None
                        value = c.value

                        # merge split cells
                        if c.value_size > 0:
                            buffer = [value]
                            # throws when early eos
                            c = await it.__anext__()

                            while c.value_size > 0:
                                f = c.family_name
                                q = c.qualifier
                                t = c.timestamp_micros
                                l = c.labels
                                if f and f != family:
                                    raise InvalidChunk("family changed mid cell")
                                if q and q != qualifier:
                                    raise InvalidChunk("qualifier changed mid cell")
                                if t and t != ts:
                                    raise InvalidChunk("timestamp changed mid cell")
                                if l and l != labels:
                                    raise InvalidChunk("labels changed mid cell")

                                buffer.append(c.value)

                                # throws when premature end
                                c = await it.__anext__()

                                if c.reset_row:
                                    raise _ResetRow()
                            else:
                                buffer.append(c.value)
                            value = b''.join(buffer)

                        cells.append(Cell(row_key, family, qualifier, value, ts, labels))
                        if c.commit_row:
                            yield Row(row_key, cells)
                            self._last_yielded_row_key = row_key
                            if self._remaining_count is not None:
                                self._remaining_count -= 1
                            break
                        c = await it.__anext__()
                except _ResetRow:
                    continue
        except StopAsyncIteration:
            pass
