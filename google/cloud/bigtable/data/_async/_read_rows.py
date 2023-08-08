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


from google.cloud.bigtable_v2.types import ReadRowsRequest as ReadRowsRequestPB
from google.cloud.bigtable_v2.types import RowSet as RowSetPB
from google.cloud.bigtable_v2.types import RowRange as RowRangePB

from google.cloud.bigtable.data.row import Row, Cell
from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data.exceptions import InvalidChunk
from google.cloud.bigtable.data.exceptions import RetryExceptionGroup
from google.cloud.bigtable.data._helpers import _attempt_timeout_generator
from google.cloud.bigtable.data._helpers import _make_metadata

from google.api_core import exceptions as core_exceptions


class _ResetRow(Exception):
    pass

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

    def __init__(
        self,
        query: ReadRowsQuery,
        table: "TableAsync",
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
        self._last_yielded_row_key: bytes | None = None
        self._remaining_count = self.request.rows_limit or None
        self._metadata = _make_metadata(
            table.table_name,
            table.app_profile_id,
        )

    def start_operation(self):
        s = self.table.client._gapic_client.read_rows(self.request)
        s = self.chunk_stream(s)
        return self.merge_rows(s)

    @staticmethod
    async def chunk_stream(stream):
        prev_key = None

        async for resp in await stream:
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

    @staticmethod
    async def merge_rows(chunks):
        it = chunks.__aiter__()
        # For each row
        while True:
            try:
                c = await it.__anext__()
            except StopAsyncIteration:
                # stream complete
                return

            row_key = c.row_key

            if not row_key:
                raise InvalidChunk("first row chunk is missing key")

            # Cells
            cells = []

            # shared per cell storage
            family = None
            qualifier = None

            try:
                # for each cell
                while True:
                    if c.reset_row:
                        break
                    k = c.row_key
                    f = c.family_name if c.HasField("family_name") else None
                    q = c.qualifier if c.HasField("qualifier") else None
                    if k and k != row_key:
                        raise InvalidChunk("unexpected new row key")
                    if f is not None:
                        family = f.value
                        if q is not None:
                            qualifier = q.value
                        else:
                            raise InvalidChunk("new family without qualifier")
                    elif q is not None:
                        qualifier = q.value

                    ts = c.timestamp_micros
                    labels = list(c.labels) if c.labels else []
                    value = c.value

                    # merge split cells
                    if c.value_size > 0:
                        buffer = [value]
                        # throws when early eos
                        c = await it.__anext__()

                        while c.value_size > 0:
                            buffer.append(c.value)

                            # throws when premature end
                            c = await it.__anext__()

                            if c.reset_row:
                                raise _ResetRow()
                        else:
                            buffer.append(c.value)
                        value = b''.join(buffer)
                    cells.append(
                        Cell(row_key, family, qualifier, value, ts, labels)
                    )
                    if c.commit_row:
                        yield Row(row_key, cells)
                        break
                    c = await it.__anext__()
            except _ResetRow:
                continue
            except StopAsyncIteration:
                raise InvalidChunk("premature end of stream")
