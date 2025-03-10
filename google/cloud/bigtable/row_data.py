# Copyright 2016 Google LLC
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

"""Container for Google Cloud Bigtable Cells and Streaming Row Contents."""


import copy

import grpc  # type: ignore
import warnings
from google.api_core import exceptions
from google.api_core import retry
from google.cloud._helpers import _to_bytes  # type: ignore

from google.cloud.bigtable.row_merger import _RowMerger, _State
from google.cloud.bigtable_v2.types import bigtable as data_messages_v2_pb2
from google.cloud.bigtable_v2.types import data as data_v2_pb2
from google.cloud.bigtable.row import Cell, InvalidChunk, PartialRowData


# Some classes need to be re-exported here to keep backwards
# compatibility. Those classes were moved to row_merger, but we dont want to
# break enduser's imports. This hack, ensures they don't get marked as unused.
_ = (Cell, InvalidChunk, PartialRowData)


"""The default retry strategy to be used on retry-able errors.

Used by
:meth:`~google.cloud.bigtable.row_data.PartialRowsData._read_next_response`.
"""


class PartialRowsData(object):
    """Convenience wrapper for consuming a ``ReadRows`` streaming response.

    :type read_method: :class:`client._table_data_client.read_rows`
    :param read_method: ``ReadRows`` method.

    :type request: :class:`data_messages_v2_pb2.ReadRowsRequest`
    :param request: The ``ReadRowsRequest`` message used to create a
                    ReadRowsResponse iterator. If the iterator fails, a new
                    iterator is created, allowing the scan to continue from
                    the point just beyond the last successfully read row,
                    identified by self.last_scanned_row_key. The retry happens
                    inside of the Retry class, using a predicate for the
                    expected exceptions during iteration.

    :type retry: :class:`~google.api_core.retry.Retry`
    :param retry: (Optional) Retry delay and deadline arguments. To override,
                  the default value :attr:`DEFAULT_RETRY_READ_ROWS` can be
                  used and modified with the
                  :meth:`~google.api_core.retry.Retry.with_delay` method
                  or the
                  :meth:`~google.api_core.retry.Retry.with_deadline` method.
    """

    def __init__(self, read_method, request, retry=DEFAULT_RETRY_READ_ROWS):
        # Counter for rows returned to the user
        self._counter = 0
        self._row_merger = _RowMerger()

        # May be cached from previous response
        self.last_scanned_row_key = None
        self.read_method = read_method
        self.request = request
        self.retry = retry


    def consume_all(self, max_loops=None):
        """Consume the streamed responses until there are no more.

        .. warning::
           This method will be removed in future releases.  Please use this
           class as a generator instead.

        :type max_loops: int
        :param max_loops: (Optional) Maximum number of times to try to consume
                          an additional ``ReadRowsResponse``. You can use this
                          to avoid long wait times.
        """
        for row in self:
            self.rows[row.row_key] = row
