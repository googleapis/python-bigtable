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


from google.cloud.bigtable.base_row_data import (
    BasePartialRowsData,
    DEFAULT_RETRY_READ_ROWS,
)


class PartialRowsData(BasePartialRowsData):
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
        # In-progress row, unset until first response, after commit/reset
        self._row = None
        # Last complete row, unset until first commit
        self._previous_row = None
        # In-progress cell, unset until first response, after completion
        self._cell = None
        # Last complete cell, unset until first completion, after new row
        self._previous_cell = None

        # May be cached from previous response
        self.last_scanned_row_key = None
        self.read_method = read_method
        self.request = request
        self.retry = retry
        self.response_iterator = read_method(request)

        self.rows = {}
        self._state = self.STATE_NEW_ROW

        # Flag to stop iteration, for any reason not related to self.retry()
        self._cancelled = False

    def _on_error(self, exc):
        """Helper for :meth:`__iter__`."""
        # restart the read scan from AFTER the last successfully read row
        retry_request = self.request
        if self.last_scanned_row_key:
            retry_request = self._create_retry_request()

        self.response_iterator = self.read_method(retry_request)
