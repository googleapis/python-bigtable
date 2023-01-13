# Copyright 2018 Google LLC
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

"""User friendly container for Google Cloud Bigtable MutationBatcher."""
import threading
import queue
import concurrent.futures


FLUSH_COUNT = 1000
MAX_MUTATIONS = 100000
MAX_ROW_BYTES = 5242880  # 5MB


class MaxMutationsError(ValueError):
    """The number of mutations for bulk request is too big."""


class _MutationsBatchQueue(object):
    """Private Threadsafe Queue to hold rows for batching."""

    def __init__(self, max_row_bytes=MAX_ROW_BYTES, flush_count=FLUSH_COUNT):
        """Specify the queue constraints"""
        self._queue = queue.Queue(maxsize=flush_count)
        self.total_mutation_count = 0
        self.total_size = 0
        self.max_row_bytes = max_row_bytes

    def get(self, block=True, timeout=None):
        """Retrieve an item from the queue. Recalculate queue size."""
        row = self._queue.get(block=block, timeout=timeout)
        mutation_size = row.get_mutations_size()
        self.total_mutation_count -= len(row._get_mutations())
        self.total_size -= mutation_size
        return row

    def put(self, item, block=True, timeout=None):
        """Insert an item to the queue. Recalculate queue size."""
        self.total_size += item.get_mutations_size()
        self.total_mutation_count += len(item._get_mutations())
        self._queue.put(item, block=block, timeout=timeout)

    def full(self):
        """Check if the queue is full."""
        if self.total_size >= self.max_row_bytes or self._queue.full():
            return True
        return False

    def empty(self):
        return self._queue.empty()


class MutationsBatcher(object):
    """A MutationsBatcher is used in batch cases where the number of mutations
    is large or unknown. It will store DirectRows in memory until one of the
    size limits is reached, or an explicit call to flush() is performed. When
    a flush event occurs, the DirectRows in memory will be sent to Cloud
    Bigtable. Batching mutations is more efficient than sending individual
    request.

    This class is not suited for usage in systems where each mutation
    must be guaranteed to be sent, since calling mutate may only result in an
    in-memory change. In a case of a system crash, any DirectRows remaining in
    memory will not necessarily be sent to the service, even after the
    completion of the mutate() method.

    TODO: Performance would dramatically improve if this class had the
    capability of asynchronous, parallel RPCs.

    :type table: class
    :param table: class:`~google.cloud.bigtable.table.Table`.

    :type flush_count: int
    :param flush_count: (Optional) Max number of rows to flush. If it
    reaches the max number of rows it calls finish_batch() to mutate the
    current row batch. Default is FLUSH_COUNT (1000 rows).

    :type max_row_bytes: int
    :param max_row_bytes: (Optional) Max number of row mutations size to
    flush. If it reaches the max number of row mutations size it calls
    finish_batch() to mutate the current row batch. Default is MAX_ROW_BYTES
    (5 MB).
    """

    def __init__(self, table, flush_count=FLUSH_COUNT, max_row_bytes=MAX_ROW_BYTES):
        self.rows = _MutationsBatchQueue(
            max_row_bytes=max_row_bytes, flush_count=flush_count
        )
        self.table = table
        self.flushed_counter = 0
        self.executor = concurrent.futures.ThreadPoolExecutor()
        threading.Timer(1, self.flush).start()

    def __enter__(self):
        """Starting the MutationsBatcher as a context manager"""
        return self

    def mutate(self, row):
        """Add a row to the batch. If the current batch meets one of the size
        limits, the batch is sent synchronously.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_api_batcher_mutate]
            :end-before: [END bigtable_api_batcher_mutate]
            :dedent: 4

        :type row: class
        :param row: class:`~google.cloud.bigtable.row.DirectRow`.

        :raises: One of the following:
                 * :exc:`~.table._BigtableRetryableError` if any
                   row returned a transient error.
                 * :exc:`RuntimeError` if the number of responses doesn't
                   match the number of rows that were retried
                 * :exc:`.batcher.MaxMutationsError` if any row exceeds max
                   mutations count.
        """
        if self.rows.full():
            self.flush_async()
        self.rows.put(row)

    def mutate_rows(self, rows):
        """Add multiple rows to the batch. If the current batch meets one of the size
        limits, the batch is sent synchronously.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_api_batcher_mutate_rows]
            :end-before: [END bigtable_api_batcher_mutate_rows]
            :dedent: 4

        :type rows: list:[`~google.cloud.bigtable.row.DirectRow`]
        :param rows: list:[`~google.cloud.bigtable.row.DirectRow`].

        :raises: One of the following:
                 * :exc:`~.table._BigtableRetryableError` if any
                   row returned a transient error.
                 * :exc:`RuntimeError` if the number of responses doesn't
                   match the number of rows that were retried
                 * :exc:`.batcher.MaxMutationsError` if any row exceeds max
                   mutations count.
        """
        for row in rows:
            self.mutate(row)

    def flush(self):
        """Sends the current. batch to Cloud Bigtable.
        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_api_batcher_flush]
            :end-before: [END bigtable_api_batcher_flush]
            :dedent: 4

        """
        rows_to_flush = []
        while not self.rows.empty():
            rows_to_flush.append(self.rows.get())
        self.flush_rows(rows_to_flush)

    def flush_async(self):
        rows_to_flush = []
        while not self.rows.empty():
            rows_to_flush.append(self.rows.get())
        self.executor.submit(self.flush_rows, rows_to_flush)

    def flush_rows(self, rows_to_flush=None):
        """Mutate the specified rows."""
        if len(rows_to_flush) > 0:
            self.table.mutate_rows(rows_to_flush)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Clean up resources. Flush and shutdown the ThreadPoolExecutor."""
        self.close()

    def close(self):
        """Clean up resources. Flush and shutdown the ThreadPoolExecutor."""
        self.flush()
        self.executor.shutdown(wait=True)
