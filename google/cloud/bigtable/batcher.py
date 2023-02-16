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
import atexit

from google.api_core.exceptions import from_grpc_status


# Max number of items in the queue. Queue will be flushed if this number is reached
FLUSH_COUNT = 100

# Max number of mutations for a single row at one time
MAX_MUTATIONS = 100000

# Max size (in bytes) for a single mutation
MAX_ROW_BYTES = 20 * 1024 * 1024  # 20MB

# Max size (in bytes) for a single request
MAX_MUTATIONS_SIZE = 100 * 1024 * 1024  # 100MB


class MutationsBatchError(Exception):
    """Error in the batch request"""

    def __init__(self, message, exc):
        self.exc = exc
        self.message = message
        super().__init__(self.message)


class MaxMutationsError(ValueError):
    """The number of mutations for bulk request is too big."""


class BatcherIsClosedError(ValueError):
    """Batcher is already closed and not accepting any mutation."""


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

        mutation_count = len(item._get_mutations())
        mutation_size = item.get_mutations_size()

        if mutation_count > MAX_MUTATIONS:
            raise MaxMutationsError(
                "The row key {} exceeds the number of mutations {}.".format(
                    item.row_key, mutation_count
                )
            )

        if mutation_size > MAX_MUTATIONS_SIZE:
            raise MaxMutationsError(
                "The row key {} exceeds the size of mutations {}.".format(
                    item.row_key, mutation_size
                )
            )

        self._queue.put(item, block=block, timeout=timeout)

        self.total_size += item.get_mutations_size()
        self.total_mutation_count += mutation_count

    def full(self):
        """Check if the queue is full."""
        if (
            self.total_mutation_count >= MAX_MUTATIONS
            or self.total_size >= self.max_row_bytes
            or self._queue.full()
        ):
            return True
        return False

    def empty(self):
        return self._queue.empty()


def _batcher_is_open(func):
    """Decorator to check if the batcher is open or closed.
    :raises:
     * :exc:`.batcher.BatcherIsClosedError` if batcher is already
       closed

    """

    def wrapper(self, *args, **kwargs):
        if not self._is_open:
            raise BatcherIsClosedError()
        func(self, *args, **kwargs)

    return wrapper


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

    Note on thread safety: The same MutationBatcher cannot be shared by multiple end-user threads.

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

    :type flush_interval: float
    :param flush_interval: (Optional) The interval (in seconds) between asynchronous flush.
    Default is 1 second.
    """

    def __init__(
        self,
        table,
        flush_count=FLUSH_COUNT,
        max_row_bytes=MAX_ROW_BYTES,
        flush_interval=1,
    ):
        self._rows = _MutationsBatchQueue(
            max_row_bytes=max_row_bytes, flush_count=flush_count
        )
        self.table = table
        self._executor = concurrent.futures.ThreadPoolExecutor()
        self._is_open = True
        atexit.register(self.close)
        self._timer = threading.Timer(flush_interval, self.flush)
        self._timer.start()

    @property
    def flush_count(self):
        return self._rows._queue.maxsize

    @property
    def max_row_bytes(self):
        return self._rows.max_row_bytes

    def __enter__(self):
        """Starting the MutationsBatcher as a context manager"""
        return self

    @_batcher_is_open
    def mutate(self, row):
        """Add a row to the batch. If the current batch meets one of the size
        limits, the batch is sent asynchronously.

        :raises:
         * :exc:`.batcher.BatcherIsClosedError` if batcher is already
           closed

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
                 * :exc:`.batcherMutationsBatchError` if there's any error in the
                   mutations.
        """
        self._rows.put(row)

        if self._rows.full():
            self.flush_async()

    @_batcher_is_open
    def mutate_rows(self, rows):
        """Add multiple rows to the batch. If the current batch meets one of the size
        limits, the batch is sent asynchronously.

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
                 * :exc:`.batcherMutationsBatchError` if there's any error in the
                   mutations.
        """
        for row in rows:
            self.mutate(row)

    def flush(self):
        """Sends the current batch to Cloud Bigtable synchronously.
        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_api_batcher_flush]
            :end-before: [END bigtable_api_batcher_flush]
            :dedent: 4

        raises:
            * :exc:`.batcherMutationsBatchError` if there's any error in the
                mutations.
        """
        rows_to_flush = []
        while not self._rows.empty():
            rows_to_flush.append(self._rows.get())
        response = self.flush_rows(rows_to_flush)
        return response

    def flush_async(self):
        """Sends the current batch to Cloud Bigtable asynchronously.

        raises:
            * :exc:`.batcherMutationsBatchError` if there's any error in the
                mutations.
        """

        rows_to_flush = []
        while not self._rows.empty():
            rows_to_flush.append(self._rows.get())
        future = self._executor.submit(self.flush_rows, rows_to_flush)
        # catch the exceptions in the mutation
        exc = future.exception()
        if exc:
            raise exc from exc
        else:
            result = future.result()
        return result

    def flush_rows(self, rows_to_flush=None):
        """Mutate the specified rows.

        raises:
            * :exc:`.batcherMutationsBatchError` if there's any error in the
                mutations.
        """
        responses = []
        if len(rows_to_flush) > 0:
            # returns a list of status codes
            response = self.table.mutate_rows(rows_to_flush)

            has_error = False
            for result in response:
                if result.code != 0:
                    has_error = True
                responses.append(result)

            if has_error:
                exc = [
                    from_grpc_status(status_code.code, status_code.message)
                    for status_code in responses
                    if status_code.code != 0
                ]
                raise MutationsBatchError(message="Errors in batch mutations.", exc=exc)

        return responses

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Clean up resources. Flush and shutdown the ThreadPoolExecutor."""
        self.close()

    def close(self):
        """Clean up resources. Flush and shutdown the ThreadPoolExecutor.
        Any errors will be raised.

        raises:
            * :exc:`.batcherMutationsBatchError` if there's any error in the
                mutations.

        """
        self._is_open = False
        self.flush()
        self._executor.shutdown(wait=True)
