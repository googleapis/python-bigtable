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

from typing import Callable, List, Tuple, Any
import time
import enum
from collections import namedtuple
from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery

from google.api_core import exceptions as core_exceptions
from google.api_core.retry import exponential_sleep_generator
from google.cloud.bigtable.data.exceptions import RetryExceptionGroup

"""
Helper functions used in various places in the library.
"""

# Type alias for the output of sample_keys
RowKeySamples = List[Tuple[bytes, int]]

# type alias for the output of query.shard()
ShardedQuery = List[ReadRowsQuery]

# used by read_rows_sharded to limit how many requests are attempted in parallel
_CONCURRENCY_LIMIT = 10

# used to register instance data with the client for channel warming
_WarmedInstanceKey = namedtuple(
    "_WarmedInstanceKey", ["instance_name", "table_name", "app_profile_id"]
)


# enum used on method calls when table defaults should be used
class TABLE_DEFAULT(enum.Enum):
    # default for mutate_row, sample_row_keys, check_and_mutate_row, and read_modify_write_row
    DEFAULT = "DEFAULT"
    # default for read_rows, read_rows_stream, read_rows_sharded, row_exists, and read_row
    READ_ROWS = "READ_ROWS_DEFAULT"
    # default for bulk_mutate_rows and mutations_batcher
    MUTATE_ROWS = "MUTATE_ROWS_DEFAULT"


def _make_metadata(
    table_name: str, app_profile_id: str | None
) -> list[tuple[str, str]]:
    """
    Create properly formatted gRPC metadata for requests.
    """
    params = []
    params.append(f"table_name={table_name}")
    if app_profile_id is not None:
        params.append(f"app_profile_id={app_profile_id}")
    params_str = "&".join(params)
    return [("x-goog-request-params", params_str)]


def _attempt_timeout_generator(
    per_request_timeout: float | None, operation_timeout: float
):
    """
    Generator that yields the timeout value for each attempt of a retry loop.

    Will return per_request_timeout until the operation_timeout is approached,
    at which point it will return the remaining time in the operation_timeout.

    Args:
      - per_request_timeout: The timeout value to use for each request, in seconds.
            If None, the operation_timeout will be used for each request.
      - operation_timeout: The timeout value to use for the entire operationm in seconds.
    Yields:
      - The timeout value to use for the next request, in seonds
    """
    per_request_timeout = (
        per_request_timeout if per_request_timeout is not None else operation_timeout
    )
    deadline = operation_timeout + time.monotonic()
    while True:
        yield max(0, min(per_request_timeout, deadline - time.monotonic()))


def backoff_generator(initial=0.01, multiplier=2, maximum=60):
    """
    Build a generator for exponential backoff sleep times.

    This implementation builds on top of api_core.retries.exponential_sleep_generator,
    adding the ability to retrieve previous values using the send(idx) method. This is
    used by the Metrics class to track the sleep times used for each attempt.
    """
    history = []
    subgenerator = exponential_sleep_generator(initial, multiplier, maximum)
    while True:
        next_backoff = next(subgenerator)
        history.append(next_backoff)
        sent_idx = yield next_backoff
        while sent_idx is not None:
            # requesting from history
            sent_idx = yield history[sent_idx]

# TODO:replace this function with an exception_factory passed into the retry when
# feature is merged:
# https://github.com/googleapis/python-bigtable/blob/ea5b4f923e42516729c57113ddbe28096841b952/google/cloud/bigtable/data/_async/_read_rows.py#L130
def _convert_retry_deadline(
    func: Callable[..., Any],
    timeout_value: float | None = None,
    retry_errors: list[Exception] | None = None,
    is_async: bool = False,
):
    """
    Decorator to convert RetryErrors raised by api_core.retry into
    DeadlineExceeded exceptions, indicating that the underlying retries have
    exhaused the timeout value.
    Optionally attaches a RetryExceptionGroup to the DeadlineExceeded.__cause__,
    detailing the failed exceptions associated with each retry.

    Supports both sync and async function wrapping.

    Args:
      - func: The function to decorate
      - timeout_value: The timeout value to display in the DeadlineExceeded error message
      - retry_errors: An optional list of exceptions to attach as a RetryExceptionGroup to the DeadlineExceeded.__cause__
    """
    timeout_str = f" of {timeout_value:.1f}s" if timeout_value is not None else ""
    error_str = f"operation_timeout{timeout_str} exceeded"

    def handle_error():
        new_exc = core_exceptions.DeadlineExceeded(
            error_str,
        )
        source_exc = None
        if retry_errors:
            source_exc = RetryExceptionGroup(retry_errors)
        new_exc.__cause__ = source_exc
        raise new_exc from source_exc

    # separate wrappers for async and sync functions
    async def wrapper_async(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except core_exceptions.RetryError:
            handle_error()

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except core_exceptions.RetryError:
            handle_error()

    return wrapper_async if is_async else wrapper


def _get_timeouts(
    operation: float | TABLE_DEFAULT, attempt: float | None | TABLE_DEFAULT, table
) -> tuple[float, float]:
    """
    Convert passed in timeout values to floats, using table defaults if necessary.

    attempt will use operation value if None, or if larger than operation.

    Will call _validate_timeouts on the outputs, and raise ValueError if the
    resulting timeouts are invalid.

    Args:
        - operation: The timeout value to use for the entire operation, in seconds.
        - attempt: The timeout value to use for each attempt, in seconds.
        - table: The table to use for default values.
    Returns:
        - A tuple of (operation_timeout, attempt_timeout)
    """
    # load table defaults if necessary
    if operation == TABLE_DEFAULT.DEFAULT:
        final_operation = table.default_operation_timeout
    elif operation == TABLE_DEFAULT.READ_ROWS:
        final_operation = table.default_read_rows_operation_timeout
    elif operation == TABLE_DEFAULT.MUTATE_ROWS:
        final_operation = table.default_mutate_rows_operation_timeout
    else:
        final_operation = operation
    if attempt == TABLE_DEFAULT.DEFAULT:
        attempt = table.default_attempt_timeout
    elif attempt == TABLE_DEFAULT.READ_ROWS:
        attempt = table.default_read_rows_attempt_timeout
    elif attempt == TABLE_DEFAULT.MUTATE_ROWS:
        attempt = table.default_mutate_rows_attempt_timeout

    if attempt is None:
        # no timeout specified, use operation timeout for both
        final_attempt = final_operation
    else:
        # cap attempt timeout at operation timeout
        final_attempt = min(attempt, final_operation) if final_operation else attempt

    _validate_timeouts(final_operation, final_attempt, allow_none=False)
    return final_operation, final_attempt


def _validate_timeouts(
    operation_timeout: float, attempt_timeout: float | None, allow_none: bool = False
):
    """
    Helper function that will verify that timeout values are valid, and raise
    an exception if they are not.

    Args:
      - operation_timeout: The timeout value to use for the entire operation, in seconds.
      - attempt_timeout: The timeout value to use for each attempt, in seconds.
      - allow_none: If True, attempt_timeout can be None. If False, None values will raise an exception.
    Raises:
      - ValueError if operation_timeout or attempt_timeout are invalid.
    """
    if operation_timeout is None:
        raise ValueError("operation_timeout cannot be None")
    if operation_timeout <= 0:
        raise ValueError("operation_timeout must be greater than 0")
    if not allow_none and attempt_timeout is None:
        raise ValueError("attempt_timeout must not be None")
    elif attempt_timeout is not None:
        if attempt_timeout <= 0:
            raise ValueError("attempt_timeout must be greater than 0")
