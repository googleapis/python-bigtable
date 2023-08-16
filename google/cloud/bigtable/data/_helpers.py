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

from typing import Callable, Any
from inspect import iscoroutinefunction
import time

from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.data.exceptions import RetryExceptionGroup

"""
Helper functions used in various places in the library.
"""


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


def _convert_retry_deadline(
    func: Callable[..., Any],
    timeout_value: float | None = None,
    retry_errors: list[Exception] | None = None,
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

    return wrapper_async if iscoroutinefunction(func) else wrapper


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
    if operation_timeout <= 0:
        raise ValueError("operation_timeout must be greater than 0")
    if not allow_none and attempt_timeout is None:
        raise ValueError("attempt_timeout must not be None")
    elif attempt_timeout is not None:
        if attempt_timeout <= 0:
            raise ValueError("attempt_timeout must be greater than 0")
