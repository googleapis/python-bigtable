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

import sys

from typing import Callable, Any

from google.api_core import exceptions as core_exceptions

is_311_plus = sys.version_info >= (3, 11)


def _convert_retry_deadline(
    func: Callable[..., Any],
    timeout_value: float,
    retry_errors: list[Exception] | None = None,
):
    """
    Decorator to convert RetryErrors raised by api_core.retry into
    DeadlineExceeded exceptions, indicating that the underlying retries have
    exhaused the timeout value.
    Optionally attaches a RetryExceptionGroup to the DeadlineExceeded.__cause__,
    detailing the failed exceptions associated with each retry.
    Args:
      - func: The function to decorate
      - timeout_value: The timeout value to display in the DeadlineExceeded error message
      - retry_errors: An optional list of exceptions to attach as a RetryExceptionGroup to the DeadlineExceeded.__cause__
    """

    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except core_exceptions.RetryError:
            new_exc = core_exceptions.DeadlineExceeded(
                f"operation_timeout of {timeout_value:0.1f}s exceeded"
            )
            source_exc = None
            if retry_errors:
                source_exc = RetryExceptionGroup(
                    f"{len(retry_errors)} failed attempts", retry_errors
                )
            new_exc.__cause__ = source_exc
            raise new_exc from source_exc

    return wrapper


class IdleTimeout(core_exceptions.DeadlineExceeded):
    """
    Exception raised by ReadRowsIterator when the generator
    has been idle for longer than the internal idle_timeout.
    """

    pass


class InvalidChunk(core_exceptions.GoogleAPICallError):
    """Exception raised to invalid chunk data from back-end."""


class RowNotFound(core_exceptions.NotFound):
    """Exception raised when a row is not found on a read_row call."""


class BigtableExceptionGroup(ExceptionGroup if is_311_plus else Exception):  # type: ignore # noqa: F821
    """
    Represents one or more exceptions that occur during a bulk Bigtable operation

    In Python 3.11+, this is an unmodified exception group. In < 3.10, it is a
    custom exception with some exception group functionality backported, but does
    Not implement the full API
    """

    def __init__(self, message, excs):
        if is_311_plus:
            super().__init__(message, excs)
        else:
            self.exceptions = excs
            revised_message = f"{message} ({len(excs)} sub-exceptions)"
            super().__init__(revised_message)


class MutationsExceptionGroup(BigtableExceptionGroup):
    """
    Represents one or more exceptions that occur during a bulk mutation operation
    """

    pass


class RetryExceptionGroup(BigtableExceptionGroup):
    """Represents one or more exceptions that occur during a retryable operation"""

    pass
