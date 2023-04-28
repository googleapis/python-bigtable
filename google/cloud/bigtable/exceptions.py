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

from typing import Callable, Any, TYPE_CHECKING

from google.api_core import exceptions as core_exceptions

is_311_plus = sys.version_info >= (3, 11)

if TYPE_CHECKING:
    from google.cloud.bigtable.mutations import BulkMutationsEntry


def _convert_retry_deadline(
    func: Callable[..., Any], timeout_value: float, retry_errors: list[Exception] | None = None
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
                source_exc = RetryExceptionGroup(retry_errors)
            new_exc.__cause__ = source_exc
            raise new_exc from source_exc

    return wrapper


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
            if len(excs) == 0:
                raise ValueError("exceptions must be a non-empty sequence")
            self.exceptions = tuple(excs)
            super().__init__(message)

    def __new__(cls, message, excs):
        if is_311_plus:
            return super().__new__(cls, message, excs)
        else:
            return super().__new__(cls)

    def __str__(self):
        """
        String representation doesn't display sub-exceptions. Subexceptions are
        described in message
        """
        return self.args[0]


class MutationsExceptionGroup(BigtableExceptionGroup):
    """
    Represents one or more exceptions that occur during a bulk mutation operation
    """

    @staticmethod
    def _format_message(excs, total_entries):
        entry_str = "entry" if total_entries == 1 else "entries"
        plural_str = "" if len(excs) == 1 else "s"
        return f"{len(excs)} sub-exception{plural_str} (from {total_entries} {entry_str} attempted)"

    def __init__(self, excs, total_entries):
        super().__init__(self._format_message(excs, total_entries), excs)

    def __new__(cls, excs, total_entries):
        return super().__new__(cls, cls._format_message(excs, total_entries), excs)


class FailedMutationEntryError(Exception):
    """
    Represents a failed mutation entry for bulk mutation operations
    """

    def __init__(
        self,
        failed_idx: int,
        failed_mutation_entry: "BulkMutationsEntry",
        cause: Exception,
    ):
        idempotent_msg = (
            "idempotent" if failed_mutation_entry.is_idempotent() else "non-idempotent"
        )
        message = f"Failed {idempotent_msg} mutation entry at index {failed_idx} with cause: {cause!r}"
        super().__init__(message)
        self.index = failed_idx
        self.entry = failed_mutation_entry
        self.__cause__ = cause


class RetryExceptionGroup(BigtableExceptionGroup):
    """Represents one or more exceptions that occur during a retryable operation"""

    @staticmethod
    def _format_message(excs):
        if len(excs) == 0:
            return "No exceptions"
        if len(excs) == 1:
            return f"1 failed attempt: {type(excs[0]).__name__}"
        else:
            return f"{len(excs)} failed attempts. Latest: {type(excs[-1]).__name__}"

    def __init__(self, excs):
        super().__init__(self._format_message(excs), excs)

    def __new__(cls, excs):
        return super().__new__(cls, cls._format_message(excs), excs)
