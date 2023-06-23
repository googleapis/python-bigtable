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

from typing import Any, TYPE_CHECKING

from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.row import Row

is_311_plus = sys.version_info >= (3, 11)

if TYPE_CHECKING:
    from google.cloud.bigtable.mutations import RowMutationEntry
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery


class IdleTimeout(core_exceptions.DeadlineExceeded):
    """
    Exception raised by ReadRowsIterator when the generator
    has been idle for longer than the internal idle_timeout.
    """

    pass


class InvalidChunk(core_exceptions.GoogleAPICallError):
    """Exception raised to invalid chunk data from back-end."""


class _RowSetComplete(Exception):
    """
    Internal exception for _ReadRowsOperation
    Raised in revise_request_rowset when there are no rows left to process when starting a retry attempt
    """

    pass


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

    Exceptions will typically be of type FailedMutationEntryError, but other exceptions may
    be included if they are raised during the mutation operation
    """

    @staticmethod
    def _format_message(
        excs: list[Exception], total_entries: int, exc_count: int | None = None
    ) -> str:
        """
        Format a message for the exception group

        Args:
          - excs: the exceptions in the group
          - total_entries: the total number of entries attempted, successful or not
          - exc_count: the number of exceptions associated with the request
             if None, this will be len(excs)
        """
        exc_count = exc_count if exc_count is not None else len(excs)
        entry_str = "entry" if exc_count == 1 else "entries"
        return f"{exc_count} failed {entry_str} from {total_entries} attempted."

    def __init__(
        self, excs: list[Exception], total_entries: int, message: str | None = None
    ):
        """
        Args:
          - excs: the exceptions in the group
          - total_entries: the total number of entries attempted, successful or not
          - message: the message for the exception group. If None, a default message
              will be generated
        """
        message = (
            message
            if message is not None
            else self._format_message(excs, total_entries)
        )
        super().__init__(message, excs)
        self.total_entries_attempted = total_entries

    def __new__(
        cls, excs: list[Exception], total_entries: int, message: str | None = None
    ):
        """
        Args:
          - excs: the exceptions in the group
          - total_entries: the total number of entries attempted, successful or not
          - message: the message for the exception group. If None, a default message
        """
        message = (
            message if message is not None else cls._format_message(excs, total_entries)
        )
        instance = super().__new__(cls, message, excs)
        instance.total_entries_attempted = total_entries
        return instance

    @classmethod
    def from_truncated_lists(
        cls,
        first_list: list[Exception],
        last_list: list[Exception],
        total_excs: int,
        entry_count: int,
    ) -> MutationsExceptionGroup:
        """
        Create a MutationsExceptionGroup from two lists of exceptions, representing
        a larger set that has been truncated. The MutationsExceptionGroup will
        contain the union of the two lists as sub-exceptions, and the error message
        describe the number of exceptions that were truncated.

        Args:
          - first_list: the set of oldest exceptions to add to the ExceptionGroup
          - last_list: the set of newest exceptions to add to the ExceptionGroup
          - total_excs: the total number of exceptions associated with the request
             Should be len(first_list) + len(last_list) + number of dropped exceptions
             in the middle
          - entry_count: the total number of entries attempted, successful or not
        """
        first_count, last_count = len(first_list), len(last_list)
        if first_count + last_count >= total_excs:
            # no exceptions were dropped
            return cls(first_list + last_list, entry_count)
        excs = first_list + last_list
        truncation_count = total_excs - (first_count + last_count)
        base_message = cls._format_message(excs, entry_count, total_excs)
        first_message = f"first {first_count}" if first_count else ""
        last_message = f"last {last_count}" if last_count else ""
        conjunction = " and " if first_message and last_message else ""
        message = f"{base_message} ({first_message}{conjunction}{last_message} attached as sub-exceptions; {truncation_count} truncated)"
        return cls(excs, entry_count, message)


class FailedMutationEntryError(Exception):
    """
    Represents a single failed RowMutationEntry in a bulk_mutate_rows request.
    A collection of FailedMutationEntryErrors will be raised in a MutationsExceptionGroup
    """

    def __init__(
        self,
        failed_idx: int | None,
        failed_mutation_entry: "RowMutationEntry",
        cause: Exception,
    ):
        idempotent_msg = (
            "idempotent" if failed_mutation_entry.is_idempotent() else "non-idempotent"
        )
        index_msg = f" at index {failed_idx} " if failed_idx is not None else " "
        message = (
            f"Failed {idempotent_msg} mutation entry{index_msg}with cause: {cause!r}"
        )
        super().__init__(message)
        self.index = failed_idx
        self.entry = failed_mutation_entry
        self.__cause__ = cause


class RetryExceptionGroup(BigtableExceptionGroup):
    """Represents one or more exceptions that occur during a retryable operation"""

    @staticmethod
    def _format_message(excs: list[Exception]):
        if len(excs) == 0:
            return "No exceptions"
        if len(excs) == 1:
            return f"1 failed attempt: {type(excs[0]).__name__}"
        else:
            return f"{len(excs)} failed attempts. Latest: {type(excs[-1]).__name__}"

    def __init__(self, excs: list[Exception]):
        super().__init__(self._format_message(excs), excs)

    def __new__(cls, excs: list[Exception]):
        return super().__new__(cls, cls._format_message(excs), excs)


class ShardedReadRowsExceptionGroup(BigtableExceptionGroup):
    """
    Represents one or more exceptions that occur during a sharded read rows operation
    """

    @staticmethod
    def _format_message(excs: list[FailedQueryShardError], total_queries: int):
        query_str = "query" if total_queries == 1 else "queries"
        plural_str = "" if len(excs) == 1 else "s"
        return f"{len(excs)} sub-exception{plural_str} (from {total_queries} {query_str} attempted)"

    def __init__(
        self,
        excs: list[FailedQueryShardError],
        succeeded: list[Row],
        total_queries: int,
    ):
        super().__init__(self._format_message(excs, total_queries), excs)
        self.successful_rows = succeeded

    def __new__(
        cls, excs: list[FailedQueryShardError], succeeded: list[Row], total_queries: int
    ):
        instance = super().__new__(cls, cls._format_message(excs, total_queries), excs)
        instance.successful_rows = succeeded
        return instance


class FailedQueryShardError(Exception):
    """
    Represents an individual failed query in a sharded read rows operation
    """

    def __init__(
        self,
        failed_index: int,
        failed_query: "ReadRowsQuery" | dict[str, Any],
        cause: Exception,
    ):
        message = f"Failed query at index {failed_index} with cause: {cause!r}"
        super().__init__(message)
        self.index = failed_index
        self.query = failed_query
        self.__cause__ = cause
