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

import sys

from typing import TYPE_CHECKING

is_311_plus = sys.version_info >= (3, 11)

if TYPE_CHECKING:
    from google.cloud.bigtable.mutations import BulkMutationsEntry


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
            super().__init__(message)

    def __new__(cls, message, excs):
        if is_311_plus:
            return super().__new__(cls, message, excs)
        else:
            return super().__new__(cls)


class MutationsExceptionGroup(BigtableExceptionGroup):
    """
    Represents one or more exceptions that occur during a bulk mutation operation
    """

    @staticmethod
    def _format_message(excs, total_entries):
        entry_str = "entry" if total_entries == 1 else "entries"
        return f"{len(excs)} out of {total_entries} mutation {entry_str} failed"

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
            raise ValueError("Empty exception list")
        elif len(excs) == 1:
            return f"1 failed attempt: {excs[0]!r}"
        else:
            return f"{len(excs)} failed attempts. Latest: {excs[-1]!r}"

    def __init__(self, excs):
        super().__init__(self._format_message(excs), excs)

    def __new__(cls, excs):
        return super().__new__(cls, cls._format_message(excs), excs)
