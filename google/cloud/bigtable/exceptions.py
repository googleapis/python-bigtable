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


is_311_plus = sys.version_info >= (3, 11)


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

class FailedMutationException(Exception):
    """
    Represents a failed mutation entry for bulk mutation operations
    """
    def __init__(self, failed_idx:int, failed_mutation_obj:"Mutation", cause:Exception):
        super.init(f"Failed mutation at index: {failed_idx} with cause: {cause}")
        self.failed_idx = failed_idx
        self.failed_mutation_obj = failed_mutation_obj
        self.__cause__ = cause

class RetryExceptionGroup(BigtableExceptionGroup):
    """Represents one or more exceptions that occur during a retryable operation"""

    pass
