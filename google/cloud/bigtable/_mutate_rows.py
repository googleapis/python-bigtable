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

from typing import TYPE_CHECKING
import functools

from google.api_core import exceptions as core_exceptions
from google.api_core import retry_async as retries
import google.cloud.bigtable.exceptions as bt_exceptions
from google.cloud.bigtable._helpers import _make_metadata
from google.cloud.bigtable._helpers import _convert_retry_deadline
from google.cloud.bigtable._helpers import _attempt_timeout_generator

if TYPE_CHECKING:
    from google.cloud.bigtable_v2.services.bigtable.async_client import (
        BigtableAsyncClient,
    )
    from google.cloud.bigtable.client import Table
    from google.cloud.bigtable.mutations import RowMutationEntry


class _MutateRowsIncomplete(RuntimeError):
    """
    Exception raised when a mutate_rows call has unfinished work.
    """

    pass


class _MutateRowsOperation:
    """
    MutateRowsOperation manages the logic of sending a set of row mutations,
    and retrying on failed entries. It manages this using the _run_attempt
    function, which attempts to mutate all outstanding entries, and raises
    _MutateRowsIncomplete if any retryable errors are encountered.

    Errors are exposed as a MutationsExceptionGroup, which contains a list of
    exceptions organized by the related failed mutation entries.
    """

    def __init__(
        self,
        gapic_client: "BigtableAsyncClient",
        table: "Table",
        mutation_entries: list["RowMutationEntry"],
        operation_timeout: float,
        per_request_timeout: float | None,
    ):
        """
        Args:
          - gapic_client: the client to use for the mutate_rows call
          - table: the table associated with the request
          - mutation_entries: a list of RowMutationEntry objects to send to the server
          - operation_timeout: the timeout t o use for the entire operation, in seconds.
          - per_request_timeout: the timeoutto use for each mutate_rows attempt, in seconds.
              If not specified, the request will run until operation_timeout is reached.
        """
        # create partial function to pass to trigger rpc call
        metadata = _make_metadata(table.table_name, table.app_profile_id)
        self._gapic_fn = functools.partial(
            gapic_client.mutate_rows,
            table_name=table.table_name,
            app_profile_id=table.app_profile_id,
            metadata=metadata,
        )
        # create predicate for determining which errors are retryable
        self.is_retryable = retries.if_exception_type(
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
            _MutateRowsIncomplete,
        )
        # build retryable operation
        retry = retries.AsyncRetry(
            predicate=self.is_retryable,
            timeout=operation_timeout,
            initial=0.01,
            multiplier=2,
            maximum=60,
        )
        retry_wrapped = retry(self._run_attempt)
        self._operation = _convert_retry_deadline(retry_wrapped, operation_timeout)
        # initialize state
        self.timeout_generator = _attempt_timeout_generator(
            per_request_timeout, operation_timeout
        )
        self.mutations = mutation_entries
        self.remaining_indices = list(range(len(self.mutations)))
        self.errors: dict[int, list[Exception]] = {}

    async def start(self):
        """
        Start the operation, and run until completion

        Raises:
          - MutationsExceptionGroup: if any mutations failed
        """
        try:
            # trigger mutate_rows
            await self._operation()
        except Exception as exc:
            # exceptions raised by retryable are added to the list of exceptions for all unfinalized mutations
            incomplete_indices = self.remaining_indices.copy()
            for idx in incomplete_indices:
                self._handle_entry_error(idx, exc)
        finally:
            # raise exception detailing incomplete mutations
            all_errors = []
            for idx, exc_list in self.errors.items():
                if len(exc_list) == 0:
                    raise core_exceptions.ClientError(
                        f"Mutation {idx} failed with no associated errors"
                    )
                elif len(exc_list) == 1:
                    cause_exc = exc_list[0]
                else:
                    cause_exc = bt_exceptions.RetryExceptionGroup(exc_list)
                entry = self.mutations[idx]
                all_errors.append(
                    bt_exceptions.FailedMutationEntryError(idx, entry, cause_exc)
                )
            if all_errors:
                raise bt_exceptions.MutationsExceptionGroup(
                    all_errors, len(self.mutations)
                )

    async def _run_attempt(self):
        """
        Run a single attempt of the mutate_rows rpc.

        Raises:
          - _MutateRowsIncomplete: if there are failed mutations eligible for
              retry after the attempt is complete
          - GoogleAPICallError: if the gapic rpc fails
        """
        request_entries = [
            self.mutations[idx]._to_dict() for idx in self.remaining_indices
        ]
        # track mutations in this request that have not been finalized yet
        active_request_indices = {
            req_idx: orig_idx for req_idx, orig_idx in enumerate(self.remaining_indices)
        }
        self.remaining_indices = []
        if not request_entries:
            # no more mutations. return early
            return
        # make gapic request
        try:
            result_generator = await self._gapic_fn(
                timeout=next(self.timeout_generator),
                entries=request_entries,
            )
            async for result_list in result_generator:
                for result in result_list.entries:
                    # convert sub-request index to global index
                    orig_idx = active_request_indices.pop(result.index)
                    entry_error = core_exceptions.from_grpc_status(
                        result.status.code,
                        result.status.message,
                        details=result.status.details,
                    )
                    if result.status.code == 0:
                        continue
                    else:
                        self._handle_entry_error(orig_idx, entry_error)
        except Exception as exc:
            # add this exception to list for each mutation that wasn't
            # already handled
            for idx in active_request_indices.values():
                self._handle_entry_error(idx, exc)
                # bubble up exception to be handled by retry wrapper
                raise
        # check if attempt succeeded, or needs to be retried
        if self.remaining_indices:
            # unfinished work; raise exception to trigger retry
            raise _MutateRowsIncomplete

    def _handle_entry_error(self, idx: int, exc: Exception):
        """
        Add an exception to the list of exceptions for a given mutation index,
        and add the index to the list of remaining indices if the exception is
        retryable.

        Args:
          - idx: the index of the mutation that failed
          - exc: the exception to add to the list
        """
        entry = self.mutations[idx]
        self.errors.setdefault(idx, []).append(exc)
        if (
            entry.is_idempotent()
            and self.is_retryable(exc)
            and idx not in self.remaining_indices
        ):
            self.remaining_indices.append(idx)
