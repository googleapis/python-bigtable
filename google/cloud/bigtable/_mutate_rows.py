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

from typing import Callable, Any, TYPE_CHECKING

from google.api_core import exceptions as core_exceptions
from google.api_core import retry_async as retries
import google.cloud.bigtable.exceptions as bt_exceptions

if TYPE_CHECKING:
    from google.cloud.bigtable_v2.services.bigtable.async_client import (
        BigtableAsyncClient,
    )
    from google.cloud.bigtable.mutations import BulkMutationsEntry


class _MutateRowsIncomplete(RuntimeError):
    """
    Exception raised when a mutate_rows call has unfinished work.
    """

    pass


async def _mutate_rows_operation(
    gapic_client: "BigtableAsyncClient",
    request: dict[str, Any],
    mutation_entries: list["BulkMutationsEntry"],
    operation_timeout: float,
    per_request_timeout: float | None,
):
    """
    Helper function for managing a single mutate_rows operation, end-to-end.

    Args:
      - gapic_client: the client to use for the mutate_rows call
      - request: A request dict containing table name, app profile id, and other details to inclide in the request
      - mutation_entries: a list of BulkMutationsEntry objects to send to the server
      - operation_timeout: the timeout to use for the entire operation, in seconds.
      - per_request_timeout: the timeout to use for each mutate_rows attempt, in seconds.
          If not specified, the request will run until operation_timeout is reached.
    """
    mutations_dict: dict[int, BulkMutationsEntry | None] = {
        idx: mut for idx, mut in enumerate(mutation_entries)
    }
    error_dict: dict[int, list[Exception]] = {idx: [] for idx in mutations_dict.keys()}

    predicate = retries.if_exception_type(
        core_exceptions.DeadlineExceeded,
        core_exceptions.ServiceUnavailable,
        core_exceptions.Aborted,
        _MutateRowsIncomplete,
    )

    def on_error_fn(exc):
        if predicate(exc) and not isinstance(exc, _MutateRowsIncomplete):
            # add this exception to list for each active mutation
            for idx in error_dict.keys():
                if mutations_dict[idx] is not None:
                    error_dict[idx].append(exc)
            # remove non-idempotent mutations from mutations_dict, so they are not retried
            for idx, mut in mutations_dict.items():
                if mut is not None and not mut.is_idempotent():
                    mutations_dict[idx] = None

    retry = retries.AsyncRetry(
        predicate=predicate,
        on_error=on_error_fn,
        timeout=operation_timeout,
        initial=0.01,
        multiplier=2,
        maximum=60,
    )
    # wrap attempt in retry logic
    retry_wrapped = retry(_mutate_rows_retryable_attempt)
    # convert RetryErrors from retry wrapper into DeadlineExceeded errors
    deadline_wrapped = bt_exceptions._convert_retry_deadline(
        retry_wrapped, operation_timeout
    )
    try:
        # trigger mutate_rows
        await deadline_wrapped(
            gapic_client,
            request,
            per_request_timeout,
            mutations_dict,
            error_dict,
            predicate,
        )
    except Exception as exc:
        # exceptions raised by retryable are added to the list of exceptions for all unprocessed mutations
        for idx in error_dict.keys():
            if mutations_dict[idx] is not None:
                error_dict[idx].append(exc)
    finally:
        # raise exception detailing incomplete mutations
        all_errors = []
        for idx, exc_list in error_dict.items():
            if exc_list:
                if len(exc_list) == 1:
                    cause_exc = exc_list[0]
                else:
                    cause_exc = bt_exceptions.RetryExceptionGroup(exc_list)
                all_errors.append(
                    bt_exceptions.FailedMutationEntryError(
                        idx, mutation_entries[idx], cause_exc
                    )
                )
        if all_errors:
            raise bt_exceptions.MutationsExceptionGroup(
                all_errors, len(mutation_entries)
            )


async def _mutate_rows_retryable_attempt(
    gapic_client: "BigtableAsyncClient",
    request: dict[str, Any],
    per_request_timeout: float | None,
    mutation_dict: dict[int, "BulkMutationsEntry" | None],
    error_dict: dict[int, list[Exception]],
    predicate: Callable[[Exception], bool],
):
    """
    Helper function for managing a single mutate_rows attempt.

    If one or more retryable mutations remain incomplete at the end of the function,
    _MutateRowsIncomplete will be raised to trigger a retry

    This function is intended to be wrapped in an api_core.retry.AsyncRetry object, which will handle
    timeouts and retrying raised exceptions.

    Args:
      - gapic_client: the client to use for the mutate_rows call
      - request: the request to send to the server, populated with table name and app profile id
      - per_request_timeout: the timeout to use for each mutate_rows attempt
      - mutation_dict: a dictionary tracking which entries are outstanding
            (stored as BulkMutationsEntry), and which have reached a terminal state (stored as None).
            At the start of the request, all entries are outstanding.
      - error_dict: a dictionary tracking errors associated with each entry index.
            Each retry will append a new error. Successful mutations will clear the error list.
      - predicate: a function that takes an exception and returns True if the exception is retryable.
    Raises:
      - _MutateRowsIncomplete: if one or more retryable mutations remain incomplete at the end of the function
      - GoogleAPICallError: if the server returns an error on the grpc call
    """
    new_request = request.copy()
    # keep map between sub-request indices and global entry indices
    index_map: dict[int, int] = {}
    # continue to retry until timeout, or all mutations are complete (success or failure)
    request_entries: list[dict[str, Any]] = []
    for index, entry in mutation_dict.items():
        if entry is not None:
            index_map[len(request_entries)] = index
            request_entries.append(entry._to_dict())
    new_request["entries"] = request_entries
    async for result_list in await gapic_client.mutate_rows(
        new_request, timeout=per_request_timeout
    ):
        for result in result_list.entries:
            # convert sub-request index to global index
            idx = index_map[result.index]
            if result.status.code == 0:
                # mutation succeeded
                mutation_dict[idx] = None
                error_dict[idx] = []
            if result.status.code != 0:
                # mutation failed
                exception = core_exceptions.from_grpc_status(
                    result.status.code,
                    result.status.message,
                    details=result.status.details,
                )
                error_dict[idx].append(exception)
                # if mutation is non-idempotent or the error is not retryable,
                # mark the mutation as terminal
                entry = mutation_dict[idx]
                if entry is not None:
                    if not predicate(exception) or not entry.is_idempotent():
                        mutation_dict[idx] = None
    if any(mutation is not None for mutation in mutation_dict.values()):
        # unfinished work; raise exception to trigger retry
        raise _MutateRowsIncomplete()
