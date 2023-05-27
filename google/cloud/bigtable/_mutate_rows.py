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

from typing import Iterator, Callable, Any, Coroutine, TYPE_CHECKING

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


async def _mutate_rows_operation(
    gapic_client: "BigtableAsyncClient",
    table: "Table",
    mutation_entries: list["RowMutationEntry"],
    operation_timeout: float,
    per_request_timeout: float | None,
    on_terminal_state: Callable[
        [int, "RowMutationEntry", Exception | None], Coroutine[None, None, None]
    ]
    | None = None,
):
    """
    Helper function for managing a single mutate_rows operation, end-to-end.

    Args:
      - gapic_client: the client to use for the mutate_rows call
      - request: A request dict containing table name, app profile id, and other details to inclide in the request
      - mutation_entries: a list of RowMutationEntry objects to send to the server
      - operation_timeout: the timeout to use for the entire operation, in seconds.
      - per_request_timeout: the timeout to use for each mutate_rows attempt, in seconds.
          If not specified, the request will run until operation_timeout is reached.
      - on_terminal_state: If given, this function will be called as soon as a mutation entry
            reaches a terminal state (success or failure).
    """
    mutations_dict: dict[int, RowMutationEntry] = {
        idx: mut for idx, mut in enumerate(mutation_entries)
    }

    error_dict: dict[int, list[Exception]] = {idx: [] for idx in mutations_dict.keys()}

    predicate = retries.if_exception_type(
        core_exceptions.DeadlineExceeded,
        core_exceptions.ServiceUnavailable,
        _MutateRowsIncomplete,
    )

    def on_error_fn(exc):
        if predicate(exc) and not isinstance(exc, _MutateRowsIncomplete):
            # add this exception to list for each active mutation
            for idx in error_dict.keys():
                if idx in mutations_dict:
                    error_dict[idx].append(exc)
            # remove non-idempotent mutations from mutations_dict, so they are not retriedi
            for idx, mut in list(mutations_dict.items()):
                if not mut.is_idempotent():
                    mutations_dict.pop(idx)

    retry = retries.AsyncRetry(
        predicate=predicate,
        on_error=on_error_fn,
        timeout=operation_timeout,
        initial=0.01,
        multiplier=2,
        maximum=60,
    )
    # use generator to lower per-attempt timeout as we approach operation_timeout deadline
    attempt_timeout_gen = _attempt_timeout_generator(
        per_request_timeout, operation_timeout
    )
    # wrap attempt in retry logic
    retry_wrapped = retry(_mutate_rows_retryable_attempt)
    # convert RetryErrors from retry wrapper into DeadlineExceeded errors
    deadline_wrapped = _convert_retry_deadline(retry_wrapped, operation_timeout)
    try:
        # trigger mutate_rows
        await deadline_wrapped(
            gapic_client,
            table,
            attempt_timeout_gen,
            mutations_dict,
            error_dict,
            predicate,
            on_terminal_state,
        )
    except Exception as exc:
        # exceptions raised by retryable are added to the list of exceptions for all unprocessed mutations
        for idx in error_dict.keys():
            if idx in mutations_dict:
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
                entry = mutation_entries[idx]
                all_errors.append(
                    bt_exceptions.FailedMutationEntryError(idx, entry, cause_exc)
                )
                # call on_terminal_state for each unreported failed mutation
                if on_terminal_state and idx in mutations_dict:
                    await on_terminal_state(idx, entry, cause_exc)
        if all_errors:
            raise bt_exceptions.MutationsExceptionGroup(
                all_errors, len(mutation_entries)
            )


async def _mutate_rows_retryable_attempt(
    gapic_client: "BigtableAsyncClient",
    table: "Table",
    timeout_generator: Iterator[float],
    mutation_dict: dict[int, "RowMutationEntry"],
    error_dict: dict[int, list[Exception]],
    predicate: Callable[[Exception], bool],
    on_terminal_state: Callable[
        [int, "RowMutationEntry", Exception | None],
        Coroutine[None, None, None],
    ]
    | None = None,
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
            (stored as RowMutationEntry), and which have reached a terminal state (stored as None).
            At the start of the request, all entries are outstanding.
      - error_dict: a dictionary tracking errors associated with each entry index.
            Each retry will append a new error. Successful mutations will clear the error list.
      - predicate: a function that takes an exception and returns True if the exception is retryable.
      - on_terminal_state: If given, this function will be called as soon as a mutation entry
            reaches a terminal state (success or failure).
    Raises:
      - _MutateRowsIncomplete: if one or more retryable mutations remain incomplete at the end of the function
      - GoogleAPICallError: if the server returns an error on the grpc call
    """
    # update on_terminal_state to remove completed mutations from mutation_dict
    input_callback = on_terminal_state

    async def on_terminal_patched(idx, entry, exc):
        mutation_dict.pop(idx)
        if input_callback is not None:
            await input_callback(idx, entry, exc)

    on_terminal_state = on_terminal_patched
    # keep map between sub-request indices and global mutation_dict indices
    index_map: dict[int, int] = {}
    request_entries: list[dict[str, Any]] = []
    for request_idx, (global_idx, m) in enumerate(mutation_dict.items()):
        index_map[request_idx] = global_idx
        request_entries.append(m._to_dict())
    # make gapic request
    metadata = _make_metadata(table.table_name, table.app_profile_id)
    async for result_list in await gapic_client.mutate_rows(
        request={
            "table_name": table.table_name,
            "app_profile_id": table.app_profile_id,
            "entries": request_entries,
        },
        timeout=next(timeout_generator),
        metadata=metadata,
    ):
        for result in result_list.entries:
            # convert sub-request index to global index
            idx = index_map[result.index]
            entry = mutation_dict[idx] if idx in mutation_dict else None
            exc = None
            if entry is None:
                # this entry has already reached a terminal state
                continue
            if result.status.code == 0:
                # mutation succeeded
                error_dict[idx] = []
                await on_terminal_state(idx, entry, None)
            else:
                # mutation failed
                exc = core_exceptions.from_grpc_status(
                    result.status.code,
                    result.status.message,
                    details=result.status.details,
                )
                error_dict[idx].append(exc)
                # if mutation is non-idempotent or the error is not retryable,
                # mark the mutation as terminal
                if not predicate(exc) or not entry.is_idempotent():
                    await on_terminal_state(idx, entry, exc)
    # check if attempt succeeded, or needs to be retried
    if mutation_dict:
        # unfinished work; raise exception to trigger retry
        raise _MutateRowsIncomplete()
