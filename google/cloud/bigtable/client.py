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

from typing import Any, AsyncIterable, TYPE_CHECKING

from google.cloud.client import ClientWithProject

from google.cloud.bigtable.row_merger import RowMerger

import google.auth.credentials
from google.api_core import retry_async as retries

if TYPE_CHECKING:
    from google.cloud.bigtable.mutations import Mutation, BulkMutationsEntry
    from google.cloud.bigtable.mutations_batcher import MutationsBatcher
    from google.cloud.bigtable.row_response import RowResponse
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery
    from google.cloud.bigtable import RowKeySamples
    from google.cloud.bigtable.row_filters import RowFilter
    from google.cloud.bigtable.read_modify_write_rules import ReadModifyWriteRule


class BigtableDataClient(ClientWithProject):
    def __init__(
        self,
        *,
        project: str | None = None,
        pool_size: int = 3,
        credentials: google.auth.credentials.Credentials | None = None,
        client_options: dict[str, Any]
        | "google.api_core.client_options.ClientOptions"
        | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ):
        """
        Create a client instance for the Bigtable Data API

        Args:
            project: the project which the client acts on behalf of.
                If not passed, falls back to the default inferred
                from the environment.
            pool_size: The number of grpc channels to maintain
                in the internal channel pool.
            credentials:
                Thehe OAuth2 Credentials to use for this
                client. If not passed (and if no ``_http`` object is
                passed), falls back to the default inferred from the
                environment.
            client_options (Optional[Union[dict, google.api_core.client_options.ClientOptions]]):
                Client options used to set user options
                on the client. API Endpoint should be set through client_options.
            metadata: a list of metadata headers to be attached to all calls with this client
        """
        raise NotImplementedError

    def get_table(
        self, instance_id: str, table_id: str, app_profile_id: str | None = None
    ) -> Table:
        """
        Return a Table instance to make API requests for a specific table.

        Args:
            instance_id: The ID of the instance that owns the table.
            table_id: The ID of the table.
            app_profile_id: (Optional) The app profile to associate with requests.
                https://cloud.google.com/bigtable/docs/app-profiles
        """
        raise NotImplementedError


class Table:
    """
    Main Data API surface

    Table object maintains instance_id, table_id, and app_profile_id context, and passes them with
    each call
    """

    def __init__(
        self,
        client: BigtableDataClient,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
    ):
        raise NotImplementedError

    async def read_rows_stream(
        self,
        query: ReadRowsQuery | dict[str, Any],
        *,
        shard: bool = False,
        limit: int | None,
        cache_size_limit: int | None = None,
        operation_timeout: int | float | None = 60,
        per_row_timeout: int | float | None = 10,
        idle_timeout: int | float | None = 300,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ) -> AsyncIterable[RowResponse]:
        """
        Returns a generator to asynchronously stream back row data.

        Failed requests within operation_timeout and operation_deadline policies will be retried.

        By default, row data is streamed eagerly over the network, and fully cached in memory
        in the generator, which can be consumed as needed. The size of the generator cache can
        be configured with cache_size_limit. When the cache is full, the read_rows_stream will pause
        the network stream until space is available

        Args:
            - query: contains details about which rows to return
            - shard: if True, will attempt to split up and distribute query to multiple
                 backend nodes in parallel
            - limit: a limit on the number of rows to return. Actual limit will be
                 min(limit, query.limit)
            - cache_size: the number of rows to cache in memory. If None, no limits.
                 Defaults to None
            - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 time is only counted while actively waiting on the network.
                 Completed and cached results can still be accessed after the deadline is complete,
                 with a DeadlineExceeded exception only raised after cached results are exhausted
            - per_row_timeout: the time budget for a single row read, in seconds. If a row takes
                longer than per_row_timeout to complete, the ongoing network request will be with a
                DeadlineExceeded exception, and a retry may be attempted
                Applies only to the underlying network call.
            - idle_timeout: the number of idle seconds before an active generator is marked as
                stale and the cache is drained. The idle count is reset each time the generator
                is yielded from
                raises DeadlineExceeded on future yields
            - per_request_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted
            - metadata: Strings which should be sent along with the request as metadata headers.

        Returns:
            - an asynchronous generator that yields rows returned by the query
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions
                from any retries that failed
            - IdleTimeout: if generator was abandoned
        """
        request = query.to_dict() if isinstance(query, ReadRowsQuery) else query
        request["table_name"] = self._gapic_client.table_name(self.table_id)

        def on_error(exc):
            print(f"RETRYING: {exc}")
            return exc
        retry = retries.AsyncRetry(
            predicate=retries.if_exception_type(
                RuntimeError,
                core_exceptions.DeadlineExceeded,
                core_exceptions.ServiceUnavailable
            ),
            timeout=timeout,
            on_error=on_error,
            initial=0.1,
            multiplier=2,
            maximum=1,
            is_generator=True
        )
        retryable_fn = retry(self._read_rows_retryable)
         emitted_rows:Set[bytes] = set({})
        async for result in retryable_fn(requestm emmited_rows, operation_timeout):
            if isinstance(result, Row):
                yield result
            elif isinstance(result, Exception):
                print(f"Exception: {result}")


    async def _read_rows_retryable(
        self, request:dict[str, Any], emitted_rows: set[bytes], operation_timeout=60.0, revise_on_retry=True
    ) -> AsyncGenerator[Row, None]:
        if revise_request_on_retry and len(emitted_rows) > 0:
            # if this is a retry, try to trim down the request to avoid ones we've already processed
            request["rows"] = self._revise_rowset(
                request.get("rows", None), emitted_rows
            )
        gapic_stream_handler = await self._gapic_client.read_rows(
            request=request,
            app_profile_id=self.app_profile_id,
            timeout=operation_timeout,
        )
        merger = RowMerger()
        async for row in merger.merge_row_stream(gapic_stream_handler):
            if row.row_key not in emitted_rows:
                yield row
                emitted_rows.add(row.row_key)

    def _revise_rowset(
        self, row_set: dict[str, Any]|None, emitted_rows: set[bytes]
    ) -> dict[str, Any]:
        # if user is doing a whole table scan, start a new one with the last seen key
        if row_set is None:
            last_seen = max(emitted_rows)
            return {
                "row_keys": [],
                "row_ranges": [{"start_key_open": last_seen}],
            }
        else:
            # remove seen keys from user-specific key list
            row_keys: List[bytes] = row_set.get("row_keys", [])
            adjusted_keys = []
            for key in row_keys:
                if key not in emitted_rows:
                    adjusted_keys.append(key)
            # if user specified only a single range, set start to the last seen key
            row_ranges: list[dict[str, Any]] = row_set.get("row_ranges", [])
            if len(row_keys) == 0 and len(row_ranges) == 1:
                row_ranges[0]["start_key_open"] = max(emitted_rows)
                if "start_key_closed" in row_ranges[0]:
                    row_ranges[0].pop("start_key_closed")
            return {"row_keys": adjusted_keys, "row_ranges": row_ranges}

    async def read_rows(
        self,
        query: ReadRowsQuery | dict[str, Any],
        *,
        shard: bool = False,
        limit: int | None,
        operation_timeout: int | float | None = 60,
        per_row_timeout: int | float | None = 10,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ) -> list[RowResponse]:
        """
        Helper function that returns a full list instead of a generator

        See read_rows_stream

        Returns:
            - a list of the rows returned by the query
        """
        raise NotImplementedError

    async def read_row(
        self,
        row_key: str | bytes,
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ) -> RowResponse:
        """
        Helper function to return a single row

        See read_rows_stream

        Returns:
            - the individual row requested
        """
        raise NotImplementedError

    async def read_rows_sharded(
        self,
        query_list: list[ReadRowsQuery] | list[dict[str, Any]],
        *,
        limit: int | None,
        cache_size_limit: int | None = None,
        operation_timeout: int | float | None = 60,
        per_row_timeout: int | float | None = 10,
        idle_timeout: int | float | None = 300,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ) -> AsyncIterable[RowResponse]:
        """
        Runs a sharded query in parallel

        Each query in query list will be run concurrently, with results yielded as they are ready
        yielded results may be out of order

        Args:
            - query_list: a list of queries to run in parallel
        """
        raise NotImplementedError

    async def row_exists(
        self,
        row_key: str | bytes,
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ) -> bool:
        """
        Helper function to determine if a row exists

        uses the filters: chain(limit cells per row = 1, strip value)

        Returns:
            - a bool indicating whether the row exists
        """
        raise NotImplementedError

    async def sample_keys(
        self,
        *,
        operation_timeout: int | float | None = 60,
        per_sample_timeout: int | float | None = 10,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ) -> RowKeySamples:
        """
        Return a set of RowKeySamples that delimit contiguous sections of the table of
        approximately equal size

        RowKeySamples output can be used with ReadRowsQuery.shard() to create a sharded query that
        can be parallelized across multiple backend nodes read_rows and read_rows_stream
        requests will call sample_keys internally for this purpose when sharding is enabled

        RowKeySamples is simply a type alias for list[tuple[bytes, int]]; a list of
            row_keys, along with offset positions in the table

        Returns:
            - a set of RowKeySamples the delimit contiguous sections of the table
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing all GoogleAPIError
                exceptions from any retries that failed
        """
        raise NotImplementedError

    def mutations_batcher(self, **kwargs) -> MutationsBatcher:
        """
        Returns a new mutations batcher instance.

        Can be used to iteratively add mutations that are flushed as a group,
        to avoid excess network calls

        Returns:
            - a MutationsBatcher context manager that can batch requests
        """
        return MutationsBatcher(self, **kwargs)

    async def mutate_row(
        self,
        row_key: str | bytes,
        mutations: list[Mutation] | Mutation,
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ):
        """
         Mutates a row atomically.

         Cells already present in the row are left unchanged unless explicitly changed
         by ``mutation``.

         Idempotent operations (i.e, all mutations have an explicit timestamp) will be
         retried on server failure. Non-idempotent operations will not.

         Args:
             - row_key: the row to apply mutations to
             - mutations: the set of mutations to apply to the row
             - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 time is only counted while actively waiting on the network.
                 DeadlineExceeded exception raised after timeout
             - per_request_timeout: the time budget for an individual network request,
               in seconds. If it takes longer than this time to complete, the request
               will be cancelled with a DeadlineExceeded exception, and a retry will be
               attempted if within operation_timeout budget
             - metadata: Strings which should be sent along with the request as metadata headers.

        Raises:
             - DeadlineExceeded: raised after operation timeout
                 will be chained with a RetryExceptionGroup containing all
                 GoogleAPIError exceptions from any retries that failed
             - GoogleAPIError: raised on non-idempotent operations that cannot be
                 safely retried.
        """
        raise NotImplementedError

    async def bulk_mutate_rows(
        self,
        mutation_entries: list[BulkMutationsEntry],
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ):
        """
        Applies mutations for multiple rows in a single batched request.

        Each individual BulkMutationsEntry is applied atomically, but separate entries
        may be applied in arbitrary order (even for entries targetting the same row)
        In total, the row_mutations can contain at most 100000 individual mutations
        across all entries

        Idempotent entries (i.e., entries with mutations with explicit timestamps)
        will be retried on failure. Non-idempotent will not, and will reported in a
        raised exception group

        Args:
            - mutation_entries: the batches of mutations to apply
                Each entry will be applied atomically, but entries will be applied
                in arbitrary order
            - operation_timeout: the time budget for the entire operation, in seconds.
                Failed requests will be retried within the budget.
                time is only counted while actively waiting on the network.
                DeadlineExceeded exception raised after timeout
            - per_request_timeout: the time budget for an individual network request,
                in seconds. If it takes longer than this time to complete, the request
                will be cancelled with a DeadlineExceeded exception, and a retry will
                be attempted if within operation_timeout budget
            - metadata: Strings which should be sent along with the request as metadata headers.

        Raises:
            - MutationsExceptionGroup if one or more mutations fails
                Contains details about any failed entries in .exceptions
        """
        raise NotImplementedError

    async def check_and_mutate_row(
        self,
        row_key: str | bytes,
        predicate: RowFilter | None,
        true_case_mutations: Mutation | list[Mutation] | None = None,
        false_case_mutations: Mutation | list[Mutation] | None = None,
        operation_timeout: int | float | None = 60,
        metadata: list[tuple[str, str]] | None = None,
    ) -> bool:
        """
        Mutates a row atomically based on the output of a predicate filter

        Non-idempotent operation: will not be retried

        Args:
            - row_key: the key of the row to mutate
            - predicate: the filter to be applied to the contents of the specified row.
                Depending on whether or not any results  are yielded,
                either true_case_mutations or false_case_mutations will be executed.
                If None, checks that the row contains any values at all.
            - true_case_mutations:
                Changes to be atomically applied to the specified row if
                predicate yields at least one cell when
                applied to row_key. Entries are applied in order,
                meaning that earlier mutations can be masked by later
                ones. Must contain at least one entry if
                false_case_mutations is empty, and at most 100000.
            - false_case_mutations:
                Changes to be atomically applied to the specified row if
                predicate_filter does not yield any cells when
                applied to row_key. Entries are applied in order,
                meaning that earlier mutations can be masked by later
                ones. Must contain at least one entry if
                `true_case_mutations is empty, and at most 100000.
            - operation_timeout: the time budget for the entire operation, in seconds.
                Failed requests will not be retried.
            - metadata: Strings which should be sent along with the request as metadata headers.
        Returns:
            - bool indicating whether the predicate was true or false
        Raises:
            - GoogleAPIError exceptions from grpc call
        """
        raise NotImplementedError

    async def read_modify_write_row(
        self,
        row_key: str | bytes,
        rules: ReadModifyWriteRule
        | list[ReadModifyWriteRule]
        | dict[str, Any]
        | list[dict[str, Any]],
        *,
        operation_timeout: int | float | None = 60,
        metadata: list[tuple[str, str]] | None = None,
    ) -> RowResponse:
        """
        Reads and modifies a row atomically according to input ReadModifyWriteRules,
        and returns the contents of all modified cells

        The new value for the timestamp is the greater of the existing timestamp or
        the current server time.

        Non-idempotent operation: will not be retried

        Args:
            - row_key: the key of the row to apply read/modify/write rules to
            - rules: A rule or set of rules to apply to the row.
                Rules are applied in order, meaning that earlier rules will affect the
                results of later ones.
           - operation_timeout: the time budget for the entire operation, in seconds.
                Failed requests will not be retried.
            - metadata: Strings which should be sent along with the request as metadata headers.
        Returns:
            - RowResponse: containing cell data that was modified as part of the
                operation
        Raises:
            - GoogleAPIError exceptions from grpc call
        """
        raise NotImplementedError
