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

from typing import cast, Any, AsyncIterable, Optional, TYPE_CHECKING

import asyncio
import grpc
import time

from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
    PooledBigtableGrpcAsyncIOTransport,
)
from google.cloud.client import ClientWithProject

import google.auth.credentials

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
        credentials: google.auth.credentials.Credentials | None = None,
        client_options: dict[str, Any]
        | "google.api_core.client_options.ClientOptions"
        | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ):
        """
        Create a client instance

        Args:
            metadata: a list of metadata headers to be attached to all calls with this client
        """
        super(BigtableDataClient, self).__init__(
            project=project,
            credentials=credentials,
            client_options=client_options,
        )
        if type(client_options) is dict:
            client_options = google.api_core.client_options.from_dict(client_options)
        client_options = cast(
            Optional["google.api_core.client_options.ClientOptions"], client_options
        )
        self._gapic_client = BigtableAsyncClient(
            credentials=credentials,
            transport="pooled_grpc_asyncio",
            client_options=client_options,
        )
        self.transport: PooledBigtableGrpcAsyncIOTransport = cast(
            PooledBigtableGrpcAsyncIOTransport, self._gapic_client.transport
        )

    async def get_table(
        self,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
        manage_channels: bool = True,
    ) -> Table:
        table = Table(self, instance_id, table_id, app_profile_id)
        if manage_channels:
            for channel_idx in range(self.transport.pool_size):
                refresh_task = asyncio.create_task(table._manage_channel(channel_idx))
                table._channel_refresh_tasks.append(refresh_task)
        return table


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
        self.client = client
        self.instance_id = instance_id
        self.table_id = table_id
        self.app_profile_id = app_profile_id
        self._channel_refresh_tasks: list[asyncio.Task[None]] = []

    async def _manage_channel(
        self,
        channel_idx: int,
        refresh_interval: float = 60 * 45,
        grace_period: float = 60 * 15,
    ) -> None:
        channel = self.client.transport.get_channel(channel_idx)
        start_timestamp = time.time()
        while True:
            # warm caches on new channel
            await self._ping_and_warm_channel(channel)
            # let channel serve rpcs until expirary
            next_sleep = refresh_interval - (time.time() - start_timestamp)
            await asyncio.sleep(next_sleep)
            start_timestamp = time.time()
            # cycle channel out of use, with long grace window
            channel = await self.client.transport.replace_channel(
                channel_idx, grace_period
            )

    async def _ping_and_warm_channel(self, channel: grpc.aio.Channel) -> None:
        ping_rpc = channel.unary_unary(
            "/google.bigtable.v2.Bigtable/PingAndWarmChannel"
        )
        await ping_rpc(
            {
                "name": self.client._gapic_client.instance_path(
                    self.client.project, self.instance_id
                )
            }
        )

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
        raise NotImplementedError

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
