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

from typing import cast, Any, Optional, AsyncIterable, Set, TYPE_CHECKING

import asyncio
import grpc
import time
import warnings
import sys

from google.cloud.bigtable_v2.services.bigtable.client import BigtableClientMeta
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.services.bigtable.async_client import DEFAULT_CLIENT_INFO
from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
    PooledBigtableGrpcAsyncIOTransport,
)
from google.cloud.client import _ClientProjectMixin
from google.api_core.exceptions import GoogleAPICallError


import google.auth.credentials
import google.auth._default
from google.api_core import client_options as client_options_lib


if TYPE_CHECKING:
    from google.cloud.bigtable.mutations import Mutation, BulkMutationsEntry
    from google.cloud.bigtable.mutations_batcher import MutationsBatcher
    from google.cloud.bigtable.row_response import RowResponse
    from google.cloud.bigtable.read_rows_query import ReadRowsQuery
    from google.cloud.bigtable import RowKeySamples
    from google.cloud.bigtable.row_filters import RowFilter
    from google.cloud.bigtable.read_modify_write_rules import ReadModifyWriteRule


class BigtableDataClient(BigtableAsyncClient, _ClientProjectMixin):
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

        Client must be created within an async run loop context

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
        Raises:
          - RuntimeError if called outside of an async run loop context
          - ValueError if pool_size is less than 1
        """
        # set up transport in registry
        transport_str = f"pooled_grpc_asyncio_{pool_size}"
        transport = PooledBigtableGrpcAsyncIOTransport.with_fixed_size(pool_size)
        BigtableClientMeta._transport_registry[transport_str] = transport
        # set up client info headers for veneer library
        client_info = DEFAULT_CLIENT_INFO
        client_info.client_library_version = client_info.gapic_version
        # parse client options
        if type(client_options) is dict:
            client_options = client_options_lib.from_dict(client_options)
        client_options = cast(
            Optional[client_options_lib.ClientOptions], client_options
        )
        mixin_args = {"project": project, "credentials": credentials}
        # support google-api-core <=1.5.0, which does not have credentials
        if "credentials" not in _ClientProjectMixin.__init__.__code__.co_varnames:
            mixin_args.pop("credentials")
        # initialize client
        _ClientProjectMixin.__init__(self, **mixin_args)
        # raises RuntimeError if called outside of an async run loop context
        BigtableAsyncClient.__init__(
            self,
            transport=transport_str,
            credentials=credentials,
            client_options=client_options,
            client_info=client_info,
        )
        self.metadata = metadata or []
        # keep track of active instances to for warmup on channel refresh
        self._active_instances: Set[str] = set()
        # attempt to start background tasks
        self._channel_init_time = time.time()
        self._channel_refresh_tasks: list[asyncio.Task[None]] = []
        try:
            self.start_background_channel_refresh()
        except RuntimeError:
            warnings.warn(
                f"{self.__class__.__name__} should be started in an "
                "asyncio event loop. Channel refresh will not be started",
                RuntimeWarning,
            )

    def start_background_channel_refresh(self) -> None:
        """
        Starts a background task to ping and warm each channel in the pool
        Raises:
          - RuntimeError if not called in an asyncio event loop
        """
        if not self._channel_refresh_tasks:
            # raise RuntimeError if there is no event loop
            asyncio.get_running_loop()
            for channel_idx in range(len(self.transport._grpc_channel._pool)):
                refresh_task = asyncio.create_task(self._manage_channel(channel_idx))
                if sys.version_info >= (3, 8):
                    refresh_task.set_name(
                        f"{self.__class__.__name__} channel refresh {channel_idx}"
                    )
                self._channel_refresh_tasks.append(refresh_task)

    @property
    def transport(self) -> PooledBigtableGrpcAsyncIOTransport:
        """Returns the transport used by the client instance.
        Returns:
            BigtableTransport: The transport used by the client instance.
        """
        return cast(PooledBigtableGrpcAsyncIOTransport, self._client.transport)

    async def close(self, timeout: float = 2.0):
        """
        Cancel all background tasks
        """
        for task in self._channel_refresh_tasks:
            task.cancel()
        group = asyncio.gather(*self._channel_refresh_tasks, return_exceptions=True)
        await asyncio.wait_for(group, timeout=timeout)
        await self.transport.close()
        self._channel_refresh_tasks = []

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Cleanly close context manager on exit
        """
        await self.close()

    async def _ping_and_warm_instances(
        self, channel: grpc.aio.Channel
    ) -> list[GoogleAPICallError | None]:
        """
        Prepares the backend for requests on a channel

        Pings each Bigtable instance registered in `_active_instances` on the client

        Args:
            channel: grpc channel to ping
        Returns:
            - squence of results or exceptions from the ping requests
        """
        ping_rpc = channel.unary_unary(
            "/google.bigtable.v2.Bigtable/PingAndWarmChannel"
        )
        tasks = [ping_rpc({"name": n}) for n in self._active_instances]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _manage_channel(
        self,
        channel_idx: int,
        refresh_interval: float = 60 * 45,
        grace_period: float = 60 * 15,
    ) -> None:
        """
        Background coroutine that periodically refreshes and warms a grpc channel

        The backend will automatically close channels after 60 minutes, so
        `refresh_interval` + `grace_period` should be < 60 minutes

        Runs continuously until the client is closed

        Args:
            channel_idx: index of the channel in the transport's channel pool
            refresh_interval: interval before initiating refresh process in seconds
            grace_period: time to allow previous channel to serve existing
                requests before closing, in seconds
        """
        first_refresh = self._channel_init_time + refresh_interval
        next_sleep = max(first_refresh - time.time(), 0)
        if next_sleep > 0:
            # warm the current channel immediately
            channel = self.transport._grpc_channel._pool[channel_idx]
            await self._ping_and_warm_instances(channel)
        # continuously refresh the channel every `refresh_interval` seconds
        while True:
            await asyncio.sleep(next_sleep)
            # prepare new channel for use
            new_channel = self.transport.create_channel(
                self.transport._host,
                credentials=self.transport._credentials,
                scopes=self.transport._scopes,
                ssl_credentials=self.transport._ssl_channel_credentials,
                quota_project_id=self.transport._quota_project_id,
                options=[
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                ],
            )
            await self._ping_and_warm_instances(new_channel)
            # cycle channel out of use, with long grace window before closure
            start_timestamp = time.time()
            await self.transport.replace_channel(channel_idx, grace_period, new_channel)
            # subtract the time spent waiting for the channel to be replaced
            next_sleep = refresh_interval - (time.time() - start_timestamp)

    async def register_instance(self, instance_id: str):
        """
        Registers an instance with the client, and warms the channel pool
        for the instance
        The client will periodically refresh grpc channel pool used to make
        requests, and new channels will be warmed for each registered instance
        Channels will not be refreshed unless at least one instance is registered
        """
        instance_name = self.instance_path(self.project, instance_id)
        if instance_name not in self._active_instances:
            self._active_instances.add(instance_name)
            if self._channel_refresh_tasks:
                # refresh tasks already running
                # call ping and warm on all existing channels
                for channel in self.transport._grpc_channel._pool:
                    await self._ping_and_warm_instances(channel)
            else:
                # refresh tasks aren't active. start them as background tasks
                self.start_background_channel_refresh()

    async def remove_instance_registration(self, instance_id: str) -> bool:
        """
        Removes an instance from the client's registered instances, to prevent
        warming new channels for the instance

        If instance_id is not registered, returns False

        Args:
            instance_id: id of the instance to remove
        Returns:
            - True if instance was removed
        """
        instance_name = self.instance_path(self.project, instance_id)
        try:
            self._active_instances.remove(instance_name)
            return True
        except KeyError:
            return False

    def get_table(
        self,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ) -> Table:
        """
        Returns a table instance for making data API requests

        Args:
            instance_id: The Bigtable instance ID to associate with this client
                instance_id is combined with the client's project to fully
                specify the instance
            table_id: The ID of the table.
            app_profile_id: (Optional) The app profile to associate with requests.
                https://cloud.google.com/bigtable/docs/app-profiles
            metadata: a list of metadata headers to be attached to all calls with this client
        """
        return Table(self, instance_id, table_id, app_profile_id, metadata)


class Table:
    """
    Main Data API surface

    Table object maintains table_id, and app_profile_id context, and passes them with
    each call
    """

    def __init__(
        self,
        client: BigtableDataClient,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
        metadata: list[tuple[str, str]] | None = None,
    ):
        """
        Initialize a Table instance

        Must be created within an async run loop context

        Args:
            instance_id: The Bigtable instance ID to associate with this client
                instance_id is combined with the client's project to fully
                specify the instance
            table_id: The ID of the table.
            app_profile_id: (Optional) The app profile to associate with requests.
                https://cloud.google.com/bigtable/docs/app-profiles
            metadata: a list of metadata headers to be attached to all calls with this client
        Raises:
          - RuntimeError if called outside of an async run loop context
        """
        self.client = client
        self.instance = instance_id
        self.table_id = table_id
        self.app_profile_id = app_profile_id
        self.metadata = metadata or []
        # raises RuntimeError if called outside of an async run loop context
        try:
            self._register_instance_task = asyncio.create_task(
                self.client.register_instance(instance_id)
            )
        except RuntimeError:
            warnings.warn(
                "Table should be created in an asyncio event loop."
                " Instance will not be registered with client for refresh",
                RuntimeWarning,
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
