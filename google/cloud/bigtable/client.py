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

from typing import (
    cast,
    Any,
    Coroutine,
    Optional,
    Set,
    Callable,
    TYPE_CHECKING,
)

import asyncio
from grpc.experimental import aio  # type: ignore
from functools import partial

from google.cloud.client import ClientWithProject
from google.api_core.exceptions import GoogleAPICallError
from google.api_core import retry_async as retries
from google.api_core import exceptions as core_exceptions

from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.services.bigtable.async_client import DEFAULT_CLIENT_INFO

import google.auth.credentials
import google.auth._default
from google.api_core import client_options as client_options_lib
from google.cloud.bigtable.row import Row
from google.cloud.bigtable.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.iterators import ReadRowsIterator
from google.cloud.bigtable._read_rows import _ReadRowsOperation
from google.cloud.bigtable.mutations import Mutation, RowMutationEntry
from google.cloud.bigtable._mutate_rows import _MutateRowsOperation
from google.cloud.bigtable._helpers import _make_metadata
from google.cloud.bigtable._helpers import _convert_retry_deadline
from google.cloud.bigtable._channel_pooling.dynamic_pooled_channel import (
    DynamicPooledChannel,
)
from google.cloud.bigtable._channel_pooling.dynamic_pooled_channel import (
    DynamicPoolOptions,
)
from google.cloud.bigtable._channel_pooling.refreshable_channel import (
    RefreshableChannel,
)
from google.cloud.bigtable._channel_pooling.pooled_channel import PooledChannel
from google.cloud.bigtable._channel_pooling.pooled_channel import StaticPoolOptions
from google.cloud.bigtable_v2.services.bigtable.transports import (
    BigtableGrpcAsyncIOTransport,
)

if TYPE_CHECKING:
    from google.cloud.bigtable.mutations_batcher import MutationsBatcher
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
        channel_pool_options: DynamicPoolOptions | StaticPoolOptions | None = None,
    ):
        """
        Create a client instance for the Bigtable Data API

        Client should be created within an async context (running event loop)

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
        Raises:
          - RuntimeError if called outside of an async context (no running event loop)
          - ValueError if pool_size is less than 1
        """

        async def destroy_channel_gracefully(channel: aio.Channel):
            await asyncio.sleep(10)
            await channel.close(grace=600)

        # set up channel pool
        warm_fn = self._ping_and_warm_instances
        create_base_channel = (
            lambda *args, **kwargs: BigtableGrpcAsyncIOTransport.create_channel(
                *args, **kwargs
            )
        )
        create_refreshable_channel = (
            lambda *args, **kwargs: RefreshableChannel(  # noqa: E731
                create_channel_fn=partial(create_base_channel, *args, **kwargs),
                on_replace=destroy_channel_gracefully,
                warm_channel_fn=warm_fn,
            )
        )
        if channel_pool_options is None:
            channel_pool_options = DynamicPoolOptions()
        if isinstance(channel_pool_options, StaticPoolOptions):
            create_pool_channel = lambda *args, **kwargs: PooledChannel(  # noqa: E731
                create_channel_fn=partial(create_refreshable_channel, *args, **kwargs),
                pool_options=channel_pool_options,
            )
        else:
            create_pool_channel = lambda *args, **kwargs: DynamicPooledChannel(  # noqa: E731
                create_channel_fn=partial(create_refreshable_channel, *args, **kwargs),
                pool_options=channel_pool_options,
                on_remove=destroy_channel_gracefully,
                # warm_channel_fn=warm_fn, # refreshable channel already warms
            )
        # set up client info headers for veneer library
        client_info = DEFAULT_CLIENT_INFO
        client_info.client_library_version = client_info.gapic_version
        # parse client options
        if type(client_options) is dict:
            client_options = client_options_lib.from_dict(client_options)
        client_options = cast(
            Optional[client_options_lib.ClientOptions], client_options
        )
        # initialize client
        ClientWithProject.__init__(
            self,
            credentials=credentials,
            project=project,
            client_options=client_options,
        )
        self._gapic_client = BigtableAsyncClient(
            credentials=credentials,
            client_options=client_options,
            client_info=client_info,
            transport=lambda *args, **kwargs: BigtableGrpcAsyncIOTransport(*args, **kwargs, channel=create_pool_channel),
        )
        transport = cast(BigtableGrpcAsyncIOTransport, self._gapic_client.transport)
        self._pool = cast(PooledChannel, transport.grpc_channel)
        # keep track of active instances to for warmup on channel refresh
        self._active_instances: Set[str] = set()
        # keep track of table objects associated with each instance
        # only remove instance from _active_instances when all associated tables remove it
        self._instance_owners: dict[str, Set[int]] = {}
        # attempt to start background tasks
        self._channel_refresh_tasks: list[asyncio.Task[None]] = []
        # TODO: make sure this check is in place
        # except RuntimeError:
        #     warnings.warn(
        #         f"{self.__class__.__name__} should be started in an "
        #         "asyncio event loop. Channel refresh will not be started",
        #         RuntimeWarning,
        #         stacklevel=2,
        #     )

    async def close(self, timeout: float = 2.0):
        """
        Cancel all background tasks
        """
        for task in self._channel_refresh_tasks:
            task.cancel()
        group = asyncio.gather(*self._channel_refresh_tasks, return_exceptions=True)
        await asyncio.wait_for(group, timeout=timeout)
        await self._gapic_client.transport.close()
        self._channel_refresh_tasks = []

    async def _ping_and_warm_instances(
        self, channel: aio.Channel, instance_id: str | None = None
    ) -> list[GoogleAPICallError | None]:
        """
        Prepares the backend for requests on a channel

        Pings each Bigtable instance registered in `_active_instances` on the client

        Args:
            channel: grpc channel to ping
        Returns:
            - sequence of results or exceptions from the ping requests
        """
        instance_list = [instance_id] if instance_id else self._active_instances
        ping_rpc = channel.unary_unary(
            "/google.bigtable.v2.Bigtable/PingAndWarmChannel"
        )
        tasks = [ping_rpc({"name": n}) for n in instance_list]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _register_instance(self, instance_id: str, owner: Table) -> None:
        """
        Registers an instance with the client, and warms the channel pool
        for the instance
        The client will periodically refresh grpc channel pool used to make
        requests, and new channels will be warmed for each registered instance
        Channels will not be refreshed unless at least one instance is registered

        Args:
          - instance_id: id of the instance to register.
          - owner: table that owns the instance. Owners will be tracked in
            _instance_owners, and instances will only be unregistered when all
            owners call _remove_instance_registration
        """
        instance_name = self._gapic_client.instance_path(self.project, instance_id)
        self._instance_owners.setdefault(instance_name, set()).add(id(owner))
        if instance_name not in self._active_instances:
            self._active_instances.add(instance_name)
            # call ping and warm on all existing channels
            for channel in self._pool.channels:
                await self._ping_and_warm_instances(channel, instance_name)

    async def _remove_instance_registration(
        self, instance_id: str, owner: Table
    ) -> bool:
        """
        Removes an instance from the client's registered instances, to prevent
        warming new channels for the instance

        If instance_id is not registered, or is still in use by other tables, returns False

        Args:
            - instance_id: id of the instance to remove
            - owner: table that owns the instance. Owners will be tracked in
              _instance_owners, and instances will only be unregistered when all
              owners call _remove_instance_registration
        Returns:
            - True if instance was removed
        """
        instance_name = self._gapic_client.instance_path(self.project, instance_id)
        owner_list = self._instance_owners.get(instance_name, set())
        try:
            owner_list.remove(id(owner))
            if len(owner_list) == 0:
                self._active_instances.remove(instance_name)
            return True
        except KeyError:
            return False

    # TODO: revisit timeouts https://github.com/googleapis/python-bigtable/issues/782
    def get_table(
        self,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
        default_operation_timeout: float = 600,
        default_per_request_timeout: float | None = None,
    ) -> Table:
        """
        Returns a table instance for making data API requests

        Args:
            instance_id: The Bigtable instance ID to associate with this client.
                instance_id is combined with the client's project to fully
                specify the instance
            table_id: The ID of the table.
            app_profile_id: (Optional) The app profile to associate with requests.
                https://cloud.google.com/bigtable/docs/app-profiles
        """
        return Table(
            self,
            instance_id,
            table_id,
            app_profile_id,
            default_operation_timeout=default_operation_timeout,
            default_per_request_timeout=default_per_request_timeout,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        await self._gapic_client.__aexit__(exc_type, exc_val, exc_tb)


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
        *,
        default_operation_timeout: float = 600,
        default_per_request_timeout: float | None = None,
    ):
        """
        Initialize a Table instance

        Must be created within an async context (running event loop)

        Args:
            instance_id: The Bigtable instance ID to associate with this client.
                instance_id is combined with the client's project to fully
                specify the instance
            table_id: The ID of the table. table_id is combined with the
                instance_id and the client's project to fully specify the table
            app_profile_id: (Optional) The app profile to associate with requests.
                https://cloud.google.com/bigtable/docs/app-profiles
            default_operation_timeout: (Optional) The default timeout, in seconds
            default_per_request_timeout: (Optional) The default timeout for individual
                rpc requests, in seconds
        Raises:
          - RuntimeError if called outside of an async context (no running event loop)
        """
        # validate timeouts
        if default_operation_timeout <= 0:
            raise ValueError("default_operation_timeout must be greater than 0")
        if default_per_request_timeout is not None and default_per_request_timeout <= 0:
            raise ValueError("default_per_request_timeout must be greater than 0")
        if (
            default_per_request_timeout is not None
            and default_per_request_timeout > default_operation_timeout
        ):
            raise ValueError(
                "default_per_request_timeout must be less than default_operation_timeout"
            )
        self.client = client
        self.instance_id = instance_id
        self.instance_name = self.client._gapic_client.instance_path(
            self.client.project, instance_id
        )
        self.table_id = table_id
        self.table_name = self.client._gapic_client.table_path(
            self.client.project, instance_id, table_id
        )
        self.app_profile_id = app_profile_id

        self.default_operation_timeout = default_operation_timeout
        self.default_per_request_timeout = default_per_request_timeout

        # raises RuntimeError if called outside of an async context (no running event loop)
        try:
            self._register_instance_task = asyncio.create_task(
                self.client._register_instance(instance_id, self)
            )
        except RuntimeError as e:
            raise RuntimeError(
                f"{self.__class__.__name__} must be created within an async event loop context."
            ) from e

    async def read_rows_stream(
        self,
        query: ReadRowsQuery | dict[str, Any],
        *,
        operation_timeout: float | None = None,
        per_request_timeout: float | None = None,
    ) -> ReadRowsIterator:
        """
        Returns an iterator to asynchronously stream back row data.

        Failed requests within operation_timeout and operation_deadline policies will be retried.

        Args:
            - query: contains details about which rows to return
            - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 time is only counted while actively waiting on the network.
                 If None, defaults to the Table's default_operation_timeout
            - per_request_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                If None, defaults to the Table's default_per_request_timeout

        Returns:
            - an asynchronous iterator that yields rows returned by the query
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions
                from any retries that failed
            - GoogleAPIError: raised if the request encounters an unrecoverable error
            - IdleTimeout: if iterator was abandoned
        """
        operation_timeout = operation_timeout or self.default_operation_timeout
        per_request_timeout = per_request_timeout or self.default_per_request_timeout

        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout <= 0:
            raise ValueError("per_request_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout > operation_timeout:
            raise ValueError(
                "per_request_timeout must not be greater than operation_timeout"
            )
        if per_request_timeout is None:
            per_request_timeout = operation_timeout
        request = query._to_dict() if isinstance(query, ReadRowsQuery) else query
        request["table_name"] = self.table_name
        if self.app_profile_id:
            request["app_profile_id"] = self.app_profile_id

        # read_rows smart retries is implemented using a series of iterators:
        # - client.read_rows: outputs raw ReadRowsResponse objects from backend. Has per_request_timeout
        # - ReadRowsOperation.merge_row_response_stream: parses chunks into rows
        # - ReadRowsOperation.retryable_merge_rows: adds retries, caching, revised requests, per_request_timeout
        # - ReadRowsIterator: adds idle_timeout, moves stats out of stream and into attribute
        row_merger = _ReadRowsOperation(
            request,
            self.client._gapic_client,
            operation_timeout=operation_timeout,
            per_request_timeout=per_request_timeout,
        )
        output_generator = ReadRowsIterator(row_merger)
        # add idle timeout to clear resources if generator is abandoned
        idle_timeout_seconds = 300
        await output_generator._start_idle_timer(idle_timeout_seconds)
        return output_generator

    async def read_rows(
        self,
        query: ReadRowsQuery | dict[str, Any],
        *,
        operation_timeout: float | None = None,
        per_request_timeout: float | None = None,
    ) -> list[Row]:
        """
        Helper function that returns a full list instead of a generator

        See read_rows_stream

        Returns:
            - a list of the rows returned by the query
        """
        row_generator = await self.read_rows_stream(
            query,
            operation_timeout=operation_timeout,
            per_request_timeout=per_request_timeout,
        )
        results = [row async for row in row_generator]
        return results

    async def read_row(
        self,
        row_key: str | bytes,
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
    ) -> Row:
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
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
    ) -> ReadRowsIterator:
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
        operation_timeout: float | None = 60,
        per_request_timeout: float | None = None,
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

        Raises:
             - DeadlineExceeded: raised after operation timeout
                 will be chained with a RetryExceptionGroup containing all
                 GoogleAPIError exceptions from any retries that failed
             - GoogleAPIError: raised on non-idempotent operations that cannot be
                 safely retried.
        """
        operation_timeout = operation_timeout or self.default_operation_timeout
        per_request_timeout = per_request_timeout or self.default_per_request_timeout

        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout <= 0:
            raise ValueError("per_request_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout > operation_timeout:
            raise ValueError("per_request_timeout must be less than operation_timeout")

        if isinstance(row_key, str):
            row_key = row_key.encode("utf-8")
        request = {"table_name": self.table_name, "row_key": row_key}
        if self.app_profile_id:
            request["app_profile_id"] = self.app_profile_id

        if isinstance(mutations, Mutation):
            mutations = [mutations]
        request["mutations"] = [mutation._to_dict() for mutation in mutations]

        if all(mutation.is_idempotent() for mutation in mutations):
            # mutations are all idempotent and safe to retry
            predicate = retries.if_exception_type(
                core_exceptions.DeadlineExceeded,
                core_exceptions.ServiceUnavailable,
            )
        else:
            # mutations should not be retried
            predicate = retries.if_exception_type()

        transient_errors = []

        def on_error_fn(exc):
            if predicate(exc):
                transient_errors.append(exc)

        retry = retries.AsyncRetry(
            predicate=predicate,
            on_error=on_error_fn,
            timeout=operation_timeout,
            initial=0.01,
            multiplier=2,
            maximum=60,
        )
        # wrap rpc in retry logic
        retry_wrapped = retry(self.client._gapic_client.mutate_row)
        # convert RetryErrors from retry wrapper into DeadlineExceeded errors
        deadline_wrapped = _convert_retry_deadline(
            retry_wrapped, operation_timeout, transient_errors
        )
        metadata = _make_metadata(self.table_name, self.app_profile_id)
        # trigger rpc
        await deadline_wrapped(request, timeout=per_request_timeout, metadata=metadata)

    async def bulk_mutate_rows(
        self,
        mutation_entries: list[RowMutationEntry],
        *,
        operation_timeout: float | None = 60,
        per_request_timeout: float | None = None,
        on_success: Callable[
            [int, RowMutationEntry], None | Coroutine[None, None, None]
        ]
        | None = None,
    ):
        """
        Applies mutations for multiple rows in a single batched request.

        Each individual RowMutationEntry is applied atomically, but separate entries
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
            - on_success: a callback function that will be called when each mutation
                entry is confirmed to be applied successfully. Will be passed the
                index and the entry itself.
        Raises:
            - MutationsExceptionGroup if one or more mutations fails
                Contains details about any failed entries in .exceptions
        """
        operation_timeout = operation_timeout or self.default_operation_timeout
        per_request_timeout = per_request_timeout or self.default_per_request_timeout

        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout <= 0:
            raise ValueError("per_request_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout > operation_timeout:
            raise ValueError("per_request_timeout must be less than operation_timeout")

        operation = _MutateRowsOperation(
            self.client._gapic_client,
            self,
            mutation_entries,
            operation_timeout,
            per_request_timeout,
        )
        await operation.start()

    async def check_and_mutate_row(
        self,
        row_key: str | bytes,
        predicate: RowFilter | None,
        true_case_mutations: Mutation | list[Mutation] | None = None,
        false_case_mutations: Mutation | list[Mutation] | None = None,
        operation_timeout: int | float | None = 60,
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
    ) -> Row:
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
        Returns:
            - Row: containing cell data that was modified as part of the
                operation
        Raises:
            - GoogleAPIError exceptions from grpc call
        """
        raise NotImplementedError

    async def close(self):
        """
        Called to close the Table instance and release any resources held by it.
        """
        await self.client._remove_instance_registration(self.instance_id, self)

    async def __aenter__(self):
        """
        Implement async context manager protocol

        Register this instance with the client, so that
        grpc channels will be warmed for the specified instance
        """
        await self.client._register_instance(self.instance_id, self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Implement async context manager protocol

        Unregister this instance with the client, so that
        grpc channels will no longer be warmed
        """
        await self.close()
