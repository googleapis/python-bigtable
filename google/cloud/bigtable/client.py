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
    Optional,
    Set,
    TYPE_CHECKING,
)

import asyncio
import grpc
import time
import warnings
import sys
import random
from itertools import chain

from google.cloud.bigtable_v2.services.bigtable.client import BigtableClientMeta
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.services.bigtable.async_client import DEFAULT_CLIENT_INFO
from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
    PooledBigtableGrpcAsyncIOTransport,
)
from google.cloud.bigtable_v2.types.bigtable import PingAndWarmRequest
from google.cloud.client import ClientWithProject
from google.api_core.exceptions import GoogleAPICallError
from google.api_core import exceptions as core_exceptions
from google.api_core import retry_async as retries
from google.cloud.bigtable._read_rows import _ReadRowsOperation

import google.auth.credentials
import google.auth._default
from google.api_core import client_options as client_options_lib
from google.cloud.bigtable.row import Row
from google.cloud.bigtable.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.iterators import ReadRowsIterator

if TYPE_CHECKING:
    from google.cloud.bigtable.mutations import Mutation, BulkMutationsEntry
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
        # initialize client
        ClientWithProject.__init__(
            self,
            credentials=credentials,
            project=project,
            client_options=client_options,
        )
        self._gapic_client = BigtableAsyncClient(
            transport=transport_str,
            credentials=credentials,
            client_options=client_options,
            client_info=client_info,
        )
        self.transport = cast(
            PooledBigtableGrpcAsyncIOTransport, self._gapic_client.transport
        )
        # keep track of active instances to for warmup on channel refresh
        self._active_instances: Set[str] = set()
        # keep track of table objects associated with each instance
        # only remove instance from _active_instances when all associated tables remove it
        self._instance_owners: dict[str, Set[int]] = {}
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
                stacklevel=2,
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
            for channel_idx in range(self.transport.pool_size):
                refresh_task = asyncio.create_task(self._manage_channel(channel_idx))
                if sys.version_info >= (3, 8):
                    # task names supported in Python 3.8+
                    refresh_task.set_name(
                        f"{self.__class__.__name__} channel refresh {channel_idx}"
                    )
                self._channel_refresh_tasks.append(refresh_task)

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

    async def _ping_and_warm_instances(
        self, channel: grpc.aio.Channel
    ) -> list[GoogleAPICallError | None]:
        """
        Prepares the backend for requests on a channel

        Pings each Bigtable instance registered in `_active_instances` on the client

        Args:
            channel: grpc channel to ping
        Returns:
            - sequence of results or exceptions from the ping requests
        """
        ping_rpc = channel.unary_unary(
            "/google.bigtable.v2.Bigtable/PingAndWarm",
            request_serializer=PingAndWarmRequest.serialize,
        )
        tasks = [ping_rpc({"name": n}) for n in self._active_instances]
        result = await asyncio.gather(*tasks, return_exceptions=True)
        # return None in place of empty successful responses
        return [r or None for r in result]

    async def _manage_channel(
        self,
        channel_idx: int,
        refresh_interval_min: float = 60 * 35,
        refresh_interval_max: float = 60 * 45,
        grace_period: float = 60 * 10,
    ) -> None:
        """
        Background coroutine that periodically refreshes and warms a grpc channel

        The backend will automatically close channels after 60 minutes, so
        `refresh_interval` + `grace_period` should be < 60 minutes

        Runs continuously until the client is closed

        Args:
            channel_idx: index of the channel in the transport's channel pool
            refresh_interval_min: minimum interval before initiating refresh
                process in seconds. Actual interval will be a random value
                between `refresh_interval_min` and `refresh_interval_max`
            refresh_interval_max: maximum interval before initiating refresh
                process in seconds. Actual interval will be a random value
                between `refresh_interval_min` and `refresh_interval_max`
            grace_period: time to allow previous channel to serve existing
                requests before closing, in seconds
        """
        first_refresh = self._channel_init_time + random.uniform(
            refresh_interval_min, refresh_interval_max
        )
        next_sleep = max(first_refresh - time.time(), 0)
        if next_sleep > 0:
            # warm the current channel immediately
            channel = self.transport.channels[channel_idx]
            await self._ping_and_warm_instances(channel)
        # continuously refresh the channel every `refresh_interval` seconds
        while True:
            await asyncio.sleep(next_sleep)
            # prepare new channel for use
            new_channel = self.transport.grpc_channel._create_channel()
            await self._ping_and_warm_instances(new_channel)
            # cycle channel out of use, with long grace window before closure
            start_timestamp = time.time()
            await self.transport.replace_channel(
                channel_idx, grace=grace_period, swap_sleep=10, new_channel=new_channel
            )
            # subtract the time spent waiting for the channel to be replaced
            next_refresh = random.uniform(refresh_interval_min, refresh_interval_max)
            next_sleep = next_refresh - (time.time() - start_timestamp)

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
            if self._channel_refresh_tasks:
                # refresh tasks already running
                # call ping and warm on all existing channels
                for channel in self.transport.channels:
                    await self._ping_and_warm_instances(channel)
            else:
                # refresh tasks aren't active. start them as background tasks
                self.start_background_channel_refresh()

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
        self.start_background_channel_refresh()
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
    ) -> list[Row]:
        """
        Runs a sharded query in parallel

        Each query in query list will be run concurrently, with results yielded as they are ready
        yielded results may be out of order

        Args:
            - query_list: a list of queries to run in parallel
        """
        if not query_list:
            raise ValueError("query_list must contain at least one query")
        routine_list = [
            self.read_rows(
                query,
                operation_timeout=operation_timeout,
                per_request_timeout=per_request_timeout,
            )
            for query in query_list
        ]
        results_lists = asyncio.gather(*routine_list)
        final_results = list(chain.from_iterable(results_lists))
        return final_results

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
        operation_timeout = operation_timeout or self.default_operation_timeout
        per_request_timeout = per_request_timeout or self.default_per_request_timeout

        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout <= 0:
            raise ValueError("per_request_timeout must be greater than 0")
        if per_request_timeout is not None and per_request_timeout > operation_timeout:
            raise ValueError("per_request_timeout must be less than operation_timeout")
        retry = retries.AsyncRetry(
            predicate=retries.if_exception_type(
                core_exceptions.DeadlineExceeded,
                core_exceptions.ServiceUnavailable,
                core_exceptions.Aborted,
            ),
            timeout=operation_timeout,
            initial=0.01,
            multiplier=2,
            maximum=60,
        )

        async def _get_rows():
            gen = await self.client._gapic_client.sample_row_keys(
                table_name=self.table_name,
                app_profile_id=self.app_profile_id,
                timeout=per_request_timeout,
            )
            return [(s.row_key, s.offset_bytes) async for s in gen]

        wrapped_call = retry(_get_rows)
        return wrapped_call()

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
        raise NotImplementedError

    async def bulk_mutate_rows(
        self,
        mutation_entries: list[BulkMutationsEntry],
        *,
        operation_timeout: int | float | None = 60,
        per_request_timeout: int | float | None = None,
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
        self._register_instance_task.cancel()
        await self.client._remove_instance_registration(self.instance_id, self)

    async def __aenter__(self):
        """
        Implement async context manager protocol

        Ensure registration task has time to run, so that
        grpc channels will be warmed for the specified instance
        """
        await self._register_instance_task
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Implement async context manager protocol

        Unregister this instance with the client, so that
        grpc channels will no longer be warmed
        """
        await self.close()
