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
    AsyncIterable,
    Optional,
    Set,
    Sequence,
    TYPE_CHECKING,
)

import asyncio
import grpc
import time
import warnings
import sys
import random
import os

from functools import partial

from google.cloud.bigtable_v2.services.bigtable.client import BigtableClientMeta
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.services.bigtable.async_client import DEFAULT_CLIENT_INFO
from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
    PooledBigtableGrpcAsyncIOTransport,
    PooledChannel,
)
from google.cloud.bigtable_v2.types.bigtable import PingAndWarmRequest
from google.cloud.client import ClientWithProject
from google.cloud.environment_vars import BIGTABLE_EMULATOR  # type: ignore
from google.api_core import retry as retries
from google.api_core.exceptions import DeadlineExceeded
from google.api_core.exceptions import ServiceUnavailable
from google.api_core.exceptions import Aborted
from google.cloud.bigtable.data._async._read_rows import _ReadRowsOperationAsync

import google.auth.credentials
import google.auth._default
from google.api_core import client_options as client_options_lib
from google.cloud.bigtable.client import _DEFAULT_BIGTABLE_EMULATOR_CLIENT
from google.cloud.bigtable.data.row import Row
from google.cloud.bigtable.data.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.data.exceptions import FailedQueryShardError
from google.cloud.bigtable.data.exceptions import ShardedReadRowsExceptionGroup

from google.cloud.bigtable.data.mutations import Mutation, RowMutationEntry
from google.cloud.bigtable.data._async._mutate_rows import _MutateRowsOperationAsync
from google.cloud.bigtable.data._helpers import TABLE_DEFAULT
from google.cloud.bigtable.data._helpers import _WarmedInstanceKey
from google.cloud.bigtable.data._helpers import _CONCURRENCY_LIMIT
from google.cloud.bigtable.data._helpers import _make_metadata
from google.cloud.bigtable.data._helpers import _retry_exception_factory
from google.cloud.bigtable.data._helpers import _validate_timeouts
from google.cloud.bigtable.data._helpers import _get_retryable_errors
from google.cloud.bigtable.data._helpers import _get_timeouts
from google.cloud.bigtable.data._helpers import _attempt_timeout_generator
from google.cloud.bigtable.data._async.mutations_batcher import MutationsBatcherAsync
from google.cloud.bigtable.data._async.mutations_batcher import _MB_SIZE
from google.cloud.bigtable.data.read_modify_write_rules import ReadModifyWriteRule
from google.cloud.bigtable.data.row_filters import RowFilter
from google.cloud.bigtable.data.row_filters import StripValueTransformerFilter
from google.cloud.bigtable.data.row_filters import CellsRowLimitFilter
from google.cloud.bigtable.data.row_filters import RowFilterChain


if TYPE_CHECKING:
    from google.cloud.bigtable.data._helpers import RowKeySamples
    from google.cloud.bigtable.data._helpers import ShardedQuery


class BigtableDataClientAsync(ClientWithProject):
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
        client_info.client_library_version = self._client_version()
        # parse client options
        if type(client_options) is dict:
            client_options = client_options_lib.from_dict(client_options)
        client_options = cast(
            Optional[client_options_lib.ClientOptions], client_options
        )
        self._emulator_host = os.getenv(BIGTABLE_EMULATOR)
        if self._emulator_host is not None:
            # use insecure channel if emulator is set
            if credentials is None:
                credentials = google.auth.credentials.AnonymousCredentials()
            if project is None:
                project = _DEFAULT_BIGTABLE_EMULATOR_CLIENT
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
        self._active_instances: Set[_WarmedInstanceKey] = set()
        # keep track of table objects associated with each instance
        # only remove instance from _active_instances when all associated tables remove it
        self._instance_owners: dict[_WarmedInstanceKey, Set[int]] = {}
        self._channel_init_time = time.monotonic()
        self._channel_refresh_tasks: list[asyncio.Task[None]] = []
        if self._emulator_host is not None:
            # connect to an emulator host
            warnings.warn(
                "Connecting to Bigtable emulator at {}".format(self._emulator_host),
                RuntimeWarning,
                stacklevel=2,
            )
            self.transport._grpc_channel = PooledChannel(
                pool_size=pool_size,
                host=self._emulator_host,
                insecure=True,
            )
            # refresh cached stubs to use emulator pool
            self.transport._stubs = {}
            self.transport._prep_wrapped_messages(client_info)
        else:
            # attempt to start background channel refresh tasks
            try:
                self._start_background_channel_refresh()
            except RuntimeError:
                warnings.warn(
                    f"{self.__class__.__name__} should be started in an "
                    "asyncio event loop. Channel refresh will not be started",
                    RuntimeWarning,
                    stacklevel=2,
                )

    @staticmethod
    def _client_version() -> str:
        """
        Helper function to return the client version string for this client
        """
        return f"{google.cloud.bigtable.__version__}-data-async"

    def _start_background_channel_refresh(self) -> None:
        """
        Starts a background task to ping and warm each channel in the pool
        Raises:
          - RuntimeError if not called in an asyncio event loop
        """
        if not self._channel_refresh_tasks and not self._emulator_host:
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
        self, channel: grpc.aio.Channel, instance_key: _WarmedInstanceKey | None = None
    ) -> list[BaseException | None]:
        """
        Prepares the backend for requests on a channel

        Pings each Bigtable instance registered in `_active_instances` on the client

        Args:
            - channel: grpc channel to warm
            - instance_key: if provided, only warm the instance associated with the key
        Returns:
            - sequence of results or exceptions from the ping requests
        """
        instance_list = (
            [instance_key] if instance_key is not None else self._active_instances
        )
        ping_rpc = channel.unary_unary(
            "/google.bigtable.v2.Bigtable/PingAndWarm",
            request_serializer=PingAndWarmRequest.serialize,
        )
        # prepare list of coroutines to run
        tasks = [
            ping_rpc(
                request={"name": instance_name, "app_profile_id": app_profile_id},
                metadata=[
                    (
                        "x-goog-request-params",
                        f"name={instance_name}&app_profile_id={app_profile_id}",
                    )
                ],
                wait_for_ready=True,
            )
            for (instance_name, table_name, app_profile_id) in instance_list
        ]
        # execute coroutines in parallel
        result_list = await asyncio.gather(*tasks, return_exceptions=True)
        # return None in place of empty successful responses
        return [r or None for r in result_list]

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
        next_sleep = max(first_refresh - time.monotonic(), 0)
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

    async def _register_instance(self, instance_id: str, owner: TableAsync) -> None:
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
        instance_key = _WarmedInstanceKey(
            instance_name, owner.table_name, owner.app_profile_id
        )
        self._instance_owners.setdefault(instance_key, set()).add(id(owner))
        if instance_name not in self._active_instances:
            self._active_instances.add(instance_key)
            if self._channel_refresh_tasks:
                # refresh tasks already running
                # call ping and warm on all existing channels
                for channel in self.transport.channels:
                    await self._ping_and_warm_instances(channel, instance_key)
            else:
                # refresh tasks aren't active. start them as background tasks
                self._start_background_channel_refresh()

    async def _remove_instance_registration(
        self, instance_id: str, owner: TableAsync
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
        instance_key = _WarmedInstanceKey(
            instance_name, owner.table_name, owner.app_profile_id
        )
        owner_list = self._instance_owners.get(instance_key, set())
        try:
            owner_list.remove(id(owner))
            if len(owner_list) == 0:
                self._active_instances.remove(instance_key)
            return True
        except KeyError:
            return False

    def get_table(self, instance_id: str, table_id: str, *args, **kwargs) -> TableAsync:
        """
        Returns a table instance for making data API requests. All arguments are passed
        directly to the TableAsync constructor.

        Args:
            instance_id: The Bigtable instance ID to associate with this client.
                instance_id is combined with the client's project to fully
                specify the instance
            table_id: The ID of the table. table_id is combined with the
                instance_id and the client's project to fully specify the table
            app_profile_id: The app profile to associate with requests.
                https://cloud.google.com/bigtable/docs/app-profiles
            default_read_rows_operation_timeout: The default timeout for read rows
                operations, in seconds. If not set, defaults to 600 seconds (10 minutes)
            default_read_rows_attempt_timeout: The default timeout for individual
                read rows rpc requests, in seconds. If not set, defaults to 20 seconds
            default_mutate_rows_operation_timeout: The default timeout for mutate rows
                operations, in seconds. If not set, defaults to 600 seconds (10 minutes)
            default_mutate_rows_attempt_timeout: The default timeout for individual
                mutate rows rpc requests, in seconds. If not set, defaults to 60 seconds
            default_operation_timeout: The default timeout for all other operations, in
                seconds. If not set, defaults to 60 seconds
            default_attempt_timeout: The default timeout for all other individual rpc
                requests, in seconds. If not set, defaults to 20 seconds
            default_read_rows_retryable_errors: a list of errors that will be retried
                if encountered during read_rows and related operations.
                Defaults to 4 (DeadlineExceeded), 14 (ServiceUnavailable), and 10 (Aborted)
            default_mutate_rows_retryable_errors: a list of errors that will be retried
                if encountered during mutate_rows and related operations.
                Defaults to 4 (DeadlineExceeded) and 14 (ServiceUnavailable)
            default_retryable_errors: a list of errors that will be retried if
                encountered during all other operations.
                Defaults to 4 (DeadlineExceeded) and 14 (ServiceUnavailable)
        """
        return TableAsync(self, instance_id, table_id, *args, **kwargs)

    async def __aenter__(self):
        self._start_background_channel_refresh()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        await self._gapic_client.__aexit__(exc_type, exc_val, exc_tb)


class TableAsync:
    """
    Main Data API surface

    Table object maintains table_id, and app_profile_id context, and passes them with
    each call
    """

    def __init__(
        self,
        client: BigtableDataClientAsync,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None = None,
        *,
        default_read_rows_operation_timeout: float = 600,
        default_read_rows_attempt_timeout: float | None = 20,
        default_mutate_rows_operation_timeout: float = 600,
        default_mutate_rows_attempt_timeout: float | None = 60,
        default_operation_timeout: float = 60,
        default_attempt_timeout: float | None = 20,
        default_read_rows_retryable_errors: Sequence[type[Exception]] = (
            DeadlineExceeded,
            ServiceUnavailable,
            Aborted,
        ),
        default_mutate_rows_retryable_errors: Sequence[type[Exception]] = (
            DeadlineExceeded,
            ServiceUnavailable,
        ),
        default_retryable_errors: Sequence[type[Exception]] = (
            DeadlineExceeded,
            ServiceUnavailable,
        ),
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
            app_profile_id: The app profile to associate with requests.
                https://cloud.google.com/bigtable/docs/app-profiles
            default_read_rows_operation_timeout: The default timeout for read rows
                operations, in seconds. If not set, defaults to 600 seconds (10 minutes)
            default_read_rows_attempt_timeout: The default timeout for individual
                read rows rpc requests, in seconds. If not set, defaults to 20 seconds
            default_mutate_rows_operation_timeout: The default timeout for mutate rows
                operations, in seconds. If not set, defaults to 600 seconds (10 minutes)
            default_mutate_rows_attempt_timeout: The default timeout for individual
                mutate rows rpc requests, in seconds. If not set, defaults to 60 seconds
            default_operation_timeout: The default timeout for all other operations, in
                seconds. If not set, defaults to 60 seconds
            default_attempt_timeout: The default timeout for all other individual rpc
                requests, in seconds. If not set, defaults to 20 seconds
            default_read_rows_retryable_errors: a list of errors that will be retried
                if encountered during read_rows and related operations.
                Defaults to 4 (DeadlineExceeded), 14 (ServiceUnavailable), and 10 (Aborted)
            default_mutate_rows_retryable_errors: a list of errors that will be retried
                if encountered during mutate_rows and related operations.
                Defaults to 4 (DeadlineExceeded) and 14 (ServiceUnavailable)
            default_retryable_errors: a list of errors that will be retried if
                encountered during all other operations.
                Defaults to 4 (DeadlineExceeded) and 14 (ServiceUnavailable)
        Raises:
          - RuntimeError if called outside of an async context (no running event loop)
        """
        # NOTE: any changes to the signature of this method should also be reflected
        # in client.get_table()
        # validate timeouts
        _validate_timeouts(
            default_operation_timeout, default_attempt_timeout, allow_none=True
        )
        _validate_timeouts(
            default_read_rows_operation_timeout,
            default_read_rows_attempt_timeout,
            allow_none=True,
        )
        _validate_timeouts(
            default_mutate_rows_operation_timeout,
            default_mutate_rows_attempt_timeout,
            allow_none=True,
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
        self.default_attempt_timeout = default_attempt_timeout
        self.default_read_rows_operation_timeout = default_read_rows_operation_timeout
        self.default_read_rows_attempt_timeout = default_read_rows_attempt_timeout
        self.default_mutate_rows_operation_timeout = (
            default_mutate_rows_operation_timeout
        )
        self.default_mutate_rows_attempt_timeout = default_mutate_rows_attempt_timeout

        self.default_read_rows_retryable_errors = (
            default_read_rows_retryable_errors or ()
        )
        self.default_mutate_rows_retryable_errors = (
            default_mutate_rows_retryable_errors or ()
        )
        self.default_retryable_errors = default_retryable_errors or ()

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
        query: ReadRowsQuery,
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
    ) -> AsyncIterable[Row]:
        """
        Read a set of rows from the table, based on the specified query.
        Returns an iterator to asynchronously stream back row data.

        Failed requests within operation_timeout will be retried based on the
        retryable_errors list until operation_timeout is reached.

        Args:
            - query: contains details about which rows to return
            - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 Defaults to the Table's default_read_rows_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_read_rows_attempt_timeout.
                If None, defaults to operation_timeout.
            - retryable_errors: a list of errors that will be retried if encountered.
                Defaults to the Table's default_read_rows_retryable_errors
        Returns:
            - an asynchronous iterator that yields rows returned by the query
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions
                from any retries that failed
            - GoogleAPIError: raised if the request encounters an unrecoverable error
        """
        operation_timeout, attempt_timeout = _get_timeouts(
            operation_timeout, attempt_timeout, self
        )
        retryable_excs = _get_retryable_errors(retryable_errors, self)

        row_merger = _ReadRowsOperationAsync(
            query,
            self,
            operation_timeout=operation_timeout,
            attempt_timeout=attempt_timeout,
            retryable_exceptions=retryable_excs,
        )
        return row_merger.start_operation()

    async def read_rows(
        self,
        query: ReadRowsQuery,
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
    ) -> list[Row]:
        """
        Read a set of rows from the table, based on the specified query.
        Retruns results as a list of Row objects when the request is complete.
        For streamed results, use read_rows_stream.

        Failed requests within operation_timeout will be retried based on the
        retryable_errors list until operation_timeout is reached.

        Args:
            - query: contains details about which rows to return
            - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 Defaults to the Table's default_read_rows_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_read_rows_attempt_timeout.
                If None, defaults to operation_timeout.
                If None, defaults to the Table's default_read_rows_attempt_timeout,
                or the operation_timeout if that is also None.
            - retryable_errors: a list of errors that will be retried if encountered.
                Defaults to the Table's default_read_rows_retryable_errors.
        Returns:
            - a list of Rows returned by the query
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions
                from any retries that failed
            - GoogleAPIError: raised if the request encounters an unrecoverable error
        """
        row_generator = await self.read_rows_stream(
            query,
            operation_timeout=operation_timeout,
            attempt_timeout=attempt_timeout,
            retryable_errors=retryable_errors,
        )
        return [row async for row in row_generator]

    async def read_row(
        self,
        row_key: str | bytes,
        *,
        row_filter: RowFilter | None = None,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
    ) -> Row | None:
        """
        Read a single row from the table, based on the specified key.

        Failed requests within operation_timeout will be retried based on the
        retryable_errors list until operation_timeout is reached.

        Args:
            - query: contains details about which rows to return
            - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 Defaults to the Table's default_read_rows_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_read_rows_attempt_timeout.
                If None, defaults to operation_timeout.
            - retryable_errors: a list of errors that will be retried if encountered.
                Defaults to the Table's default_read_rows_retryable_errors.
        Returns:
            - a Row object if the row exists, otherwise None
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions
                from any retries that failed
            - GoogleAPIError: raised if the request encounters an unrecoverable error
        """
        if row_key is None:
            raise ValueError("row_key must be string or bytes")
        query = ReadRowsQuery(row_keys=row_key, row_filter=row_filter, limit=1)
        results = await self.read_rows(
            query,
            operation_timeout=operation_timeout,
            attempt_timeout=attempt_timeout,
            retryable_errors=retryable_errors,
        )
        if len(results) == 0:
            return None
        return results[0]

    async def read_rows_sharded(
        self,
        sharded_query: ShardedQuery,
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
    ) -> list[Row]:
        """
        Runs a sharded query in parallel, then return the results in a single list.
        Results will be returned in the order of the input queries.

        This function is intended to be run on the results on a query.shard() call:

        ```
        table_shard_keys = await table.sample_row_keys()
        query = ReadRowsQuery(...)
        shard_queries = query.shard(table_shard_keys)
        results = await table.read_rows_sharded(shard_queries)
        ```

        Args:
            - sharded_query: a sharded query to execute
            - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 Defaults to the Table's default_read_rows_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_read_rows_attempt_timeout.
                If None, defaults to operation_timeout.
            - retryable_errors: a list of errors that will be retried if encountered.
                Defaults to the Table's default_read_rows_retryable_errors.
        Raises:
            - ShardedReadRowsExceptionGroup: if any of the queries failed
            - ValueError: if the query_list is empty
        """
        if not sharded_query:
            raise ValueError("empty sharded_query")
        # reduce operation_timeout between batches
        operation_timeout, attempt_timeout = _get_timeouts(
            operation_timeout, attempt_timeout, self
        )
        timeout_generator = _attempt_timeout_generator(
            operation_timeout, operation_timeout
        )
        # submit shards in batches if the number of shards goes over _CONCURRENCY_LIMIT
        batched_queries = [
            sharded_query[i : i + _CONCURRENCY_LIMIT]
            for i in range(0, len(sharded_query), _CONCURRENCY_LIMIT)
        ]
        # run batches and collect results
        results_list = []
        error_dict = {}
        shard_idx = 0
        for batch in batched_queries:
            batch_operation_timeout = next(timeout_generator)
            routine_list = [
                self.read_rows(
                    query,
                    operation_timeout=batch_operation_timeout,
                    attempt_timeout=min(attempt_timeout, batch_operation_timeout),
                    retryable_errors=retryable_errors,
                )
                for query in batch
            ]
            batch_result = await asyncio.gather(*routine_list, return_exceptions=True)
            for result in batch_result:
                if isinstance(result, Exception):
                    error_dict[shard_idx] = result
                elif isinstance(result, BaseException):
                    # BaseException not expected; raise immediately
                    raise result
                else:
                    results_list.extend(result)
                shard_idx += 1
        if error_dict:
            # if any sub-request failed, raise an exception instead of returning results
            raise ShardedReadRowsExceptionGroup(
                [
                    FailedQueryShardError(idx, sharded_query[idx], e)
                    for idx, e in error_dict.items()
                ],
                results_list,
                len(sharded_query),
            )
        return results_list

    async def row_exists(
        self,
        row_key: str | bytes,
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.READ_ROWS,
    ) -> bool:
        """
        Return a boolean indicating whether the specified row exists in the table.
        uses the filters: chain(limit cells per row = 1, strip value)

        Args:
            - row_key: the key of the row to check
            - operation_timeout: the time budget for the entire operation, in seconds.
                 Failed requests will be retried within the budget.
                 Defaults to the Table's default_read_rows_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_read_rows_attempt_timeout.
                If None, defaults to operation_timeout.
            - retryable_errors: a list of errors that will be retried if encountered.
                Defaults to the Table's default_read_rows_retryable_errors.
        Returns:
            - a bool indicating whether the row exists
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions
                from any retries that failed
            - GoogleAPIError: raised if the request encounters an unrecoverable error
        """
        if row_key is None:
            raise ValueError("row_key must be string or bytes")

        strip_filter = StripValueTransformerFilter(flag=True)
        limit_filter = CellsRowLimitFilter(1)
        chain_filter = RowFilterChain(filters=[limit_filter, strip_filter])
        query = ReadRowsQuery(row_keys=row_key, limit=1, row_filter=chain_filter)
        results = await self.read_rows(
            query,
            operation_timeout=operation_timeout,
            attempt_timeout=attempt_timeout,
            retryable_errors=retryable_errors,
        )
        return len(results) > 0

    async def sample_row_keys(
        self,
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
    ) -> RowKeySamples:
        """
        Return a set of RowKeySamples that delimit contiguous sections of the table of
        approximately equal size

        RowKeySamples output can be used with ReadRowsQuery.shard() to create a sharded query that
        can be parallelized across multiple backend nodes read_rows and read_rows_stream
        requests will call sample_row_keys internally for this purpose when sharding is enabled

        RowKeySamples is simply a type alias for list[tuple[bytes, int]]; a list of
            row_keys, along with offset positions in the table

        Args:
            - operation_timeout: the time budget for the entire operation, in seconds.
                Failed requests will be retried within the budget.i
                Defaults to the Table's default_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_attempt_timeout.
                If None, defaults to operation_timeout.
            - retryable_errors: a list of errors that will be retried if encountered.
                Defaults to the Table's default_retryable_errors.
        Returns:
            - a set of RowKeySamples the delimit contiguous sections of the table
        Raises:
            - DeadlineExceeded: raised after operation timeout
                will be chained with a RetryExceptionGroup containing GoogleAPIError exceptions
                from any retries that failed
            - GoogleAPIError: raised if the request encounters an unrecoverable error
        """
        # prepare timeouts
        operation_timeout, attempt_timeout = _get_timeouts(
            operation_timeout, attempt_timeout, self
        )
        attempt_timeout_gen = _attempt_timeout_generator(
            attempt_timeout, operation_timeout
        )
        # prepare retryable
        retryable_excs = _get_retryable_errors(retryable_errors, self)
        predicate = retries.if_exception_type(*retryable_excs)

        sleep_generator = retries.exponential_sleep_generator(0.01, 2, 60)

        # prepare request
        metadata = _make_metadata(self.table_name, self.app_profile_id)

        async def execute_rpc():
            results = await self.client._gapic_client.sample_row_keys(
                table_name=self.table_name,
                app_profile_id=self.app_profile_id,
                timeout=next(attempt_timeout_gen),
                metadata=metadata,
                retry=None,
            )
            return [(s.row_key, s.offset_bytes) async for s in results]

        return await retries.retry_target_async(
            execute_rpc,
            predicate,
            sleep_generator,
            operation_timeout,
            exception_factory=_retry_exception_factory,
        )

    def mutations_batcher(
        self,
        *,
        flush_interval: float | None = 5,
        flush_limit_mutation_count: int | None = 1000,
        flush_limit_bytes: int = 20 * _MB_SIZE,
        flow_control_max_mutation_count: int = 100_000,
        flow_control_max_bytes: int = 100 * _MB_SIZE,
        batch_operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
        batch_attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
        batch_retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
    ) -> MutationsBatcherAsync:
        """
        Returns a new mutations batcher instance.

        Can be used to iteratively add mutations that are flushed as a group,
        to avoid excess network calls

        Args:
          - flush_interval: Automatically flush every flush_interval seconds. If None,
              a table default will be used
          - flush_limit_mutation_count: Flush immediately after flush_limit_mutation_count
              mutations are added across all entries. If None, this limit is ignored.
          - flush_limit_bytes: Flush immediately after flush_limit_bytes bytes are added.
          - flow_control_max_mutation_count: Maximum number of inflight mutations.
          - flow_control_max_bytes: Maximum number of inflight bytes.
          - batch_operation_timeout: timeout for each mutate_rows operation, in seconds.
              Defaults to the Table's default_mutate_rows_operation_timeout
          - batch_attempt_timeout: timeout for each individual request, in seconds.
              Defaults to the Table's default_mutate_rows_attempt_timeout.
              If None, defaults to batch_operation_timeout.
          - batch_retryable_errors: a list of errors that will be retried if encountered.
              Defaults to the Table's default_mutate_rows_retryable_errors.
        Returns:
            - a MutationsBatcherAsync context manager that can batch requests
        """
        return MutationsBatcherAsync(
            self,
            flush_interval=flush_interval,
            flush_limit_mutation_count=flush_limit_mutation_count,
            flush_limit_bytes=flush_limit_bytes,
            flow_control_max_mutation_count=flow_control_max_mutation_count,
            flow_control_max_bytes=flow_control_max_bytes,
            batch_operation_timeout=batch_operation_timeout,
            batch_attempt_timeout=batch_attempt_timeout,
            batch_retryable_errors=batch_retryable_errors,
        )

    async def mutate_row(
        self,
        row_key: str | bytes,
        mutations: list[Mutation] | Mutation,
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
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
                Defaults to the Table's default_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_attempt_timeout.
                If None, defaults to operation_timeout.
            - retryable_errors: a list of errors that will be retried if encountered.
                Only idempotent mutations will be retried. Defaults to the Table's
                default_retryable_errors.
        Raises:
             - DeadlineExceeded: raised after operation timeout
                 will be chained with a RetryExceptionGroup containing all
                 GoogleAPIError exceptions from any retries that failed
             - GoogleAPIError: raised on non-idempotent operations that cannot be
                 safely retried.
            - ValueError if invalid arguments are provided
        """
        operation_timeout, attempt_timeout = _get_timeouts(
            operation_timeout, attempt_timeout, self
        )

        if not mutations:
            raise ValueError("No mutations provided")
        mutations_list = mutations if isinstance(mutations, list) else [mutations]

        if all(mutation.is_idempotent() for mutation in mutations_list):
            # mutations are all idempotent and safe to retry
            predicate = retries.if_exception_type(
                *_get_retryable_errors(retryable_errors, self)
            )
        else:
            # mutations should not be retried
            predicate = retries.if_exception_type()

        sleep_generator = retries.exponential_sleep_generator(0.01, 2, 60)

        target = partial(
            self.client._gapic_client.mutate_row,
            row_key=row_key.encode("utf-8") if isinstance(row_key, str) else row_key,
            mutations=[mutation._to_pb() for mutation in mutations_list],
            table_name=self.table_name,
            app_profile_id=self.app_profile_id,
            timeout=attempt_timeout,
            metadata=_make_metadata(self.table_name, self.app_profile_id),
            retry=None,
        )
        return await retries.retry_target_async(
            target,
            predicate,
            sleep_generator,
            operation_timeout,
            exception_factory=_retry_exception_factory,
        )

    async def bulk_mutate_rows(
        self,
        mutation_entries: list[RowMutationEntry],
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
        attempt_timeout: float | None | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
        retryable_errors: Sequence[type[Exception]]
        | TABLE_DEFAULT = TABLE_DEFAULT.MUTATE_ROWS,
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
                Defaults to the Table's default_mutate_rows_operation_timeout
            - attempt_timeout: the time budget for an individual network request, in seconds.
                If it takes longer than this time to complete, the request will be cancelled with
                a DeadlineExceeded exception, and a retry will be attempted.
                Defaults to the Table's default_mutate_rows_attempt_timeout.
                If None, defaults to operation_timeout.
            - retryable_errors: a list of errors that will be retried if encountered.
                Defaults to the Table's default_mutate_rows_retryable_errors
        Raises:
            - MutationsExceptionGroup if one or more mutations fails
                Contains details about any failed entries in .exceptions
            - ValueError if invalid arguments are provided
        """
        operation_timeout, attempt_timeout = _get_timeouts(
            operation_timeout, attempt_timeout, self
        )
        retryable_excs = _get_retryable_errors(retryable_errors, self)

        operation = _MutateRowsOperationAsync(
            self.client._gapic_client,
            self,
            mutation_entries,
            operation_timeout,
            attempt_timeout,
            retryable_exceptions=retryable_excs,
        )
        await operation.start()

    async def check_and_mutate_row(
        self,
        row_key: str | bytes,
        predicate: RowFilter | None,
        *,
        true_case_mutations: Mutation | list[Mutation] | None = None,
        false_case_mutations: Mutation | list[Mutation] | None = None,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
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
                Failed requests will not be retried. Defaults to the Table's default_operation_timeout
        Returns:
            - bool indicating whether the predicate was true or false
        Raises:
            - GoogleAPIError exceptions from grpc call
        """
        operation_timeout, _ = _get_timeouts(operation_timeout, None, self)
        if true_case_mutations is not None and not isinstance(
            true_case_mutations, list
        ):
            true_case_mutations = [true_case_mutations]
        true_case_list = [m._to_pb() for m in true_case_mutations or []]
        if false_case_mutations is not None and not isinstance(
            false_case_mutations, list
        ):
            false_case_mutations = [false_case_mutations]
        false_case_list = [m._to_pb() for m in false_case_mutations or []]
        metadata = _make_metadata(self.table_name, self.app_profile_id)
        result = await self.client._gapic_client.check_and_mutate_row(
            true_mutations=true_case_list,
            false_mutations=false_case_list,
            predicate_filter=predicate._to_pb() if predicate is not None else None,
            row_key=row_key.encode("utf-8") if isinstance(row_key, str) else row_key,
            table_name=self.table_name,
            app_profile_id=self.app_profile_id,
            metadata=metadata,
            timeout=operation_timeout,
            retry=None,
        )
        return result.predicate_matched

    async def read_modify_write_row(
        self,
        row_key: str | bytes,
        rules: ReadModifyWriteRule | list[ReadModifyWriteRule],
        *,
        operation_timeout: float | TABLE_DEFAULT = TABLE_DEFAULT.DEFAULT,
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
                Defaults to the Table's default_operation_timeout.
        Returns:
            - Row: containing cell data that was modified as part of the
                operation
        Raises:
            - GoogleAPIError exceptions from grpc call
            - ValueError if invalid arguments are provided
        """
        operation_timeout, _ = _get_timeouts(operation_timeout, None, self)
        if operation_timeout <= 0:
            raise ValueError("operation_timeout must be greater than 0")
        if rules is not None and not isinstance(rules, list):
            rules = [rules]
        if not rules:
            raise ValueError("rules must contain at least one item")
        metadata = _make_metadata(self.table_name, self.app_profile_id)
        result = await self.client._gapic_client.read_modify_write_row(
            rules=[rule._to_pb() for rule in rules],
            row_key=row_key.encode("utf-8") if isinstance(row_key, str) else row_key,
            table_name=self.table_name,
            app_profile_id=self.app_profile_id,
            metadata=metadata,
            timeout=operation_timeout,
            retry=None,
        )
        # construct Row from result
        return Row._from_pb(result.row)

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
