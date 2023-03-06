# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
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
import warnings
from typing import Awaitable, Callable, Dict, Optional, Sequence, Tuple, Union, List

from google.api_core import gapic_v1
from google.api_core import grpc_helpers_async
from google.auth import credentials as ga_credentials  # type: ignore
from google.auth.transport.grpc import SslCredentials  # type: ignore

import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore

from google.cloud.bigtable_v2.types import bigtable
from .base import BigtableTransport, DEFAULT_CLIENT_INFO
from .grpc import BigtableGrpcTransport


class PooledBigtableGrpcAsyncIOTransport(BigtableTransport):
    """Pooled gRPC AsyncIO backend transport for Bigtable.

    Service for reading from and writing to existing Bigtable
    tables.

    This class defines the same methods as the primary client, so the
    primary client can load the underlying transport implementation
    and call it.

    It sends protocol buffers over the wire using gRPC (which is built on
    top of HTTP/2); the ``grpcio`` package must be installed.

    This class allows channel pooling, so multiple channels can be used concurrently
    when making requests. Channels are rotated in a round-robin fashion.
    """

    _channel_pool: List[aio.Channel] = []
    _stubs: Dict[Tuple[aio.Channel, str], Callable] = {}
    _next_idx: int = 0

    @classmethod
    def create_channel(
        cls,
        host: str = "bigtable.googleapis.com",
        credentials: Optional[ga_credentials.Credentials] = None,
        credentials_file: Optional[str] = None,
        scopes: Optional[Sequence[str]] = None,
        quota_project_id: Optional[str] = None,
        **kwargs,
    ) -> aio.Channel:
        """Create and return a gRPC AsyncIO channel object.
        Args:
            host (Optional[str]): The host for the channel to use.
            credentials (Optional[~.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify this application to the service. If
                none are specified, the client will attempt to ascertain
                the credentials from the environment.
            credentials_file (Optional[str]): A file with credentials that can
                be loaded with :func:`google.auth.load_credentials_from_file`.
                This argument is ignored if ``channel`` is provided.
            scopes (Optional[Sequence[str]]): A optional list of scopes needed for this
                service. These are only used when credentials are not specified and
                are passed to :func:`google.auth.default`.
            quota_project_id (Optional[str]): An optional project to use for billing
                and quota.
            kwargs (Optional[dict]): Keyword arguments, which are passed to the
                channel creation.
        Returns:
            aio.Channel: A gRPC AsyncIO channel object.
        """

        return grpc_helpers_async.create_channel(
            host,
            credentials=credentials,
            credentials_file=credentials_file,
            quota_project_id=quota_project_id,
            default_scopes=cls.AUTH_SCOPES,
            scopes=scopes,
            default_host=cls.DEFAULT_HOST,
            **kwargs,
        )

    def __init__(
        self,
        *,
        pool_size: int = 3,
        host: str = "bigtable.googleapis.com",
        credentials: Optional[ga_credentials.Credentials] = None,
        credentials_file: Optional[str] = None,
        scopes: Optional[Sequence[str]] = None,
        api_mtls_endpoint: Optional[str] = None,
        client_cert_source: Optional[Callable[[], Tuple[bytes, bytes]]] = None,
        ssl_channel_credentials: Optional[grpc.ChannelCredentials] = None,
        client_cert_source_for_mtls: Optional[Callable[[], Tuple[bytes, bytes]]] = None,
        quota_project_id: Optional[str] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
        always_use_jwt_access: Optional[bool] = False,
        api_audience: Optional[str] = None,
    ) -> None:
        """Instantiate the transport.

        Args:
            pool_size (int): the number of grpc channels to maintain in a pool
            host (Optional[str]):
                 The hostname to connect to.
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
                This argument is ignored if ``channel`` is provided.
            credentials_file (Optional[str]): A file with credentials that can
                be loaded with :func:`google.auth.load_credentials_from_file`.
                This argument is ignored if ``channel`` is provided.
            scopes (Optional[Sequence[str]]): A optional list of scopes needed for this
                service. These are only used when credentials are not specified and
                are passed to :func:`google.auth.default`.
            api_mtls_endpoint (Optional[str]): Deprecated. The mutual TLS endpoint.
                If provided, it overrides the ``host`` argument and tries to create
                a mutual TLS channel with client SSL credentials from
                ``client_cert_source`` or application default SSL credentials.
            client_cert_source (Optional[Callable[[], Tuple[bytes, bytes]]]):
                Deprecated. A callback to provide client SSL certificate bytes and
                private key bytes, both in PEM format. It is ignored if
                ``api_mtls_endpoint`` is None.
            ssl_channel_credentials (grpc.ChannelCredentials): SSL credentials
                for the grpc channel. It is ignored if ``channel`` is provided.
            client_cert_source_for_mtls (Optional[Callable[[], Tuple[bytes, bytes]]]):
                A callback to provide client certificate bytes and private key bytes,
                both in PEM format. It is used to configure a mutual TLS channel. It is
                ignored if ``channel`` or ``ssl_channel_credentials`` is provided.
            quota_project_id (Optional[str]): An optional project to use for billing
                and quota.
            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.
            always_use_jwt_access (Optional[bool]): Whether self signed JWT should
                be used for service account credentials.

        Raises:
            google.auth.exceptions.MutualTlsChannelError: If mutual TLS transport
              creation failed for any reason.
          google.api_core.exceptions.DuplicateCredentialArgs: If both ``credentials``
              and ``credentials_file`` are passed.
        """
        self._ssl_channel_credentials = ssl_channel_credentials
        self._stubs: Dict[str, Callable] = {}

        if api_mtls_endpoint:
            warnings.warn("api_mtls_endpoint is deprecated", DeprecationWarning)
        if client_cert_source:
            warnings.warn("client_cert_source is deprecated", DeprecationWarning)

        if api_mtls_endpoint:
            host = api_mtls_endpoint

            # Create SSL credentials with client_cert_source or application
            # default SSL credentials.
            if client_cert_source:
                cert, key = client_cert_source()
                self._ssl_channel_credentials = grpc.ssl_channel_credentials(
                    certificate_chain=cert, private_key=key
                )
            else:
                self._ssl_channel_credentials = SslCredentials().ssl_credentials

        else:
            if client_cert_source_for_mtls and not ssl_channel_credentials:
                cert, key = client_cert_source_for_mtls()
                self._ssl_channel_credentials = grpc.ssl_channel_credentials(
                    certificate_chain=cert, private_key=key
                )

        # The base transport sets the host, credentials and scopes
        super().__init__(
            host=host,
            credentials=credentials,
            credentials_file=credentials_file,
            scopes=scopes,
            quota_project_id=quota_project_id,
            client_info=client_info,
            always_use_jwt_access=always_use_jwt_access,
            api_audience=api_audience,
        )
        self._quota_project_id = quota_project_id
        self._channel_pool = []
        for i in range(pool_size):
            new_channel = type(self).create_channel(
                self._host,
                # use the credentials which are saved
                credentials=self._credentials,
                # Set ``credentials_file`` to ``None`` here as
                # the credentials that we saved earlier should be used.
                credentials_file=None,
                scopes=self._scopes,
                ssl_credentials=self._ssl_channel_credentials,
                quota_project_id=self._quota_project_id,
                options=[
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                ],
            )
            self._channel_pool.append(new_channel)

        # Wrap messages. This must be done after self._channel_pool is populated
        self._prep_wrapped_messages(client_info)

    def next_channel(self) -> aio.Channel:
        """Returns the next channel in the round robin pool."""
        # Return the channel from cache.
        channel = self._channel_pool[self._next_idx]
        self._next_idx = (self._next_idx + 1) % len(self._channel_pool)
        return channel

    def get_channel(self, channel_idx) -> aio.Channel:
        """Returns the a specified channel from the pool.

        Args:
          channel_idx(int): the requested channel index
        """
        # Return the channel from cache.
        return self._channel_pool[self._next_idx]

    async def replace_channel(
        self, channel_idx, grace=None, new_channel=None
    ) -> aio.Channel:
        """
        Immediately closes a channel in the pool, and replaces it with a new one.
        Returns the newly created channel

        Args:
          channel_idx(int): the channel index in the pool to replace
          grace(Optional[float]): The time to wait until all active RPCs are
            finished. If a grace period is not specified (by passing None for
            grace), all existing RPCs are cancelled immediately.
          new_channel(asyncio.Channel): a new channel to insert into the pool
            at `channel_idx`. If `None`, a new channel will be created.
        """
        if new_channel is None:
            new_channel = self.create_channel(
                self._host,
                credentials=self._credentials,
                credentials_file=None,
                scopes=self._scopes,
                ssl_credentials=self._ssl_channel_credentials,
                quota_project_id=self._quota_project_id,
                options=[
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                ],
            )
        old_channel = self._channel_pool[channel_idx]
        self._channel_pool[channel_idx] = new_channel
        await old_channel.close(grace=grace)
        # invalidate stubs
        for stub_channel, stub_func in self._stubs.keys():
            if stub_channel == old_channel:
                del self._stubs[(stub_channel, stub_func)]
        return new_channel

    @property
    def read_rows(
        self,
    ) -> Callable[[bigtable.ReadRowsRequest], Awaitable[bigtable.ReadRowsResponse]]:
        r"""Return a callable for the read rows method over gRPC.

        Streams back the contents of all requested rows in
        key order, optionally applying the same Reader filter to
        each. Depending on their size, rows and cells may be
        broken up across multiple responses, but atomicity of
        each row will still be preserved. See the
        ReadRowsResponse documentation for details.

        Returns:
            Callable[[~.ReadRowsRequest],
                    Awaitable[~.ReadRowsResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "read_rows")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_stream(
                "/google.bigtable.v2.Bigtable/ReadRows",
                request_serializer=bigtable.ReadRowsRequest.serialize,
                response_deserializer=bigtable.ReadRowsResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def sample_row_keys(
        self,
    ) -> Callable[
        [bigtable.SampleRowKeysRequest], Awaitable[bigtable.SampleRowKeysResponse]
    ]:
        r"""Return a callable for the sample row keys method over gRPC.

        Returns a sample of row keys in the table. The
        returned row keys will delimit contiguous sections of
        the table of approximately equal size, which can be used
        to break up the data for distributed tasks like
        mapreduces.

        Returns:
            Callable[[~.SampleRowKeysRequest],
                    Awaitable[~.SampleRowKeysResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "sample_row_keys")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_stream(
                "/google.bigtable.v2.Bigtable/SampleRowKeys",
                request_serializer=bigtable.SampleRowKeysRequest.serialize,
                response_deserializer=bigtable.SampleRowKeysResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def mutate_row(
        self,
    ) -> Callable[[bigtable.MutateRowRequest], Awaitable[bigtable.MutateRowResponse]]:
        r"""Return a callable for the mutate row method over gRPC.

        Mutates a row atomically. Cells already present in the row are
        left unchanged unless explicitly changed by ``mutation``.

        Returns:
            Callable[[~.MutateRowRequest],
                    Awaitable[~.MutateRowResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "mutate_row")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_unary(
                "/google.bigtable.v2.Bigtable/MutateRow",
                request_serializer=bigtable.MutateRowRequest.serialize,
                response_deserializer=bigtable.MutateRowResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def mutate_rows(
        self,
    ) -> Callable[[bigtable.MutateRowsRequest], Awaitable[bigtable.MutateRowsResponse]]:
        r"""Return a callable for the mutate rows method over gRPC.

        Mutates multiple rows in a batch. Each individual row
        is mutated atomically as in MutateRow, but the entire
        batch is not executed atomically.

        Returns:
            Callable[[~.MutateRowsRequest],
                    Awaitable[~.MutateRowsResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "mutate_rows")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_stream(
                "/google.bigtable.v2.Bigtable/MutateRows",
                request_serializer=bigtable.MutateRowsRequest.serialize,
                response_deserializer=bigtable.MutateRowsResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def check_and_mutate_row(
        self,
    ) -> Callable[
        [bigtable.CheckAndMutateRowRequest],
        Awaitable[bigtable.CheckAndMutateRowResponse],
    ]:
        r"""Return a callable for the check and mutate row method over gRPC.

        Mutates a row atomically based on the output of a
        predicate Reader filter.

        Returns:
            Callable[[~.CheckAndMutateRowRequest],
                    Awaitable[~.CheckAndMutateRowResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "check_and_mutate_row")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_unary(
                "/google.bigtable.v2.Bigtable/CheckAndMutateRow",
                request_serializer=bigtable.CheckAndMutateRowRequest.serialize,
                response_deserializer=bigtable.CheckAndMutateRowResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def ping_and_warm(
        self,
    ) -> Callable[
        [bigtable.PingAndWarmRequest], Awaitable[bigtable.PingAndWarmResponse]
    ]:
        r"""Return a callable for the ping and warm method over gRPC.

        Warm up associated instance metadata for this
        connection. This call is not required but may be useful
        for connection keep-alive.

        Returns:
            Callable[[~.PingAndWarmRequest],
                    Awaitable[~.PingAndWarmResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "ping_and_warm")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_unary(
                "/google.bigtable.v2.Bigtable/PingAndWarm",
                request_serializer=bigtable.PingAndWarmRequest.serialize,
                response_deserializer=bigtable.PingAndWarmResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def read_modify_write_row(
        self,
    ) -> Callable[
        [bigtable.ReadModifyWriteRowRequest],
        Awaitable[bigtable.ReadModifyWriteRowResponse],
    ]:
        r"""Return a callable for the read modify write row method over gRPC.

        Modifies a row atomically on the server. The method
        reads the latest existing timestamp and value from the
        specified columns and writes a new entry based on
        pre-defined read/modify/write rules. The new value for
        the timestamp is the greater of the existing timestamp
        or the current server time. The method returns the new
        contents of all modified cells.

        Returns:
            Callable[[~.ReadModifyWriteRowRequest],
                    Awaitable[~.ReadModifyWriteRowResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "read_modify_write_row")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_unary(
                "/google.bigtable.v2.Bigtable/ReadModifyWriteRow",
                request_serializer=bigtable.ReadModifyWriteRowRequest.serialize,
                response_deserializer=bigtable.ReadModifyWriteRowResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def generate_initial_change_stream_partitions(
        self,
    ) -> Callable[
        [bigtable.GenerateInitialChangeStreamPartitionsRequest],
        Awaitable[bigtable.GenerateInitialChangeStreamPartitionsResponse],
    ]:
        r"""Return a callable for the generate initial change stream
        partitions method over gRPC.

        NOTE: This API is intended to be used by Apache Beam BigtableIO.
        Returns the current list of partitions that make up the table's
        change stream. The union of partitions will cover the entire
        keyspace. Partitions can be read with ``ReadChangeStream``.

        Returns:
            Callable[[~.GenerateInitialChangeStreamPartitionsRequest],
                    Awaitable[~.GenerateInitialChangeStreamPartitionsResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "generate_initial_change_stream_partitions")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_stream(
                "/google.bigtable.v2.Bigtable/GenerateInitialChangeStreamPartitions",
                request_serializer=bigtable.GenerateInitialChangeStreamPartitionsRequest.serialize,
                response_deserializer=bigtable.GenerateInitialChangeStreamPartitionsResponse.deserialize,
            )
        return self._stubs[stub_key]

    @property
    def read_change_stream(
        self,
    ) -> Callable[
        [bigtable.ReadChangeStreamRequest], Awaitable[bigtable.ReadChangeStreamResponse]
    ]:
        r"""Return a callable for the read change stream method over gRPC.

        NOTE: This API is intended to be used by Apache Beam
        BigtableIO. Reads changes from a table's change stream.
        Changes will reflect both user-initiated mutations and
        mutations that are caused by garbage collection.

        Returns:
            Callable[[~.ReadChangeStreamRequest],
                    Awaitable[~.ReadChangeStreamResponse]]:
                A function that, when called, will call the underlying RPC
                on the server.
        """
        # Generate a "stub function" on-the-fly which will actually make
        # the request.
        # gRPC handles serialization and deserialization, so we just need
        # to pass in the functions for each.
        next_channel = self.next_channel()
        print(f"USING CHANNEL: {self._next_idx}")
        stub_key = (next_channel, "read_change_stream")
        if stub_key not in self._stubs:
            self._stubs[stub_key] = next_channel.unary_stream(
                "/google.bigtable.v2.Bigtable/ReadChangeStream",
                request_serializer=bigtable.ReadChangeStreamRequest.serialize,
                response_deserializer=bigtable.ReadChangeStreamResponse.deserialize,
            )
        return self._stubs[stub_key]

    def close(self):
        for channel in self._channel_pool:
            channel.close()


__all__ = ("PooledBigtableGrpcAsyncIOTransport",)
