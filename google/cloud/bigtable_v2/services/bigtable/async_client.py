# -*- coding: utf-8 -*-
# Copyright 2024 Google LLC
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
from collections import OrderedDict
import re
from typing import (
    Dict,
    Callable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    AsyncIterable,
    Awaitable,
    Sequence,
    Tuple,
    Type,
    Union,
)

from google.cloud.bigtable_v2 import gapic_version as package_version

from google.api_core.client_options import ClientOptions
from google.api_core import exceptions as core_exceptions
from google.api_core import gapic_v1
from google.api_core import retry_async as retries
from google.auth import credentials as ga_credentials  # type: ignore
from google.oauth2 import service_account  # type: ignore


try:
    OptionalRetry = Union[retries.AsyncRetry, gapic_v1.method._MethodDefault, None]
except AttributeError:  # pragma: NO COVER
    OptionalRetry = Union[retries.AsyncRetry, object, None]  # type: ignore

from google.cloud.bigtable_v2.types import bigtable
from google.cloud.bigtable_v2.types import data
from google.cloud.bigtable_v2.types import request_stats
from .transports.base import BigtableTransport, DEFAULT_CLIENT_INFO
from .transports.grpc_asyncio import BigtableGrpcAsyncIOTransport
from .client import BigtableClient


class BigtableAsyncClient:
    """Service for reading from and writing to existing Bigtable
    tables.
    """

    _client: BigtableClient

    # Copy defaults from the synchronous client for use here.
    # Note: DEFAULT_ENDPOINT is deprecated. Use _DEFAULT_ENDPOINT_TEMPLATE instead.
    DEFAULT_ENDPOINT = BigtableClient.DEFAULT_ENDPOINT
    DEFAULT_MTLS_ENDPOINT = BigtableClient.DEFAULT_MTLS_ENDPOINT
    _DEFAULT_ENDPOINT_TEMPLATE = BigtableClient._DEFAULT_ENDPOINT_TEMPLATE
    _DEFAULT_UNIVERSE = BigtableClient._DEFAULT_UNIVERSE

    authorized_view_path = staticmethod(BigtableClient.authorized_view_path)
    parse_authorized_view_path = staticmethod(BigtableClient.parse_authorized_view_path)
    instance_path = staticmethod(BigtableClient.instance_path)
    parse_instance_path = staticmethod(BigtableClient.parse_instance_path)
    table_path = staticmethod(BigtableClient.table_path)
    parse_table_path = staticmethod(BigtableClient.parse_table_path)
    common_billing_account_path = staticmethod(
        BigtableClient.common_billing_account_path
    )
    parse_common_billing_account_path = staticmethod(
        BigtableClient.parse_common_billing_account_path
    )
    common_folder_path = staticmethod(BigtableClient.common_folder_path)
    parse_common_folder_path = staticmethod(BigtableClient.parse_common_folder_path)
    common_organization_path = staticmethod(BigtableClient.common_organization_path)
    parse_common_organization_path = staticmethod(
        BigtableClient.parse_common_organization_path
    )
    common_project_path = staticmethod(BigtableClient.common_project_path)
    parse_common_project_path = staticmethod(BigtableClient.parse_common_project_path)
    common_location_path = staticmethod(BigtableClient.common_location_path)
    parse_common_location_path = staticmethod(BigtableClient.parse_common_location_path)

    @classmethod
    def from_service_account_info(cls, info: dict, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
            info.

        Args:
            info (dict): The service account private key info.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            BigtableAsyncClient: The constructed client.
        """
        return BigtableClient.from_service_account_info.__func__(BigtableAsyncClient, info, *args, **kwargs)  # type: ignore

    @classmethod
    def from_service_account_file(cls, filename: str, *args, **kwargs):
        """Creates an instance of this client using the provided credentials
            file.

        Args:
            filename (str): The path to the service account private key json
                file.
            args: Additional arguments to pass to the constructor.
            kwargs: Additional arguments to pass to the constructor.

        Returns:
            BigtableAsyncClient: The constructed client.
        """
        return BigtableClient.from_service_account_file.__func__(BigtableAsyncClient, filename, *args, **kwargs)  # type: ignore

    from_service_account_json = from_service_account_file

    @classmethod
    def get_mtls_endpoint_and_cert_source(
        cls, client_options: Optional[ClientOptions] = None
    ):
        """Return the API endpoint and client cert source for mutual TLS.

        The client cert source is determined in the following order:
        (1) if `GOOGLE_API_USE_CLIENT_CERTIFICATE` environment variable is not "true", the
        client cert source is None.
        (2) if `client_options.client_cert_source` is provided, use the provided one; if the
        default client cert source exists, use the default one; otherwise the client cert
        source is None.

        The API endpoint is determined in the following order:
        (1) if `client_options.api_endpoint` if provided, use the provided one.
        (2) if `GOOGLE_API_USE_CLIENT_CERTIFICATE` environment variable is "always", use the
        default mTLS endpoint; if the environment variable is "never", use the default API
        endpoint; otherwise if client cert source exists, use the default mTLS endpoint, otherwise
        use the default API endpoint.

        More details can be found at https://google.aip.dev/auth/4114.

        Args:
            client_options (google.api_core.client_options.ClientOptions): Custom options for the
                client. Only the `api_endpoint` and `client_cert_source` properties may be used
                in this method.

        Returns:
            Tuple[str, Callable[[], Tuple[bytes, bytes]]]: returns the API endpoint and the
                client cert source to use.

        Raises:
            google.auth.exceptions.MutualTLSChannelError: If any errors happen.
        """
        return BigtableClient.get_mtls_endpoint_and_cert_source(client_options)  # type: ignore

    @property
    def transport(self) -> BigtableTransport:
        """Returns the transport used by the client instance.

        Returns:
            BigtableTransport: The transport used by the client instance.
        """
        return self._client.transport

    @property
    def api_endpoint(self):
        """Return the API endpoint used by the client instance.

        Returns:
            str: The API endpoint used by the client instance.
        """
        return self._client._api_endpoint

    @property
    def universe_domain(self) -> str:
        """Return the universe domain used by the client instance.

        Returns:
            str: The universe domain used
                by the client instance.
        """
        return self._client._universe_domain

    get_transport_class = BigtableClient.get_transport_class

    def __init__(
        self,
        *,
        credentials: Optional[ga_credentials.Credentials] = None,
        transport: Optional[
            Union[str, BigtableTransport, Callable[..., BigtableTransport]]
        ] = "grpc_asyncio",
        client_options: Optional[ClientOptions] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
    ) -> None:
        """Instantiates the bigtable async client.

        Args:
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            transport (Optional[Union[str,BigtableTransport,Callable[..., BigtableTransport]]]):
                The transport to use, or a Callable that constructs and returns a new transport to use.
                If a Callable is given, it will be called with the same set of initialization
                arguments as used in the BigtableTransport constructor.
                If set to None, a transport is chosen automatically.
            client_options (Optional[Union[google.api_core.client_options.ClientOptions, dict]]):
                Custom options for the client.

                1. The ``api_endpoint`` property can be used to override the
                default endpoint provided by the client when ``transport`` is
                not explicitly provided. Only if this property is not set and
                ``transport`` was not explicitly provided, the endpoint is
                determined by the GOOGLE_API_USE_MTLS_ENDPOINT environment
                variable, which have one of the following values:
                "always" (always use the default mTLS endpoint), "never" (always
                use the default regular endpoint) and "auto" (auto-switch to the
                default mTLS endpoint if client certificate is present; this is
                the default value).

                2. If the GOOGLE_API_USE_CLIENT_CERTIFICATE environment variable
                is "true", then the ``client_cert_source`` property can be used
                to provide a client certificate for mTLS transport. If
                not provided, the default SSL client certificate will be used if
                present. If GOOGLE_API_USE_CLIENT_CERTIFICATE is "false" or not
                set, no client certificate will be used.

                3. The ``universe_domain`` property can be used to override the
                default "googleapis.com" universe. Note that ``api_endpoint``
                property still takes precedence; and ``universe_domain`` is
                currently not supported for mTLS.

            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.

        Raises:
            google.auth.exceptions.MutualTlsChannelError: If mutual TLS transport
                creation failed for any reason.
        """
        self._client = BigtableClient(
            credentials=credentials,
            transport=transport,
            client_options=client_options,
            client_info=client_info,
        )



    async def mutate_row(
        self,
        request: Optional[Union[bigtable.MutateRowRequest, dict]] = None,
        *,
        table_name: Optional[str] = None,
        row_key: Optional[bytes] = None,
        mutations: Optional[MutableSequence[data.Mutation]] = None,
        app_profile_id: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> bigtable.MutateRowResponse:
        r"""Mutates a row atomically. Cells already present in the row are
        left unchanged unless explicitly changed by ``mutation``.

        Args:
            request (Optional[Union[google.cloud.bigtable_v2.types.MutateRowRequest, dict]]):
                The request object. Request message for
                Bigtable.MutateRow.
            table_name (:class:`str`):
                Optional. The unique name of the table to which the
                mutation should be applied.

                Values are of the form
                ``projects/<project>/instances/<instance>/tables/<table>``.

                This corresponds to the ``table_name`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            row_key (:class:`bytes`):
                Required. The key of the row to which
                the mutation should be applied.

                This corresponds to the ``row_key`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            mutations (:class:`MutableSequence[google.cloud.bigtable_v2.types.Mutation]`):
                Required. Changes to be atomically
                applied to the specified row. Entries
                are applied in order, meaning that
                earlier mutations can be masked by later
                ones. Must contain at least one entry
                and at most 100000.

                This corresponds to the ``mutations`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            app_profile_id (:class:`str`):
                This value specifies routing for
                replication. If not specified, the
                "default" application profile will be
                used.

                This corresponds to the ``app_profile_id`` field
                on the ``request`` instance; if ``request`` is provided, this
                should not be set.
            retry (google.api_core.retry_async.AsyncRetry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, str]]): Strings which should be
                sent along with the request as metadata.

        Returns:
            google.cloud.bigtable_v2.types.MutateRowResponse:
                Response message for
                Bigtable.MutateRow.

        """
        # Create or coerce a protobuf request object.
        # - Quick check: If we got a request object, we should *not* have
        #   gotten any keyword arguments that map to the request.
        has_flattened_params = any([table_name, row_key, mutations, app_profile_id])
        if request is not None and has_flattened_params:
            raise ValueError(
                "If the `request` argument is set, then none of "
                "the individual field arguments should be set."
            )

        # - Use the request object if provided (there's no risk of modifying the input as
        #   there are no flattened fields), or create one.
        if not isinstance(request, bigtable.MutateRowRequest):
            request = bigtable.MutateRowRequest(request)

        # If we have keyword arguments corresponding to fields on the
        # request, apply these.
        if table_name is not None:
            request.table_name = table_name
        if row_key is not None:
            request.row_key = row_key
        if app_profile_id is not None:
            request.app_profile_id = app_profile_id
        if mutations:
            request.mutations.extend(mutations)

        # Wrap the RPC method; this adds retry and timeout information,
        # and friendly error handling.
        rpc = self._client._transport._wrapped_methods[
            self._client._transport.mutate_row
        ]

        # Certain fields should be provided within the metadata header;
        # add these here.
        metadata = tuple(metadata)
        if all(m[0] != gapic_v1.routing_header.ROUTING_METADATA_KEY for m in metadata):
            metadata += (
                gapic_v1.routing_header.to_grpc_metadata(
                    (("table_name", request.table_name),)
                ),
            )

        # Validate the universe domain.
        self._client._validate_universe_domain()

        # Send the request.
        response = await rpc(
            request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Done; return the response.
        return response

    async def __aenter__(self) -> "BigtableAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.transport.close()


DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
    gapic_version=package_version.__version__
)


__all__ = ("BigtableAsyncClient",)
