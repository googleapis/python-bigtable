# -*- coding: utf-8 -*-

# Copyright 2020 Google LLC
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

import abc
import typing
import pkg_resources

from google import auth  # type: ignore
from google.api_core import exceptions  # type: ignore
from google.api_core import gapic_v1  # type: ignore
from google.api_core import retry as retries  # type: ignore
from google.api_core import operations_v1  # type: ignore
from google.auth import credentials  # type: ignore

from google.cloud.bigtable_admin_v2.types import bigtable_instance_admin
from google.cloud.bigtable_admin_v2.types import instance
from google.iam.v1 import iam_policy_pb2 as iam_policy  # type: ignore
from google.iam.v1 import policy_pb2 as policy  # type: ignore
from google.longrunning import operations_pb2 as operations  # type: ignore
from google.protobuf import empty_pb2 as empty  # type: ignore


try:
    DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo(
        gapic_version=pkg_resources.get_distribution(
            "google-cloud-bigtable-admin",
        ).version,
    )
except pkg_resources.DistributionNotFound:
    DEFAULT_CLIENT_INFO = gapic_v1.client_info.ClientInfo()


class BigtableInstanceAdminTransport(abc.ABC):
    """Abstract transport class for BigtableInstanceAdmin."""

    AUTH_SCOPES = (
        "https://www.googleapis.com/auth/bigtable.admin",
        "https://www.googleapis.com/auth/bigtable.admin.cluster",
        "https://www.googleapis.com/auth/bigtable.admin.instance",
        "https://www.googleapis.com/auth/cloud-bigtable.admin",
        "https://www.googleapis.com/auth/cloud-bigtable.admin.cluster",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/cloud-platform.read-only",
    )

    def __init__(
        self,
        *,
        host: str = "bigtableadmin.googleapis.com",
        credentials: credentials.Credentials = None,
        credentials_file: typing.Optional[str] = None,
        scopes: typing.Optional[typing.Sequence[str]] = AUTH_SCOPES,
        quota_project_id: typing.Optional[str] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
        **kwargs,
    ) -> None:
        """Instantiate the transport.

        Args:
            host (Optional[str]): The hostname to connect to.
            credentials (Optional[google.auth.credentials.Credentials]): The
                authorization credentials to attach to requests. These
                credentials identify the application to the service; if none
                are specified, the client will attempt to ascertain the
                credentials from the environment.
            credentials_file (Optional[str]): A file with credentials that can
                be loaded with :func:`google.auth.load_credentials_from_file`.
                This argument is mutually exclusive with credentials.
            scope (Optional[Sequence[str]]): A list of scopes.
            quota_project_id (Optional[str]): An optional project to use for billing
                and quota.
            client_info (google.api_core.gapic_v1.client_info.ClientInfo):
                The client info used to send a user-agent string along with
                API requests. If ``None``, then default info will be used.
                Generally, you only need to set this if you're developing
                your own client library.
        """
        # Save the hostname. Default to port 443 (HTTPS) if none is specified.
        if ":" not in host:
            host += ":443"
        self._host = host

        # Save the scopes.
        self._scopes = scopes or self.AUTH_SCOPES

        # If no credentials are provided, then determine the appropriate
        # defaults.
        if credentials and credentials_file:
            raise exceptions.DuplicateCredentialArgs(
                "'credentials_file' and 'credentials' are mutually exclusive"
            )

        if credentials_file is not None:
            credentials, _ = auth.load_credentials_from_file(
                credentials_file, scopes=self._scopes, quota_project_id=quota_project_id
            )

        elif credentials is None:
            credentials, _ = auth.default(
                scopes=self._scopes, quota_project_id=quota_project_id
            )

        # Save the credentials.
        self._credentials = credentials

    def _prep_wrapped_messages(self, client_info):
        # Precompute the wrapped methods.
        self._wrapped_methods = {
            self.create_instance: gapic_v1.method.wrap_method(
                self.create_instance, default_timeout=300.0, client_info=client_info,
            ),
            self.get_instance: gapic_v1.method.wrap_method(
                self.get_instance,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.list_instances: gapic_v1.method.wrap_method(
                self.list_instances,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.update_instance: gapic_v1.method.wrap_method(
                self.update_instance,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.partial_update_instance: gapic_v1.method.wrap_method(
                self.partial_update_instance,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.delete_instance: gapic_v1.method.wrap_method(
                self.delete_instance, default_timeout=60.0, client_info=client_info,
            ),
            self.create_cluster: gapic_v1.method.wrap_method(
                self.create_cluster, default_timeout=60.0, client_info=client_info,
            ),
            self.get_cluster: gapic_v1.method.wrap_method(
                self.get_cluster,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.list_clusters: gapic_v1.method.wrap_method(
                self.list_clusters,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.update_cluster: gapic_v1.method.wrap_method(
                self.update_cluster,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.delete_cluster: gapic_v1.method.wrap_method(
                self.delete_cluster, default_timeout=60.0, client_info=client_info,
            ),
            self.create_app_profile: gapic_v1.method.wrap_method(
                self.create_app_profile, default_timeout=60.0, client_info=client_info,
            ),
            self.get_app_profile: gapic_v1.method.wrap_method(
                self.get_app_profile,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.list_app_profiles: gapic_v1.method.wrap_method(
                self.list_app_profiles,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.update_app_profile: gapic_v1.method.wrap_method(
                self.update_app_profile,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.delete_app_profile: gapic_v1.method.wrap_method(
                self.delete_app_profile, default_timeout=60.0, client_info=client_info,
            ),
            self.get_iam_policy: gapic_v1.method.wrap_method(
                self.get_iam_policy,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
            self.set_iam_policy: gapic_v1.method.wrap_method(
                self.set_iam_policy, default_timeout=60.0, client_info=client_info,
            ),
            self.test_iam_permissions: gapic_v1.method.wrap_method(
                self.test_iam_permissions,
                default_retry=retries.Retry(
                    initial=1.0,
                    maximum=60.0,
                    multiplier=2,
                    predicate=retries.if_exception_type(
                        exceptions.DeadlineExceeded, exceptions.ServiceUnavailable,
                    ),
                    deadline=60.0,
                ),
                default_timeout=60.0,
                client_info=client_info,
            ),
        }

    @property
    def operations_client(self) -> operations_v1.OperationsClient:
        """Return the client designed to process long-running operations."""
        raise NotImplementedError()

    @property
    def create_instance(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.CreateInstanceRequest],
        typing.Union[operations.Operation, typing.Awaitable[operations.Operation]],
    ]:
        raise NotImplementedError()

    @property
    def get_instance(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.GetInstanceRequest],
        typing.Union[instance.Instance, typing.Awaitable[instance.Instance]],
    ]:
        raise NotImplementedError()

    @property
    def list_instances(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.ListInstancesRequest],
        typing.Union[
            bigtable_instance_admin.ListInstancesResponse,
            typing.Awaitable[bigtable_instance_admin.ListInstancesResponse],
        ],
    ]:
        raise NotImplementedError()

    @property
    def update_instance(
        self,
    ) -> typing.Callable[
        [instance.Instance],
        typing.Union[instance.Instance, typing.Awaitable[instance.Instance]],
    ]:
        raise NotImplementedError()

    @property
    def partial_update_instance(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.PartialUpdateInstanceRequest],
        typing.Union[operations.Operation, typing.Awaitable[operations.Operation]],
    ]:
        raise NotImplementedError()

    @property
    def delete_instance(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.DeleteInstanceRequest],
        typing.Union[empty.Empty, typing.Awaitable[empty.Empty]],
    ]:
        raise NotImplementedError()

    @property
    def create_cluster(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.CreateClusterRequest],
        typing.Union[operations.Operation, typing.Awaitable[operations.Operation]],
    ]:
        raise NotImplementedError()

    @property
    def get_cluster(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.GetClusterRequest],
        typing.Union[instance.Cluster, typing.Awaitable[instance.Cluster]],
    ]:
        raise NotImplementedError()

    @property
    def list_clusters(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.ListClustersRequest],
        typing.Union[
            bigtable_instance_admin.ListClustersResponse,
            typing.Awaitable[bigtable_instance_admin.ListClustersResponse],
        ],
    ]:
        raise NotImplementedError()

    @property
    def update_cluster(
        self,
    ) -> typing.Callable[
        [instance.Cluster],
        typing.Union[operations.Operation, typing.Awaitable[operations.Operation]],
    ]:
        raise NotImplementedError()

    @property
    def delete_cluster(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.DeleteClusterRequest],
        typing.Union[empty.Empty, typing.Awaitable[empty.Empty]],
    ]:
        raise NotImplementedError()

    @property
    def create_app_profile(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.CreateAppProfileRequest],
        typing.Union[instance.AppProfile, typing.Awaitable[instance.AppProfile]],
    ]:
        raise NotImplementedError()

    @property
    def get_app_profile(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.GetAppProfileRequest],
        typing.Union[instance.AppProfile, typing.Awaitable[instance.AppProfile]],
    ]:
        raise NotImplementedError()

    @property
    def list_app_profiles(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.ListAppProfilesRequest],
        typing.Union[
            bigtable_instance_admin.ListAppProfilesResponse,
            typing.Awaitable[bigtable_instance_admin.ListAppProfilesResponse],
        ],
    ]:
        raise NotImplementedError()

    @property
    def update_app_profile(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.UpdateAppProfileRequest],
        typing.Union[operations.Operation, typing.Awaitable[operations.Operation]],
    ]:
        raise NotImplementedError()

    @property
    def delete_app_profile(
        self,
    ) -> typing.Callable[
        [bigtable_instance_admin.DeleteAppProfileRequest],
        typing.Union[empty.Empty, typing.Awaitable[empty.Empty]],
    ]:
        raise NotImplementedError()

    @property
    def get_iam_policy(
        self,
    ) -> typing.Callable[
        [iam_policy.GetIamPolicyRequest],
        typing.Union[policy.Policy, typing.Awaitable[policy.Policy]],
    ]:
        raise NotImplementedError()

    @property
    def set_iam_policy(
        self,
    ) -> typing.Callable[
        [iam_policy.SetIamPolicyRequest],
        typing.Union[policy.Policy, typing.Awaitable[policy.Policy]],
    ]:
        raise NotImplementedError()

    @property
    def test_iam_permissions(
        self,
    ) -> typing.Callable[
        [iam_policy.TestIamPermissionsRequest],
        typing.Union[
            iam_policy.TestIamPermissionsResponse,
            typing.Awaitable[iam_policy.TestIamPermissionsResponse],
        ],
    ]:
        raise NotImplementedError()


__all__ = ("BigtableInstanceAdminTransport",)
