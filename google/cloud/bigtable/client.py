# Copyright 2015 Google LLC
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

"""Parent client for calling the Google Cloud Bigtable API.

This is the base from which all interactions with the API occur.

In the hierarchy of API concepts

* a :class:`~google.cloud.bigtable.client.Client` owns an
  :class:`~google.cloud.bigtable.instance.Instance`
* an :class:`~google.cloud.bigtable.instance.Instance` owns a
  :class:`~google.cloud.bigtable.table.Table`
* a :class:`~google.cloud.bigtable.table.Table` owns a
  :class:`~.column_family.ColumnFamily`
* a :class:`~google.cloud.bigtable.table.Table` owns a
  :class:`~google.cloud.bigtable.row.Row` (and all the cells in the row)
"""

from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.cluster import Cluster

from google.cloud.bigtable.base_cluster import _CLUSTER_NAME_RE

from google.cloud.bigtable.base_client import (
    BaseClient,
    _CLIENT_INFO,
)


class Client(BaseClient):
    """Client for interacting with Google Cloud Bigtable API.

    .. note::

        Since the Cloud Bigtable API requires the gRPC transport, no
        ``_http`` argument is accepted by this class.

    :type project: :class:`str` or :func:`unicode <unicode>`
    :param project: (Optional) The ID of the project which owns the
                    instances, tables and data. If not provided, will
                    attempt to determine from the environment.

    :type credentials: :class:`~google.auth.credentials.Credentials`
    :param credentials: (Optional) The OAuth2 Credentials to use for this
                        client. If not passed, falls back to the default
                        inferred from the environment.

    :type read_only: bool
    :param read_only: (Optional) Boolean indicating if the data scope should be
                      for reading only (or for writing as well). Defaults to
                      :data:`False`.

    :type admin: bool
    :param admin: (Optional) Boolean indicating if the client will be used to
                  interact with the Instance Admin or Table Admin APIs. This
                  requires the :const:`ADMIN_SCOPE`. Defaults to :data:`False`.

    :type: client_info: :class:`google.api_core.gapic_v1.client_info.ClientInfo`
    :param client_info:
        The client info used to send a user-agent string along with API
        requests. If ``None``, then default info will be used. Generally,
        you only need to set this if you're developing your own library
        or partner tool.

    :type client_options: :class:`~google.api_core.client_options.ClientOptions`
        or :class:`dict`
    :param client_options: (Optional) Client options used to set user options
        on the client. API Endpoint should be set through client_options.

    :type admin_client_options:
        :class:`~google.api_core.client_options.ClientOptions` or :class:`dict`
    :param admin_client_options: (Optional) Client options used to set user
        options on the client. API Endpoint for admin operations should be set
        through admin_client_options.

    :type channel: :instance: grpc.Channel
    :param channel (grpc.Channel): (Optional) DEPRECATED:
            A ``Channel`` instance through which to make calls.
            This argument is mutually exclusive with ``credentials``;
            providing both will raise an exception. No longer used.

    :raises: :class:`ValueError <exceptions.ValueError>` if both ``read_only``
             and ``admin`` are :data:`True`
    """

    def __init__(
        self,
        project=None,
        credentials=None,
        read_only=False,
        admin=False,
        client_info=_CLIENT_INFO,
        client_options=None,
        admin_client_options=None,
        channel=None,
    ):
        super(Client, self).__init__(
            project=project,
            credentials=credentials,
            read_only=read_only,
            admin=admin,
            client_info=client_info,
            client_options=client_options,
            admin_client_options=admin_client_options,
            channel=channel,
        )

    def instance(self, instance_id, display_name=None, instance_type=None, labels=None):
        """Factory to create a instance associated with this client.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_create_prod_instance]
            :end-before: [END bigtable_create_prod_instance]

        :type instance_id: str
        :param instance_id: The ID of the instance.

        :type display_name: str
        :param display_name: (Optional) The display name for the instance in
                             the Cloud Console UI. (Must be between 4 and 30
                             characters.) If this value is not set in the
                             constructor, will fall back to the instance ID.

        :type instance_type: int
        :param instance_type: (Optional) The type of the instance.
                               Possible values are represented
                               by the following constants:
                               :data:`google.cloud.bigtable.enums.InstanceType.PRODUCTION`.
                               :data:`google.cloud.bigtable.enums.InstanceType.DEVELOPMENT`,
                               Defaults to
                               :data:`google.cloud.bigtable.enums.InstanceType.UNSPECIFIED`.

        :type labels: dict
        :param labels: (Optional) Labels are a flexible and lightweight
                       mechanism for organizing cloud resources into groups
                       that reflect a customer's organizational needs and
                       deployment strategies. They can be used to filter
                       resources and aggregate metrics. Label keys must be
                       between 1 and 63 characters long. Maximum 64 labels can
                       be associated with a given resource. Label values must
                       be between 0 and 63 characters long. Keys and values
                       must both be under 128 bytes.

        :rtype: :class:`~google.cloud.bigtable.instance.Instance`
        :returns: an instance owned by this client.
        """
        return Instance(
            instance_id,
            self,
            display_name=display_name,
            instance_type=instance_type,
            labels=labels,
        )

    def list_instances(self):
        """List instances owned by the project.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_list_instances]
            :end-before: [END bigtable_list_instances]

        :rtype: tuple
        :returns:
            (instances, failed_locations), where 'instances' is list of
            :class:`google.cloud.bigtable.instance.Instance`, and
            'failed_locations' is a list of locations which could not
            be resolved.
        """
        resp = self.instance_admin_client.list_instances(self.project_path)
        instances = [Instance.from_pb(instance, self) for instance in resp.instances]
        return instances, resp.failed_locations

    def list_clusters(self):
        """List the clusters in the project.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_list_clusters_in_project]
            :end-before: [END bigtable_list_clusters_in_project]

        :rtype: tuple
        :returns:
            (clusters, failed_locations), where 'clusters' is list of
            :class:`google.cloud.bigtable.instance.Cluster`, and
            'failed_locations' is a list of strings representing
            locations which could not be resolved.
        """
        resp = self.instance_admin_client.list_clusters(
            self.instance_admin_client.instance_path(self.project, "-")
        )
        clusters = []
        instances = {}
        for cluster in resp.clusters:
            match_cluster_name = _CLUSTER_NAME_RE.match(cluster.name)
            instance_id = match_cluster_name.group("instance")
            if instance_id not in instances:
                instances[instance_id] = self.instance(instance_id)
            clusters.append(Cluster.from_pb(cluster, instances[instance_id]))
        return clusters, resp.failed_locations
