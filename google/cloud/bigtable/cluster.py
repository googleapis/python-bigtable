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

"""User friendly container for Google Cloud Bigtable Cluster."""


from google.api_core.exceptions import NotFound

from google.cloud.bigtable.base_cluster import BaseCluster


class Cluster(BaseCluster):
    """Representation of a Google Cloud Bigtable Cluster.

    We can use a :class:`Cluster` to:

    * :meth:`reload` itself
    * :meth:`create` itself
    * :meth:`update` itself
    * :meth:`delete` itself

    :type cluster_id: str
    :param cluster_id: The ID of the cluster.

    :type instance: :class:`~google.cloud.bigtable.instance.Instance`
    :param instance: The instance where the cluster resides.

    :type location_id: str
    :param location_id: (Creation Only) The location where this cluster's
                        nodes and storage reside . For best performance,
                        clients should be located as close as possible to
                        this cluster.
                        For list of supported locations refer to
                        https://cloud.google.com/bigtable/docs/locations

    :type serve_nodes: int
    :param serve_nodes: (Optional) The number of nodes in the cluster.

    :type default_storage_type: int
    :param default_storage_type: (Optional) The type of storage
                                 Possible values are represented by the
                                 following constants:
                                 :data:`google.cloud.bigtable.enums.StorageType.SSD`.
                                 :data:`google.cloud.bigtable.enums.StorageType.SHD`,
                                 Defaults to
                                 :data:`google.cloud.bigtable.enums.StorageType.UNSPECIFIED`.

    :type _state: int
    :param _state: (`OutputOnly`)
                   The current state of the cluster.
                   Possible values are represented by the following constants:
                   :data:`google.cloud.bigtable.enums.Cluster.State.NOT_KNOWN`.
                   :data:`google.cloud.bigtable.enums.Cluster.State.READY`.
                   :data:`google.cloud.bigtable.enums.Cluster.State.CREATING`.
                   :data:`google.cloud.bigtable.enums.Cluster.State.RESIZING`.
                   :data:`google.cloud.bigtable.enums.Cluster.State.DISABLED`.
    """

    def __init__(
        self,
        cluster_id,
        instance,
        location_id=None,
        serve_nodes=None,
        default_storage_type=None,
        _state=None,
    ):
        super(Cluster, self).__init__(
            cluster_id,
            instance,
            location_id=location_id,
            serve_nodes=serve_nodes,
            default_storage_type=default_storage_type,
            _state=_state,
        )

    def reload(self):
        """Reload the metadata for this cluster.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_reload_cluster]
            :end-before: [END bigtable_reload_cluster]
        """
        cluster_pb = self._instance._client.instance_admin_client.get_cluster(self.name)

        # NOTE: _update_from_pb does not check that the project and
        #       cluster ID on the response match the request.
        self._update_from_pb(cluster_pb)

    def exists(self):
        """Check whether the cluster already exists.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_check_cluster_exists]
            :end-before: [END bigtable_check_cluster_exists]

        :rtype: bool
        :returns: True if the table exists, else False.
        """
        client = self._instance._client
        try:
            client.instance_admin_client.get_cluster(name=self.name)
            return True
        # NOTE: There could be other exceptions that are returned to the user.
        except NotFound:
            return False

    def create(self):
        """Create this cluster.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_create_cluster]
            :end-before: [END bigtable_create_cluster]

        .. note::

            Uses the ``project``, ``instance`` and ``cluster_id`` on the
            current :class:`Cluster` in addition to the ``serve_nodes``.
            To change them before creating, reset the values via

            .. code:: python

                cluster.serve_nodes = 8
                cluster.cluster_id = 'i-changed-my-mind'

            before calling :meth:`create`.

        :rtype: :class:`~google.api_core.operation.Operation`
        :returns: The long-running operation corresponding to the
                  create operation.
        """
        client = self._instance._client
        cluster_pb = self._to_pb()

        return client.instance_admin_client.create_cluster(
            self._instance.name, self.cluster_id, cluster_pb
        )

    def update(self):
        """Update this cluster.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_update_cluster]
            :end-before: [END bigtable_update_cluster]

        .. note::

            Updates the ``serve_nodes``. If you'd like to
            change them before updating, reset the values via

            .. code:: python

                cluster.serve_nodes = 8

            before calling :meth:`update`.

        :rtype: :class:`Operation`
        :returns: The long-running operation corresponding to the
                  update operation.
        """
        client = self._instance._client
        # We are passing `None` for third argument location.
        # Location is set only at the time of creation of a cluster
        # and can not be changed after cluster has been created.
        return client.instance_admin_client.update_cluster(
            name=self.name, serve_nodes=self.serve_nodes, location=None
        )

    def delete(self):
        """Delete this cluster.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_delete_cluster]
            :end-before: [END bigtable_delete_cluster]

        Marks a cluster and all of its tables for permanent deletion in 7 days.

        Immediately upon completion of the request:

        * Billing will cease for all of the cluster's reserved resources.
        * The cluster's ``delete_time`` field will be set 7 days in the future.

        Soon afterward:

        * All tables within the cluster will become unavailable.

        At the cluster's ``delete_time``:

        * The cluster and **all of its tables** will immediately and
          irrevocably disappear from the API, and their data will be
          permanently deleted.
        """
        client = self._instance._client
        client.instance_admin_client.delete_cluster(self.name)
