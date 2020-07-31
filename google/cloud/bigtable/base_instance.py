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

"""User-friendly container for Google Cloud Bigtable Instance."""

import re


_INSTANCE_NAME_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/" r"instances/(?P<instance_id>[a-z][-a-z0-9]*)$"
)

_INSTANCE_CREATE_WARNING = """
Use of `instance.create({0}, {1}, {2})` will be deprecated.
Please replace with
`cluster = instance.cluster({0}, {1}, {2})`
`instance.create(clusters=[cluster])`."""


class BaseInstance(object):
    """Representation of a Google Cloud Bigtable Instance.

    We can use an :class:`Instance` to:

    * :meth:`reload` itself
    * :meth:`create` itself
    * :meth:`update` itself
    * :meth:`delete` itself

    .. note::

        For now, we leave out the ``default_storage_type`` (an enum)
        which if not sent will end up as :data:`.data_v2_pb2.STORAGE_SSD`.

    :type instance_id: str
    :param instance_id: The ID of the instance.

    :type client: :class:`Client <google.cloud.bigtable.client.Client>`
    :param client: The client that owns the instance. Provides
                   authorization and a project ID.

    :type display_name: str
    :param display_name: (Optional) The display name for the instance in the
                         Cloud Console UI. (Must be between 4 and 30
                         characters.) If this value is not set in the
                         constructor, will fall back to the instance ID.

    :type instance_type: int
    :param instance_type: (Optional) The type of the instance.
                          Possible values are represented
                          by the following constants:
                          :data:`google.cloud.bigtable.enums.Instance.Type.PRODUCTION`.
                          :data:`google.cloud.bigtable.enums.Instance.Type.DEVELOPMENT`,
                          Defaults to
                          :data:`google.cloud.bigtable.enums.Instance.Type.UNSPECIFIED`.

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

    :type _state: int
    :param _state: (`OutputOnly`)
                   The current state of the instance.
                   Possible values are represented by the following constants:
                   :data:`google.cloud.bigtable.enums.Instance.State.STATE_NOT_KNOWN`.
                   :data:`google.cloud.bigtable.enums.Instance.State.READY`.
                   :data:`google.cloud.bigtable.enums.Instance.State.CREATING`.
    """

    def __init__(
        self,
        instance_id,
        client,
        display_name=None,
        instance_type=None,
        labels=None,
        _state=None,
    ):
        self.instance_id = instance_id
        self._client = client
        self.display_name = display_name or instance_id
        self.type_ = instance_type
        self.labels = labels
        self._state = _state

    def _update_from_pb(self, instance_pb):
        """Refresh self from the server-provided protobuf.
        Helper for :meth:`from_pb` and :meth:`reload`.
        """
        if not instance_pb.display_name:  # Simple field (string)
            raise ValueError("Instance protobuf does not contain display_name")
        self.display_name = instance_pb.display_name
        self.type_ = instance_pb.type
        self.labels = dict(instance_pb.labels)
        self._state = instance_pb.state

    @classmethod
    def from_pb(cls, instance_pb, client):
        """Creates an instance instance from a protobuf.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_instance_from_pb]
            :end-before: [END bigtable_instance_from_pb]

        :type instance_pb: :class:`instance_pb2.Instance`
        :param instance_pb: An instance protobuf object.

        :type client: :class:`Client <google.cloud.bigtable.client.Client>`
        :param client: The client that owns the instance.

        :rtype: :class:`Instance`
        :returns: The instance parsed from the protobuf response.
        :raises: :class:`ValueError <exceptions.ValueError>` if the instance
                 name does not match
                 ``projects/{project}/instances/{instance_id}``
                 or if the parsed project ID does not match the project ID
                 on the client.
        """
        match = _INSTANCE_NAME_RE.match(instance_pb.name)
        if match is None:
            raise ValueError(
                "Instance protobuf name was not in the " "expected format.",
                instance_pb.name,
            )
        if match.group("project") != client.project:
            raise ValueError(
                "Project ID on instance does not match the " "project ID on the client"
            )
        instance_id = match.group("instance_id")

        result = cls(instance_id, client)
        result._update_from_pb(instance_pb)
        return result

    @property
    def name(self):
        """Instance name used in requests.

        .. note::
          This property will not change if ``instance_id`` does not,
          but the return value is not cached.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_instance_name]
            :end-before: [END bigtable_instance_name]

        The instance name is of the form

            ``"projects/{project}/instances/{instance_id}"``

        :rtype: str
        :returns: Return a fully-qualified instance string.
        """
        return self._client.instance_admin_client.instance_path(
            project=self._client.project, instance=self.instance_id
        )

    @property
    def state(self):
        """google.cloud.bigtable.enums.Instance.State: state of Instance.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_instance_state]
            :end-before: [END bigtable_instance_state]

        """
        return self._state

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        # NOTE: This does not compare the configuration values, such as
        #       the display_name. Instead, it only compares
        #       identifying values instance ID and client. This is
        #       intentional, since the same instance can be in different states
        #       if not synchronized. Instances with similar instance
        #       settings but different clients can't be used in the same way.
        return other.instance_id == self.instance_id and other._client == self._client

    def __ne__(self, other):
        return not self == other

    def create(
        self,
        location_id=None,
        serve_nodes=None,
        default_storage_type=None,
        clusters=None,
    ):
        raise NotImplementedError

    def exists(self):
        raise NotImplementedError

    def reload(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def get_iam_policy(self, requested_policy_version=None):
        raise NotImplementedError

    def set_iam_policy(self, policy):
        raise NotImplementedError

    def test_iam_permissions(self, permissions):
        raise NotImplementedError

    def cluster(
        self, cluster_id, location_id=None, serve_nodes=None, default_storage_type=None
    ):
        raise NotImplementedError

    def list_clusters(self):
        raise NotImplementedError

    def table(self, table_id, mutation_timeout=None, app_profile_id=None):
        raise NotImplementedError

    def list_tables(self):
        raise NotImplementedError

    def app_profile(
        self,
        app_profile_id,
        routing_policy_type=None,
        description=None,
        cluster_id=None,
        allow_transactional_writes=None,
    ):
        raise NotImplementedError

    def list_app_profiles(self):
        raise NotImplementedError
