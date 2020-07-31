# Copyright 2018 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""User-friendly container for Google Cloud Bigtable AppProfile."""


from google.cloud.bigtable.enums import RoutingPolicyType
from google.protobuf import field_mask_pb2
from google.api_core.exceptions import NotFound

from google.cloud.bigtable.base_app_profile import BaseAppProfile


class AppProfile(BaseAppProfile):
    """Representation of a Google Cloud Bigtable AppProfile.

    We can use a :class:`AppProfile` to:

    * :meth:`reload` itself
    * :meth:`create` itself
    * :meth:`update` itself
    * :meth:`delete` itself

    :type app_profile_id: str
    :param app_profile_id: The ID of the AppProfile. Must be of the form
                           ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

    :type: routing_policy_type: int
    :param: routing_policy_type: (Optional) The type of the routing policy.
                                 Possible values are represented
                                 by the following constants:
                                 :data:`google.cloud.bigtable.enums.RoutingPolicyType.ANY`
                                 :data:`google.cloud.bigtable.enums.RoutingPolicyType.SINGLE`

    :type: description: str
    :param: description: (Optional) Long form description of the use
                         case for this AppProfile.

    :type: cluster_id: str
    :param: cluster_id: (Optional) Unique cluster_id which is only required
                        when routing_policy_type is
                        ROUTING_POLICY_TYPE_SINGLE.

    :type: allow_transactional_writes: bool
    :param: allow_transactional_writes: (Optional) If true, allow
                                        transactional writes for
                                        ROUTING_POLICY_TYPE_SINGLE.
    """

    def __init__(
        self,
        app_profile_id,
        instance,
        routing_policy_type=None,
        description=None,
        cluster_id=None,
        allow_transactional_writes=None,
    ):
        super(AppProfile, self).__init__(
            app_profile_id,
            instance,
            routing_policy_type=routing_policy_type,
            description=description,
            cluster_id=cluster_id,
            allow_transactional_writes=allow_transactional_writes,
        )

    def reload(self):
        """Reload the metadata for this cluster

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_reload_app_profile]
            :end-before: [END bigtable_reload_app_profile]
        """

        app_profile_pb = self.instance_admin_client.get_app_profile(self.name)

        # NOTE: _update_from_pb does not check that the project and
        #       app_profile ID on the response match the request.
        self._update_from_pb(app_profile_pb)

    def exists(self):
        """Check whether the AppProfile already exists.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_app_profile_exists]
            :end-before: [END bigtable_app_profile_exists]

        :rtype: bool
        :returns: True if the AppProfile exists, else False.
        """
        try:
            self.instance_admin_client.get_app_profile(self.name)
            return True
        # NOTE: There could be other exceptions that are returned to the user.
        except NotFound:
            return False

    def create(self, ignore_warnings=None):
        """Create this AppProfile.

        .. note::

            Uses the ``instance`` and ``app_profile_id`` on the current
            :class:`AppProfile` in addition to the ``routing_policy_type``,
            ``description``, ``cluster_id`` and ``allow_transactional_writes``.
            To change them before creating, reset the values via

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_create_app_profile]
            :end-before: [END bigtable_create_app_profile]

        :type: ignore_warnings: bool
        :param: ignore_warnings: (Optional) If true, ignore safety checks when
                                 creating the AppProfile.
        """
        return self.from_pb(
            self.instance_admin_client.create_app_profile(
                parent=self._instance.name,
                app_profile_id=self.app_profile_id,
                app_profile=self._to_pb(),
                ignore_warnings=ignore_warnings,
            ),
            self._instance,
        )

    def update(self, ignore_warnings=None):
        """Update this app_profile.

        .. note::

            Update any or all of the following values:
            ``routing_policy_type``
            ``description``
            ``cluster_id``
            ``allow_transactional_writes``

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_update_app_profile]
            :end-before: [END bigtable_update_app_profile]
        """
        update_mask_pb = field_mask_pb2.FieldMask()

        if self.description is not None:
            update_mask_pb.paths.append("description")

        if self.routing_policy_type == RoutingPolicyType.ANY:
            update_mask_pb.paths.append("multi_cluster_routing_use_any")
        else:
            update_mask_pb.paths.append("single_cluster_routing")

        return self.instance_admin_client.update_app_profile(
            app_profile=self._to_pb(),
            update_mask=update_mask_pb,
            ignore_warnings=ignore_warnings,
        )

    def delete(self, ignore_warnings=None):
        """Delete this AppProfile.

        For example:

        .. literalinclude:: snippets.py
            :start-after: [START bigtable_delete_app_profile]
            :end-before: [END bigtable_delete_app_profile]

        :type: ignore_warnings: bool
        :param: ignore_warnings: If true, ignore safety checks when deleting
                the AppProfile.

        :raises: google.api_core.exceptions.GoogleAPICallError: If the request
                 failed for any reason. google.api_core.exceptions.RetryError:
                 If the request failed due to a retryable error and retry
                 attempts failed. ValueError: If the parameters are invalid.
        """
        self.instance_admin_client.delete_app_profile(self.name, ignore_warnings)
