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

"""A user-friendly wrapper for a Google Cloud Bigtable Backup."""

from google.cloud._helpers import _datetime_to_pb_timestamp
from google.cloud.bigtable_admin_v2.types import table_pb2
from google.cloud.exceptions import NotFound
from google.protobuf import field_mask_pb2

from google.cloud.bigtable.base_backup import BaseBackup


class Backup(BaseBackup):
    """Representation of a Google Cloud Bigtable Backup.

    A :class: `Backup` can be used to:

    * :meth:`create` the backup
    * :meth:`update` the backup
    * :meth:`delete` the backup

    :type backup_id: str
    :param backup_id: The ID of the backup.

    :type instance: :class:`~google.cloud.bigtable.instance.Instance`
    :param instance: The Instance that owns this Backup.

    :type cluster_id: str
    :param cluster_id: (Optional) The ID of the Cluster that contains this Backup.
                       Required for calling 'delete', 'exists' etc. methods.

    :type table_id: str
    :param table_id: (Optional) The ID of the Table that the Backup is for.
                     Required if the 'create' method will be called.

    :type expire_time: :class:`datetime.datetime`
    :param expire_time: (Optional) The expiration time after which the Backup
                        will be automatically deleted. Required if the `create`
                        method will be called.
    """

    def __init__(
        self, backup_id, instance, cluster_id=None, table_id=None, expire_time=None
    ):
        super(Backup, self).__init__(
            backup_id,
            instance,
            cluster_id=cluster_id,
            table_id=table_id,
            expire_time=expire_time,
        )

    def create(self, cluster_id=None):
        """Creates this backup within its instance.

        :type cluster_id: str
        :param cluster_id: (Optional) The ID of the Cluster for the newly
                           created Backup.

        :rtype: :class:`~google.api_core.operation.Operation`
        :returns: :class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`
                  instance, to be used to poll the status of the 'create' request
        :raises Conflict: if the Backup already exists
        :raises NotFound: if the Instance owning the Backup does not exist
        :raises BadRequest: if the `table` or `expire_time` values are invalid,
                            or `expire_time` is not set
        """
        if not self._expire_time:
            raise ValueError('"expire_time" parameter must be set')
            # TODO: Consider implementing a method that sets a default value of
            #  `expire_time`, e.g. 1 week from the creation of the Backup.
        if not self.table_id:
            raise ValueError('"table" parameter must be set')

        if cluster_id:
            self._cluster = cluster_id

        if not self._cluster:
            raise ValueError('"cluster" parameter must be set')

        backup = table_pb2.Backup(
            source_table=self.source_table,
            expire_time=_datetime_to_pb_timestamp(self.expire_time),
        )

        api = self._instance._client.table_admin_client
        return api.create_backup(self.parent, self.backup_id, backup)

    def get(self):
        """Retrieves metadata of a pending or completed Backup.

        :returns: An instance of
                 :class:`~google.cloud.bigtable_admin_v2.types.Backup`

        :raises google.api_core.exceptions.GoogleAPICallError: If the request
                failed for any reason.
        :raises google.api_core.exceptions.RetryError: If the request failed
                due to a retryable error and retry attempts failed.
        :raises ValueError: If the parameters are invalid.
        """
        api = self._instance._client.table_admin_client
        try:
            return api.get_backup(self.name)
        except NotFound:
            return None

    def reload(self):
        """Refreshes the stored backup properties."""
        backup = self.get()
        self._source_table = backup.source_table
        self._expire_time = backup.expire_time
        self._start_time = backup.start_time
        self._end_time = backup.end_time
        self._size_bytes = backup.size_bytes
        self._state = backup.state

    def exists(self):
        """Tests whether this Backup exists.

        :rtype: bool
        :returns: True if the Backup exists, else False.
        """
        return self.get() is not None

    def update_expire_time(self, new_expire_time):
        """Update the expire time of this Backup.

        :type new_expire_time: :class:`datetime.datetime`
        :param new_expire_time: the new expiration time timestamp
        """
        backup_update = table_pb2.Backup(
            name=self.name, expire_time=_datetime_to_pb_timestamp(new_expire_time),
        )
        update_mask = field_mask_pb2.FieldMask(paths=["expire_time"])
        api = self._instance._client.table_admin_client
        api.update_backup(backup_update, update_mask)
        self._expire_time = new_expire_time

    def delete(self):
        """Delete this Backup."""
        self._instance._client.table_admin_client.delete_backup(self.name)

    def restore(self, table_id):
        """Creates a new Table by restoring from this Backup. The new Table
        must be in the same Instance as the Instance containing the Backup.
        The returned Table ``long-running operation`` can be used to track the
        progress of the operation and to cancel it. The ``response`` type is
        ``Table``, if successful.

        :param table_id: The ID of the Table to create and restore to.
                         This Table must not already exist.
        :returns: An instance of
             :class:`~google.cloud.bigtable_admin_v2.types._OperationFuture`.

        :raises: google.api_core.exceptions.AlreadyExists: If the table
                 already exists.
        :raises: google.api_core.exceptions.GoogleAPICallError: If the request
                 failed for any reason.
        :raises: google.api_core.exceptions.RetryError: If the request failed
                 due to a retryable error and retry attempts failed.
        :raises: ValueError: If the parameters are invalid.
        """
        api = self._instance._client.table_admin_client
        return api.restore_table(self._instance.name, table_id, self.name)
