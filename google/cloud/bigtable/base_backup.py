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

import re

from google.cloud.bigtable_admin_v2.gapic.bigtable_table_admin_client import (
    BigtableTableAdminClient,
)

_BACKUP_NAME_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/"
    r"instances/(?P<instance_id>[a-z][-a-z0-9]*)/"
    r"clusters/(?P<cluster_id>[a-z][-a-z0-9]*)/"
    r"backups/(?P<backup_id>[a-z][a-z0-9_\-]*[a-z0-9])$"
)

_TABLE_NAME_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/"
    r"instances/(?P<instance_id>[a-z][-a-z0-9]*)/"
    r"tables/(?P<table_id>[_a-zA-Z0-9][-_.a-zA-Z0-9]*)$"
)


class BaseBackup(object):
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
        self.backup_id = backup_id
        self._instance = instance
        self._cluster = cluster_id
        self.table_id = table_id
        self._expire_time = expire_time

        self._parent = None
        self._source_table = None
        self._start_time = None
        self._end_time = None
        self._size_bytes = None
        self._state = None

    @property
    def name(self):
        """Backup name used in requests.

        The Backup name is of the form

            ``"projects/../instances/../clusters/../backups/{backup_id}"``

        :rtype: str
        :returns: The Backup name.

        :raises: ValueError: If the 'cluster' has not been set.
        """
        if not self._cluster:
            raise ValueError('"cluster" parameter must be set')

        return BigtableTableAdminClient.backup_path(
            project=self._instance._client.project,
            instance=self._instance.instance_id,
            cluster=self._cluster,
            backup=self.backup_id,
        )

    @property
    def cluster(self):
        """The ID of the [parent] cluster used in requests.

        :rtype: str
        :returns: The ID of the cluster containing the Backup.
        """
        return self._cluster

    @cluster.setter
    def cluster(self, cluster_id):
        self._cluster = cluster_id

    @property
    def parent(self):
        """Name of the parent cluster used in requests.

        .. note::
          This property will return None if ``cluster`` is not set.

        The parent name is of the form

            ``"projects/{project}/instances/{instance_id}/clusters/{cluster}"``

        :rtype: str
        :returns: A full path to the parent cluster.
        """
        if not self._parent and self._cluster:
            self._parent = BigtableTableAdminClient.cluster_path(
                project=self._instance._client.project,
                instance=self._instance.instance_id,
                cluster=self._cluster,
            )
        return self._parent

    @property
    def source_table(self):
        """The full name of the Table from which this Backup is created.

        .. note::
          This property will return None if ``table_id`` is not set.

        The table name is of the form

            ``"projects/../instances/../tables/{source_table}"``

        :rtype: str
        :returns: The Table name.
        """
        if not self._source_table and self.table_id:
            self._source_table = BigtableTableAdminClient.table_path(
                project=self._instance._client.project,
                instance=self._instance.instance_id,
                table=self.table_id,
            )
        return self._source_table

    @property
    def expire_time(self):
        """Expiration time used in the creation requests.

        :rtype: :class:`datetime.datetime`
        :returns: A 'datetime' object representing the expiration time of
                  this Backup.
        """
        return self._expire_time

    @expire_time.setter
    def expire_time(self, new_expire_time):
        self._expire_time = new_expire_time

    @property
    def start_time(self):
        """The time this Backup was started.

        :rtype: :class:`datetime.datetime`
        :returns: A 'datetime' object representing the time when the creation
                  of this Backup had started.
        """
        return self._start_time

    @property
    def end_time(self):
        """The time this Backup was finished.

        :rtype: :class:`datetime.datetime`
        :returns: A 'datetime' object representing the time when the creation
                  of this Backup was finished.
        """
        return self._end_time

    @property
    def size_bytes(self):
        """The size of this Backup, in bytes.

        :rtype: int
        :returns: The size of this Backup, in bytes.
        """
        return self._size_bytes

    @property
    def state(self):
        """ The current state of this Backup.

        :rtype: :class:`~google.cloud.bigtable_admin_v2.gapic.enums.Backup.State`
        :returns: The current state of this Backup.
        """
        return self._state

    @classmethod
    def from_pb(cls, backup_pb, instance):
        """Creates a Backup instance from a protobuf message.

        :type backup_pb: :class:`table_pb2.Backup`
        :param backup_pb: A Backup protobuf object.

        :type instance: :class:`Instance <google.cloud.bigtable.instance.Instance>`
        :param instance: The Instance that owns the Backup.

        :rtype: :class:`~google.cloud.bigtable.backup.Backup`
        :returns: The backup parsed from the protobuf response.
        :raises: ValueError: If the backup name does not match the expected
                             format or the parsed project ID does not match the
                             project ID on the Instance's client, or if the
                             parsed instance ID does not match the Instance ID.
        """
        match = _BACKUP_NAME_RE.match(backup_pb.name)
        if match is None:
            raise ValueError(
                "Backup protobuf name was not in the expected format.", backup_pb.name
            )
        if match.group("project") != instance._client.project:
            raise ValueError(
                "Project ID of the Backup does not match the Project ID "
                "of the instance's client"
            )

        instance_id = match.group("instance_id")
        if instance_id != instance.instance_id:
            raise ValueError(
                "Instance ID of the Backup does not match the Instance ID "
                "of the instance"
            )
        backup_id = match.group("backup_id")
        cluster_id = match.group("cluster_id")

        match = _TABLE_NAME_RE.match(backup_pb.source_table)
        table_id = match.group("table_id") if match else None

        expire_time = backup_pb.expire_time

        backup = cls(
            backup_id,
            instance,
            cluster_id=cluster_id,
            table_id=table_id,
            expire_time=expire_time,
        )
        backup._start_time = backup_pb.start_time
        backup._end_time = backup_pb.end_time
        backup._size_bytes = backup_pb.size_bytes
        backup._state = backup_pb.state

        return backup

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return other.backup_id == self.backup_id and other._instance == self._instance

    def __ne__(self, other):
        return not self == other
