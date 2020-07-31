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

"""User friendly container for Google Cloud Bigtable Column Family."""


from google.cloud.bigtable_admin_v2.proto import (
    bigtable_table_admin_pb2 as table_admin_v2_pb2,
)

from google.cloud.bigtable.base_column_family import (  # noqa: F401
    BaseColumnFamily,
    GarbageCollectionRule,
    MaxVersionsGCRule,
    MaxAgeGCRule,
    GCRuleUnion,
    GCRuleIntersection,
)


class ColumnFamily(BaseColumnFamily):
    """Representation of a Google Cloud Bigtable Column Family.

    We can use a :class:`ColumnFamily` to:

    * :meth:`create` itself
    * :meth:`update` itself
    * :meth:`delete` itself

    :type column_family_id: str
    :param column_family_id: The ID of the column family. Must be of the
                             form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

    :type table: :class:`Table <google.cloud.bigtable.table.Table>`
    :param table: The table that owns the column family.

    :type gc_rule: :class:`GarbageCollectionRule`
    :param gc_rule: (Optional) The garbage collection settings for this
                    column family.
    """

    def __init__(self, column_family_id, table, gc_rule=None):
        super(ColumnFamily, self).__init__(column_family_id, table, gc_rule=gc_rule)

    def create(self):
        """Create this column family.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_create_column_family]
            :end-before: [END bigtable_create_column_family]

        """
        column_family = self.to_pb()
        modification = table_admin_v2_pb2.ModifyColumnFamiliesRequest.Modification(
            id=self.column_family_id, create=column_family
        )

        client = self._table._instance._client
        # data it contains are the GC rule and the column family ID already
        # stored on this instance.
        client.table_admin_client.modify_column_families(
            self._table.name, [modification]
        )

    def update(self):
        """Update this column family.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_update_column_family]
            :end-before: [END bigtable_update_column_family]

        .. note::

            Only the GC rule can be updated. By changing the column family ID,
            you will simply be referring to a different column family.
        """
        column_family = self.to_pb()
        modification = table_admin_v2_pb2.ModifyColumnFamiliesRequest.Modification(
            id=self.column_family_id, update=column_family
        )

        client = self._table._instance._client
        # data it contains are the GC rule and the column family ID already
        # stored on this instance.
        client.table_admin_client.modify_column_families(
            self._table.name, [modification]
        )

    def delete(self):
        """Delete this column family.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_delete_column_family]
            :end-before: [END bigtable_delete_column_family]

        """
        modification = table_admin_v2_pb2.ModifyColumnFamiliesRequest.Modification(
            id=self.column_family_id, drop=True
        )

        client = self._table._instance._client
        # data it contains are the GC rule and the column family ID already
        # stored on this instance.
        client.table_admin_client.modify_column_families(
            self._table.name, [modification]
        )
