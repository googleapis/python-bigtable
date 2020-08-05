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

"""User-friendly container for Google Cloud Bigtable Table."""

from google.api_core.exceptions import NotFound
from google.cloud._helpers import _to_bytes
from google.cloud.bigtable.base_column_family import _gc_rule_from_pb
from google.cloud.bigtable.column_family import ColumnFamily
from google.cloud.bigtable.policy import Policy
from google.cloud.bigtable.row import AppendRow
from google.cloud.bigtable.row import ConditionalRow
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.row_data import PartialRowsData
from google.cloud.bigtable.row_data import DEFAULT_RETRY_READ_ROWS
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable import enums
from google.cloud.bigtable_admin_v2.proto import table_pb2 as admin_messages_v2_pb2
from google.cloud.bigtable_admin_v2.proto import (
    bigtable_table_admin_pb2 as table_admin_messages_v2_pb2,
)

import warnings

from google.cloud.bigtable.base_table import (
    BaseTable,
    _RetryableMutateRowsWorker,
    VIEW_NAME_ONLY,
    DEFAULT_RETRY,
    ClusterState,
    _create_row_request,
)


class Table(BaseTable):
    """Representation of a Google Cloud Bigtable Table.

    .. note::

        We don't define any properties on a table other than the name.
        The only other fields are ``column_families`` and ``granularity``,
        The ``column_families`` are not stored locally and
        ``granularity`` is an enum with only one value.

    We can use a :class:`Table` to:

    * :meth:`create` the table
    * :meth:`delete` the table
    * :meth:`list_column_families` in the table

    :type table_id: str
    :param table_id: The ID of the table.

    :type instance: :class:`~google.cloud.bigtable.instance.Instance`
    :param instance: The instance that owns the table.

    :type app_profile_id: str
    :param app_profile_id: (Optional) The unique name of the AppProfile.
    """

    def __init__(self, table_id, instance, mutation_timeout=None, app_profile_id=None):
        super(Table, self).__init__(
            table_id,
            instance,
            mutation_timeout=mutation_timeout,
            app_profile_id=app_profile_id,
        )

    def get_iam_policy(self):
        """Gets the IAM access control policy for this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_get_iam_policy]
            :end-before: [END bigtable_table_get_iam_policy]

        :rtype: :class:`google.cloud.bigtable.policy.Policy`
        :returns: The current IAM policy of this table.
        """
        table_client = self._instance._client.table_admin_client
        resp = table_client.get_iam_policy(resource=self.name)
        return Policy.from_pb(resp)

    def set_iam_policy(self, policy):
        """Sets the IAM access control policy for this table. Replaces any
        existing policy.

        For more information about policy, please see documentation of
        class `google.cloud.bigtable.policy.Policy`

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_set_iam_policy]
            :end-before: [END bigtable_table_set_iam_policy]

        :type policy: :class:`google.cloud.bigtable.policy.Policy`
        :param policy: A new IAM policy to replace the current IAM policy
                       of this table.

        :rtype: :class:`google.cloud.bigtable.policy.Policy`
        :returns: The current IAM policy of this table.
        """
        table_client = self._instance._client.table_admin_client
        resp = table_client.set_iam_policy(resource=self.name, policy=policy.to_pb())
        return Policy.from_pb(resp)

    def test_iam_permissions(self, permissions):
        """Tests whether the caller has the given permissions for this table.
        Returns the permissions that the caller has.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_test_iam_permissions]
            :end-before: [END bigtable_table_test_iam_permissions]

        :type permissions: list
        :param permissions: The set of permissions to check for
               the ``resource``. Permissions with wildcards (such as '*'
               or 'storage.*') are not allowed. For more information see
               `IAM Overview
               <https://cloud.google.com/iam/docs/overview#permissions>`_.
               `Bigtable Permissions
               <https://cloud.google.com/bigtable/docs/access-control>`_.

        :rtype: list
        :returns: A List(string) of permissions allowed on the table.
        """
        table_client = self._instance._client.table_admin_client
        resp = table_client.test_iam_permissions(
            resource=self.name, permissions=permissions
        )
        return list(resp.permissions)

    def column_family(self, column_family_id, gc_rule=None):
        """Factory to create a column family associated with this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_column_family]
            :end-before: [END bigtable_table_column_family]

        :type column_family_id: str
        :param column_family_id: The ID of the column family. Must be of the
                                 form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

        :type gc_rule: :class:`.GarbageCollectionRule`
        :param gc_rule: (Optional) The garbage collection settings for this
                        column family.

        :rtype: :class:`.ColumnFamily`
        :returns: A column family owned by this table.
        """
        return ColumnFamily(column_family_id, self, gc_rule=gc_rule)

    def row(self, row_key, filter_=None, append=False):
        """Factory to create a row associated with this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_row]
            :end-before: [END bigtable_table_row]

        .. warning::

           At most one of ``filter_`` and ``append`` can be used in a
           :class:`~google.cloud.bigtable.row.Row`.

        :type row_key: bytes
        :param row_key: The key for the row being created.

        :type filter_: :class:`.RowFilter`
        :param filter_: (Optional) Filter to be used for conditional mutations.
                        See :class:`.ConditionalRow` for more details.

        :type append: bool
        :param append: (Optional) Flag to determine if the row should be used
                       for append mutations.

        :rtype: :class:`~google.cloud.bigtable.row.Row`
        :returns: A row owned by this table.
        :raises: :class:`ValueError <exceptions.ValueError>` if both
                 ``filter_`` and ``append`` are used.
        """
        warnings.warn(
            "This method will be deprecated in future versions. Please "
            "use Table.append_row(), Table.conditional_row() "
            "and Table.direct_row() methods instead.",
            PendingDeprecationWarning,
            stacklevel=2,
        )

        if append and filter_ is not None:
            raise ValueError("At most one of filter_ and append can be set")
        if append:
            return AppendRow(row_key, self)
        elif filter_ is not None:
            return ConditionalRow(row_key, self, filter_=filter_)
        else:
            return DirectRow(row_key, self)

    def append_row(self, row_key):
        """Create a :class:`~google.cloud.bigtable.row.AppendRow` associated with this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_append_row]
            :end-before: [END bigtable_table_append_row]

        Args:
            row_key (bytes): The key for the row being created.

        Returns:
            A row owned by this table.
        """
        return AppendRow(row_key, self)

    def direct_row(self, row_key):
        """Create a :class:`~google.cloud.bigtable.row.DirectRow` associated with this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_direct_row]
            :end-before: [END bigtable_table_direct_row]

        Args:
            row_key (bytes): The key for the row being created.

        Returns:
            A row owned by this table.
        """
        return DirectRow(row_key, self)

    def conditional_row(self, row_key, filter_):
        """Create a :class:`~google.cloud.bigtable.row.ConditionalRow` associated with this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_table_conditional_row]
            :end-before: [END bigtable_table_conditional_row]

        Args:
            row_key (bytes): The key for the row being created.

            filter_ (:class:`.RowFilter`): (Optional) Filter to be used for
                conditional mutations. See :class:`.ConditionalRow` for more details.

        Returns:
            A row owned by this table.
        """
        return ConditionalRow(row_key, self, filter_=filter_)

    def create(self, initial_split_keys=[], column_families={}):
        """Creates this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_create_table]
            :end-before: [END bigtable_create_table]

        .. note::

            A create request returns a
            :class:`._generated.table_pb2.Table` but we don't use
            this response.

        :type initial_split_keys: list
        :param initial_split_keys: (Optional) list of row keys in bytes that
                                   will be used to initially split the table
                                   into several tablets.

        :type column_families: dict
        :param column_families: (Optional) A map columns to create.  The key is
                               the column_id str and the value is a
                               :class:`GarbageCollectionRule`
        """
        table_client = self._instance._client.table_admin_client
        instance_name = self._instance.name

        families = {
            id: ColumnFamily(id, self, rule).to_pb()
            for (id, rule) in column_families.items()
        }
        table = admin_messages_v2_pb2.Table(column_families=families)

        split = table_admin_messages_v2_pb2.CreateTableRequest.Split
        splits = [split(key=_to_bytes(key)) for key in initial_split_keys]

        table_client.create_table(
            parent=instance_name,
            table_id=self.table_id,
            table=table,
            initial_splits=splits,
        )

    def exists(self):
        """Check whether the table exists.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_check_table_exists]
            :end-before: [END bigtable_check_table_exists]

        :rtype: bool
        :returns: True if the table exists, else False.
        """
        table_client = self._instance._client.table_admin_client
        try:
            table_client.get_table(name=self.name, view=VIEW_NAME_ONLY)
            return True
        except NotFound:
            return False

    def delete(self):
        """Delete this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_delete_table]
            :end-before: [END bigtable_delete_table]

        """
        table_client = self._instance._client.table_admin_client
        table_client.delete_table(name=self.name)

    def list_column_families(self):
        """List the column families owned by this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_list_column_families]
            :end-before: [END bigtable_list_column_families]

        :rtype: dict
        :returns: Dictionary of column families attached to this table. Keys
                  are strings (column family names) and values are
                  :class:`.ColumnFamily` instances.
        :raises: :class:`ValueError <exceptions.ValueError>` if the column
                 family name from the response does not agree with the computed
                 name from the column family ID.
        """
        table_client = self._instance._client.table_admin_client
        table_pb = table_client.get_table(self.name)

        result = {}
        for column_family_id, value_pb in table_pb.column_families.items():
            gc_rule = _gc_rule_from_pb(value_pb.gc_rule)
            column_family = self.column_family(column_family_id, gc_rule=gc_rule)
            result[column_family_id] = column_family
        return result

    def get_cluster_states(self):
        """List the cluster states owned by this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_get_cluster_states]
            :end-before: [END bigtable_get_cluster_states]

        :rtype: dict
        :returns: Dictionary of cluster states for this table.
                  Keys are cluster ids and values are
                  :class: 'ClusterState' instances.
        """

        REPLICATION_VIEW = enums.Table.View.REPLICATION_VIEW
        table_client = self._instance._client.table_admin_client
        table_pb = table_client.get_table(self.name, view=REPLICATION_VIEW)

        return {
            cluster_id: ClusterState(value_pb.replication_state)
            for cluster_id, value_pb in table_pb.cluster_states.items()
        }

    def read_row(self, row_key, filter_=None):
        """Read a single row from this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_read_row]
            :end-before: [END bigtable_read_row]

        :type row_key: bytes
        :param row_key: The key of the row to read from.

        :type filter_: :class:`.RowFilter`
        :param filter_: (Optional) The filter to apply to the contents of the
                        row. If unset, returns the entire row.

        :rtype: :class:`.PartialRowData`, :data:`NoneType <types.NoneType>`
        :returns: The contents of the row if any chunks were returned in
                  the response, otherwise :data:`None`.
        :raises: :class:`ValueError <exceptions.ValueError>` if a commit row
                 chunk is never encountered.
        """
        row_set = RowSet()
        row_set.add_row_key(row_key)
        result_iter = iter(self.read_rows(filter_=filter_, row_set=row_set))
        row = next(result_iter, None)
        if next(result_iter, None) is not None:
            raise ValueError("More than one row was returned.")
        return row

    def read_rows(
        self,
        start_key=None,
        end_key=None,
        limit=None,
        filter_=None,
        end_inclusive=False,
        row_set=None,
        retry=DEFAULT_RETRY_READ_ROWS,
    ):
        """Read rows from this table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_read_rows]
            :end-before: [END bigtable_read_rows]

        :type start_key: bytes
        :param start_key: (Optional) The beginning of a range of row keys to
                          read from. The range will include ``start_key``. If
                          left empty, will be interpreted as the empty string.

        :type end_key: bytes
        :param end_key: (Optional) The end of a range of row keys to read from.
                        The range will not include ``end_key``. If left empty,
                        will be interpreted as an infinite string.

        :type limit: int
        :param limit: (Optional) The read will terminate after committing to N
                      rows' worth of results. The default (zero) is to return
                      all results.

        :type filter_: :class:`.RowFilter`
        :param filter_: (Optional) The filter to apply to the contents of the
                        specified row(s). If unset, reads every column in
                        each row.

        :type end_inclusive: bool
        :param end_inclusive: (Optional) Whether the ``end_key`` should be
                      considered inclusive. The default is False (exclusive).

        :type row_set: :class:`.RowSet`
        :param row_set: (Optional) The row set containing multiple row keys and
                        row_ranges.

        :type retry: :class:`~google.api_core.retry.Retry`
        :param retry:
            (Optional) Retry delay and deadline arguments. To override, the
            default value :attr:`DEFAULT_RETRY_READ_ROWS` can be used and
            modified with the :meth:`~google.api_core.retry.Retry.with_delay`
            method or the :meth:`~google.api_core.retry.Retry.with_deadline`
            method.

        :rtype: :class:`.PartialRowsData`
        :returns: A :class:`.PartialRowsData` a generator for consuming
                  the streamed results.
        """
        request_pb = _create_row_request(
            self.name,
            start_key=start_key,
            end_key=end_key,
            filter_=filter_,
            limit=limit,
            end_inclusive=end_inclusive,
            app_profile_id=self._app_profile_id,
            row_set=row_set,
        )
        data_client = self._instance._client.table_data_client
        return PartialRowsData(data_client.transport.read_rows, request_pb, retry)

    def yield_rows(self, **kwargs):
        """Read rows from this table.

        .. warning::
           This method will be removed in future releases.  Please use
           ``read_rows`` instead.

        :type start_key: bytes
        :param start_key: (Optional) The beginning of a range of row keys to
                          read from. The range will include ``start_key``. If
                          left empty, will be interpreted as the empty string.

        :type end_key: bytes
        :param end_key: (Optional) The end of a range of row keys to read from.
                        The range will not include ``end_key``. If left empty,
                        will be interpreted as an infinite string.

        :type limit: int
        :param limit: (Optional) The read will terminate after committing to N
                      rows' worth of results. The default (zero) is to return
                      all results.

        :type filter_: :class:`.RowFilter`
        :param filter_: (Optional) The filter to apply to the contents of the
                        specified row(s). If unset, reads every column in
                        each row.

        :type row_set: :class:`.RowSet`
        :param row_set: (Optional) The row set containing multiple row keys and
                        row_ranges.

        :rtype: :class:`.PartialRowData`
        :returns: A :class:`.PartialRowData` for each row returned
        """
        warnings.warn(
            "`yield_rows()` is deprecated; use `read_rows()` instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.read_rows(**kwargs)

    def mutate_rows(self, rows, retry=DEFAULT_RETRY):
        """Mutates multiple rows in bulk.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_mutate_rows]
            :end-before: [END bigtable_mutate_rows]

        The method tries to update all specified rows.
        If some of the rows weren't updated, it would not remove mutations.
        They can be applied to the row separately.
        If row mutations finished successfully, they would be cleaned up.

        Optionally, a ``retry`` strategy can be specified to re-attempt
        mutations on rows that return transient errors. This method will retry
        until all rows succeed or until the request deadline is reached. To
        specify a ``retry`` strategy of "do-nothing", a deadline of ``0.0``
        can be specified.

        :type rows: list
        :param rows: List or other iterable of :class:`.DirectRow` instances.

        :type retry: :class:`~google.api_core.retry.Retry`
        :param retry:
            (Optional) Retry delay and deadline arguments. To override, the
            default value :attr:`DEFAULT_RETRY` can be used and modified with
            the :meth:`~google.api_core.retry.Retry.with_delay` method or the
            :meth:`~google.api_core.retry.Retry.with_deadline` method.

        :rtype: list
        :returns: A list of response statuses (`google.rpc.status_pb2.Status`)
                  corresponding to success or failure of each row mutation
                  sent. These will be in the same order as the `rows`.
        """
        retryable_mutate_rows = _RetryableMutateRowsWorker(
            self._instance._client,
            self.name,
            rows,
            app_profile_id=self._app_profile_id,
            timeout=self.mutation_timeout,
        )
        return retryable_mutate_rows(retry=retry)

    def sample_row_keys(self):
        """Read a sample of row keys in the table.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_sample_row_keys]
            :end-before: [END bigtable_sample_row_keys]

        The returned row keys will delimit contiguous sections of the table of
        approximately equal size, which can be used to break up the data for
        distributed tasks like mapreduces.

        The elements in the iterator are a SampleRowKeys response and they have
        the properties ``offset_bytes`` and ``row_key``. They occur in sorted
        order. The table might have contents before the first row key in the
        list and after the last one, but a key containing the empty string
        indicates "end of table" and will be the last response given, if
        present.

        .. note::

            Row keys in this list may not have ever been written to or read
            from, and users should therefore not make any assumptions about the
            row key structure that are specific to their use case.

        The ``offset_bytes`` field on a response indicates the approximate
        total storage space used by all rows in the table which precede
        ``row_key``. Buffering the contents of all rows between two subsequent
        samples would require space roughly equal to the difference in their
        ``offset_bytes`` fields.

        :rtype: :class:`~google.cloud.exceptions.GrpcRendezvous`
        :returns: A cancel-able iterator. Can be consumed by calling ``next()``
                  or by casting to a :class:`list` and can be cancelled by
                  calling ``cancel()``.
        """
        data_client = self._instance._client.table_data_client
        response_iterator = data_client.sample_row_keys(
            self.name, app_profile_id=self._app_profile_id
        )

        return response_iterator

    def truncate(self, timeout=None):
        """Truncate the table

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_truncate_table]
            :end-before: [END bigtable_truncate_table]

        :type timeout: float
        :param timeout: (Optional) The amount of time, in seconds, to wait
                        for the request to complete.

        :raise: google.api_core.exceptions.GoogleAPICallError: If the
                request failed for any reason.
                google.api_core.exceptions.RetryError: If the request failed
                due to a retryable error and retry attempts failed.
                ValueError: If the parameters are invalid.
        """
        client = self._instance._client
        table_admin_client = client.table_admin_client
        if timeout:
            table_admin_client.drop_row_range(
                self.name, delete_all_data_from_table=True, timeout=timeout
            )
        else:
            table_admin_client.drop_row_range(
                self.name, delete_all_data_from_table=True
            )

    def drop_by_prefix(self, row_key_prefix, timeout=None):
        """

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_drop_by_prefix]
            :end-before: [END bigtable_drop_by_prefix]

        :type row_key_prefix: bytes
        :param row_key_prefix: Delete all rows that start with this row key
                            prefix. Prefix cannot be zero length.

        :type timeout: float
        :param timeout: (Optional) The amount of time, in seconds, to wait
                        for the request to complete.

        :raise: google.api_core.exceptions.GoogleAPICallError: If the
                request failed for any reason.
                google.api_core.exceptions.RetryError: If the request failed
                due to a retryable error and retry attempts failed.
                ValueError: If the parameters are invalid.
        """
        client = self._instance._client
        table_admin_client = client.table_admin_client
        if timeout:
            table_admin_client.drop_row_range(
                self.name, row_key_prefix=_to_bytes(row_key_prefix), timeout=timeout
            )
        else:
            table_admin_client.drop_row_range(
                self.name, row_key_prefix=_to_bytes(row_key_prefix)
            )
