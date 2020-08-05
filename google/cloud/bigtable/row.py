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

"""User-friendly container for Google Cloud Bigtable Row."""


from google.cloud.bigtable.base_row import (
    BaseDirectRow,
    BaseConditionalRow,
    BaseAppendRow,
    _parse_rmw_row_response,
    MAX_MUTATIONS,
)


class DirectRow(BaseDirectRow):
    """Google Cloud Bigtable Row for sending "direct" mutations.

    These mutations directly set or delete cell contents:

    * :meth:`set_cell`
    * :meth:`delete`
    * :meth:`delete_cell`
    * :meth:`delete_cells`

    These methods can be used directly::

       >>> row = table.row(b'row-key1')
       >>> row.set_cell(u'fam', b'col1', b'cell-val')
       >>> row.delete_cell(u'fam', b'col2')

    .. note::

        A :class:`DirectRow` accumulates mutations locally via the
        :meth:`set_cell`, :meth:`delete`, :meth:`delete_cell` and
        :meth:`delete_cells` methods. To actually send these mutations to the
        Google Cloud Bigtable API, you must call :meth:`commit`.

    :type row_key: bytes
    :param row_key: The key for the current row.

    :type table: :class:`Table <google.cloud.bigtable.table.Table>`
    :param table: (Optional) The table that owns the row. This is
                  used for the :meth: `commit` only.  Alternatively,
                  DirectRows can be persisted via
                  :meth:`~google.cloud.bigtable.table.Table.mutate_rows`.
    """

    def __init__(self, row_key, table=None):
        super(DirectRow, self).__init__(row_key, table)

    def commit(self):
        """Makes a ``MutateRow`` API request.

        If no mutations have been created in the row, no request is made.

        Mutations are applied atomically and in order, meaning that earlier
        mutations can be masked / negated by later ones. Cells already present
        in the row are left unchanged unless explicitly changed by a mutation.

        After committing the accumulated mutations, resets the local
        mutations to an empty list.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_row_commit]
            :end-before: [END bigtable_row_commit]

        :raises: :exc:`~.table.TooManyMutationsError` if the number of
                 mutations is greater than 100,000.
        """
        self._table.mutate_rows([self])
        self.clear()


class ConditionalRow(BaseConditionalRow):
    """Google Cloud Bigtable Row for sending mutations conditionally.

    Each mutation has an associated state: :data:`True` or :data:`False`.
    When :meth:`commit`-ed, the mutations for the :data:`True`
    state will be applied if the filter matches any cells in
    the row, otherwise the :data:`False` state will be applied.

    A :class:`ConditionalRow` accumulates mutations in the same way a
    :class:`DirectRow` does:

    * :meth:`set_cell`
    * :meth:`delete`
    * :meth:`delete_cell`
    * :meth:`delete_cells`

    with the only change the extra ``state`` parameter::

       >>> row_cond = table.row(b'row-key2', filter_=row_filter)
       >>> row_cond.set_cell(u'fam', b'col', b'cell-val', state=True)
       >>> row_cond.delete_cell(u'fam', b'col', state=False)

    .. note::

        As with :class:`DirectRow`, to actually send these mutations to the
        Google Cloud Bigtable API, you must call :meth:`commit`.

    :type row_key: bytes
    :param row_key: The key for the current row.

    :type table: :class:`Table <google.cloud.bigtable.table.Table>`
    :param table: The table that owns the row.

    :type filter_: :class:`.RowFilter`
    :param filter_: Filter to be used for conditional mutations.
    """

    def __init__(self, row_key, table, filter_):
        super(ConditionalRow, self).__init__(row_key, table, filter_)

    def commit(self):
        """Makes a ``CheckAndMutateRow`` API request.

        If no mutations have been created in the row, no request is made.

        The mutations will be applied conditionally, based on whether the
        filter matches any cells in the :class:`ConditionalRow` or not. (Each
        method which adds a mutation has a ``state`` parameter for this
        purpose.)

        Mutations are applied atomically and in order, meaning that earlier
        mutations can be masked / negated by later ones. Cells already present
        in the row are left unchanged unless explicitly changed by a mutation.

        After committing the accumulated mutations, resets the local
        mutations.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_row_commit]
            :end-before: [END bigtable_row_commit]

        :rtype: bool
        :returns: Flag indicating if the filter was matched (which also
                  indicates which set of mutations were applied by the server).
        :raises: :class:`ValueError <exceptions.ValueError>` if the number of
                 mutations exceeds the :data:`MAX_MUTATIONS`.
        """
        true_mutations = self._get_mutations(state=True)
        false_mutations = self._get_mutations(state=False)
        num_true_mutations = len(true_mutations)
        num_false_mutations = len(false_mutations)
        if num_true_mutations == 0 and num_false_mutations == 0:
            return
        if num_true_mutations > MAX_MUTATIONS or num_false_mutations > MAX_MUTATIONS:
            raise ValueError(
                "Exceed the maximum allowable mutations (%d). Had %s true "
                "mutations and %d false mutations."
                % (MAX_MUTATIONS, num_true_mutations, num_false_mutations)
            )

        data_client = self._table._instance._client.table_data_client
        resp = data_client.check_and_mutate_row(
            table_name=self._table.name,
            row_key=self._row_key,
            predicate_filter=self._filter.to_pb(),
            app_profile_id=self._table._app_profile_id,
            true_mutations=true_mutations,
            false_mutations=false_mutations,
        )
        self.clear()
        return resp.predicate_matched


class AppendRow(BaseAppendRow):
    """Google Cloud Bigtable Row for sending append mutations.

    These mutations are intended to augment the value of an existing cell
    and uses the methods:

    * :meth:`append_cell_value`
    * :meth:`increment_cell_value`

    The first works by appending bytes and the second by incrementing an
    integer (stored in the cell as 8 bytes). In either case, if the
    cell is empty, assumes the default empty value (empty string for
    bytes or 0 for integer).

    :type row_key: bytes
    :param row_key: The key for the current row.

    :type table: :class:`Table <google.cloud.bigtable.table.Table>`
    :param table: The table that owns the row.
    """

    def __init__(self, row_key, table):
        super(AppendRow, self).__init__(row_key, table)

    def commit(self):
        """Makes a ``ReadModifyWriteRow`` API request.

        This commits modifications made by :meth:`append_cell_value` and
        :meth:`increment_cell_value`. If no modifications were made, makes
        no API request and just returns ``{}``.

        Modifies a row atomically, reading the latest existing
        timestamp / value from the specified columns and writing a new value by
        appending / incrementing. The new cell created uses either the current
        server time or the highest timestamp of a cell in that column (if it
        exceeds the server time).

        After committing the accumulated mutations, resets the local mutations.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_row_commit]
            :end-before: [END bigtable_row_commit]

        :rtype: dict
        :returns: The new contents of all modified cells. Returned as a
                  dictionary of column families, each of which holds a
                  dictionary of columns. Each column contains a list of cells
                  modified. Each cell is represented with a two-tuple with the
                  value (in bytes) and the timestamp for the cell.
        :raises: :class:`ValueError <exceptions.ValueError>` if the number of
                 mutations exceeds the :data:`MAX_MUTATIONS`.
        """
        num_mutations = len(self._rule_pb_list)
        if num_mutations == 0:
            return {}
        if num_mutations > MAX_MUTATIONS:
            raise ValueError(
                "%d total append mutations exceed the maximum "
                "allowable %d." % (num_mutations, MAX_MUTATIONS)
            )

        data_client = self._table._instance._client.table_data_client
        row_response = data_client.read_modify_write_row(
            table_name=self._table.name,
            row_key=self._row_key,
            rules=self._rule_pb_list,
            app_profile_id=self._table._app_profile_id,
        )

        # Reset modifications after commit-ing request.
        self.clear()

        # NOTE: We expect row_response.key == self._row_key but don't check.
        return _parse_rmw_row_response(row_response)
