# Copyright 2016 Google LLC
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

"""Filters for Google Cloud Bigtable Row classes."""

import struct


from google.cloud.bigtable.data.row_filters import (
    RowFilter,
    SinkFilter,
    _BoolFilter as _BaseBoolFilter,
    PassAllFilter,
    BlockAllFilter,
    _RegexFilter as _BaseRegexFilter,
    RowKeyRegexFilter,
    RowSampleFilter,
    FamilyNameRegexFilter,
    ColumnQualifierRegexFilter,
    TimestampRange,
    TimestampRangeFilter as BaseTimestampRangeFilter,
    ColumnRangeFilter as BaseColumnRangeFilter,
    ValueRegexFilter,
    ValueRangeFilter,
    _CellCountFilter as _BaseCellCountFilter,
    CellsRowOffsetFilter,
    CellsRowLimitFilter,
    CellsColumnLimitFilter,
    StripValueTransformerFilter,
    ApplyLabelFilter,
    _FilterCombination as _BaseFilterCombination,
    RowFilterChain,
    RowFilterUnion,
    ConditionalRowFilter as BaseConditionalRowFilter,
)

_PACK_I64 = struct.Struct(">q").pack

# The classes defined below are to provide constructors and members
# that have an interface that does not match the one used by the data
# client, for backwards compatibility purposes.

# Each underscored class is an ABC. Make them into classes that can be
# instantiated with a placeholder to_dict method for consistency.


class _BoolFilter(_BaseBoolFilter):
    """Row filter that uses a boolean flag.

    :type flag: bool
    :param flag: An indicator if a setting is turned on or off.
    """

    def _to_dict(self):
        pass


class _RegexFilter(_BaseRegexFilter):
    """Row filter that uses a regular expression.

    The ``regex`` must be valid RE2 patterns. See Google's
    `RE2 reference`_ for the accepted syntax.

    .. _RE2 reference: https://github.com/google/re2/wiki/Syntax

    :type regex: bytes or str
    :param regex:
        A regular expression (RE2) for some row filter.  String values
        will be encoded as ASCII.
    """

    def _to_dict(self):
        pass


class TimestampRangeFilter(BaseTimestampRangeFilter):
    """Row filter that limits cells to a range of time.

    :type range_: :class:`TimestampRange`
    :param range_: Range of time that cells should match against.
    """

    def __init__(self, range_):
        self.range_ = range_


class ExactValueFilter(ValueRegexFilter):
    """Row filter for an exact value.


    :type value: bytes or str or int
    :param value:
        a literal string encodable as ASCII, or the
        equivalent bytes, or an integer (which will be packed into 8-bytes).
    """

    def __init__(self, value):
        if isinstance(value, int):
            value = _PACK_I64(value)
        super(ExactValueFilter, self).__init__(value)


class _CellCountFilter(_BaseCellCountFilter):
    """Row filter that uses an integer count of cells.

    The cell count is used as an offset or a limit for the number
    of results returned.

    :type num_cells: int
    :param num_cells: An integer count / offset / limit.
    """

    def _to_dict(self):
        pass


class ColumnRangeFilter(BaseColumnRangeFilter):
    """A row filter to restrict to a range of columns.

    Both the start and end column can be included or excluded in the range.
    By default, we include them both, but this can be changed with optional
    flags.

    :type column_family_id: str
    :param column_family_id: The column family that contains the columns. Must
                             be of the form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

    :type start_column: bytes
    :param start_column: The start of the range of columns. If no value is
                         used, the backend applies no upper bound to the
                         values.

    :type end_column: bytes
    :param end_column: The end of the range of columns. If no value is used,
                       the backend applies no upper bound to the values.

    :type inclusive_start: bool
    :param inclusive_start: Boolean indicating if the start column should be
                            included in the range (or excluded). Defaults
                            to :data:`True` if ``start_column`` is passed and
                            no ``inclusive_start`` was given.

    :type inclusive_end: bool
    :param inclusive_end: Boolean indicating if the end column should be
                          included in the range (or excluded). Defaults
                          to :data:`True` if ``end_column`` is passed and
                          no ``inclusive_end`` was given.

    :raises: :class:`ValueError <exceptions.ValueError>` if ``inclusive_start``
             is set but no ``start_column`` is given or if ``inclusive_end``
             is set but no ``end_column`` is given
    """

    def __init__(
        self,
        column_family_id,
        start_column=None,
        end_column=None,
        inclusive_start=None,
        inclusive_end=None,
    ):
        super(ColumnRangeFilter, self).__init__(
            family_id=column_family_id,
            start_qualifier=start_column,
            end_qualifier=end_column,
            inclusive_start=inclusive_start,
            inclusive_end=inclusive_end,
        )

    @property
    def column_family_id(self):
        return self.family_id

    @column_family_id.setter
    def column_family_id(self, column_family_id):
        self.family_id = column_family_id

    @property
    def start_column(self):
        return self.start_qualifier

    @start_column.setter
    def start_column(self, start_column):
        self.start_qualifier = start_column

    @property
    def end_column(self):
        return self.end_qualifier

    @end_column.setter
    def end_column(self, end_column):
        self.end_qualifier = end_column


class _FilterCombination(_BaseFilterCombination):
    """Chain of row filters.

    Sends rows through several filters in sequence. The filters are "chained"
    together to process a row. After the first filter is applied, the second
    is applied to the filtered output and so on for subsequent filters.

    :type filters: list
    :param filters: List of :class:`RowFilter`
    """

    def _to_dict(self):
        pass


class ConditionalRowFilter(BaseConditionalRowFilter):
    """Conditional row filter which exhibits ternary behavior.

    Executes one of two filters based on another filter. If the ``base_filter``
    returns any cells in the row, then ``true_filter`` is executed. If not,
    then ``false_filter`` is executed.

    .. note::

        The ``base_filter`` does not execute atomically with the true and false
        filters, which may lead to inconsistent or unexpected results.

        Additionally, executing a :class:`ConditionalRowFilter` has poor
        performance on the server, especially when ``false_filter`` is set.

    :type base_filter: :class:`RowFilter`
    :param base_filter: The filter to condition on before executing the
                        true/false filters.

    :type true_filter: :class:`RowFilter`
    :param true_filter: (Optional) The filter to execute if there are any cells
                        matching ``base_filter``. If not provided, no results
                        will be returned in the true case.

    :type false_filter: :class:`RowFilter`
    :param false_filter: (Optional) The filter to execute if there are no cells
                         matching ``base_filter``. If not provided, no results
                         will be returned in the false case.
    """

    @property
    def base_filter(self):
        return self.predicate_filter

    @base_filter.setter
    def base_filter(self, value: RowFilter):
        self.predicate_filter = value


__all__ = (
    RowFilter,
    SinkFilter,
    PassAllFilter,
    BlockAllFilter,
    RowKeyRegexFilter,
    RowSampleFilter,
    FamilyNameRegexFilter,
    ColumnQualifierRegexFilter,
    TimestampRange,
    TimestampRangeFilter,
    ColumnRangeFilter,
    ValueRegexFilter,
    ExactValueFilter,
    ValueRangeFilter,
    CellsRowOffsetFilter,
    CellsRowLimitFilter,
    CellsColumnLimitFilter,
    StripValueTransformerFilter,
    ApplyLabelFilter,
    RowFilterChain,
    RowFilterUnion,
    ConditionalRowFilter,
)
