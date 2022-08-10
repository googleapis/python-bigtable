from enum import Enum
from collections import OrderedDict
from google.cloud._helpers import _datetime_from_microseconds  # type: ignore
from google.cloud._helpers import _to_bytes  # type: ignore

_MISSING_COLUMN_FAMILY = "Column family {} is not among the cells stored in this row."
_MISSING_COLUMN = (
    "Column {} is not among the cells stored in this row in the column family {}."
)
_MISSING_INDEX = (
    "Index {!r} is not valid for the cells stored in this row for column {} "
    "in the column family {}. There are {} such cells."
)


class _State(Enum):
    ROW_START = "ROW_START"
    CELL_START = "CELL_START"
    CELL_IN_PROGRESS = "CELL_IN_PROGRESS"
    CELL_COMPLETE = "CELL_COMPLETE"
    ROW_COMPLETE = "COMPLETE_ROW"


class _PartialRow(object):
    __slots__ = [
        "row_key",
        "cells",
        "last_family",
        "last_family_cells",
        "last_qualifier",
        "last_qualifier_cells",
        "cell",
    ]

    def __init__(self, row_key):
        self.row_key = row_key
        self.cells = OrderedDict()

        self.last_family = None
        self.last_family_cells = OrderedDict()
        self.last_qualifier = None
        self.last_qualifier_cells = []

        self.cell = None


class _PartialCell(object):
    __slots__ = ["family", "qualifier", "timestamp", "labels", "value", "value_index"]

    def __init__(self):
        self.family = None
        self.qualifier = None
        self.timestamp = None
        self.labels = None
        self.value = None
        self.value_index = 0


class PartialRowData(object):
    """Representation of partial row in a Google Cloud Bigtable Table.

    These are expected to be updated directly from a
    :class:`._generated.bigtable_service_messages_pb2.ReadRowsResponse`

    :type row_key: bytes
    :param row_key: The key for the row holding the (partial) data.
    """

    def __init__(self, row_key):
        self._row_key = row_key
        self._cells = OrderedDict()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return other._row_key == self._row_key and other._cells == self._cells

    def __ne__(self, other):
        return not self == other

    def to_dict(self):
        """Convert the cells to a dictionary.

        This is intended to be used with HappyBase, so the column family and
        column qualiers are combined (with ``:``).

        :rtype: dict
        :returns: Dictionary containing all the data in the cells of this row.
        """
        result = {}
        for column_family_id, columns in self._cells.items():
            for column_qual, cells in columns.items():
                key = _to_bytes(column_family_id) + b":" + _to_bytes(column_qual)
                result[key] = cells
        return result

    @property
    def cells(self):
        """Property returning all the cells accumulated on this partial row.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_api_row_data_cells]
            :end-before: [END bigtable_api_row_data_cells]
            :dedent: 4

        :rtype: dict
        :returns: Dictionary of the :class:`Cell` objects accumulated. This
                  dictionary has two-levels of keys (first for column families
                  and second for column names/qualifiers within a family). For
                  a given column, a list of :class:`Cell` objects is stored.
        """
        return self._cells

    @property
    def row_key(self):
        """Getter for the current (partial) row's key.

        :rtype: bytes
        :returns: The current (partial) row's key.
        """
        return self._row_key

    def find_cells(self, column_family_id, column):
        """Get a time series of cells stored on this instance.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_api_row_find_cells]
            :end-before: [END bigtable_api_row_find_cells]
            :dedent: 4

        Args:
            column_family_id (str): The ID of the column family. Must be of the
                form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.
            column (bytes): The column within the column family where the cells
                are located.

        Returns:
            List[~google.cloud.bigtable.row_data.Cell]: The cells stored in the
            specified column.

        Raises:
            KeyError: If ``column_family_id`` is not among the cells stored
                in this row.
            KeyError: If ``column`` is not among the cells stored in this row
                for the given ``column_family_id``.
        """
        try:
            column_family = self._cells[column_family_id]
        except KeyError:
            raise KeyError(_MISSING_COLUMN_FAMILY.format(column_family_id))

        try:
            cells = column_family[column]
        except KeyError:
            raise KeyError(_MISSING_COLUMN.format(column, column_family_id))

        return cells

    def cell_value(self, column_family_id, column, index=0):
        """Get a single cell value stored on this instance.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_api_row_cell_value]
            :end-before: [END bigtable_api_row_cell_value]
            :dedent: 4

        Args:
            column_family_id (str): The ID of the column family. Must be of the
                form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.
            column (bytes): The column within the column family where the cell
                is located.
            index (Optional[int]): The offset within the series of values. If
                not specified, will return the first cell.

        Returns:
            ~google.cloud.bigtable.row_data.Cell value: The cell value stored
            in the specified column and specified index.

        Raises:
            KeyError: If ``column_family_id`` is not among the cells stored
                in this row.
            KeyError: If ``column`` is not among the cells stored in this row
                for the given ``column_family_id``.
            IndexError: If ``index`` cannot be found within the cells stored
                in this row for the given ``column_family_id``, ``column``
                pair.
        """
        cells = self.find_cells(column_family_id, column)

        try:
            cell = cells[index]
        except (TypeError, IndexError):
            num_cells = len(cells)
            msg = _MISSING_INDEX.format(index, column, column_family_id, num_cells)
            raise IndexError(msg)

        return cell.value

    def cell_values(self, column_family_id, column, max_count=None):
        """Get a time series of cells stored on this instance.

        For example:

        .. literalinclude:: snippets_table.py
            :start-after: [START bigtable_api_row_cell_values]
            :end-before: [END bigtable_api_row_cell_values]
            :dedent: 4

        Args:
            column_family_id (str): The ID of the column family. Must be of the
                form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.
            column (bytes): The column within the column family where the cells
                are located.
            max_count (int): The maximum number of cells to use.

        Returns:
            A generator which provides: cell.value, cell.timestamp_micros
                for each cell in the list of cells

        Raises:
            KeyError: If ``column_family_id`` is not among the cells stored
                in this row.
            KeyError: If ``column`` is not among the cells stored in this row
                for the given ``column_family_id``.
        """
        cells = self.find_cells(column_family_id, column)
        if max_count is None:
            max_count = len(cells)

        for index, cell in enumerate(cells):
            if index == max_count:
                break

            yield cell.value, cell.timestamp_micros


class Cell(object):
    """Representation of a Google Cloud Bigtable Cell.

    :type value: bytes
    :param value: The value stored in the cell.

    :type timestamp_micros: int
    :param timestamp_micros: The timestamp_micros when the cell was stored.

    :type labels: list
    :param labels: (Optional) List of strings. Labels applied to the cell.
    """

    __slots__ = ["value", "timestamp_micros", "labels"]

    def __init__(self, value, timestamp_micros, labels=None):
        self.value = value
        self.timestamp_micros = timestamp_micros
        self.labels = list(labels) if labels else []

    @classmethod
    def from_pb(cls, cell_pb):
        """Create a new cell from a Cell protobuf.

        :type cell_pb: :class:`._generated.data_pb2.Cell`
        :param cell_pb: The protobuf to convert.

        :rtype: :class:`Cell`
        :returns: The cell corresponding to the protobuf.
        """
        return cls(cell_pb.value, cell_pb.timestamp_micros, labels=cell_pb.labels)

    @property
    def timestamp(self):
        return _datetime_from_microseconds(self.timestamp_micros)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return (
            other.value == self.value
            and other.timestamp_micros == self.timestamp_micros
            and other.labels == self.labels
        )

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "<{name} value={value!r} timestamp={timestamp}>".format(
            name=self.__class__.__name__, value=self.value, timestamp=self.timestamp
        )


class _RowMerger(object):
    """
    State machine to merge chunks from a response stream into logical rows.

    The implementation is a fairly linear state machine that is implemented as
    a method for every state in the _State enum. In general the states flow
    from top to bottom with some repetition. Each state handler will do some
    sanity checks, update in progress data and set the next state.

    There can be multiple state transitions for each chunk, ie. a single chunk
    row will flow from ROW_START -> CELL_START -> CELL_COMPLETE -> ROW_COMPLETE
    in a single iteration.
    """

    __slots__ = ["state", "last_seen_row_key", "row"]

    def __init__(self, last_seen_row=b""):
        self.last_seen_row_key = last_seen_row
        self.state = _State.ROW_START
        self.row = None

    def process_chunks(self, response):
        """
        Process the chunks in the given response and yield logical rows.
        The internal state of this class will maintain state across multiple
        response protos.
        """
        if response.last_scanned_row_key:
            if self.last_seen_row_key >= response.last_scanned_row_key:
                raise InvalidChunk("Last scanned row key is out of order")
            self.last_seen_row_key = response.last_scanned_row_key

        for chunk in response.chunks:
            if chunk.reset_row:
                self._handle_reset(chunk)
                continue

            if self.state == _State.ROW_START:
                self._handle_row_start(chunk)

            if self.state == _State.CELL_START:
                self._handle_cell_start(chunk)

            if self.state == _State.CELL_IN_PROGRESS:
                self._handle_cell_in_progress(chunk)

            if self.state == _State.CELL_COMPLETE:
                self._handle_cell_complete(chunk)

            if self.state == _State.ROW_COMPLETE:
                yield self._handle_row_complete(chunk)
            elif chunk.commit_row:
                raise InvalidChunk(
                    f"Chunk tried to commit row in wrong state (${self.state})"
                )

    def _handle_reset(self, chunk):
        if self.state == _State.ROW_START:
            raise InvalidChunk("Bare reset")
        if chunk.row_key:
            raise InvalidChunk("Reset chunk has a row key")
        if chunk.HasField("family_name"):
            raise InvalidChunk("Reset chunk has family_name")
        if chunk.HasField("qualifier"):
            raise InvalidChunk("Reset chunk has qualifier")
        if chunk.timestamp_micros:
            raise InvalidChunk("Reset chunk has a timestamp")
        if chunk.labels:
            raise InvalidChunk("Reset chunk has labels")
        if chunk.value:
            raise InvalidChunk("Reset chunk has a value")

        self.state = _State.ROW_START
        self.row = None

    def _handle_row_start(self, chunk):
        if not chunk.row_key:
            raise InvalidChunk("New row is missing a row key")
        if self.last_seen_row_key and self.last_seen_row_key >= chunk.row_key:
            raise InvalidChunk("Out of order row keys")

        self.row = _PartialRow(chunk.row_key)
        self.state = _State.CELL_START

    def _handle_cell_start(self, chunk):
        # Ensure that all chunks after the first one either are missing a row
        # key or the row is the same
        if self.row.cells and chunk.row_key and chunk.row_key != self.row.row_key:
            raise InvalidChunk("row key changed mid row")

        if not self.row.cell:
            self.row.cell = _PartialCell()

        # Cells can inherit family/qualifier from previous cells
        # However if the family changes, then qualifier must be specified as well
        if chunk.HasField("family_name"):
            self.row.cell.family = chunk.family_name.value
            self.row.cell.qualifier = None
        if not self.row.cell.family:
            raise InvalidChunk("missing family for a new cell")

        if chunk.HasField("qualifier"):
            self.row.cell.qualifier = chunk.qualifier.value
        if self.row.cell.qualifier is None:
            raise InvalidChunk("missing qualifier for a new cell")

        self.row.cell.timestamp = chunk.timestamp_micros
        self.row.cell.labels = chunk.labels

        if chunk.value_size > 0:
            # explicitly avoid pre-allocation as it seems that bytearray
            # concatenation performs better than slice copies.
            self.row.cell.value = bytearray()
            self.state = _State.CELL_IN_PROGRESS
        else:
            self.row.cell.value = chunk.value
            self.state = _State.CELL_COMPLETE

    def _handle_cell_in_progress(self, chunk):
        # if this isn't the first cell chunk, make sure that everything except
        # the value stayed constant.
        if self.row.cell.value_index > 0:
            if chunk.row_key:
                raise InvalidChunk("found row key mid cell")
            if chunk.HasField("family_name"):
                raise InvalidChunk("In progress cell had a family name")
            if chunk.HasField("qualifier"):
                raise InvalidChunk("In progress cell had a qualifier")
            if chunk.timestamp_micros:
                raise InvalidChunk("In progress cell had a timestamp")
            if chunk.labels:
                raise InvalidChunk("In progress cell had labels")

        self.row.cell.value += chunk.value
        self.row.cell.value_index += len(chunk.value)

        if chunk.value_size > 0:
            self.state = _State.CELL_IN_PROGRESS
        else:
            self.row.cell.value = bytes(self.row.cell.value)
            self.state = _State.CELL_COMPLETE

    def _handle_cell_complete(self, chunk):
        # since we are guaranteed that all family & qualifier cells are
        # contiguous, we can optimize away the dict lookup by caching the last
        # family/qualifier and simply comparing and appending
        family_changed = False
        if self.row.last_family != self.row.cell.family:
            family_changed = True
            self.row.last_family = self.row.cell.family
            self.row.cells[
                self.row.cell.family
            ] = self.row.last_family_cells = OrderedDict()

        if family_changed or self.row.last_qualifier != self.row.cell.qualifier:
            self.row.last_qualifier = self.row.cell.qualifier
            self.row.last_family_cells[
                self.row.cell.qualifier
            ] = self.row.last_qualifier_cells = []

        self.row.last_qualifier_cells.append(
            Cell(
                self.row.cell.value,
                self.row.cell.timestamp,
                self.row.cell.labels,
            )
        )

        self.row.cell.timestamp = 0
        self.row.cell.value = None
        self.row.cell.value_index = 0

        if not chunk.commit_row:
            self.state = _State.CELL_START
        else:
            self.state = _State.ROW_COMPLETE

    def _handle_row_complete(self, chunk):
        new_row = PartialRowData(self.row.row_key)
        new_row._cells = self.row.cells

        self.last_seen_row_key = new_row.row_key
        self.row = None
        self.state = _State.ROW_START

        return new_row

    def finalize(self):
        """
        Must be called at the end of the stream to ensure there are no unmerged
        rows.
        """
        if self.row or self.state != _State.ROW_START:
            raise ValueError("The row remains partial / is not committed.")


class InvalidChunk(RuntimeError):
    """Exception raised to invalid chunk data from back-end."""
