# Copyright 2024 Google LLC
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

from typing import (
    TypeVar,
    Generic,
    Iterable,
    Optional,
    List,
    Sequence,
)
from abc import ABC, abstractmethod

from google.cloud.bigtable_v2.types.data import ProtoRows
from google.cloud.bigtable_v2.types.bigtable import ExecuteQueryResponse
from google.cloud.bigtable.byte_cursor import _ByteCursor

from google.cloud.bigtable.query_result_parsing_utils import (
    parse_pb_value_to_python_value,
)

from google.cloud.bigtable.execute_query import QueryResultRow


T = TypeVar("T")


class _Reader(ABC, Generic[T]):
    """
    An interface for classes that consume and parse bytes returned by ``_ByteCursor``.
    Parsed bytes should be gathered into bundles (rows or columns) of expected size
    and converted to an appropriate type ``T`` that will be returned as a semantically
    meaningful result to the library user by
    :meth:`google.cloud.bigtable.instance.Instance.execute_query` or
    :meth:`google.cloud.bigtable.data._async.client.BigtableDataClientAsync.execute_query`
    methods.

    This class consumes data obtained externally to be usable in both sync and async clients.

    See :class:`google.cloud.bigtable.byte_cursor._ByteCursor` for more context.
    """

    @abstractmethod
    def consume(self, bytes_to_consume: bytes) -> Optional[Iterable[T]]:
        """This method receives a parsable chunk of bytes and returns either a None if there is
        not enough chunks to return to the user yet (e.g. we haven't received all columns in a
        row yet), or a list of appropriate values gathered from one or more parsable chunks.

        Args:
            bytes_to_consume (bytes): chunk of parsable bytes received from
                :meth:`google.cloud.bigtable.byte_cursor._ByteCursor.consume`
                method.

        Returns:
            Iterable[T] or None: Iterable if gathered values can form one or more instances of T,
                or None if there is not enough data to construct at least one instance of T with
                appropriate number of entries.
        """
        raise NotImplementedError


class _QueryResultRowReader(_Reader[QueryResultRow]):
    """
    A :class:`._Reader` consuming bytes representing
    :class:`google.cloud.bigtable_v2.types.ProtoRows.Type`
    and producing :class:`google.cloud.bigtable.execute_query.QueryResultRow`.

    Number of entries in each row is determined by number of columns in
    metadata obtained from :class:`google.cloud.bigtable.byte_cursor._ByteCursor`
    passed in the constructor.
    """

    def __init__(self, byte_cursor: _ByteCursor):
        """
        Constructs new instance of ``_QueryResultRowReader``.

        Args:
            byte_cursor (google.cloud.bigtable.byte_cursor._ByteCursor):
                byte_cursor that will be used to gather bytes for this instance of ``_Reader``,
                needed to obtain metadata about processed stream.
        """
        self._values: List[ProtoRows.Value] = []
        self._byte_cursor = byte_cursor
        self._field_mapping = None

    @property
    def metadata(self) -> Optional[ExecuteQueryResponse.ResultSetMetadata]:
        return self._byte_cursor.metadata

    def _get_field_mapping(self):
        if self._field_mapping is None:
            if not self.metadata:
                return None

            self._field_mapping = QueryResultRow._construct_field_mapping(
                (column.name for column in self.metadata.proto_schema.columns)
            )

        return self._field_mapping

    def _construct_query_result_row(
        self, values: Sequence[ProtoRows.Value]
    ) -> QueryResultRow:
        columns = self.metadata.proto_schema.columns

        assert len(values) == len(
            columns
        ), "This function should be called only when count of values matches count of columns."

        result = QueryResultRow(
            values=(
                parse_pb_value_to_python_value(value, column.type)
                for column, value in zip(columns, values)
            ),
            field_mapping=self._get_field_mapping(),
        )
        return result

    def _parse_proto_rows(self, bytes_to_parse: bytes) -> Iterable[ProtoRows.Value]:
        proto_rows = ProtoRows.deserialize(bytes_to_parse)
        return proto_rows.values

    def consume(self, bytes_to_consume: bytes) -> Optional[Iterable[QueryResultRow]]:
        if bytes_to_consume is None:
            raise ValueError("bytes_to_consume shouldn't be None")

        self._values.extend(self._parse_proto_rows(bytes_to_consume))

        num_columns = len(self.metadata.proto_schema.columns)

        if len(self._values) < num_columns:
            return None

        rows = []
        for index in range(0, len(self._values), num_columns):
            batch = self._values[index : index + num_columns]
            if len(batch) != num_columns:
                raise ValueError(
                    "Number of received columns does not allow us to construct a full row."
                )
            rows.append(self._construct_query_result_row(batch))

        self._values = []

        return rows
