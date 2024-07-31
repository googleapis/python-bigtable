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

"""
This module provides the SqlType class used for specifying types in
ExecuteQuery and some utilities.

The SqlTypes are used in Metadata returned by the ExecuteQuery operation as well
as for specifying query parameter types explicitly.
"""

from collections import defaultdict
from typing import (
    Optional,
    List,
    Dict,
    Set,
    Union,
    Tuple,
    Any,
)
from google.cloud.bigtable.execute_query_values import _NamedList, Struct
from google.cloud.bigtable_v2.types.bigtable import ExecuteQueryResponse
from google.cloud.bigtable_v2.types.data import Type as PBType
from google.type import date_pb2
from google.protobuf import timestamp_pb2
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
import datetime


class SqlType:
    """
    Classes denoting types of values returned by Bigtable's ExecuteQuery operation.

    Used in :class:`.Metadata`.
    """

    class Type:
        expected_type = None
        value_pb_dict_field_name = None
        type_field_name = None

        @classmethod
        def from_pb_type(cls, pb_type: Optional[PBType] = None):
            return cls()

        def _to_type_pb_dict(self) -> dict:
            if not self.type_field_name:
                raise NotImplementedError(
                    "Fill in expected_type and value_pb_dict_field_name"
                )

            return {self.type_field_name: {}}

        def _to_value_pb_dict(self, value: Any) -> dict:
            if self.expected_type is None or self.value_pb_dict_field_name is None:
                raise NotImplementedError(
                    "Fill in expected_type and value_pb_dict_field_name"
                )

            if value is None:
                return {}

            if not isinstance(value, self.expected_type):
                raise ValueError(
                    f"Expected query parameter of type {self.expected_type.__name__}, got {type(value).__name__}"
                )

            return {self.value_pb_dict_field_name: value}

        def __eq__(self, other):
            return isinstance(other, type(self))

        def __str__(self) -> str:
            return self.__class__.__name__

        def __repr__(self) -> str:
            return self.__str__()

    class Struct(_NamedList["SqlType"], Type):
        @classmethod
        def from_pb_type(cls, type_pb: PBType) -> "Metadata.Struct":
            fields = []
            for field in type_pb.struct_type.fields:
                fields.append((field.field_name, pb_type_to_metadata_type(field.type)))
            return cls(fields)

        def _to_value_pb_dict(self, value: Struct):
            raise NotImplementedError("Struct is not supported as a query parameter")

        def _to_type_pb_dict(self) -> dict:
            raise NotImplementedError("Struct is not supported as a query parameter")

        def __eq__(self, other):
            # Cannot use super() here - we'd either have to:
            # - call super() in these base classes, which would in turn call Object.__eq__
            #   to compare objects by identity and return a False, or
            # - do not call super() in these base classes, which would result in calling only
            #   one of the __eq__ methods (a super() in the base class would be required to call the other one), or
            # - call super() in only one of the base classes, but that would be error prone and changing
            #   the order of base classes would introduce unexpected behaviour.
            return SqlType.Type.__eq__(self, other) and _NamedList.__eq__(self, other)

        def __str__(self):
            return _NamedList.__str__(self)

    class Array(Type):
        def __init__(self, element_type: "SqlType"):
            if isinstance(element_type, SqlType.Array):
                raise ValueError("Arrays of arrays are not supported.")
            self._element_type = element_type

        @property
        def element_type(self):
            return self._element_type

        @classmethod
        def from_pb_type(cls, type_pb: PBType) -> "Metadata.Array":
            return cls(pb_type_to_metadata_type(type_pb.array_type.element_type))

        def _to_value_pb_dict(self, value: list):
            raise NotImplementedError("Array is not supported as a query parameter")

        def _to_type_pb_dict(self) -> dict:
            raise NotImplementedError("Array is not supported as a query parameter")

        def __eq__(self, other):
            return super().__eq__(other) and self.element_type == other.element_type

        def __str__(self) -> str:
            return f"{self.__class__.__name__}<{str(self.element_type)}>"

    class Map(Type):
        def __init__(self, key_type: Union[str, bytes, int], value_type: "SqlType"):
            self._key_type = key_type
            self._value_type = value_type

        @property
        def key_type(self):
            return self._key_type

        @property
        def value_type(self):
            return self._value_type

        @classmethod
        def from_pb_type(cls, type_pb: PBType) -> "Metadata.Map":
            return cls(
                pb_type_to_metadata_type(type_pb.map_type.key_type),
                pb_type_to_metadata_type(type_pb.map_type.value_type),
            )

        def _to_type_pb_dict(self) -> dict:
            raise NotImplementedError("Map is not supported as a query parameter")

        def _to_value_pb_dict(self, value: dict):
            raise NotImplementedError("Map is not supported as a query parameter")

        def __eq__(self, other):
            return (
                super().__eq__(other)
                and self.key_type == other.key_type
                and self.value_type == other.value_type
            )

        def __str__(self) -> str:
            return (
                f"{self.__class__.__name__}<"
                f"{str(self._key_type)},{str(self._value_type)}>"
            )

    class Bytes(Type):
        expected_type = bytes
        value_pb_dict_field_name = "bytes_value"
        type_field_name = "bytes_type"

    class String(Type):
        expected_type = str
        value_pb_dict_field_name = "string_value"
        type_field_name = "string_type"

    class Int64(Type):
        expected_type = int
        value_pb_dict_field_name = "int_value"
        type_field_name = "int64_type"

    class Float64(Type):
        expected_type = float
        value_pb_dict_field_name = "float_value"
        type_field_name = "float64_type"

    class Bool(Type):
        expected_type = bool
        value_pb_dict_field_name = "bool_value"
        type_field_name = "bool_type"

    class Timestamp(Type):
        type_field_name = "timestamp_type"
        expected_types = (
            datetime.datetime,
            DatetimeWithNanoseconds,
        )

        def _to_value_pb_dict(self, value: Any) -> dict:
            if value is None:
                return {}

            if not isinstance(value, self.expected_types):
                raise ValueError(
                    f"Expected one of {', '.join((_type.__name__ for _type in self.expected_type))}"
                )

            if isinstance(value, DatetimeWithNanoseconds):
                return {"timestamp_value": value.timestamp_pb()}
            else:  # value must be an instance of datetime.datetime
                ts = timestamp_pb2.Timestamp()
                ts.FromDatetime(value)
                return {"timestamp_value": ts}

    class Date(Type):
        type_field_name = "date_type"
        expected_type = datetime.date

        def _to_value_pb_dict(self, value: Any) -> dict:
            if value is None:
                return {}

            if not isinstance(value, self.expected_type):
                raise ValueError(
                    f"Expected query parameter of type {self.expected_type.__name__}, got {type(value).__name__}"
                )

            return {
                "date_value": date_pb2.Date(
                    year=value.year,
                    month=value.month,
                    day=value.day,
                )
            }


class Metadata:
    pass


class ProtoMetadata(Metadata):
    class Column:
        def __init__(self, column_name: Optional[str], column_type: SqlType):
            self._column_name = column_name
            self._column_type = column_type

        @property
        def column_name(self) -> Optional[str]:
            return self._column_name

        @property
        def column_type(self) -> SqlType:
            return self._column_type

    @property
    def columns(self) -> List[Column]:
        return self._columns

    def __init__(self, columns: Optional[List[Tuple[Optional[str], SqlType]]] = None):
        self._columns: List[Tuple[Optional[str], SqlType]] = []
        self._column_indexes: Dict[str, List[int]] = defaultdict(list)
        self._duplicate_names: Set[str] = set()

        if columns:
            for column_name, column_type in columns:
                if column_name in self._column_indexes:
                    self._duplicate_names.add(column_name)
                self._column_indexes[column_name].append(len(self._columns))
                self._columns.append(ProtoMetadata.Column(column_name, column_type))

    def __getitem__(self, index_or_name: Union[str, int]) -> Column:
        if isinstance(index_or_name, str):
            if index_or_name in self._duplicate_names:
                raise KeyError(
                    f"Ambigious column name: '{index_or_name}', use index instead."
                    f" Field present on indexes {', '.join(map(str, self._column_indexes[index_or_name]))}."
                )
            if index_or_name not in self._column_indexes:
                raise KeyError(f"No such column: {index_or_name}")
            index = self._column_indexes[index_or_name][0]
        else:
            index = index_or_name
        return self._columns[index]

    def __len__(self):
        return len(self._columns)

    def __str__(self) -> str:
        columns_str = ", ".join([str(column) for column in self._columns])
        return f"{self.__class__.__name__}([{columns_str}])"

    def __repr__(self) -> str:
        return self.__str__()


def pb_metadata_to_metadata_types(
    metadata_pb: ExecuteQueryResponse.ResultSetMetadata,
) -> Metadata:
    if "proto_schema" in metadata_pb:
        fields = []
        for column_metadata in metadata_pb.proto_schema.columns:
            fields.append(
                (column_metadata.name, pb_type_to_metadata_type(column_metadata.type))
            )
        return ProtoMetadata(fields)
    raise ValueError("Invalid ResultSetMetadata object received.")


PROTO_TYPE_TO_METADATA_TYPE_FACTORY = {
    "bytes_type": SqlType.Bytes,
    "string_type": SqlType.String,
    "int64_type": SqlType.Int64,
    "float64_type": SqlType.Float64,
    "bool_type": SqlType.Bool,
    "timestamp_type": SqlType.Timestamp,
    "date_type": SqlType.Date,
    "struct_type": SqlType.Struct,
    "array_type": SqlType.Array,
    "map_type": SqlType.Map,
}


def pb_type_to_metadata_type(type_pb: PBType) -> SqlType:
    kind = PBType.pb(type_pb).WhichOneof("kind")
    if kind in PROTO_TYPE_TO_METADATA_TYPE_FACTORY:
        return PROTO_TYPE_TO_METADATA_TYPE_FACTORY[kind].from_pb_type(type_pb)
    raise ValueError(f"Unrecognized response data type: {type_pb}")
