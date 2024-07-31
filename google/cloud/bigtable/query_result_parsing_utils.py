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

from typing import Any
from google.cloud.bigtable.execute_query_values import Struct
from google.cloud.bigtable.execute_query_metadata import SqlType
from google.cloud.bigtable_v2.types.data import ProtoRows
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

REQUIRED_PROTO_FIELDS = {
    SqlType.Bytes: "bytes_value",
    SqlType.String: "string_value",
    SqlType.Int64: "int_value",
    SqlType.Float64: "float_value",
    SqlType.Bool: "bool_value",
    SqlType.Timestamp: "timestamp_value",
    SqlType.Date: "date_value",
    SqlType.Struct: "array_value",
    SqlType.Array: "array_value",
    SqlType.Map: "array_value",
}


def _parse_array_type(value: ProtoRows.Value, metadata_type: SqlType.Array) -> list:
    return list(
        map(
            lambda val: parse_pb_value_to_python_value(val, metadata_type.element_type),
            value.array_value.values,
        )
    )


def _parse_map_type(value: ProtoRows.Value, metadata_type: SqlType.Map) -> dict:
    # Values of type `Map` are stored in a `Value.array_value` where each entry
    # is another `Value.array_value` with two elements (the key and the value,
    # in that order).
    # Normally encoded Map values won't have repeated keys, however, clients are
    # expected to handle the case in which they do. If the same key appears
    # multiple times, the _last_ value takes precedence.

    try:
        return dict(
            map(
                lambda map_entry: (
                    parse_pb_value_to_python_value(
                        map_entry.array_value.values[0], metadata_type.key_type
                    ),
                    parse_pb_value_to_python_value(
                        map_entry.array_value.values[1], metadata_type.value_type
                    ),
                ),
                value.array_value.values,
            )
        )
    except IndexError:
        raise ValueError("Invalid map entry - less or more than two values.")


def _parse_struct_type(value: ProtoRows.Value, metadata_type: SqlType.Struct) -> Struct:
    if len(value.array_value.values) != len(metadata_type.fields):
        raise ValueError("Mismatched lengths of values and types.")

    struct = Struct()
    for value, field in zip(value.array_value.values, metadata_type.fields):
        field_name, field_type = field
        struct.add_field(field_name, parse_pb_value_to_python_value(value, field_type))

    return struct


def _parse_timestamp_type(
    value: ProtoRows.Value, metadata_type: SqlType.Timestamp
) -> DatetimeWithNanoseconds:
    return DatetimeWithNanoseconds.from_timestamp_pb(value.timestamp_value)


TYPE_PARSERS = {
    SqlType.Timestamp: _parse_timestamp_type,
    SqlType.Struct: _parse_struct_type,
    SqlType.Array: _parse_array_type,
    SqlType.Map: _parse_map_type,
}


def parse_pb_value_to_python_value(
    value: ProtoRows.Value, metadata_type: SqlType.Type
) -> Any:
    value_kind = value.WhichOneof("kind")
    if not value_kind:
        return None

    kind = type(metadata_type)
    if not value.HasField(REQUIRED_PROTO_FIELDS[kind]):
        raise ValueError(
            f"{REQUIRED_PROTO_FIELDS[kind]} field for {kind.__name__} type not found in a Value."
        )

    if kind in TYPE_PARSERS:
        parser = TYPE_PARSERS[kind]
        return parser(value, metadata_type)
    elif kind in REQUIRED_PROTO_FIELDS:
        field_name = REQUIRED_PROTO_FIELDS[kind]
        return getattr(value, field_name)
    else:
        raise ValueError(f"Unknown kind {kind}")
