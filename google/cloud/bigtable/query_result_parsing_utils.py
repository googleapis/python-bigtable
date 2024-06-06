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
from google.cloud.bigtable.execute_query import Struct
from google.cloud.bigtable_v2.types.data import ProtoRows, Type

REQUIRED_PROTO_FIELDS = {
    "bytes_type": "bytes_value",
    "string_type": "string_value",
    "int64_type": "int_value",
    "float64_type": "float_value",
    "bool_type": "bool_value",
    "timestamp_type": "timestamp_value",
    "date_type": "date_value",
    "struct_type": "array_value",
    "array_type": "array_value",
    "map_type": "array_value",
}


TYPE_TO_VALUE_FIELD_NAME = {
    "bytes_type": "bytes_value",
    "string_type": "string_value",
    "int64_type": "int_value",
    "float64_type": "float_value",
    "bool_type": "bool_value",
    "timestamp_type": "timestamp_value",
    "date_type": "date_value",
}


def _parse_array_type(value: ProtoRows.Value, pb_type: Type) -> list:
    return list(
        map(
            lambda val: parse_pb_value_to_python_value(
                val, pb_type.array_type.element_type
            ),
            value.array_value.values,
        )
    )


def _parse_map_type(value: ProtoRows.Value, pb_type: Type) -> dict:
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
                        map_entry.array_value.values[0], pb_type.map_type.key_type
                    ),
                    parse_pb_value_to_python_value(
                        map_entry.array_value.values[1], pb_type.map_type.value_type
                    ),
                ),
                value.array_value.values,
            )
        )
    except IndexError:
        raise ValueError("Invalid map entry - less or more than two values.")


def _parse_struct_type(value: ProtoRows.Value, pb_type: Type) -> Struct:
    if len(value.array_value.values) != len(pb_type.struct_type.fields):
        raise ValueError("Mismatched lengths of values and types.")

    field_mapping = Struct._construct_field_mapping(
        (field.field_name for field in pb_type.struct_type.fields)
    )

    struct = Struct(
        values=(
            parse_pb_value_to_python_value(value, field.type)
            for value, field in zip(
                value.array_value.values, pb_type.struct_type.fields
            )
        ),
        field_mapping=field_mapping,
    )

    return struct


TYPE_PARSERS = {
    "struct_type": _parse_struct_type,
    "array_type": _parse_array_type,
    "map_type": _parse_map_type,
}


def parse_pb_value_to_python_value(value: ProtoRows.Value, pb_type: Type) -> Any:
    kind = Type.pb(pb_type).WhichOneof("kind")
    if kind in REQUIRED_PROTO_FIELDS and REQUIRED_PROTO_FIELDS[kind] not in value:
        raise ValueError(
            f"{REQUIRED_PROTO_FIELDS[kind]} field for {kind} type not found in a Value."
        )
    if kind in TYPE_TO_VALUE_FIELD_NAME:
        field_name = TYPE_TO_VALUE_FIELD_NAME[kind]
        return getattr(value, field_name)
    elif kind in TYPE_PARSERS:
        parser = TYPE_PARSERS[kind]
        return parser(value, pb_type)
    else:
        raise ValueError(f"Type {kind} not supported by current client version")
