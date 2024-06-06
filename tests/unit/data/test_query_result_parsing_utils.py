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

import pytest
import proto

from google.cloud.bigtable_v2.types.data import ProtoRows, Type
from google.cloud.bigtable.query_result_parsing_utils import (
    parse_pb_value_to_python_value,
)
from google.cloud.bigtable.execute_query import Struct

from google.type import date_pb2
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

import datetime


TYPE_INT = {
    "int64_type": {
        "encoding": {"big_endian_bytes": {"bytes_type": {"encoding": {"raw": {}}}}}
    }
}


class ExtendedType(Type):
    new_type: Type.String = proto.Field(
        Type.String,
        number=1000,
        oneof="kind",
    )


class TestQueryResultParsingUtils:
    @pytest.mark.parametrize(
        "type_dict,value_dict,expected_value",
        [
            (TYPE_INT, {"int_value": 1}, 1),
            ({"string_type": {}}, {"string_value": "test"}, "test"),
            ({"bool_type": {}}, {"bool_value": False}, False),
            ({"bytes_type": {}}, {"bytes_value": b"test"}, b"test"),
            ({"float64_type": {}}, {"float_value": 17.21}, 17.21),
            (
                {"timestamp_type": {}},
                {"timestamp_value": {"seconds": 1715864647, "nanos": 12}},
                DatetimeWithNanoseconds(
                    2024, 5, 16, 13, 4, 7, nanosecond=12, tzinfo=datetime.timezone.utc
                ),
            ),
            (
                {"date_type": {}},
                {"date_value": {"year": 1800, "month": 12, "day": 0}},
                date_pb2.Date(year=1800, month=12, day=0),
            ),
            (
                {"array_type": {"element_type": TYPE_INT}},
                {
                    "array_value": {
                        "values": [
                            {"int_value": 1},
                            {"int_value": 2},
                            {"int_value": 3},
                            {"int_value": 4},
                        ]
                    }
                },
                [1, 2, 3, 4],
            ),
        ],
    )
    def test_basic_types(self, type_dict, value_dict, expected_value):
        _type = Type(type_dict)
        value = ProtoRows.Value(value_dict)
        assert parse_pb_value_to_python_value(value, _type) == expected_value

    # Larger test cases were extracted for readability
    def test__struct(self):
        _type = Type(
            {
                "struct_type": {
                    "fields": [
                        {
                            "field_name": "field1",
                            "type": TYPE_INT,
                        },
                        {
                            "field_name": None,
                            "type": {"string_type": {}},
                        },
                        {
                            "field_name": "field3",
                            "type": {"array_type": {"element_type": TYPE_INT}},
                        },
                    ]
                }
            }
        )
        value = ProtoRows.Value(
            {
                "array_value": {
                    "values": [
                        {"int_value": 1},
                        {"string_value": "test2"},
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 2},
                                    {"int_value": 3},
                                    {"int_value": 4},
                                    {"int_value": 5},
                                ]
                            }
                        },
                    ]
                }
            }
        )

        result = parse_pb_value_to_python_value(value, _type)
        assert isinstance(result, Struct)
        assert result["field1"] == result[0] == 1
        assert result[1] == "test2"
        assert result["field3"] == result[2] == [2, 3, 4, 5]

    def test__struct__duplicate_field_names(self):
        _type = Type(
            {
                "struct_type": {
                    "fields": [
                        {
                            "field_name": "field1",
                            "type": TYPE_INT,
                        },
                        {
                            "field_name": "field1",
                            "type": {"string_type": {}},
                        },
                    ]
                }
            }
        )
        value = ProtoRows.Value(
            {
                "array_value": {
                    "values": [
                        {"int_value": 1},
                        {"string_value": "test2"},
                    ]
                }
            }
        )

        result = parse_pb_value_to_python_value(value, _type)
        assert isinstance(result, Struct)
        with pytest.raises(
            KeyError,
            match="Ambigious field name: 'field1', use index instead. Field present on indexes 0, 1.",
        ):
            result["field1"]
        assert result[0] == 1
        assert result[1] == "test2"

    def test__array_of_structs(self):
        _type = Type(
            {
                "array_type": {
                    "element_type": {
                        "struct_type": {
                            "fields": [
                                {
                                    "field_name": "field1",
                                    "type": TYPE_INT,
                                },
                                {
                                    "field_name": None,
                                    "type": {"string_type": {}},
                                },
                                {
                                    "field_name": "field3",
                                    "type": {"bool_type": {}},
                                },
                            ]
                        }
                    }
                }
            }
        )
        value = ProtoRows.Value(
            {
                "array_value": {
                    "values": [
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 1},
                                    {"string_value": "test1"},
                                    {"bool_value": True},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 2},
                                    {"string_value": "test2"},
                                    {"bool_value": False},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 3},
                                    {"string_value": "test3"},
                                    {"bool_value": True},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 4},
                                    {"string_value": "test4"},
                                    {"bool_value": False},
                                ]
                            }
                        },
                    ]
                }
            }
        )

        result = parse_pb_value_to_python_value(value, _type)
        assert isinstance(result, list)
        assert len(result) == 4

        assert isinstance(result[0], Struct)
        assert result[0]["field1"] == 1
        assert result[0][1] == "test1"
        assert result[0]["field3"]

        assert isinstance(result[1], Struct)
        assert result[1]["field1"] == 2
        assert result[1][1] == "test2"
        assert not result[1]["field3"]

        assert isinstance(result[2], Struct)
        assert result[2]["field1"] == 3
        assert result[2][1] == "test3"
        assert result[2]["field3"]

        assert isinstance(result[3], Struct)
        assert result[3]["field1"] == 4
        assert result[3][1] == "test4"
        assert not result[3]["field3"]

    def test__map(self):
        _type = Type(
            {
                "map_type": {
                    "key_type": TYPE_INT,
                    "value_type": {"string_type": {}},
                }
            }
        )
        value = ProtoRows.Value(
            {
                "array_value": {
                    "values": [
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 1},
                                    {"string_value": "test1"},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 2},
                                    {"string_value": "test2"},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 3},
                                    {"string_value": "test3"},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 4},
                                    {"string_value": "test4"},
                                ]
                            }
                        },
                    ]
                }
            }
        )

        result = parse_pb_value_to_python_value(value, _type)
        assert isinstance(result, dict)
        assert len(result) == 4

        assert result == {
            1: "test1",
            2: "test2",
            3: "test3",
            4: "test4",
        }

    def test__map_repeated_values(self):
        _type = Type(
            {
                "map_type": {
                    "key_type": TYPE_INT,
                    "value_type": {"string_type": {}},
                }
            },
        )
        value = ProtoRows.Value(
            {
                "array_value": {
                    "values": [
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 1},
                                    {"string_value": "test1"},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 1},
                                    {"string_value": "test2"},
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [
                                    {"int_value": 1},
                                    {"string_value": "test3"},
                                ]
                            }
                        },
                    ]
                }
            }
        )

        result = parse_pb_value_to_python_value(value, _type)
        assert len(result) == 1

        assert result == {
            1: "test3",
        }

    def test__map_of_maps_of_structs(self):
        _type = Type(
            {
                "map_type": {
                    "key_type": TYPE_INT,
                    "value_type": {
                        "map_type": {
                            "key_type": {"string_type": {}},
                            "value_type": {
                                "struct_type": {
                                    "fields": [
                                        {
                                            "field_name": "field1",
                                            "type": TYPE_INT,
                                        },
                                        {
                                            "field_name": "field2",
                                            "type": {"string_type": {}},
                                        },
                                    ]
                                }
                            },
                        }
                    },
                }
            }
        )
        value = ProtoRows.Value(
            {
                "array_value": {
                    "values": [  # list of (int, map) tuples
                        {
                            "array_value": {
                                "values": [  # (int, map) tuple
                                    {"int_value": 1},
                                    {
                                        "array_value": {
                                            "values": [  # list of (str, struct) tuples
                                                {
                                                    "array_value": {
                                                        "values": [  # (str, struct) tuple
                                                            {"string_value": "1_1"},
                                                            {
                                                                "array_value": {
                                                                    "values": [
                                                                        {
                                                                            "int_value": 1
                                                                        },
                                                                        {
                                                                            "string_value": "test1"
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                        ]
                                                    }
                                                },
                                                {
                                                    "array_value": {
                                                        "values": [  # (str, struct) tuple
                                                            {"string_value": "1_2"},
                                                            {
                                                                "array_value": {
                                                                    "values": [
                                                                        {
                                                                            "int_value": 2
                                                                        },
                                                                        {
                                                                            "string_value": "test2"
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                        ]
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                ]
                            }
                        },
                        {
                            "array_value": {
                                "values": [  # (int, map) tuple
                                    {"int_value": 2},
                                    {
                                        "array_value": {
                                            "values": [  # list of (str, struct) tuples
                                                {
                                                    "array_value": {
                                                        "values": [  # (str, struct) tuple
                                                            {"string_value": "2_1"},
                                                            {
                                                                "array_value": {
                                                                    "values": [
                                                                        {
                                                                            "int_value": 3
                                                                        },
                                                                        {
                                                                            "string_value": "test3"
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                        ]
                                                    }
                                                },
                                                {
                                                    "array_value": {
                                                        "values": [  # (str, struct) tuple
                                                            {"string_value": "2_2"},
                                                            {
                                                                "array_value": {
                                                                    "values": [
                                                                        {
                                                                            "int_value": 4
                                                                        },
                                                                        {
                                                                            "string_value": "test4"
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                        ]
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                ]
                            }
                        },
                    ]
                }
            }
        )
        result = parse_pb_value_to_python_value(value, _type)

        assert result[1]["1_1"]["field1"] == 1
        assert result[1]["1_1"]["field2"] == "test1"

        assert result[1]["1_2"]["field1"] == 2
        assert result[1]["1_2"]["field2"] == "test2"

        assert result[2]["2_1"]["field1"] == 3
        assert result[2]["2_1"]["field2"] == "test3"

        assert result[2]["2_2"]["field1"] == 4
        assert result[2]["2_2"]["field2"] == "test4"

    def test__invalid_type_throws_exception(self):
        _type = Type({"string_type": {}})
        value = ProtoRows.Value({"int_value": 1})

        with pytest.raises(
            ValueError,
            match="string_value field for string_type type not found in a Value.",
        ):
            parse_pb_value_to_python_value(value, _type)

    def test__unknown_kind(self):
        _type = ExtendedType({"new_type": {}})
        value = ProtoRows.Value({"int_value": 1})

        with pytest.raises(
            ValueError, match="Type new_type not supported by current client version"
        ):
            parse_pb_value_to_python_value(value, _type)
