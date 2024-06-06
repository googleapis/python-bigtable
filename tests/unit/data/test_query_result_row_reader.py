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
from unittest import mock
from google.cloud.bigtable_v2.types.bigtable import ExecuteQueryResponse
from google.cloud.bigtable_v2.types.data import ProtoRows, Type
from google.cloud.bigtable.execute_query_reader import (
    _QueryResultRowReader,
)

import google.cloud.bigtable.execute_query_reader


TYPE_INT = {
    "int64_type": {
        "encoding": {"big_endian_bytes": {"bytes_type": {"encoding": {"raw": {}}}}}
    }
}


def proto_rows_bytes(*args):
    return ProtoRows.serialize(
        ProtoRows(values=[ProtoRows.Value(**arg) for arg in args])
    )


class TestQueryResultRowReader:
    def test__single_values_received(self):
        byte_cursor = mock.Mock(
            metadata=ExecuteQueryResponse.ResultSetMetadata(
                {
                    "proto_schema": {
                        "columns": [
                            {"name": "test1", "type": TYPE_INT},
                            {"name": "test2", "type": TYPE_INT},
                        ]
                    }
                }
            )
        )
        values = [
            proto_rows_bytes({"int_value": 1}),
            proto_rows_bytes({"int_value": 2}),
            proto_rows_bytes({"int_value": 3}),
        ]

        reader = _QueryResultRowReader(byte_cursor)

        assert reader.consume(values[0]) is None
        result = reader.consume(values[1])
        assert len(result) == 1
        assert len(result[0]) == 2
        assert reader.consume(values[2]) is None

    def test__multiple_rows_received(self):
        value = proto_rows_bytes(
            {"int_value": 1},
            {"int_value": 2},
            {"int_value": 3},
            {"int_value": 4},
            {"int_value": 5},
        )

        byte_cursor = mock.Mock(
            metadata=ExecuteQueryResponse.ResultSetMetadata(
                {
                    "proto_schema": {
                        "columns": [
                            {"name": "test1", "type": TYPE_INT},
                            {"name": "test2", "type": TYPE_INT},
                        ]
                    }
                }
            )
        )

        reader = _QueryResultRowReader(byte_cursor)

        with pytest.raises(ValueError):
            reader.consume(value)

    def test__received_values_are_passed_to_parser_in_batches(self):
        byte_cursor = mock.Mock(
            metadata=ExecuteQueryResponse.ResultSetMetadata(
                {
                    "proto_schema": {
                        "columns": [
                            {"name": "test1", "type": TYPE_INT},
                            {"name": "test2", "type": TYPE_INT},
                        ]
                    }
                }
            )
        )

        values = [
            {"int_value": 1},
            {"int_value": 2},
        ]

        reader = _QueryResultRowReader(byte_cursor)
        with mock.patch.object(
            google.cloud.bigtable.execute_query_reader,
            "parse_pb_value_to_python_value",
        ) as parse_mock:
            reader.consume(proto_rows_bytes(values[0]))
            parse_mock.assert_not_called()
            reader.consume(proto_rows_bytes(values[1]))
            parse_mock.assert_has_calls(
                [
                    mock.call(ProtoRows.Value(values[0]), Type(TYPE_INT)),
                    mock.call(ProtoRows.Value(values[1]), Type(TYPE_INT)),
                ]
            )

    def test__parser_errors_are_forwarded(self):
        byte_cursor = mock.Mock(
            metadata=ExecuteQueryResponse.ResultSetMetadata(
                {
                    "proto_schema": {
                        "columns": [
                            {"name": "test1", "type": TYPE_INT},
                        ]
                    }
                }
            )
        )

        values = [
            {"string_value": "test"},
        ]

        reader = _QueryResultRowReader(byte_cursor)
        with mock.patch.object(
            google.cloud.bigtable.execute_query_reader,
            "parse_pb_value_to_python_value",
            side_effect=ValueError("test"),
        ) as parse_mock:
            with pytest.raises(ValueError, match="test"):
                reader.consume(proto_rows_bytes(values[0]))

            parse_mock.assert_has_calls(
                [
                    mock.call(ProtoRows.Value(values[0]), Type(TYPE_INT)),
                ]
            )

    def test__multiple_proto_rows_received_with_one_resumption_token(self):
        from google.cloud.bigtable.byte_cursor import _ByteCursor

        def split_bytes_into_chunks(bytes_to_split, num_chunks):
            from google.cloud.bigtable.helpers import batched

            assert num_chunks <= len(bytes_to_split)
            bytes_per_part = (len(bytes_to_split) - 1) // num_chunks + 1
            result = list(map(bytes, batched(bytes_to_split, bytes_per_part)))
            assert len(result) == num_chunks
            return result

        def pass_values_to_byte_cursor(byte_cursor, iterable):
            for value in iterable:
                result = byte_cursor.consume(value)
                if result is not None:
                    yield result

        proto_rows = [
            proto_rows_bytes({"int_value": 1}, {"int_value": 2}, {"int_value": 3}),
            proto_rows_bytes({"int_value": 4}, {"int_value": 5}),
            proto_rows_bytes({"int_value": 6}),
        ]

        messages = [
            *split_bytes_into_chunks(proto_rows[0], num_chunks=2),
            *split_bytes_into_chunks(proto_rows[1], num_chunks=3),
            proto_rows[2],
        ]

        stream = [
            ExecuteQueryResponse(
                metadata={
                    "proto_schema": {
                        "columns": [
                            {"name": "test1", "type": TYPE_INT},
                            {"name": "test2", "type": TYPE_INT},
                        ]
                    }
                }
            ),
            ExecuteQueryResponse(
                results={"proto_bytes": {"proto_rows_bytes": messages[0]}}
            ),
            ExecuteQueryResponse(
                results={"proto_bytes": {"proto_rows_bytes": messages[1]}}
            ),
            ExecuteQueryResponse(
                results={"proto_bytes": {"proto_rows_bytes": messages[2]}}
            ),
            ExecuteQueryResponse(
                results={"proto_bytes": {"proto_rows_bytes": messages[3]}}
            ),
            ExecuteQueryResponse(
                results={
                    "proto_bytes": {"proto_rows_bytes": messages[4]},
                }
            ),
            ExecuteQueryResponse(
                results={
                    "proto_bytes": {"proto_rows_bytes": messages[5]},
                    "resumption_token": b"token2",
                }
            ),
        ]

        byte_cursor = _ByteCursor()

        reader = _QueryResultRowReader(byte_cursor)

        byte_cursor_iter = pass_values_to_byte_cursor(byte_cursor, stream)

        returned_values = []

        def intercept_return_values(func):
            nonlocal intercept_return_values

            def wrapped(*args, **kwargs):
                value = func(*args, **kwargs)
                returned_values.append(value)
                return value

            return wrapped

        with mock.patch.object(
            reader,
            "_parse_proto_rows",
            wraps=intercept_return_values(reader._parse_proto_rows),
        ):
            result = reader.consume(next(byte_cursor_iter))

        # Despite the fact that two ProtoRows were received, a single resumption_token after the second ProtoRows object forces us to parse them together.
        # We will interpret them as one larger ProtoRows object.
        assert len(returned_values) == 1
        assert len(returned_values[0]) == 6
        assert returned_values[0][0].int_value == 1
        assert returned_values[0][1].int_value == 2
        assert returned_values[0][2].int_value == 3
        assert returned_values[0][3].int_value == 4
        assert returned_values[0][4].int_value == 5
        assert returned_values[0][5].int_value == 6

        assert len(result) == 3
        assert len(result[0]) == 2
        assert result[0][0] == 1
        assert result[0]["test1"] == 1
        assert result[0][1] == 2
        assert result[0]["test2"] == 2
        assert len(result[1]) == 2
        assert result[1][0] == 3
        assert result[1]["test1"] == 3
        assert result[1][1] == 4
        assert result[1]["test2"] == 4
        assert len(result[2]) == 2
        assert result[2][0] == 5
        assert result[2]["test1"] == 5
        assert result[2][1] == 6
        assert result[2]["test2"] == 6
        assert byte_cursor._resumption_token == b"token2"

    def test__duplicate_result_field_name(self):
        byte_cursor = mock.Mock(
            metadata=ExecuteQueryResponse.ResultSetMetadata(
                {
                    "proto_schema": {
                        "columns": [
                            {"name": "test1", "type": TYPE_INT},
                            {"name": "test1", "type": TYPE_INT},
                        ]
                    }
                }
            )
        )
        values = [
            proto_rows_bytes({"int_value": 1}, {"int_value": 2}),
        ]

        reader = _QueryResultRowReader(byte_cursor)

        result = reader.consume(values[0])
        assert len(result) == 1
        assert len(result[0]) == 2

        with pytest.raises(
            KeyError,
            match="Ambigious field name: 'test1', use index instead. Field present on indexes 0, 1.",
        ):
            result[0]["test1"]

        assert result[0][0] == 1
        assert result[0][1] == 2
