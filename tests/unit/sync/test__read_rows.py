# Copyright 2023 Google LLC
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
#

from __future__ import annotations

from unittest import mock
import pytest

TEST_FAMILY = "family_name"
TEST_QUALIFIER = b"qualifier"
TEST_TIMESTAMP = 123456789
TEST_LABELS = ["label1", "label2"]


class TestReadRowsOperation_Sync_Concrete:
    """
    Tests for ReadRowsOperation_Sync_Concrete class
    """

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable._sync._concrete import (
            _ReadRowsOperation_Sync_Concrete,
        )

        return _ReadRowsOperation_Sync_Concrete

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor_defaults(self):
        request = {}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        client.read_rows.return_value = None
        instance = self._make_one(request, client)
        assert instance.transient_errors == []
        assert instance._last_seen_row_key is None
        assert instance._emit_count == 0
        assert instance.operation_timeout is None
        retryable_fn = instance._partial_retryable
        assert retryable_fn.func == instance._read_rows_retryable_attempt
        assert retryable_fn.args[0] == client.read_rows
        assert retryable_fn.args[1] == 0
        assert retryable_fn.args[2] is None
        assert retryable_fn.args[3] == 0
        assert client.read_rows.call_count == 0

    def test_ctor(self):
        row_limit = 91
        request = {"rows_limit": row_limit}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        client.read_rows.return_value = None
        expected_buffer_size = 21
        expected_operation_timeout = 42
        expected_request_timeout = 44
        instance = self._make_one(
            request,
            client,
            buffer_size=expected_buffer_size,
            operation_timeout=expected_operation_timeout,
            per_request_timeout=expected_request_timeout,
        )
        assert instance.transient_errors == []
        assert instance._last_seen_row_key is None
        assert instance._emit_count == 0
        assert instance.operation_timeout == expected_operation_timeout
        retryable_fn = instance._partial_retryable
        assert retryable_fn.func == instance._read_rows_retryable_attempt
        assert retryable_fn.args[0] == client.read_rows
        assert retryable_fn.args[1] == expected_buffer_size
        assert retryable_fn.args[2] == expected_request_timeout
        assert retryable_fn.args[3] == row_limit
        assert client.read_rows.call_count == 0

    def test___iter__(self):
        request = {}
        client = mock.Mock()
        client.read_rows = mock.Mock()
        instance = self._make_one(request, client)
        assert instance.__iter__() is instance

    def test_transient_error_capture(self):
        from google.api_core import exceptions as core_exceptions

        client = mock.Mock()
        client.read_rows = mock.Mock()
        test_exc = core_exceptions.Aborted("test")
        test_exc2 = core_exceptions.DeadlineExceeded("test")
        client.read_rows.side_effect = [test_exc, test_exc2]
        instance = self._make_one({}, client)
        with pytest.raises(RuntimeError):
            instance.__next__()
        assert len(instance.transient_errors) == 2
        assert instance.transient_errors[0] == test_exc
        assert instance.transient_errors[1] == test_exc2

    @pytest.mark.parametrize(
        "in_keys,last_key,expected",
        [
            (["b", "c", "d"], "a", ["b", "c", "d"]),
            (["a", "b", "c"], "b", ["c"]),
            (["a", "b", "c"], "c", []),
            (["a", "b", "c"], "d", []),
            (["d", "c", "b", "a"], "b", ["d", "c"]),
        ],
    )
    def test_revise_request_rowset_keys(self, in_keys, last_key, expected):
        sample_range = {"start_key_open": last_key}
        row_set = {"row_keys": in_keys, "row_ranges": [sample_range]}
        revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
        assert revised["row_keys"] == expected
        assert revised["row_ranges"] == [sample_range]

    @pytest.mark.parametrize(
        "in_ranges,last_key,expected",
        [
            (
                [{"start_key_open": "b", "end_key_closed": "d"}],
                "a",
                [{"start_key_open": "b", "end_key_closed": "d"}],
            ),
            (
                [{"start_key_closed": "b", "end_key_closed": "d"}],
                "a",
                [{"start_key_closed": "b", "end_key_closed": "d"}],
            ),
            (
                [{"start_key_open": "a", "end_key_closed": "d"}],
                "b",
                [{"start_key_open": "b", "end_key_closed": "d"}],
            ),
            (
                [{"start_key_closed": "a", "end_key_open": "d"}],
                "b",
                [{"start_key_open": "b", "end_key_open": "d"}],
            ),
            (
                [{"start_key_closed": "b", "end_key_closed": "d"}],
                "b",
                [{"start_key_open": "b", "end_key_closed": "d"}],
            ),
            ([{"start_key_closed": "b", "end_key_closed": "d"}], "d", []),
            ([{"start_key_closed": "b", "end_key_open": "d"}], "d", []),
            ([{"start_key_closed": "b", "end_key_closed": "d"}], "e", []),
            ([{"start_key_closed": "b"}], "z", [{"start_key_open": "z"}]),
            ([{"start_key_closed": "b"}], "a", [{"start_key_closed": "b"}]),
            (
                [{"end_key_closed": "z"}],
                "a",
                [{"start_key_open": "a", "end_key_closed": "z"}],
            ),
            (
                [{"end_key_open": "z"}],
                "a",
                [{"start_key_open": "a", "end_key_open": "z"}],
            ),
        ],
    )
    def test_revise_request_rowset_ranges(self, in_ranges, last_key, expected):
        next_key = last_key + "a"
        row_set = {"row_keys": [next_key], "row_ranges": in_ranges}
        revised = self._get_target_class()._revise_request_rowset(row_set, last_key)
        assert revised["row_keys"] == [next_key]
        assert revised["row_ranges"] == expected

    @pytest.mark.parametrize("last_key", ["a", "b", "c"])
    def test_revise_request_full_table(self, last_key):
        row_set = {"row_keys": [], "row_ranges": []}
        for selected_set in [row_set, None]:
            revised = self._get_target_class()._revise_request_rowset(
                selected_set, last_key
            )
            assert revised["row_keys"] == []
            assert len(revised["row_ranges"]) == 1
            assert revised["row_ranges"][0]["start_key_open"] == last_key

    def test_revise_to_empty_rowset(self):
        # ensure that the _revise_to_empty_set method
        # does not return a full table scan
        row_keys = ["a", "b", "c"]
        row_set = {"row_keys": row_keys, "row_ranges": [{"end_key_open": "c"}]}
        revised = self._get_target_class()._revise_request_rowset(row_set, "d")
        assert revised == row_set
        assert len(revised["row_keys"]) == 3
        assert revised["row_keys"] == row_keys

    @pytest.mark.parametrize(
        "start_limit,emit_num,expected_limit",
        [
            (10, 0, 10),
            (10, 1, 9),
            (10, 10, 0),
            (0, 10, 0),
            (0, 0, 0),
            (4, 2, 2),
            (3, 9, 0),
        ],
    )
    def test_revise_limit(self, start_limit, emit_num, expected_limit):
        request = {"rows_limit": start_limit}
        instance = self._make_one(request, mock.Mock())
        instance._emit_count = emit_num
        instance._last_seen_row_key = "a"
        gapic_mock = mock.Mock()
        gapic_mock.side_effect = [RuntimeError("stop_fn")]
        attempt = instance._read_rows_retryable_attempt(
            gapic_mock, 0, None, start_limit
        )
        if start_limit != 0 and expected_limit == 0:
            # if we emitted the expected number of rows, we should receive a StopIteration
            with pytest.raises(StopIteration):
                attempt.__next__()
        else:
            with pytest.raises(RuntimeError):
                attempt.__next__()
            assert request["rows_limit"] == expected_limit

    def test_close(self):
        instance = self._make_one({}, mock.Mock())
        instance.close()
        assert instance._stream is None
        assert instance._last_seen_row_key is None
        with pytest.raises(GeneratorExit):
            instance.__next__()
        # try calling a second time
        instance.close()

    @pytest.mark.parametrize("limit", [1, 3, 10])
    def test_retryable_attempt_hit_limit(self, limit):
        """
        Stream should end after hitting the limit
        """
        from google.cloud.bigtable_v2.types.bigtable import ReadRowsResponse

        instance = self._make_one({}, mock.Mock())

        def mock_gapic(*args, **kwargs):
            for i in range(limit * 2):
                chunk = ReadRowsResponse.CellChunk(
                    row_key=str(i).encode(),
                    family_name="family_name",
                    qualifier=b"qualifier",
                    commit_row=True,
                )
                yield ReadRowsResponse(chunks=[chunk])

        gen = instance._read_rows_retryable_attempt(mock_gapic, 0, None, limit)
        # should yield values up to the limit
        for i in range(limit):
            gen.__next__()
        # next value should be StopIteration
        with pytest.raises(StopIteration):
            gen.__next__()

    def test_retryable_ignore_repeated_rows(self):
        """
        Duplicate rows emitted by stream should be ignored by _read_rows_retryable_attempt
        """
        from google.cloud.bigtable.row import Row

        def mock_stream():
            while True:
                yield Row(b"dup_key", cells=[])
                yield Row(b"dup_key", cells=[])
                yield Row(b"new", cells=[])

        with mock.patch.object(
            self._get_target_class(), "merge_row_response_stream"
        ) as mock_stream_fn:
            mock_stream_fn.return_value = mock_stream()
            instance = self._make_one({}, mock.Mock())
            first_row = instance.__next__()
            assert first_row.row_key == b"dup_key"
            second_row = instance.__next__()
            assert second_row.row_key == b"new"

    def test_retryable_ignore_last_scanned_rows(self):
        """
        Duplicate rows emitted by stream should be ignored by _read_rows_retryable_attempt
        """
        from google.cloud.bigtable.row import Row, _LastScannedRow

        def mock_stream():
            while True:
                yield Row(b"key1", cells=[])
                yield _LastScannedRow(b"ignored")
                yield Row(b"key2", cells=[])

        with mock.patch.object(
            self._get_target_class(), "merge_row_response_stream"
        ) as mock_stream_fn:
            mock_stream_fn.return_value = mock_stream()
            instance = self._make_one({}, mock.Mock())
            first_row = instance.__next__()
            assert first_row.row_key == b"key1"
            second_row = instance.__next__()
            assert second_row.row_key == b"key2"
