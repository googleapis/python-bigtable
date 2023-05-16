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

import unittest
from unittest import mock
import pytest

from google.cloud.bigtable.exceptions import InvalidChunk
from google.cloud.bigtable._read_rows import AWAITING_NEW_ROW
from google.cloud.bigtable._read_rows import AWAITING_NEW_CELL
from google.cloud.bigtable._read_rows import AWAITING_CELL_VALUE
from google.api_core import exceptions as core_exceptions

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
        from google.cloud.bigtable._sync_customizations import _ReadRowsOperation_Sync_Concrete

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


class TestBigtableDataClient_Sync_Concrete:
    def _get_target_class(self):
        from google.cloud.bigtable._sync_customizations import BigtableDataClient_Sync_Concrete

        return BigtableDataClient_Sync_Concrete

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        from google.auth.credentials import AnonymousCredentials
        expected_project = "project-id"
        expected_credentials = AnonymousCredentials()
        client = self._make_one(
            project="project-id",
            credentials=expected_credentials,
        )
        assert client.project == expected_project
        assert not client._active_instances
        assert len(client._channel_refresh_tasks) == 0
        assert client.transport._credentials == expected_credentials
        client.close()

    def test_ctor_super_inits(self):
        from google.auth.credentials import AnonymousCredentials
        from google.cloud.bigtable_v2.services.bigtable.client import (
            BigtableClient,
        )
        from google.cloud.client import ClientWithProject
        from google.api_core import client_options as client_options_lib

        project = "project-id"
        credentials = AnonymousCredentials()
        client_options = {"api_endpoint": "foo.bar:1234"}
        options_parsed = client_options_lib.from_dict(client_options)
        transport_str = f"grpc"
        with mock.patch.object(BigtableClient, "__init__") as bigtable_client_init:
            bigtable_client_init.return_value = None
            with mock.patch.object(
                ClientWithProject, "__init__"
            ) as client_project_init:
                client_project_init.return_value = None
                try:
                    self._make_one(
                        project=project,
                        credentials=credentials,
                        client_options=options_parsed,
                    )
                except AttributeError:
                    pass
                # test gapic superclass init was called
                assert bigtable_client_init.call_count == 1
                kwargs = bigtable_client_init.call_args[1]
                assert kwargs["transport"] == transport_str
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed
                # test mixin superclass init was called
                assert client_project_init.call_count == 1
                kwargs = client_project_init.call_args[1]
                assert kwargs["project"] == project
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed

    def test_ctor_dict_options(self):
        from google.cloud.bigtable_v2.services.bigtable.client import (
            BigtableClient,
        )
        from google.api_core.client_options import ClientOptions
        from google.cloud.bigtable.client import BigtableDataClient

        client_options = {"api_endpoint": "foo.bar:1234"}
        with mock.patch.object(BigtableClient, "__init__") as bigtable_client_init:
            try:
                self._make_one(client_options=client_options)
            except TypeError:
                pass
            bigtable_client_init.assert_called_once()
            kwargs = bigtable_client_init.call_args[1]
            called_options = kwargs["client_options"]
            assert called_options.api_endpoint == "foo.bar:1234"
            assert isinstance(called_options, ClientOptions)
        with mock.patch.object(
            BigtableDataClient, "start_background_channel_refresh"
        ) as start_background_refresh:
            client = self._make_one(client_options=client_options)
            start_background_refresh.assert_not_called()
            client.close()

    def test_veneer_grpc_headers(self):
        # client_info should be populated with headers to
        # detect as a veneer client
        import re
        VENEER_HEADER_REGEX = re.compile(
            r"gapic\/[0-9]+\.[\w.-]+ gax\/[0-9]+\.[\w.-]+ gccl\/[0-9]+\.[\w.-]+ gl-python\/[0-9]+\.[\w.-]+ grpc\/[0-9]+\.[\w.-]+"
        )
        patch = mock.patch("google.api_core.gapic_v1.method.wrap_method")
        with patch as gapic_mock:
            client = self._make_one(project="project-id")
            wrapped_call_list = gapic_mock.call_args_list
            assert len(wrapped_call_list) > 0
            # each wrapped call should have veneer headers
            for call in wrapped_call_list:
                client_info = call.kwargs["client_info"]
                assert client_info is not None, f"{call} has no client_info"
                wrapped_user_agent_sorted = " ".join(
                    sorted(client_info.to_user_agent().split(" "))
                )
                assert VENEER_HEADER_REGEX.match(
                    wrapped_user_agent_sorted
                ), f"'{wrapped_user_agent_sorted}' does not match {VENEER_HEADER_REGEX}"
            client.close()


    def test__ping_and_warm_instances(self):
        # test with no instances
        client = self._make_one(project="project-id")
        with mock.patch.object(client._gapic_client, "ping_and_warm") as ping_mock:
            results = client._ping_and_warm_instances()
            ping_mock.assert_not_called()
            assert results == []
            # # test with instances
            client._active_instances = [
                "instance-1",
                "instance-2",
                "instance-3",
                "instance-4",
            ]
            results = client._ping_and_warm_instances()
            assert ping_mock.call_count == 4
            warmed_instances = [call.kwargs["name"] for call in ping_mock.call_args_list]
            for instance in ["instance-1", "instance-2", "instance-3", "instance-4"]:
                assert instance in warmed_instances
            assert results == [None, None, None, None]
        client.close()

    def test__register_instance(self):
        with self._make_one(project="project-id") as client:
            with mock.patch.object(client, "_ping_and_warm_instances") as ping_mock:
                # client should start with no instances
                assert client._active_instances == set()
                assert ping_mock.call_count == 0
                # registering instances should update _active_instances
                owner1 = object()
                client._register_instance("instance-1", owner1)
                assert ping_mock.call_count == 1
                assert len(client._active_instances) == 1
                assert client._active_instances == {"projects/project-id/instances/instance-1"}
                assert client._instance_owners["projects/project-id/instances/instance-1"] == {id(owner1)}
                owner2 = object()
                client._register_instance("instance-2", owner2)
                assert ping_mock.call_count == 2
                assert len(client._active_instances) == 2
                assert client._active_instances == {
                    "projects/project-id/instances/instance-1",
                    "projects/project-id/instances/instance-2",
                }
                assert client._instance_owners["projects/project-id/instances/instance-1"] == {id(owner1)}
                assert client._instance_owners["projects/project-id/instances/instance-2"] == {id(owner2)}

    def test__remove_instance_registration(self):
        client = self._make_one(project="project-id")
        table = mock.Mock()
        client._register_instance("instance-1", table)
        client._register_instance("instance-2", table)
        assert len(client._active_instances) == 2
        assert len(client._instance_owners.keys()) == 2
        instance_1_path = client._gapic_client.instance_path(
            client.project, "instance-1"
        )
        instance_2_path = client._gapic_client.instance_path(
            client.project, "instance-2"
        )
        assert len(client._instance_owners[instance_1_path]) == 1
        assert list(client._instance_owners[instance_1_path])[0] == id(table)
        assert len(client._instance_owners[instance_2_path]) == 1
        assert list(client._instance_owners[instance_2_path])[0] == id(table)
        success = client._remove_instance_registration("instance-1", table)
        assert success
        assert len(client._active_instances) == 1
        assert len(client._instance_owners[instance_1_path]) == 0
        assert len(client._instance_owners[instance_2_path]) == 1
        assert client._active_instances == {"projects/project-id/instances/instance-2"}
        success = client._remove_instance_registration("nonexistant", table)
        assert not success
        assert len(client._active_instances) == 1
        client.close()

    def test__multiple_table_registration(self):
        with self._make_one(project="project-id") as client:
            with client.get_table("instance_1", "table_1") as table_1:
                instance_1_path = client._gapic_client.instance_path(
                    client.project, "instance_1"
                )
                assert len(client._instance_owners[instance_1_path]) == 1
                assert len(client._active_instances) == 1
                assert id(table_1) in client._instance_owners[instance_1_path]
                with client.get_table("instance_1", "table_2") as table_2:
                    assert len(client._instance_owners[instance_1_path]) == 2
                    assert len(client._active_instances) == 1
                    assert id(table_1) in client._instance_owners[instance_1_path]
                    assert id(table_2) in client._instance_owners[instance_1_path]
                # table_2 should be unregistered, but instance should still be active
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert id(table_2) not in client._instance_owners[instance_1_path]
            # both tables are gone. instance should be unregistered
            assert len(client._active_instances) == 0
            assert instance_1_path not in client._active_instances
            assert len(client._instance_owners[instance_1_path]) == 0

    def test__multiple_instance_registration(self):
        with self._make_one(project="project-id") as client:
            with client.get_table("instance_1", "table_1") as table_1:
                with client.get_table("instance_2", "table_2") as table_2:
                    instance_1_path = client._gapic_client.instance_path(
                        client.project, "instance_1"
                    )
                    instance_2_path = client._gapic_client.instance_path(
                        client.project, "instance_2"
                    )
                    assert len(client._instance_owners[instance_1_path]) == 1
                    assert len(client._instance_owners[instance_2_path]) == 1
                    assert len(client._active_instances) == 2
                    assert id(table_1) in client._instance_owners[instance_1_path]
                    assert id(table_2) in client._instance_owners[instance_2_path]
                # instance2 should be unregistered, but instance1 should still be active
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert len(client._instance_owners[instance_2_path]) == 0
                assert len(client._instance_owners[instance_1_path]) == 1
                assert id(table_1) in client._instance_owners[instance_1_path]
            # both tables are gone. instances should both be unregistered
            assert len(client._active_instances) == 0
            assert len(client._instance_owners[instance_1_path]) == 0
            assert len(client._instance_owners[instance_2_path]) == 0

    def test_get_table(self):
        from google.cloud.bigtable._sync_customizations import Table_Sync_Concrete as Table

        client = self._make_one(project="project-id")
        assert not client._active_instances
        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        table = client.get_table(
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
        )
        assert isinstance(table, Table)
        assert table.table_id == expected_table_id
        assert (
            table.table_name
            == f"projects/{client.project}/instances/{expected_instance_id}/tables/{expected_table_id}"
        )
        assert table.instance_id == expected_instance_id
        assert (
            table.instance_name
            == f"projects/{client.project}/instances/{expected_instance_id}"
        )
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        assert table.instance_name in client._active_instances
        client.close()

    def test_get_table_context_manager(self):
        from google.cloud.bigtable._sync_customizations import Table_Sync_Concrete as Table

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_project_id = "project-id"

        with mock.patch.object(Table, "close") as close_mock:
            with self._make_one(project=expected_project_id) as client:
                with client.get_table(
                    expected_instance_id,
                    expected_table_id,
                    expected_app_profile_id,
                ) as table:
                    assert isinstance(table, Table)
                    assert table.table_id == expected_table_id
                    assert (
                        table.table_name
                        == f"projects/{expected_project_id}/instances/{expected_instance_id}/tables/{expected_table_id}"
                    )
                    assert table.instance_id == expected_instance_id
                    assert (
                        table.instance_name
                        == f"projects/{expected_project_id}/instances/{expected_instance_id}"
                    )
                    assert table.app_profile_id == expected_app_profile_id
                    assert table.client is client
                    assert table.instance_name in client._active_instances
            assert close_mock.call_count == 1

    def test_close(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.grpc import (
            BigtableGrpcTransport
        )

        client = self._make_one(project="project-id")
        with mock.patch.object(BigtableGrpcTransport, "close", mock.Mock()) as close_mock:
            client.close()
            close_mock.assert_called_once()
        assert client._channel_refresh_tasks == []

    def test_context_manager(self):
        # context manager should close the client cleanly
        close_mock = mock.Mock()
        true_close = None
        with self._make_one(project="project-id") as client:
            true_close = client.close
            client.close = close_mock
            for task in client._channel_refresh_tasks:
                assert not task.done()
            assert client.project == "project-id"
            assert client._active_instances == set()
            close_mock.assert_not_called()
        close_mock.assert_called_once()
        # actually close the client
        true_close()


class TestTable_Sync_Concrete:

    def test_table_ctor(self):
        from google.cloud.bigtable._sync_customizations import Table_Sync_Concrete as Table
        from google.cloud.bigtable._sync_customizations import BigtableDataClient_Sync_Concrete as Client

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_operation_timeout = 123
        expected_per_request_timeout = 12
        client = Client()
        assert not client._active_instances

        table = Table(
            client,
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
            default_operation_timeout=expected_operation_timeout,
            default_per_request_timeout=expected_per_request_timeout,
        )
        assert table.table_id == expected_table_id
        assert table.instance_id == expected_instance_id
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        assert table.instance_name in client._active_instances
        assert table.default_operation_timeout == expected_operation_timeout
        assert table.default_per_request_timeout == expected_per_request_timeout
        client.close()

    def test_table_ctor_bad_timeout_values(self):
        from google.cloud.bigtable._sync_customizations import Table_Sync_Concrete as Table
        from google.cloud.bigtable._sync_customizations import BigtableDataClient_Sync_Concrete as Client

        client = Client()

        with pytest.raises(ValueError) as e:
            Table(client, "", "", default_per_request_timeout=-1)
        assert "default_per_request_timeout must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            Table(client, "", "", default_operation_timeout=-1)
        assert "default_operation_timeout must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            Table(
                client,
                "",
                "",
                default_operation_timeout=1,
                default_per_request_timeout=2,
            )
        assert (
            "default_per_request_timeout must be less than default_operation_timeout"
            in str(e.value)
        )
        client.close()


class TestReadRows_Sync:
    """
    Tests for table.read_rows and related methods.
    """

    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable._sync_customizations import BigtableDataClient_Sync_Concrete

        return BigtableDataClient_Sync_Concrete(*args, **kwargs)

    def _make_stats(self):
        from google.cloud.bigtable_v2.types import RequestStats
        from google.cloud.bigtable_v2.types import FullReadStatsView
        from google.cloud.bigtable_v2.types import ReadIterationStats

        return RequestStats(
            full_read_stats_view=FullReadStatsView(
                read_iteration_stats=ReadIterationStats(
                    rows_seen_count=1,
                    rows_returned_count=2,
                    cells_seen_count=3,
                    cells_returned_count=4,
                )
            )
        )

    def _make_chunk(self, *args, **kwargs):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        kwargs["row_key"] = kwargs.get("row_key", b"row_key")
        kwargs["family_name"] = kwargs.get("family_name", "family_name")
        kwargs["qualifier"] = kwargs.get("qualifier", b"qualifier")
        kwargs["value"] = kwargs.get("value", b"value")
        kwargs["commit_row"] = kwargs.get("commit_row", True)

        return ReadRowsResponse.CellChunk(*args, **kwargs)

    def _make_gapic_stream(
        self,
        chunk_list: list["ReadRowsResponse.CellChunk" | Exception],
        request_stats: "RequestStats" | None = None,
        sleep_time=0,
    ):
        from google.cloud.bigtable_v2 import ReadRowsResponse
        import time

        for chunk in chunk_list:
            if sleep_time:
                time.sleep(sleep_time)
            if isinstance(chunk, Exception):
                raise chunk
            else:
                yield ReadRowsResponse(chunks=[chunk])
        if request_stats:
            yield ReadRowsResponse(request_stats=request_stats)


    def test_read_rows(self):
        with self._make_client() as client:
            table = client.get_table("instance", "table")
            chunks = [
                self._make_chunk(row_key=b"test_1"),
                self._make_chunk(row_key=b"test_2"),
            ]
            with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks
                )
                results = table.read_rows({}, operation_timeout=3)
                assert len(results) == 2
                assert results[0].row_key == b"test_1"
                assert results[1].row_key == b"test_2"

    def test_read_rows_stream(self):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [
                self._make_chunk(row_key=b"test_1"),
                self._make_chunk(row_key=b"test_2"),
            ]
            with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks
                )
                gen = table.read_rows_stream(query, operation_timeout=3)
                results = [row for row in gen]
                assert len(results) == 2
                assert results[0].row_key == b"test_1"
                assert results[1].row_key == b"test_2"

    @pytest.mark.parametrize("include_app_profile", [True, False])
    def test_read_rows_query_matches_request(self, include_app_profile):
        from google.cloud.bigtable import RowRange
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery

        with self._make_client() as client:
            app_profile_id = "app_profile_id" if include_app_profile else None
            table = client.get_table("instance", "table", app_profile_id=app_profile_id)
            row_keys = [b"test_1", "test_2"]
            row_ranges = RowRange("start", "end")
            filter_ = {"test": "filter"}
            limit = 99
            query = ReadRowsQuery(
                row_keys=row_keys,
                row_ranges=row_ranges,
                row_filter=filter_,
                limit=limit,
            )
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    []
                )
                results = table.read_rows(query, operation_timeout=3)
                assert len(results) == 0
                call_request = read_rows.call_args_list[0][0][0]
                query_dict = query._to_dict()
                if include_app_profile:
                    assert set(call_request.keys()) == set(query_dict.keys()) | {
                        "table_name",
                        "app_profile_id",
                    }
                else:
                    assert set(call_request.keys()) == set(query_dict.keys()) | {
                        "table_name"
                    }
                assert call_request["rows"] == query_dict["rows"]
                assert call_request["filter"] == filter_
                assert call_request["rows_limit"] == limit
                assert call_request["table_name"] == table.table_name
                if include_app_profile:
                    assert call_request["app_profile_id"] == app_profile_id

    @pytest.mark.parametrize("operation_timeout", [0.001, 0.023, 0.1])
    def test_read_rows_timeout(self, operation_timeout):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks, sleep_time=1
                )
                try:
                    table.read_rows(query, operation_timeout=operation_timeout)
                except core_exceptions.DeadlineExceeded as e:
                    assert (
                        e.message
                        == f"operation_timeout of {operation_timeout:0.1f}s exceeded"
                    )

    @pytest.mark.parametrize(
        "per_request_t, operation_t, expected_num",
        [
            (0.05, 0.08, 2),
            (0.05, 0.54, 11),
            (0.05, 0.14, 3),
            (0.05, 0.24, 5),
        ],
    )
    def test_read_rows_per_request_timeout(
        self, per_request_t, operation_t, expected_num
    ):
        """
        Ensures that the per_request_timeout is respected and that the number of
        requests is as expected.

        operation_timeout does not cancel the request, so we expect the number of
        requests to be the ceiling of operation_timeout / per_request_timeout.
        """
        from google.cloud.bigtable.exceptions import RetryExceptionGroup
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery

        # mocking uniform ensures there are no sleeps between retries
        with mock.patch("random.uniform", side_effect=lambda a, b: 0):
            with self._make_client() as client:
                table = client.get_table("instance", "table")
                query = ReadRowsQuery()
                chunks = [core_exceptions.DeadlineExceeded("mock deadline")]
                with mock.patch.object(
                    table.client._gapic_client, "read_rows"
                ) as read_rows:
                    read_rows.side_effect = (
                        lambda *args, **kwargs: self._make_gapic_stream(
                            chunks, sleep_time=per_request_t
                        )
                    )
                    try:
                        table.read_rows(
                            query,
                            operation_timeout=operation_t,
                            per_request_timeout=per_request_t,
                        )
                    except core_exceptions.DeadlineExceeded as e:
                        retry_exc = e.__cause__
                        if expected_num == 0:
                            assert retry_exc is None
                        else:
                            assert type(retry_exc) == RetryExceptionGroup
                            assert f"{expected_num} failed attempts" in str(retry_exc)
                            assert len(retry_exc.exceptions) == expected_num
                            for sub_exc in retry_exc.exceptions:
                                assert sub_exc.message == "mock deadline"
                    assert read_rows.call_count == expected_num
                    called_kwargs = read_rows.call_args[1]
                    assert called_kwargs["timeout"] == per_request_t

    @pytest.mark.parametrize(
        "exc_type",
        [
            core_exceptions.Aborted,
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    def test_read_rows_retryable_error(self, exc_type):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            expected_error = exc_type("mock error")
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    [expected_error]
                )
                try:
                    table.read_rows(query, operation_timeout=0.1)
                except core_exceptions.DeadlineExceeded as e:
                    retry_exc = e.__cause__
                    root_cause = retry_exc.exceptions[0]
                    assert type(root_cause) == exc_type
                    assert root_cause == expected_error

    @pytest.mark.parametrize(
        "exc_type",
        [
            core_exceptions.Cancelled,
            core_exceptions.PreconditionFailed,
            core_exceptions.NotFound,
            core_exceptions.PermissionDenied,
            core_exceptions.Conflict,
            core_exceptions.InternalServerError,
            core_exceptions.TooManyRequests,
            core_exceptions.ResourceExhausted,
            InvalidChunk,
        ],
    )
    def test_read_rows_non_retryable_error(self, exc_type):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            expected_error = exc_type("mock error")
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    [expected_error]
                )
                try:
                    table.read_rows(query, operation_timeout=0.1)
                except exc_type as e:
                    assert e == expected_error

    def test_read_rows_request_stats(self):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            stats = self._make_stats()
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks, request_stats=stats
                )
                gen = table.read_rows_stream(query)
                [row for row in gen]
                assert gen.request_stats == stats

    def test_read_rows_request_stats_missing(self):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        with self._make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [self._make_chunk(row_key=b"test_1")]
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: self._make_gapic_stream(
                    chunks, request_stats=None
                )
                gen = table.read_rows_stream(query)
                [row for row in gen]
                assert gen.request_stats is None

    def test_read_rows_revise_request(self):
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        from google.cloud.bigtable._sync_customizations import _ReadRowsOperation_Sync_Concrete as _ReadRowsOperation

        with mock.patch.object(
            _ReadRowsOperation, "_revise_request_rowset"
        ) as revise_rowset:
            with mock.patch.object(_ReadRowsOperation, "close"):
                revise_rowset.side_effect = [
                    "modified",
                    core_exceptions.Cancelled("mock error"),
                ]
                with self._make_client() as client:
                    table = client.get_table("instance", "table")
                    row_keys = [b"test_1", b"test_2", b"test_3"]
                    query = ReadRowsQuery(row_keys=row_keys)
                    chunks = [
                        self._make_chunk(row_key=b"test_1"),
                        core_exceptions.Aborted("mock retryable error"),
                    ]
                    with mock.patch.object(
                        table.client._gapic_client, "read_rows"
                    ) as read_rows:
                        read_rows.side_effect = (
                            lambda *args, **kwargs: self._make_gapic_stream(
                                chunks, request_stats=None
                            )
                        )
                        try:
                            table.read_rows(query)
                        except core_exceptions.Cancelled:
                            revise_rowset.assert_called()
                            first_call_kwargs = revise_rowset.call_args_list[0].kwargs
                            assert (
                                first_call_kwargs["row_set"] == query._to_dict()["rows"]
                            )
                            assert first_call_kwargs["last_seen_row_key"] == b"test_1"
                            second_call_kwargs = revise_rowset.call_args_list[1].kwargs
                            assert second_call_kwargs["row_set"] == "modified"
                            assert second_call_kwargs["last_seen_row_key"] == b"test_1"

    def test_read_rows_default_timeouts(self):
        """
        Ensure that the default timeouts are set on the read rows operation when not overridden
        """
        from google.cloud.bigtable._sync_customizations import _ReadRowsOperation_Sync_Concrete as _ReadRowsOperation
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        operation_timeout = 8
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            with self._make_client() as client:
                with client.get_table(
                    "instance",
                    "table",
                    default_operation_timeout=operation_timeout,
                    default_per_request_timeout=per_request_timeout,
                ) as table:
                    try:
                        table.read_rows(ReadRowsQuery())
                    except RuntimeError:
                        pass
                    kwargs = mock_op.call_args_list[0].kwargs
                    assert kwargs["operation_timeout"] == operation_timeout
                    assert kwargs["per_request_timeout"] == per_request_timeout

    def test_read_rows_default_timeout_override(self):
        """
        When timeouts are passed, they overwrite default values
        """
        from google.cloud.bigtable._sync_customizations import _ReadRowsOperation_Sync_Concrete as _ReadRowsOperation
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        operation_timeout = 8
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation, "__init__") as mock_op:
            mock_op.side_effect = RuntimeError("mock error")
            with self._make_client() as client:
                with client.get_table(
                    "instance",
                    "table",
                    default_operation_timeout=99,
                    default_per_request_timeout=97,
                ) as table:
                    try:
                        table.read_rows(
                            ReadRowsQuery(),
                            operation_timeout=operation_timeout,
                            per_request_timeout=per_request_timeout,
                        )
                    except RuntimeError:
                        pass
                    kwargs = mock_op.call_args_list[0].kwargs
                    assert kwargs["operation_timeout"] == operation_timeout
                    assert kwargs["per_request_timeout"] == per_request_timeout


from google.cloud.bigtable._sync_customizations import _ReadRowsOperation_Sync_Concrete
class MockStream(_ReadRowsOperation_Sync_Concrete):
    """
    Mock a _ReadRowsOperation stream for testing
    """

    def __init__(self, items=None, errors=None, operation_timeout=None):
        self.transient_errors = errors
        self.operation_timeout = operation_timeout
        self.next_idx = 0
        if items is None:
            items = list(range(10))
        self.items = items

    def __iter__(self):
        return self

    def __next__(self):
        if self.next_idx >= len(self.items):
            raise StopIteration
        item = self.items[self.next_idx]
        self.next_idx += 1
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        pass


class TestReadRowsIteratorSyncConcrete:
    def mock_stream(self, size=10):
        for i in range(size):
            yield i

    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable._sync_customizations import ReadRowsIterator_Sync_Concrete

        stream = MockStream(*args, **kwargs)
        return ReadRowsIterator_Sync_Concrete(stream)

    def test_ctor(self):
        with mock.patch("time.time", return_value=0):
            iterator = self._make_one()
            assert iterator.last_interaction_time == 0
            assert iterator._idle_timeout_task is None
            assert iterator.request_stats is None
            assert iterator.active is True

    def test___iter__(self):
        iterator = self._make_one()
        assert iterator.__iter__() is iterator

    def test___next__(self):
        num_rows = 10
        iterator = self._make_one(items=list(range(num_rows)))
        for i in range(num_rows):
            assert iterator.__next__() == i
        with pytest.raises(StopIteration):
            iterator.__next__()

    def test___next__with_request_stats(self):
        """
        Request stats should not be yielded, but should be set on the iterator object
        """
        from google.cloud.bigtable_v2.types import RequestStats

        stats = RequestStats()
        items = [1, 2, stats, 3]
        iterator = self._make_one(items=items)
        assert iterator.__next__() == 1
        assert iterator.__next__() == 2
        assert iterator.request_stats is None
        assert iterator.__next__() == 3
        with pytest.raises(StopIteration):
            iterator.__next__()
        assert iterator.request_stats == stats

    def test___anext__with_deadline_error(self):
        """
        RetryErrors mean a deadline has been hit.
        Should be wrapped in a DeadlineExceeded exception
        """
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        iterator = self._make_one(items=items, operation_timeout=expected_timeout)
        assert  iterator.__next__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            iterator.__next__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        assert exc.value.__cause__ is None

    def test___next__with_deadline_error_with_cause(self):
        """
        Transient errors should be exposed as an error group
        """
        from google.api_core import exceptions as core_exceptions
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        items = [1, core_exceptions.RetryError("retry error", None)]
        expected_timeout = 99
        errors = [RuntimeError("error1"), ValueError("error2")]
        iterator = self._make_one(
            items=items, operation_timeout=expected_timeout, errors=errors
        )
        assert iterator.__next__() == 1
        with pytest.raises(core_exceptions.DeadlineExceeded) as exc:
            iterator.__next__()
        assert f"operation_timeout of {expected_timeout:0.1f}s exceeded" in str(
            exc.value
        )
        error_group = exc.value.__cause__
        assert isinstance(error_group, RetryExceptionGroup)
        assert len(error_group.exceptions) == 2
        assert error_group.exceptions[0] is errors[0]
        assert error_group.exceptions[1] is errors[1]
        assert "2 failed attempts" in str(error_group)

    def test___next__with_error(self):
        """
        Other errors should be raised as-is
        """
        from google.api_core import exceptions as core_exceptions

        items = [1, core_exceptions.InternalServerError("mock error")]
        iterator = self._make_one(items=items)
        assert iterator.__next__() == 1
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            iterator.__next__()
        assert exc.value is items[1]
        assert iterator.active is False
        # next call should raise same error
        with pytest.raises(core_exceptions.InternalServerError) as exc:
            iterator.__next__()

    def test__finish_with_error(self):
        iterator = self._make_one()
        iterator._start_idle_timer(10)
        assert iterator.__next__() == 0
        assert iterator.active is True
        err = ZeroDivisionError("mock error")
        iterator._finish_with_error(err)
        assert iterator.active is False
        assert iterator._error is err
        with pytest.raises(ZeroDivisionError) as exc:
            iterator.__next__()
            assert exc.value is err

    def test_close(self):
        iterator = self._make_one()
        iterator._start_idle_timer(10)
        assert iterator.__next__() == 0
        assert iterator.active is True
        iterator.close()
        assert iterator.active is False
        assert isinstance(iterator._error, StopIteration)
        assert iterator._idle_timeout_task is None
        with pytest.raises(StopIteration) as e:
            iterator.__next__()
            assert "closed" in str(e.value)
