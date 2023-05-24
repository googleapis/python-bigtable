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
import re
import time

from google.api_core import exceptions as core_exceptions
from google.auth.credentials import AnonymousCredentials
from google.cloud.bigtable.exceptions import InvalidChunk
from google.cloud.bigtable.read_rows_query import ReadRowsQuery
from google.cloud.bigtable_v2 import ReadRowsResponse
from google.cloud.bigtable_v2.services.bigtable.client import BigtableClient
from google.cloud.bigtable_v2.services.bigtable.transports.grpc import (
    BigtableGrpcTransport,
)
from google.cloud.bigtable_v2.types import ReadRowsResponse
import google.cloud.bigtable
from google.cloud.bigtable._sync._autogen import BigtableDataClient_Sync
from google.cloud.bigtable._sync._autogen import Table_Sync
from google.cloud.bigtable._sync._autogen import _ReadRowsOperation_Sync
from google.cloud.bigtable._sync._autogen import ReadRowsIterator_Sync

try:
    from unittest import mock
except ImportError:
    import mock
VENEER_HEADER_REGEX = re.compile(
    "gapic\\/[0-9]+\\.[\\w.-]+ gax\\/[0-9]+\\.[\\w.-]+ gccl\\/[0-9]+\\.[\\w.-]+ gl-python\\/[0-9]+\\.[\\w.-]+ grpc\\/[0-9]+\\.[\\w.-]+"
)


class TestBigtableDataClient:
    def _get_target_class(self):
        from google.cloud.bigtable._sync._concrete import (
            BigtableDataClient_Sync_Concrete,
        )

        return BigtableDataClient_Sync_Concrete

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        expected_project = "project-id"
        expected_credentials = AnonymousCredentials()
        client = self._make_one(
            project="project-id",
            credentials=expected_credentials,
        )
        time.sleep(0.1)
        assert client.project == expected_project
        assert not client._active_instances
        assert client.transport._credentials == expected_credentials
        client.close()

    def test_ctor_super_inits(self):
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
                assert bigtable_client_init.call_count == 1
                kwargs = bigtable_client_init.call_args[1]
                assert kwargs["transport"] == transport_str
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed
                assert client_project_init.call_count == 1
                kwargs = client_project_init.call_args[1]
                assert kwargs["project"] == project
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed

    def test_ctor_dict_options(self):
        from google.api_core.client_options import ClientOptions

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
            BigtableDataClient_Sync, "start_background_channel_refresh"
        ) as start_background_refresh:
            client = self._make_one(client_options=client_options)
            start_background_refresh.assert_called_once()
            client.close()

    def test_veneer_grpc_headers(self):
        patch = mock.patch("google.api_core.gapic_v1.method.wrap_method")
        with patch as gapic_mock:
            client = self._make_one(project="project-id")
            wrapped_call_list = gapic_mock.call_args_list
            assert len(wrapped_call_list) > 0
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
            warmed_instances = [
                call.kwargs["name"] for call in ping_mock.call_args_list
            ]
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
                assert client._active_instances == {
                    "projects/project-id/instances/instance-1"
                }
                assert client._instance_owners[
                    "projects/project-id/instances/instance-1"
                ] == {id(owner1)}
                owner2 = object()
                client._register_instance("instance-2", owner2)
                assert ping_mock.call_count == 2
                assert len(client._active_instances) == 2
                assert client._active_instances == {
                    "projects/project-id/instances/instance-1",
                    "projects/project-id/instances/instance-2",
                }
                assert client._instance_owners[
                    "projects/project-id/instances/instance-1"
                ] == {id(owner1)}
                assert client._instance_owners[
                    "projects/project-id/instances/instance-2"
                ] == {id(owner2)}

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
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert id(table_2) not in client._instance_owners[instance_1_path]
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
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert len(client._instance_owners[instance_2_path]) == 0
                assert len(client._instance_owners[instance_1_path]) == 1
                assert id(table_1) in client._instance_owners[instance_1_path]
            assert len(client._active_instances) == 0
            assert len(client._instance_owners[instance_1_path]) == 0
            assert len(client._instance_owners[instance_2_path]) == 0

    def test_get_table(self):
        client = self._make_one(project="project-id")
        assert not client._active_instances
        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        table = client.get_table(
            expected_instance_id, expected_table_id, expected_app_profile_id
        )
        time.sleep(0)
        assert isinstance(table, Table_Sync)
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
        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_project_id = "project-id"
        with mock.patch.object(Table_Sync, "close") as close_mock:
            with self._make_one(project=expected_project_id) as client:
                with client.get_table(
                    expected_instance_id, expected_table_id, expected_app_profile_id
                ) as table:
                    time.sleep(0)
                    assert isinstance(table, Table_Sync)
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
        client = self._make_one(project="project-id")
        tasks_list = list(client._channel_refresh_tasks)
        for task in client._channel_refresh_tasks:
            assert not task.done()
        with mock.patch.object(BigtableGrpcTransport, "close") as close_mock:
            client.close()
            close_mock.assert_called_once()
        for task in tasks_list:
            assert task.done()
            assert task.cancelled()
        assert client._channel_refresh_tasks == []

    def test_context_manager(self):
        close_mock = mock.Mock()
        with self._make_one(project="project-id") as client:
            client.close = close_mock
            for task in client._channel_refresh_tasks:
                assert not task.done()
            assert client.project == "project-id"
            assert client._active_instances == set()
            close_mock.assert_not_called()
        close_mock.assert_called_once()


class TestTable:
    def test_table_ctor(self):
        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_operation_timeout = 123
        expected_per_request_timeout = 12
        client = (
            google.cloud.bigtable._sync._concrete.BigtableDataClient_Sync_Concrete()
        )
        assert not client._active_instances
        table = google.cloud.bigtable._sync._concrete.Table_Sync_Concrete(
            client,
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
            default_operation_timeout=expected_operation_timeout,
            default_per_request_timeout=expected_per_request_timeout,
        )
        time.sleep(0)
        assert table.table_id == expected_table_id
        assert table.instance_id == expected_instance_id
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        assert table.instance_name in client._active_instances
        assert table.default_operation_timeout == expected_operation_timeout
        assert table.default_per_request_timeout == expected_per_request_timeout
        client.close()

    def test_table_ctor_bad_timeout_values(self):
        client = (
            google.cloud.bigtable._sync._concrete.BigtableDataClient_Sync_Concrete()
        )
        with pytest.raises(ValueError) as e:
            google.cloud.bigtable._sync._concrete.Table_Sync_Concrete(
                client, "", "", default_per_request_timeout=-1
            )
        assert "default_per_request_timeout must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            google.cloud.bigtable._sync._concrete.Table_Sync_Concrete(
                client, "", "", default_operation_timeout=-1
            )
        assert "default_operation_timeout must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            google.cloud.bigtable._sync._concrete.Table_Sync_Concrete(
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


class TestReadRows:
    """
    Tests for table.read_rows and related methods.
    """

    def _make_client(self, *args, **kwargs):
        return google.cloud.bigtable._sync._concrete.BigtableDataClient_Sync_Concrete(
            *args, **kwargs
        )

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
        self, chunk_list: list[ReadRowsResponse.CellChunk | Exception], sleep_time=0
    ):
        from google.cloud.bigtable_v2 import ReadRowsResponse

        class mock_stream:
            def __init__(self, chunk_list, sleep_time):
                self.chunk_list = chunk_list
                self.idx = -1
                self.sleep_time = sleep_time

            def __iter__(self):
                return self

            def __next__(self):
                self.idx += 1
                if len(self.chunk_list) > self.idx:
                    if sleep_time:
                        time.sleep(self.sleep_time)
                    chunk = self.chunk_list[self.idx]
                    if isinstance(chunk, Exception):
                        raise chunk
                    else:
                        return ReadRowsResponse(chunks=[chunk])
                raise StopIteration

            def cancel(self):
                pass

        return mock_stream(chunk_list, sleep_time)

    def test_read_rows(self):
        client = self._make_client()
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
            results = table.read_rows(query, operation_timeout=3)
            assert len(results) == 2
            assert results[0].row_key == b"test_1"
            assert results[1].row_key == b"test_2"
        client.close()

    def test_read_rows_stream(self):
        client = self._make_client()
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
        client.close()

    @pytest.mark.parametrize("include_app_profile", [True, False])
    def test_read_rows_query_matches_request(self, include_app_profile):
        from google.cloud.bigtable import RowRange

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
        [(0.05, 0.08, 2), (0.05, 0.54, 11), (0.05, 0.14, 3), (0.05, 0.24, 5)],
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

        expected_last_timeout = operation_t - (expected_num - 1) * per_request_t
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
                    for (_, call_kwargs) in read_rows.call_args_list[:-1]:
                        assert call_kwargs["timeout"] == per_request_t
                    assert (
                        abs(
                            read_rows.call_args_list[-1][1]["timeout"]
                            - expected_last_timeout
                        )
                        < 0.05
                    )

    @pytest.mark.parametrize(
        "exc_type",
        [
            core_exceptions.Aborted,
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    def test_read_rows_retryable_error(self, exc_type):
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

    def test_read_rows_revise_request(self):
        """Ensure that _revise_request is called between retries"""
        from google.cloud.bigtable.exceptions import InvalidChunk

        with mock.patch.object(
            _ReadRowsOperation_Sync, "_revise_request_rowset"
        ) as revise_rowset:
            with mock.patch.object(_ReadRowsOperation_Sync, "close"):
                revise_rowset.return_value = "modified"
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
                            lambda *args, **kwargs: self._make_gapic_stream(chunks)
                        )
                        try:
                            table.read_rows(query)
                        except InvalidChunk:
                            revise_rowset.assert_called()
                            revise_call_kwargs = revise_rowset.call_args_list[0].kwargs
                            assert (
                                revise_call_kwargs["row_set"]
                                == query._to_dict()["rows"]
                            )
                            assert revise_call_kwargs["last_seen_row_key"] == b"test_1"
                            read_rows_request = read_rows.call_args_list[1].args[0]
                            assert read_rows_request["rows"] == "modified"

    def test_read_rows_default_timeouts(self):
        """Ensure that the default timeouts are set on the read rows operation when not overridden"""
        operation_timeout = 8
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation_Sync, "__init__") as mock_op:
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
        """When timeouts are passed, they overwrite default values"""
        operation_timeout = 8
        per_request_timeout = 4
        with mock.patch.object(_ReadRowsOperation_Sync, "__init__") as mock_op:
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
