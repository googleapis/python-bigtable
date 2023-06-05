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

import pytest

from google.cloud.bigtable_v2.types import MutateRowsResponse
from google.rpc import status_pb2
import google.api_core.exceptions as core_exceptions

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


class TestMutateRowsOperation:

    def _target_class(self):
        from google.cloud.bigtable._mutate_rows import _MutateRowsOperation

        return _MutateRowsOperation

    def _make_one(self, *args, **kwargs):
        if not args:
            kwargs["gapic_client"] = kwargs.pop("gapic_client", mock.Mock())
            kwargs["table"] = kwargs.pop("table", AsyncMock())
            kwargs["mutation_entries"] = kwargs.pop("mutation_entries", [])
            kwargs["operation_timeout"] = kwargs.pop("operation_timeout", 5)
            kwargs["per_request_timeout"] = kwargs.pop("per_request_timeout", 0.1)
        return self._target_class()(*args, **kwargs)

    async def _mock_stream(self, mutation_list, error_dict):
        for idx, entry in enumerate(mutation_list):
            code = error_dict.get(idx, 0)
            yield MutateRowsResponse(
                entries=[
                    MutateRowsResponse.Entry(
                        index=idx, status=status_pb2.Status(code=code)
                    )
                ]
            )

    def _make_mock_gapic(self, mutation_list, error_dict=None):
        mock_fn = AsyncMock()
        if error_dict is None:
            error_dict = {}
        mock_fn.side_effect = lambda *args, **kwargs: self._mock_stream(
            mutation_list, error_dict
        )
        return mock_fn

    def test_ctor(self):
        """
        test that constructor sets all the attributes correctly
        """
        from google.cloud.bigtable._mutate_rows import _MutateRowsIncomplete
        from google.api_core.exceptions import DeadlineExceeded
        from google.api_core.exceptions import ServiceUnavailable

        client = mock.Mock()
        table = mock.Mock()
        entries = [mock.Mock(), mock.Mock()]
        operation_timeout = 0.05
        attempt_timeout = 0.01
        instance = self._make_one(
            client, table, entries, operation_timeout, attempt_timeout
        )
        # running gapic_fn should trigger a client call
        assert client.mutate_rows.call_count == 0
        instance._gapic_fn()
        assert client.mutate_rows.call_count == 1
        # gapic_fn should call with table details
        inner_kwargs = client.mutate_rows.call_args[1]
        assert len(inner_kwargs) == 3
        assert inner_kwargs["table_name"] == table.table_name
        assert inner_kwargs["app_profile_id"] == table.app_profile_id
        metadata = inner_kwargs["metadata"]
        assert len(metadata) == 1
        assert metadata[0][0] == "x-goog-request-params"
        assert str(table.table_name) in metadata[0][1]
        assert str(table.app_profile_id) in metadata[0][1]
        # entries should be passed down
        assert instance.mutations == entries
        # timeout_gen should generate per-attempt timeout
        assert next(instance.timeout_generator) == attempt_timeout
        # ensure predicate is set
        assert instance.is_retryable is not None
        assert instance.is_retryable(DeadlineExceeded("")) is True
        assert instance.is_retryable(ServiceUnavailable("")) is True
        assert instance.is_retryable(_MutateRowsIncomplete("")) is True
        assert instance.is_retryable(RuntimeError("")) is False
        assert instance.remaining_indices == list(range(len(entries)))
        assert instance.errors == {}

    @pytest.mark.asyncio
    async def test_mutate_rows_operation(self):
        """
        Test successful case of mutate_rows_operation
        """
        client = mock.Mock()
        table = mock.Mock()
        entries = [mock.Mock(), mock.Mock()]
        operation_timeout = 0.05
        instance = self._make_one(
            client, table, entries, operation_timeout, operation_timeout
        )
        with mock.patch.object(instance, "_operation", AsyncMock()) as attempt_mock:
            attempt_mock.return_value = None
            await instance.start()
            assert attempt_mock.call_count == 1

    @pytest.mark.parametrize(
        "exc_type", [RuntimeError, ZeroDivisionError, core_exceptions.Forbidden]
    )
    @pytest.mark.asyncio
    async def test_mutate_rows_exception(self, exc_type):
        """
        exceptions raised from retryable should be raised in MutationsExceptionGroup
        """
        from google.cloud.bigtable.exceptions import MutationsExceptionGroup
        from google.cloud.bigtable.exceptions import FailedMutationEntryError

        client = mock.Mock()
        table = mock.Mock()
        entries = [mock.Mock()]
        operation_timeout = 0.05
        expected_cause = exc_type("abort")
        with mock.patch.object(
            self._target_class(),
            "_run_attempt",
            AsyncMock(),
        ) as attempt_mock:
            attempt_mock.side_effect = expected_cause
            found_exc = None
            try:
                instance = self._make_one(
                    client, table, entries, operation_timeout, operation_timeout
                )
                await instance.start()
            except MutationsExceptionGroup as e:
                found_exc = e
            assert attempt_mock.call_count == 1
            assert len(found_exc.exceptions) == 1
            assert isinstance(found_exc.exceptions[0], FailedMutationEntryError)
            assert found_exc.exceptions[0].__cause__ == expected_cause

    @pytest.mark.parametrize(
        "exc_type",
        [core_exceptions.DeadlineExceeded, core_exceptions.ServiceUnavailable],
    )
    @pytest.mark.asyncio
    async def test_mutate_rows_exception_retryable_eventually_pass(self, exc_type):
        """
        If an exception fails but eventually passes, it should not raise an exception
        """
        from google.cloud.bigtable._mutate_rows import _MutateRowsOperation
        client = mock.Mock()
        table = mock.Mock()
        entries = [mock.Mock()]
        operation_timeout = 1
        expected_cause = exc_type("retry")
        num_retries = 2
        with mock.patch.object(
            _MutateRowsOperation,
            "_run_attempt",
            AsyncMock(),
        ) as attempt_mock:
            attempt_mock.side_effect = [expected_cause] * num_retries + [None]
            instance = self._make_one(
                client, table, entries, operation_timeout, operation_timeout
            )
            await instance.start()
            assert attempt_mock.call_count == num_retries + 1

    @pytest.mark.asyncio
    async def test_mutate_rows_incomplete_ignored(self):
        """
        MutateRowsIncomplete exceptions should not be added to error list
        """
        from google.cloud.bigtable._mutate_rows import _MutateRowsIncomplete
        from google.cloud.bigtable.exceptions import MutationsExceptionGroup
        from google.api_core.exceptions import DeadlineExceeded

        client = mock.Mock()
        table = mock.Mock()
        entries = [mock.Mock()]
        operation_timeout = 0.05
        with mock.patch.object(
            self._target_class(),
            "_run_attempt",
            AsyncMock(),
        ) as attempt_mock:
            attempt_mock.side_effect = _MutateRowsIncomplete("ignored")
            found_exc = None
            try:
                instance = self._make_one(
                    client, table, entries, operation_timeout, operation_timeout
                )
                await instance.start()
            except MutationsExceptionGroup as e:
                found_exc = e
            assert attempt_mock.call_count > 0
            assert len(found_exc.exceptions) == 1
            assert isinstance(found_exc.exceptions[0].__cause__, DeadlineExceeded)

    @pytest.mark.asyncio
    async def test_run_attempt_single_entry_success(self):
        """Test mutating a single entry"""
        mutation = mock.Mock()
        mutations = {0: mutation}
        expected_timeout = 1.3
        mock_gapic_fn = self._make_mock_gapic(mutations)
        instance = self._make_one(
            mutation_entries=mutations,
            per_request_timeout=expected_timeout,
        )
        with mock.patch.object(instance, "_gapic_fn", mock_gapic_fn):
            await instance._run_attempt()
        assert len(instance.remaining_indices) == 0
        assert mock_gapic_fn.call_count == 1
        _, kwargs = mock_gapic_fn.call_args
        assert kwargs["timeout"] == expected_timeout
        assert kwargs["entries"] == [mutation._to_dict()]

    @pytest.mark.asyncio
    async def test_run_attempt_empty_request(self):
        """Calling with no mutations should result in no API calls"""
        mock_gapic_fn = self._make_mock_gapic([])
        instance = self._make_one(
            mutation_entries=[],
        )
        await instance._run_attempt()
        assert mock_gapic_fn.call_count == 0

    @pytest.mark.asyncio
    async def test_run_attempt_partial_success_retryable(self):
        """Some entries succeed, but one fails. Should report the proper index, and raise incomplete exception"""
        from google.cloud.bigtable._mutate_rows import _MutateRowsIncomplete

        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = [success_mutation, failure_mutation, success_mutation_2]
        mock_gapic_fn = self._make_mock_gapic(mutations, error_dict={1: 300})
        instance = self._make_one(
            mutation_entries=mutations,
        )
        instance.is_retryable = lambda x: True
        with mock.patch.object(instance, "_gapic_fn", mock_gapic_fn):
            with pytest.raises(_MutateRowsIncomplete):
                await instance._run_attempt()
        assert instance.remaining_indices == [1]
        assert 0 not in instance.errors
        assert len(instance.errors[1]) == 1
        assert instance.errors[1][0].grpc_status_code == 300
        assert 2 not in instance.errors

    @pytest.mark.asyncio
    async def test_run_attempt_partial_success_non_retryable(self):
        """Some entries succeed, but one fails. Exception marked as non-retryable. Do not raise incomplete error"""
        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = [success_mutation, failure_mutation, success_mutation_2]
        mock_gapic_fn = self._make_mock_gapic(mutations, error_dict={1: 300})
        instance = self._make_one(
            mutation_entries=mutations,
        )
        instance.is_retryable = lambda x: False
        with mock.patch.object(instance, "_gapic_fn", mock_gapic_fn):
            await instance._run_attempt()
        assert instance.remaining_indices == []
        assert 0 not in instance.errors
        assert len(instance.errors[1]) == 1
        assert instance.errors[1][0].grpc_status_code == 300
        assert 2 not in instance.errors
