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


def _make_mutation(count=1, size=1):
    mutation = mock.Mock()
    mutation.size.return_value = size
    mutation.mutations = [mock.Mock()] * count
    return mutation


class TestMutateRowsOperation:
    def _target_class(self):
        from google.cloud.bigtable.data._async._mutate_rows import (
            _MutateRowsOperationAsync,
        )

        return _MutateRowsOperationAsync

    def _make_one(self, *args, **kwargs):
        if not args:
            kwargs["gapic_client"] = kwargs.pop("gapic_client", mock.Mock())
            kwargs["table"] = kwargs.pop("table", AsyncMock())
            kwargs["operation_timeout"] = kwargs.pop("operation_timeout", 5)
            kwargs["attempt_timeout"] = kwargs.pop("attempt_timeout", 0.1)
            kwargs["retryable_exceptions"] = kwargs.pop("retryable_exceptions", ())
            kwargs["mutation_entries"] = kwargs.pop("mutation_entries", [])
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
        from google.cloud.bigtable.data._async._mutate_rows import _EntryWithProto
        from google.cloud.bigtable.data.exceptions import _MutateRowsIncomplete
        from google.api_core.exceptions import DeadlineExceeded
        from google.api_core.exceptions import Aborted

        client = mock.Mock()
        table = mock.Mock()
        entries = [_make_mutation(), _make_mutation()]
        operation_timeout = 0.05
        attempt_timeout = 0.01
        retryable_exceptions = ()
        instance = self._make_one(
            client,
            table,
            entries,
            operation_timeout,
            attempt_timeout,
            retryable_exceptions,
        )
        # running gapic_fn should trigger a client call
        assert client.mutate_rows.call_count == 0
        instance._gapic_fn()
        assert client.mutate_rows.call_count == 1
        # gapic_fn should call with table details
        inner_kwargs = client.mutate_rows.call_args[1]
        assert len(inner_kwargs) == 4
        assert inner_kwargs["table_name"] == table.table_name
        assert inner_kwargs["app_profile_id"] == table.app_profile_id
        assert inner_kwargs["retry"] is None
        metadata = inner_kwargs["metadata"]
        assert len(metadata) == 1
        assert metadata[0][0] == "x-goog-request-params"
        assert str(table.table_name) in metadata[0][1]
        assert str(table.app_profile_id) in metadata[0][1]
        # entries should be passed down
        entries_w_pb = [_EntryWithProto(e, e._to_pb()) for e in entries]
        assert instance.mutations == entries_w_pb
        # timeout_gen should generate per-attempt timeout
        assert next(instance.timeout_generator) == attempt_timeout
        # ensure predicate is set
        assert instance.is_retryable is not None
        assert instance.is_retryable(DeadlineExceeded("")) is False
        assert instance.is_retryable(Aborted("")) is False
        assert instance.is_retryable(_MutateRowsIncomplete("")) is True
        assert instance.is_retryable(RuntimeError("")) is False
        assert instance.remaining_indices == list(range(len(entries)))
        assert instance.errors == {}

    def test_ctor_too_many_entries(self):
        """
        should raise an error if an operation is created with more than 100,000 entries
        """
        from google.cloud.bigtable.data._async._mutate_rows import (
            _MUTATE_ROWS_REQUEST_MUTATION_LIMIT,
        )

        assert _MUTATE_ROWS_REQUEST_MUTATION_LIMIT == 100_000

        client = mock.Mock()
        table = mock.Mock()
        entries = [_make_mutation()] * _MUTATE_ROWS_REQUEST_MUTATION_LIMIT
        operation_timeout = 0.05
        attempt_timeout = 0.01
        # no errors if at limit
        self._make_one(client, table, entries, operation_timeout, attempt_timeout)
        # raise error after crossing
        with pytest.raises(ValueError) as e:
            self._make_one(
                client,
                table,
                entries + [_make_mutation()],
                operation_timeout,
                attempt_timeout,
            )
        assert "mutate_rows requests can contain at most 100000 mutations" in str(
            e.value
        )
        assert "Found 100001" in str(e.value)

    @pytest.mark.asyncio
    async def test_mutate_rows_operation(self):
        """
        Test successful case of mutate_rows_operation
        """
        client = mock.Mock()
        table = mock.Mock()
        entries = [_make_mutation(), _make_mutation()]
        operation_timeout = 0.05
        cls = self._target_class()
        with mock.patch(
            f"{cls.__module__}.{cls.__name__}._run_attempt", AsyncMock()
        ) as attempt_mock:
            instance = self._make_one(
                client, table, entries, operation_timeout, operation_timeout
            )
            await instance.start()
            assert attempt_mock.call_count == 1

    @pytest.mark.parametrize(
        "exc_type", [RuntimeError, ZeroDivisionError, core_exceptions.Forbidden]
    )
    @pytest.mark.asyncio
    async def test_mutate_rows_attempt_exception(self, exc_type):
        """
        exceptions raised from attempt should be raised in MutationsExceptionGroup
        """
        client = AsyncMock()
        table = mock.Mock()
        entries = [_make_mutation(), _make_mutation()]
        operation_timeout = 0.05
        expected_exception = exc_type("test")
        client.mutate_rows.side_effect = expected_exception
        found_exc = None
        try:
            instance = self._make_one(
                client, table, entries, operation_timeout, operation_timeout
            )
            await instance._run_attempt()
        except Exception as e:
            found_exc = e
        assert client.mutate_rows.call_count == 1
        assert type(found_exc) is exc_type
        assert found_exc == expected_exception
        assert len(instance.errors) == 2
        assert len(instance.remaining_indices) == 0

    @pytest.mark.parametrize(
        "exc_type", [RuntimeError, ZeroDivisionError, core_exceptions.Forbidden]
    )
    @pytest.mark.asyncio
    async def test_mutate_rows_exception(self, exc_type):
        """
        exceptions raised from retryable should be raised in MutationsExceptionGroup
        """
        from google.cloud.bigtable.data.exceptions import MutationsExceptionGroup
        from google.cloud.bigtable.data.exceptions import FailedMutationEntryError

        client = mock.Mock()
        table = mock.Mock()
        entries = [_make_mutation(), _make_mutation()]
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
            assert len(found_exc.exceptions) == 2
            assert isinstance(found_exc.exceptions[0], FailedMutationEntryError)
            assert isinstance(found_exc.exceptions[1], FailedMutationEntryError)
            assert found_exc.exceptions[0].__cause__ == expected_cause
            assert found_exc.exceptions[1].__cause__ == expected_cause

    @pytest.mark.parametrize(
        "exc_type",
        [core_exceptions.DeadlineExceeded, RuntimeError],
    )
    @pytest.mark.asyncio
    async def test_mutate_rows_exception_retryable_eventually_pass(self, exc_type):
        """
        If an exception fails but eventually passes, it should not raise an exception
        """
        from google.cloud.bigtable.data._async._mutate_rows import (
            _MutateRowsOperationAsync,
        )

        client = mock.Mock()
        table = mock.Mock()
        entries = [_make_mutation()]
        operation_timeout = 1
        expected_cause = exc_type("retry")
        num_retries = 2
        with mock.patch.object(
            _MutateRowsOperationAsync,
            "_run_attempt",
            AsyncMock(),
        ) as attempt_mock:
            attempt_mock.side_effect = [expected_cause] * num_retries + [None]
            instance = self._make_one(
                client,
                table,
                entries,
                operation_timeout,
                operation_timeout,
                retryable_exceptions=(exc_type,),
            )
            await instance.start()
            assert attempt_mock.call_count == num_retries + 1

    @pytest.mark.asyncio
    async def test_mutate_rows_incomplete_ignored(self):
        """
        MutateRowsIncomplete exceptions should not be added to error list
        """
        from google.cloud.bigtable.data.exceptions import _MutateRowsIncomplete
        from google.cloud.bigtable.data.exceptions import MutationsExceptionGroup
        from google.api_core.exceptions import DeadlineExceeded

        client = mock.Mock()
        table = mock.Mock()
        entries = [_make_mutation()]
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
        mutation = _make_mutation()
        expected_timeout = 1.3
        mock_gapic_fn = self._make_mock_gapic({0: mutation})
        instance = self._make_one(
            mutation_entries=[mutation],
            attempt_timeout=expected_timeout,
        )
        with mock.patch.object(instance, "_gapic_fn", mock_gapic_fn):
            await instance._run_attempt()
        assert len(instance.remaining_indices) == 0
        assert mock_gapic_fn.call_count == 1
        _, kwargs = mock_gapic_fn.call_args
        assert kwargs["timeout"] == expected_timeout
        assert kwargs["entries"] == [mutation._to_pb()]

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
        from google.cloud.bigtable.data.exceptions import _MutateRowsIncomplete

        success_mutation = _make_mutation()
        success_mutation_2 = _make_mutation()
        failure_mutation = _make_mutation()
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
        success_mutation = _make_mutation()
        success_mutation_2 = _make_mutation()
        failure_mutation = _make_mutation()
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
