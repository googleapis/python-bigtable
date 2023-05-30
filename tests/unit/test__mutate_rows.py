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

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


class Test_MutateRowsRetryableAttempt:
    async def _mock_stream(self, mutation_dict, error_dict):
        items = list(mutation_dict.items())
        for idx, entry in items:
            code = error_dict.get(idx, 0)
            yield MutateRowsResponse(
                entries=[
                    MutateRowsResponse.Entry(
                        index=idx, status=status_pb2.Status(code=code)
                    )
                ]
            )

    def _make_mock_client(self, mutation_dict, error_dict=None):
        client = mock.Mock()
        client.mutate_rows = AsyncMock()
        if error_dict is None:
            error_dict = {}
        client.mutate_rows.side_effect = lambda *args, **kwargs: self._mock_stream(
            mutation_dict, error_dict
        )
        return client

    @pytest.mark.asyncio
    async def test_single_entry_success(self):
        """Test mutating a single entry"""
        from google.cloud.bigtable._mutate_rows import _mutate_rows_retryable_attempt
        import itertools

        mutation = mock.Mock()
        mutations = {0: mutation}
        client = self._make_mock_client(mutations)
        errors = {0: []}
        expected_table = mock.Mock()
        expected_timeout = 9
        mock_timeout_gen = itertools.repeat(expected_timeout)
        await _mutate_rows_retryable_attempt(
            client,
            expected_table,
            mock_timeout_gen,
            mutations,
            errors,
            lambda x: False,
        )
        assert len(mutations) == 0
        assert 0 not in errors
        assert client.mutate_rows.call_count == 1
        _, kwargs = client.mutate_rows.call_args
        assert kwargs["timeout"] == expected_timeout
        request = kwargs["request"]
        assert request["table_name"] == expected_table.table_name
        assert request["app_profile_id"] == expected_table.app_profile_id
        assert request["entries"] == [mutation._to_dict()]

    @pytest.mark.asyncio
    async def test_empty_request(self):
        """Calling with no mutations should result in a single API call"""
        from google.cloud.bigtable._mutate_rows import _mutate_rows_retryable_attempt

        client = self._make_mock_client({})
        expected_table = mock.Mock()
        await _mutate_rows_retryable_attempt(
            client, expected_table, iter([0]), {}, {}, lambda x: False
        )
        assert client.mutate_rows.call_count == 1

    @pytest.mark.asyncio
    async def test_partial_success_retryable(self):
        """Some entries succeed, but one fails. Should report the proper index, and raise incomplete exception"""
        from google.cloud.bigtable._mutate_rows import (
            _mutate_rows_retryable_attempt,
            _MutateRowsIncomplete,
        )

        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = {0: success_mutation, 1: failure_mutation, 2: success_mutation_2}
        errors = {0: [], 1: [], 2: []}
        client = self._make_mock_client(mutations, error_dict={1: 300})
        # raise retryable error 3 times, then raise non-retryable error
        expected_table = mock.Mock()
        expected_timeout = 9
        with pytest.raises(_MutateRowsIncomplete):
            await _mutate_rows_retryable_attempt(
                client,
                expected_table,
                iter([expected_timeout]),
                mutations,
                errors,
                lambda x: True,
            )
        assert mutations == {1: failure_mutation}
        assert 0 not in errors
        assert len(errors[1]) == 1
        assert errors[1][0].grpc_status_code == 300
        assert 2 not in errors

    @pytest.mark.asyncio
    async def test_partial_success_non_retryable(self):
        """Some entries succeed, but one fails. Exception marked as non-retryable. Do not raise incomplete error"""
        from google.cloud.bigtable._mutate_rows import _mutate_rows_retryable_attempt

        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = {0: success_mutation, 1: failure_mutation, 2: success_mutation_2}
        errors = {0: [], 1: [], 2: []}
        client = self._make_mock_client(mutations, error_dict={1: 300})
        expected_timeout = 9
        expected_table = mock.Mock()
        await _mutate_rows_retryable_attempt(
            client,
            expected_table,
            iter([expected_timeout]),
            mutations,
            errors,
            lambda x: False,
        )
        assert len(mutations) == 0
        assert 0 not in errors
        assert len(errors[1]) == 1
        assert errors[1][0].grpc_status_code == 300
        assert 2 not in errors

    @pytest.mark.asyncio
    async def test_on_terminal_state_no_retries(self):
        """
        Should call on_terminal_state for each successful or non-retryable mutation
        """
        from google.cloud.bigtable._mutate_rows import _mutate_rows_retryable_attempt

        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = {0: success_mutation, 1: failure_mutation, 2: success_mutation_2}
        callback = AsyncMock()
        errors = {0: [], 1: [], 2: []}
        client = self._make_mock_client(mutations, error_dict={1: 300})
        expected_table = mock.Mock()
        # raise retryable error 3 times, then raise non-retryable error
        await _mutate_rows_retryable_attempt(
            client,
            expected_table,
            iter([9]),
            mutations,
            errors,
            lambda x: False,
            callback,
        )
        assert callback.call_count == 3
        assert callback.await_count == 3
        call_args = callback.call_args_list
        assert call_args[0][0][0] == 0  # index
        assert call_args[0][0][1] == success_mutation
        assert call_args[0][0][2] is None
        assert call_args[1][0][0] == 1  # index
        assert call_args[1][0][1] == failure_mutation
        error_list = call_args[1][0][2]
        assert len(error_list) == 1
        assert error_list[0].grpc_status_code == 300
        assert call_args[2][0][0] == 2  # index
        assert call_args[2][0][1] == success_mutation_2
        assert call_args[2][0][2] is None

    @pytest.mark.asyncio
    async def test_on_terminal_state_with_retries(self):
        """
        Should not call on_terminal_state for retryable mutations
        """
        from google.cloud.bigtable._mutate_rows import (
            _mutate_rows_retryable_attempt,
            _MutateRowsIncomplete,
        )

        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = {0: success_mutation, 1: failure_mutation, 2: success_mutation_2}
        callback = AsyncMock()
        errors = {0: [], 1: [], 2: []}
        client = self._make_mock_client(mutations, error_dict={1: 300})
        expected_table = mock.Mock()
        # raise retryable error 3 times, then raise non-retryable error
        with pytest.raises(_MutateRowsIncomplete):
            await _mutate_rows_retryable_attempt(
                client,
                expected_table,
                iter([9]),
                mutations,
                errors,
                lambda x: True,
                callback,
            )
        assert callback.call_count == 2
        assert callback.await_count == 2
        call_args = callback.call_args_list
        assert call_args[0][0][0] == 0  # index
        assert call_args[0][0][1] == success_mutation
        assert call_args[0][0][2] is None
        assert call_args[1][0][0] == 2  # index
        assert call_args[1][0][1] == success_mutation_2
        assert call_args[1][0][2] is None
