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


class TestMutateRowsAttemptContext:
    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable._mutate_rows import _MutateRowsAttemptContext

        return _MutateRowsAttemptContext(*args, **kwargs)

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
        mock_gapic = mock.Mock()
        mutations = list(range(10))
        timeout_gen = mock.Mock()
        predicate = mock.Mock()
        instance = self._make_one(
            mock_gapic,
            mutations,
            timeout_gen,
            predicate,
        )
        assert instance.gapic_fn == mock_gapic
        assert instance.mutations == mutations
        assert instance.remaining_indices == list(range(10))
        assert instance.timeout_generator == timeout_gen
        assert instance.is_retryable == predicate
        assert instance.errors == {}

    @pytest.mark.asyncio
    async def test_single_entry_success(self):
        """Test mutating a single entry"""
        import itertools

        mutation = mock.Mock()
        mutations = {0: mutation}
        expected_timeout = 9
        mock_timeout_gen = itertools.repeat(expected_timeout)
        mock_gapic_fn = self._make_mock_gapic(mutations)
        instance = self._make_one(
            mock_gapic_fn,
            mutations,
            mock_timeout_gen,
            lambda x: False,
        )
        await instance.run_attempt()
        assert len(instance.remaining_indices) == 0
        assert mock_gapic_fn.call_count == 1
        _, kwargs = mock_gapic_fn.call_args
        assert kwargs["timeout"] == expected_timeout
        assert kwargs["entries"] == [mutation._to_dict()]

    @pytest.mark.asyncio
    async def test_empty_request(self):
        """Calling with no mutations should result in no API calls"""
        mock_timeout_gen = iter([0] * 10)
        mock_gapic_fn = self._make_mock_gapic([])
        instance = self._make_one(
            mock_gapic_fn,
            [],
            mock_timeout_gen,
            lambda x: False,
        )
        await instance.run_attempt()
        assert mock_gapic_fn.call_count == 0

    @pytest.mark.asyncio
    async def test_partial_success_retryable(self):
        """Some entries succeed, but one fails. Should report the proper index, and raise incomplete exception"""
        from google.cloud.bigtable._mutate_rows import _MutateRowsIncomplete

        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = [success_mutation, failure_mutation, success_mutation_2]
        mock_timeout_gen = iter([0] * 10)
        mock_gapic_fn = self._make_mock_gapic(mutations, error_dict={1: 300})
        instance = self._make_one(
            mock_gapic_fn,
            mutations,
            mock_timeout_gen,
            lambda x: True,
        )
        with pytest.raises(_MutateRowsIncomplete):
            await instance.run_attempt()
        assert instance.remaining_indices == [1]
        assert 0 not in instance.errors
        assert len(instance.errors[1]) == 1
        assert instance.errors[1][0].grpc_status_code == 300
        assert 2 not in instance.errors

    @pytest.mark.asyncio
    async def test_partial_success_non_retryable(self):
        """Some entries succeed, but one fails. Exception marked as non-retryable. Do not raise incomplete error"""
        success_mutation = mock.Mock()
        success_mutation_2 = mock.Mock()
        failure_mutation = mock.Mock()
        mutations = [success_mutation, failure_mutation, success_mutation_2]
        mock_timeout_gen = iter([0] * 10)
        mock_gapic_fn = self._make_mock_gapic(mutations, error_dict={1: 300})
        instance = self._make_one(
            mock_gapic_fn,
            mutations,
            mock_timeout_gen,
            lambda x: False,
        )
        await instance.run_attempt()
        assert instance.remaining_indices == []
        assert 0 not in instance.errors
        assert len(instance.errors[1]) == 1
        assert instance.errors[1][0].grpc_status_code == 300
        assert 2 not in instance.errors
