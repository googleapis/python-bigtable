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
import asyncio

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore


class Test_FlowControl:
    def _make_one(self, max_mutation_count=10, max_mutation_bytes=100):
        from google.cloud.bigtable.mutations_batcher import _FlowControl

        return _FlowControl(max_mutation_count, max_mutation_bytes)

    def _make_mutation(self, count=1, size=1):
        mutation = mock.Mock()
        mutation.size.return_value = size
        mutation.mutations = [mock.Mock()] * count
        return mutation

    def test_ctor(self):
        max_mutation_count = 9
        max_mutation_bytes = 19
        instance = self._make_one(max_mutation_count, max_mutation_bytes)
        assert instance.max_mutation_count == max_mutation_count
        assert instance.max_mutation_bytes == max_mutation_bytes
        assert instance.in_flight_mutation_count == 0
        assert instance.in_flight_mutation_bytes == 0
        assert isinstance(instance.capacity_condition, asyncio.Condition)

    def test_ctor_empty_values(self):
        """Test constructor with None count and bytes"""
        instance = self._make_one(None, None)
        assert instance.max_mutation_count == float("inf")
        assert instance.max_mutation_bytes == float("inf")

    @pytest.mark.parametrize(
        "max_count,max_size,existing_count,existing_size,new_count,new_size,expected",
        [
            (0, 0, 0, 0, 0, 0, True),
            (0, 0, 1, 1, 1, 1, False),
            (10, 10, 0, 0, 0, 0, True),
            (10, 10, 0, 0, 9, 9, True),
            (10, 10, 0, 0, 11, 9, False),
            (10, 10, 0, 0, 9, 11, False),
            (10, 1, 0, 0, 1, 0, True),
            (1, 10, 0, 0, 0, 8, True),
            (float("inf"), float("inf"), 0, 0, 1e10, 1e10, True),
            (8, 8, 0, 0, 1e10, 1e10, False),
            (12, 12, 6, 6, 5, 5, True),
            (12, 12, 5, 5, 6, 6, True),
            (12, 12, 6, 6, 6, 6, True),
            (12, 12, 6, 6, 7, 7, False),
        ],
    )
    def test__has_capacity(
        self,
        max_count,
        max_size,
        existing_count,
        existing_size,
        new_count,
        new_size,
        expected,
    ):
        """
        _has_capacity should return True if the new mutation will will not exceed the max count or size
        """
        instance = self._make_one(max_count, max_size)
        instance.in_flight_mutation_count = existing_count
        instance.in_flight_mutation_bytes = existing_size
        assert instance._has_capacity(new_count, new_size) == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "existing_count,existing_size,added_count,added_size,new_count,new_size",
        [
            (0, 0, 0, 0, 0, 0),
            (2, 2, 1, 1, 1, 1),
            (2, 0, 1, 0, 1, 0),
            (0, 2, 0, 1, 0, 1),
            (10, 10, 0, 0, 10, 10),
            (10, 10, 5, 5, 5, 5),
            (0, 0, 1, 1, -1, -1),
        ],
    )
    async def test_remove_from_flow_value_update(
        self,
        existing_count,
        existing_size,
        added_count,
        added_size,
        new_count,
        new_size,
    ):
        """
        completed mutations should lower the inflight values
        """
        instance = self._make_one()
        instance.in_flight_mutation_count = existing_count
        instance.in_flight_mutation_bytes = existing_size
        mutation = self._make_mutation(added_count, added_size)
        await instance.remove_from_flow(mutation)
        assert instance.in_flight_mutation_count == new_count
        assert instance.in_flight_mutation_bytes == new_size

    @pytest.mark.asyncio
    async def test__remove_from_flow_unlock(self):
        """capacity condition should notify after mutation is complete"""
        instance = self._make_one(10, 10)
        instance.in_flight_mutation_count = 10
        instance.in_flight_mutation_bytes = 10

        async def task_routine():
            async with instance.capacity_condition:
                await instance.capacity_condition.wait_for(
                    lambda: instance._has_capacity(1, 1)
                )

        task = asyncio.create_task(task_routine())
        await asyncio.sleep(0.05)
        # should be blocked due to capacity
        assert task.done() is False
        # try changing size
        mutation = self._make_mutation(count=0, size=5)
        await instance.remove_from_flow(mutation)
        await asyncio.sleep(0.05)
        assert instance.in_flight_mutation_count == 10
        assert instance.in_flight_mutation_bytes == 5
        assert task.done() is False
        # try changing count
        instance.in_flight_mutation_bytes = 10
        mutation = self._make_mutation(count=5, size=0)
        await instance.remove_from_flow(mutation)
        await asyncio.sleep(0.05)
        assert instance.in_flight_mutation_count == 5
        assert instance.in_flight_mutation_bytes == 10
        assert task.done() is False
        # try changing both
        instance.in_flight_mutation_count = 10
        mutation = self._make_mutation(count=5, size=5)
        await instance.remove_from_flow(mutation)
        await asyncio.sleep(0.05)
        assert instance.in_flight_mutation_count == 5
        assert instance.in_flight_mutation_bytes == 5
        # task should be complete
        assert task.done() is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mutations,count_cap,size_cap,expected_results",
        [
            # high capacity results in no batching
            ([(5, 5), (1, 1), (1, 1)], 10, 10, [[(5, 5), (1, 1), (1, 1)]]),
            # low capacity splits up into batches
            ([(1, 1), (1, 1), (1, 1)], 1, 1, [[(1, 1)], [(1, 1)], [(1, 1)]]),
            # test count as limiting factor
            ([(1, 1), (1, 1), (1, 1)], 2, 10, [[(1, 1), (1, 1)], [(1, 1)]]),
            # test size as limiting factor
            ([(1, 1), (1, 1), (1, 1)], 10, 2, [[(1, 1), (1, 1)], [(1, 1)]]),
            # test with some bloackages and some flows
            (
                [(1, 1), (5, 5), (4, 1), (1, 4), (1, 1)],
                5,
                5,
                [[(1, 1)], [(5, 5)], [(4, 1), (1, 4)], [(1, 1)]],
            ),
        ],
    )
    async def test_add_to_flow(self, mutations, count_cap, size_cap, expected_results):
        """
        Test batching with various flow control settings
        """
        mutation_objs = [self._make_mutation(count=m[0], size=m[1]) for m in mutations]
        instance = self._make_one(count_cap, size_cap)
        i = 0
        async for batch in instance.add_to_flow(mutation_objs):
            expected_batch = expected_results[i]
            assert len(batch) == len(expected_batch)
            for j in range(len(expected_batch)):
                # check counts
                assert len(batch[j].mutations) == expected_batch[j][0]
                # check sizes
                assert batch[j].size() == expected_batch[j][1]
            # update lock
            for entry in batch:
                await instance.remove_from_flow(entry)
            i += 1
        assert i == len(expected_results)

    @pytest.mark.asyncio
    async def test_add_to_flow_invalid_mutation(self):
        """
        batching should raise exception for mutations larger than limits to avoid deadlock
        """
        instance = self._make_one(2, 3)
        large_size_mutation = self._make_mutation(count=1, size=10)
        large_count_mutation = self._make_mutation(count=10, size=1)
        with pytest.raises(ValueError) as e:
            async for _ in instance.add_to_flow([large_size_mutation]):
                pass
        assert "Mutation size 10 exceeds maximum: 3" in str(e.value)
        with pytest.raises(ValueError) as e:
            async for _ in instance.add_to_flow([large_count_mutation]):
                pass
        assert "Mutation count 10 exceeds maximum: 2" in str(e.value)


class TestMutationsBatcher:
    def _make_one(self, table=None, **kwargs):
        from google.cloud.firestore_v1.batch import MutationsBatcher

        if table is None:
            table = mock.Mock()

        return MutationsBatcher(table, **kwargs)

    def test_ctor(self):
        pass

    def test_context_manager(self):
        pass

    def test__flush_timer(self):
        pass

    def test_close(self):
        pass

    def test_append(self):
        pass

    def test_flush(self):
        pass

    def test__raise_exceptions(self):
        pass
