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
import unittest

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


def _make_mutation(count=1, size=1):
    mutation = mock.Mock()
    mutation.size.return_value = size
    mutation.mutations = [mock.Mock()] * count
    return mutation


class Test_FlowControl:
    def _make_one(self, max_mutation_count=10, max_mutation_bytes=100):
        from google.cloud.bigtable.mutations_batcher import _FlowControl

        return _FlowControl(max_mutation_count, max_mutation_bytes)

    def test_ctor(self):
        max_mutation_count = 9
        max_mutation_bytes = 19
        instance = self._make_one(max_mutation_count, max_mutation_bytes)
        assert instance._max_mutation_count == max_mutation_count
        assert instance._max_mutation_bytes == max_mutation_bytes
        assert instance._in_flight_mutation_count == 0
        assert instance._in_flight_mutation_bytes == 0
        assert isinstance(instance._capacity_condition, asyncio.Condition)

    def test_ctor_empty_values(self):
        """Test constructor with None count and bytes"""
        instance = self._make_one(None, None)
        assert instance._max_mutation_count == float("inf")
        assert instance._max_mutation_bytes == float("inf")

    def test_ctor_invalid_values(self):
        """Test that values are positive, and fit within expected limits"""
        with pytest.raises(ValueError) as e:
            self._make_one(0, 1)
            assert "max_mutation_count must be greater than 0" in str(e.value)
        with pytest.raises(ValueError) as e:
            self._make_one(1, 0)
            assert "max_mutation_bytes must be greater than 0" in str(e.value)

    @pytest.mark.parametrize(
        "max_count,max_size,existing_count,existing_size,new_count,new_size,expected",
        [
            (1, 1, 0, 0, 0, 0, True),
            (1, 1, 1, 1, 1, 1, False),
            (10, 10, 0, 0, 0, 0, True),
            (10, 10, 0, 0, 9, 9, True),
            (10, 10, 0, 0, 11, 9, True),
            (10, 10, 0, 1, 11, 9, True),
            (10, 10, 1, 0, 11, 9, False),
            (10, 10, 0, 0, 9, 11, True),
            (10, 10, 1, 0, 9, 11, True),
            (10, 10, 0, 1, 9, 11, False),
            (10, 1, 0, 0, 1, 0, True),
            (1, 10, 0, 0, 0, 8, True),
            (float("inf"), float("inf"), 0, 0, 1e10, 1e10, True),
            (8, 8, 0, 0, 1e10, 1e10, True),
            (12, 12, 6, 6, 5, 5, True),
            (12, 12, 5, 5, 6, 6, True),
            (12, 12, 6, 6, 6, 6, True),
            (12, 12, 6, 6, 7, 7, False),
            # allow capacity check if new_count or new_size exceeds limits
            (12, 12, 0, 0, 13, 13, True),
            (12, 12, 12, 0, 0, 13, True),
            (12, 12, 0, 12, 13, 0, True),
            # but not if there's already values in flight
            (12, 12, 1, 1, 13, 13, False),
            (12, 12, 1, 1, 0, 13, False),
            (12, 12, 1, 1, 13, 0, False),
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
        instance._in_flight_mutation_count = existing_count
        instance._in_flight_mutation_bytes = existing_size
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
        instance._in_flight_mutation_count = existing_count
        instance._in_flight_mutation_bytes = existing_size
        mutation = _make_mutation(added_count, added_size)
        await instance.remove_from_flow(mutation)
        assert instance._in_flight_mutation_count == new_count
        assert instance._in_flight_mutation_bytes == new_size

    @pytest.mark.asyncio
    async def test__remove_from_flow_unlock(self):
        """capacity condition should notify after mutation is complete"""
        instance = self._make_one(10, 10)
        instance._in_flight_mutation_count = 10
        instance._in_flight_mutation_bytes = 10

        async def task_routine():
            async with instance._capacity_condition:
                await instance._capacity_condition.wait_for(
                    lambda: instance._has_capacity(1, 1)
                )

        task = asyncio.create_task(task_routine())
        await asyncio.sleep(0.05)
        # should be blocked due to capacity
        assert task.done() is False
        # try changing size
        mutation = _make_mutation(count=0, size=5)
        await instance.remove_from_flow([mutation])
        await asyncio.sleep(0.05)
        assert instance._in_flight_mutation_count == 10
        assert instance._in_flight_mutation_bytes == 5
        assert task.done() is False
        # try changing count
        instance._in_flight_mutation_bytes = 10
        mutation = _make_mutation(count=5, size=0)
        await instance.remove_from_flow([mutation])
        await asyncio.sleep(0.05)
        assert instance._in_flight_mutation_count == 5
        assert instance._in_flight_mutation_bytes == 10
        assert task.done() is False
        # try changing both
        instance._in_flight_mutation_count = 10
        mutation = _make_mutation(count=5, size=5)
        await instance.remove_from_flow([mutation])
        await asyncio.sleep(0.05)
        assert instance._in_flight_mutation_count == 5
        assert instance._in_flight_mutation_bytes == 5
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
        mutation_objs = [_make_mutation(count=m[0], size=m[1]) for m in mutations]
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
            await instance.remove_from_flow(batch)
            i += 1
        assert i == len(expected_results)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mutations,max_limit,expected_results",
        [
            ([(1, 1)] * 11, 10, [[(1, 1)] * 10, [(1, 1)]]),
            ([(1, 1)] * 10, 1, [[(1, 1)] for _ in range(10)]),
            ([(1, 1)] * 10, 2, [[(1, 1), (1, 1)] for _ in range(5)]),
        ],
    )
    async def test_add_to_flow_max_mutation_limits(
        self, mutations, max_limit, expected_results
    ):
        """
        Test flow control running up against the max API limit
        Should submit request early, even if the flow control has room for more
        """
        with mock.patch(
            "google.cloud.bigtable.mutations_batcher.MUTATE_ROWS_REQUEST_MUTATION_LIMIT",
            max_limit,
        ):
            mutation_objs = [_make_mutation(count=m[0], size=m[1]) for m in mutations]
            # flow control has no limits except API restrictions
            instance = self._make_one(None, None)
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
                await instance.remove_from_flow(batch)
                i += 1
            assert i == len(expected_results)

    @pytest.mark.asyncio
    async def test_add_to_flow_oversize(self):
        """
        mutations over the flow control limits should still be accepted
        """
        instance = self._make_one(2, 3)
        large_size_mutation = _make_mutation(count=1, size=10)
        large_count_mutation = _make_mutation(count=10, size=1)
        results = [out async for out in instance.add_to_flow([large_size_mutation])]
        assert len(results) == 1
        await instance.remove_from_flow(results[0])
        count_results = [
            out async for out in instance.add_to_flow(large_count_mutation)
        ]
        assert len(count_results) == 1


class TestMutationsBatcher:
    def _make_one(self, table=None, **kwargs):
        from google.cloud.bigtable.mutations_batcher import MutationsBatcher

        if table is None:
            table = mock.Mock()
            table.default_operation_timeout = 10
            table.default_per_request_timeout = 10

        return MutationsBatcher(table, **kwargs)

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._flush_timer"
    )
    @pytest.mark.asyncio
    async def test_ctor_defaults(self, flush_timer_mock):
        table = mock.Mock()
        async with self._make_one(table) as instance:
            assert instance._table == table
            assert instance.closed is False
            assert instance._staged_mutations == []
            assert instance.exceptions == []
            assert instance._flow_control._max_mutation_count == 100000
            assert instance._flow_control._max_mutation_bytes == 104857600
            assert instance._flow_control._in_flight_mutation_count == 0
            assert instance._flow_control._in_flight_mutation_bytes == 0
            assert instance._entries_processed_since_last_raise == 0
            await asyncio.sleep(0)
            assert flush_timer_mock.call_count == 1
            assert flush_timer_mock.call_args[0][0] == 5
            assert isinstance(instance._flush_timer_task, asyncio.Task)

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._flush_timer"
    )
    @pytest.mark.asyncio
    async def test_ctor_explicit(self, flush_timer_mock):
        """Test with explicit parameters"""
        table = mock.Mock()
        flush_interval = 20
        flush_limit_count = 17
        flush_limit_bytes = 19
        flow_control_max_count = 1001
        flow_control_max_bytes = 12
        async with self._make_one(
            table,
            flush_interval=flush_interval,
            flush_limit_mutation_count=flush_limit_count,
            flush_limit_bytes=flush_limit_bytes,
            flow_control_max_count=flow_control_max_count,
            flow_control_max_bytes=flow_control_max_bytes,
        ) as instance:
            assert instance._table == table
            assert instance.closed is False
            assert instance._staged_mutations == []
            assert instance.exceptions == []
            assert instance._flow_control._max_mutation_count == flow_control_max_count
            assert instance._flow_control._max_mutation_bytes == flow_control_max_bytes
            assert instance._flow_control._in_flight_mutation_count == 0
            assert instance._flow_control._in_flight_mutation_bytes == 0
            assert instance._entries_processed_since_last_raise == 0
            await asyncio.sleep(0)
            assert flush_timer_mock.call_count == 1
            assert flush_timer_mock.call_args[0][0] == flush_interval
            assert isinstance(instance._flush_timer_task, asyncio.Task)

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._flush_timer"
    )
    @pytest.mark.asyncio
    async def test_ctor_no_limits(self, flush_timer_mock):
        """Test with None for flow control and flush limits"""
        table = mock.Mock()
        flush_interval = None
        flush_limit_count = None
        flush_limit_bytes = None
        flow_control_max_count = None
        flow_control_max_bytes = None
        async with self._make_one(
            table,
            flush_interval=flush_interval,
            flush_limit_mutation_count=flush_limit_count,
            flush_limit_bytes=flush_limit_bytes,
            flow_control_max_count=flow_control_max_count,
            flow_control_max_bytes=flow_control_max_bytes,
        ) as instance:
            assert instance._table == table
            assert instance.closed is False
            assert instance._staged_mutations == []
            assert instance.exceptions == []
            assert instance._flow_control._max_mutation_count == float("inf")
            assert instance._flow_control._max_mutation_bytes == float("inf")
            assert instance._flow_control._in_flight_mutation_count == 0
            assert instance._flow_control._in_flight_mutation_bytes == 0
            assert instance._entries_processed_since_last_raise == 0
            await asyncio.sleep(0)
            assert flush_timer_mock.call_count == 1
            assert flush_timer_mock.call_args[0][0] is None
            assert isinstance(instance._flush_timer_task, asyncio.Task)

    def test_default_argument_consistency(self):
        """
        We supply default arguments in MutationsBatcher.__init__, and in
        table.mutations_batcher. Make sure any changes to defaults are applied to
        both places
        """
        from google.cloud.bigtable.client import Table
        from google.cloud.bigtable.mutations_batcher import MutationsBatcher
        import inspect

        get_batcher_signature = dict(
            inspect.signature(Table.mutations_batcher).parameters
        )
        get_batcher_signature.pop("self")
        batcher_init_signature = dict(inspect.signature(MutationsBatcher).parameters)
        batcher_init_signature.pop("table")
        # both should have same number of arguments
        assert len(get_batcher_signature) == len(batcher_init_signature)
        assert len(get_batcher_signature) == 5
        # both should have same argument names
        assert set(get_batcher_signature.keys()) == set(batcher_init_signature.keys())
        # both should have same default values
        for arg_name in get_batcher_signature.keys():
            assert (
                get_batcher_signature[arg_name].default
                == batcher_init_signature[arg_name].default
            )

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._schedule_flush"
    )
    @pytest.mark.asyncio
    async def test__flush_timer_w_None(self, flush_mock):
        """Empty timer should return immediately"""
        async with self._make_one() as instance:
            with mock.patch("asyncio.sleep") as sleep_mock:
                await instance._flush_timer(None)
                assert sleep_mock.call_count == 0
                assert flush_mock.call_count == 0

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._schedule_flush"
    )
    @pytest.mark.asyncio
    async def test__flush_timer_call_when_closed(self, flush_mock):
        """closed batcher's timer should return immediately"""
        async with self._make_one() as instance:
            await instance.close()
            flush_mock.reset_mock()
            with mock.patch("asyncio.sleep") as sleep_mock:
                await instance._flush_timer(1)
                assert sleep_mock.call_count == 0
                assert flush_mock.call_count == 0

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._schedule_flush"
    )
    @pytest.mark.asyncio
    async def test__flush_timer(self, flush_mock):
        """Timer should continue to call _schedule_flush in a loop"""
        async with self._make_one() as instance:
            instance._staged_mutations = [mock.Mock()]
            loop_num = 3
            expected_sleep = 12
            with mock.patch("asyncio.sleep") as sleep_mock:
                sleep_mock.side_effect = [None] * loop_num + [asyncio.CancelledError()]
                try:
                    await instance._flush_timer(expected_sleep)
                except asyncio.CancelledError:
                    pass
                assert sleep_mock.call_count == loop_num + 1
                sleep_mock.assert_called_with(expected_sleep)
                assert flush_mock.call_count == loop_num

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._schedule_flush"
    )
    @pytest.mark.asyncio
    async def test__flush_timer_no_mutations(self, flush_mock):
        """Timer should not flush if no new mutations have been staged"""
        async with self._make_one() as instance:
            loop_num = 3
            expected_sleep = 12
            with mock.patch("asyncio.sleep") as sleep_mock:
                sleep_mock.side_effect = [None] * loop_num + [asyncio.CancelledError()]
                try:
                    await instance._flush_timer(expected_sleep)
                except asyncio.CancelledError:
                    pass
                assert sleep_mock.call_count == loop_num + 1
                sleep_mock.assert_called_with(expected_sleep)
                assert flush_mock.call_count == 0

    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher.MutationsBatcher._schedule_flush"
    )
    @pytest.mark.asyncio
    async def test__flush_timer_close(self, flush_mock):
        """Timer should continue terminate after close"""
        async with self._make_one() as instance:
            expected_sleep = 12
            with mock.patch("asyncio.sleep"):
                task = asyncio.create_task(instance._flush_timer(expected_sleep))
                # let task run in background
                await asyncio.sleep(0.5)
                # close the batcher
                await instance.close()
                await asyncio.sleep(0.1)
                # task should be complete
                assert task.done()

    @pytest.mark.asyncio
    async def test_append_closed(self):
        """Should raise exception"""
        with pytest.raises(RuntimeError):
            instance = self._make_one()
            await instance.close()
            instance.append(mock.Mock())

    @pytest.mark.asyncio
    async def test_append_wrong_mutation(self):
        """
        Mutation objects should raise an exception.
        Only support RowMutationEntry
        """
        from google.cloud.bigtable.mutations import DeleteAllFromRow

        instance = self._make_one()
        expected_error = "invalid mutation type: DeleteAllFromRow. Only RowMutationEntry objects are supported by batcher"
        with pytest.raises(ValueError) as e:
            instance.append(DeleteAllFromRow())
        assert str(e.value) == expected_error

    @pytest.mark.asyncio
    async def test_append_outside_flow_limits(self):
        """entries larger than mutation limits are still processed"""
        async with self._make_one(
            flow_control_max_count=1, flow_control_max_bytes=1
        ) as instance:
            oversized_entry = _make_mutation(count=0, size=2)
            instance.append(oversized_entry)
            assert instance._staged_mutations == [oversized_entry]
            assert instance._staged_count == 0
            assert instance._staged_bytes == 2
            instance._staged_mutations = []
        async with self._make_one(
            flow_control_max_count=1, flow_control_max_bytes=1
        ) as instance:
            overcount_entry = _make_mutation(count=2, size=0)
            instance.append(overcount_entry)
            assert instance._staged_mutations == [overcount_entry]
            assert instance._staged_count == 2
            assert instance._staged_bytes == 0
            instance._staged_mutations = []

    @pytest.mark.parametrize(
        "flush_count,flush_bytes,mutation_count,mutation_bytes,expect_flush",
        [
            (10, 10, 1, 1, False),
            (10, 10, 9, 9, False),
            (10, 10, 10, 1, True),
            (10, 10, 1, 10, True),
            (10, 10, 10, 10, True),
            (1, 1, 10, 10, True),
            (1, 1, 0, 0, False),
        ],
    )
    @pytest.mark.asyncio
    async def test_append(
        self, flush_count, flush_bytes, mutation_count, mutation_bytes, expect_flush
    ):
        """test appending different mutations, and checking if it causes a flush"""
        async with self._make_one(
            flush_limit_mutation_count=flush_count, flush_limit_bytes=flush_bytes
        ) as instance:
            assert instance._staged_count == 0
            assert instance._staged_bytes == 0
            assert instance._staged_mutations == []
            mutation = _make_mutation(count=mutation_count, size=mutation_bytes)
            with mock.patch.object(instance, "_schedule_flush") as flush_mock:
                instance.append(mutation)
            assert flush_mock.call_count == bool(expect_flush)
            assert instance._staged_count == mutation_count
            assert instance._staged_bytes == mutation_bytes
            assert instance._staged_mutations == [mutation]
            instance._staged_mutations = []

    @pytest.mark.asyncio
    async def test_append_multiple_sequentially(self):
        """Append multiple mutations"""
        async with self._make_one(
            flush_limit_mutation_count=8, flush_limit_bytes=8
        ) as instance:
            assert instance._staged_count == 0
            assert instance._staged_bytes == 0
            assert instance._staged_mutations == []
            mutation = _make_mutation(count=2, size=3)
            with mock.patch.object(instance, "_schedule_flush") as flush_mock:
                instance.append(mutation)
                assert flush_mock.call_count == 0
                assert instance._staged_count == 2
                assert instance._staged_bytes == 3
                assert len(instance._staged_mutations) == 1
                instance.append(mutation)
                assert flush_mock.call_count == 0
                assert instance._staged_count == 4
                assert instance._staged_bytes == 6
                assert len(instance._staged_mutations) == 2
                instance.append(mutation)
                assert flush_mock.call_count == 1
                assert instance._staged_count == 6
                assert instance._staged_bytes == 9
                assert len(instance._staged_mutations) == 3
            instance._staged_mutations = []

    @pytest.mark.parametrize("raise_exceptions", [True, False])
    @pytest.mark.asyncio
    async def test_flush_no_timeout(self, raise_exceptions):
        """flush should internally call _schedule_flush"""
        mock_obj = AsyncMock()
        async with self._make_one() as instance:
            with mock.patch.object(instance, "_schedule_flush") as flush_mock:
                with mock.patch.object(instance, "_raise_exceptions") as raise_mock:
                    flush_mock.return_value = mock_obj.__call__()
                    await instance.flush(
                        raise_exceptions=raise_exceptions, timeout=None
                    )
                    assert flush_mock.call_count == 1
                    assert mock_obj.await_count == 1
                    assert raise_mock.call_count == int(raise_exceptions)

    @pytest.mark.asyncio
    async def test_flush_w_timeout(self):
        """
        flush should raise TimeoutError if incomplete by timeline, but flush
        task should continue internally
        """
        async with self._make_one() as instance:
            # create mock internal flush job
            instance._prev_flush = asyncio.create_task(asyncio.sleep(0.5))
            with pytest.raises(asyncio.TimeoutError):
                await instance.flush(timeout=0.01)
            # ensure that underlying flush task is still running
            assert not instance._prev_flush.done()
            # ensure flush task can complete without error
            await instance._prev_flush
            assert instance._prev_flush.done()
            assert instance._prev_flush.exception() is None

    @pytest.mark.asyncio
    async def test_flush_concurrent_requests(self):
        """
        requests should happen in parallel if multiple flushes overlap
        """
        import time

        num_flushes = 10
        fake_mutations = [_make_mutation() for _ in range(num_flushes)]
        async with self._make_one() as instance:
            with mock.patch.object(
                instance, "_execute_mutate_rows", AsyncMock()
            ) as op_mock:
                # mock network calls
                async def mock_call(*args, **kwargs):
                    await asyncio.sleep(0.1)
                    return []

                op_mock.side_effect = mock_call
                start_time = time.monotonic()
                # create a few concurrent flushes
                for i in range(num_flushes):
                    instance._staged_mutations = [fake_mutations[i]]
                    try:
                        await instance.flush(timeout=0.01)
                    except asyncio.TimeoutError:
                        pass
                # allow flushes to complete
                await instance.flush()
                duration = time.monotonic() - start_time
                # if flushes were sequential, total duration would be 1s
                assert duration < 0.25
                assert op_mock.call_count == num_flushes

    @pytest.mark.asyncio
    async def test_flush_flow_control_concurrent_requests(self):
        """
        requests should happen in parallel if flow control breaks up single flush into batches
        """
        import time

        num_calls = 10
        fake_mutations = [_make_mutation(count=1) for _ in range(num_calls)]
        async with self._make_one(flow_control_max_count=1) as instance:
            with mock.patch.object(
                instance, "_execute_mutate_rows", AsyncMock()
            ) as op_mock:
                # mock network calls
                async def mock_call(*args, **kwargs):
                    await asyncio.sleep(0.1)
                    return []

                op_mock.side_effect = mock_call
                start_time = time.monotonic()
                # flush one large batch, that will be broken up into smaller batches
                instance._staged_mutations = fake_mutations
                try:
                    await instance.flush(timeout=0.01)
                except asyncio.TimeoutError:
                    pass
                # make room for new mutations
                for i in range(num_calls):
                    await instance._flow_control.remove_from_flow(
                        [_make_mutation(count=1)]
                    )
                    await asyncio.sleep(0.01)
                # allow flushes to complete
                await instance.flush()
                duration = time.monotonic() - start_time
                # if flushes were sequential, total duration would be 1s
                assert instance.exceptions == []
                assert duration < 0.25
                assert op_mock.call_count == num_calls

    @pytest.mark.asyncio
    async def test_overlapping_flush_requests(self):
        """
        Should allow multiple flushes to be scheduled concurrently, with
        each flush raising the errors related to the mutations at flush time
        """
        from google.cloud.bigtable.exceptions import (
            MutationsExceptionGroup,
            FailedMutationEntryError,
        )

        exception1 = RuntimeError("test error1")
        exception2 = ValueError("test error2")
        wrapped_exception_list = [
            FailedMutationEntryError(2, mock.Mock(), exc)
            for exc in [exception1, exception2]
        ]
        # excpetion1 is flushed first, but finishes second
        sleep_times = [0.1, 0.05]

        async with self._make_one() as instance:
            with mock.patch.object(
                instance, "_execute_mutate_rows", AsyncMock()
            ) as op_mock:
                # mock network calls
                async def mock_call(*args, **kwargs):
                    time, exception = sleep_times.pop(0), wrapped_exception_list.pop(0)
                    await asyncio.sleep(time)
                    return [exception]

                op_mock.side_effect = mock_call
                # create a few concurrent flushes
                instance._staged_mutations = [_make_mutation()]
                flush_task1 = asyncio.create_task(instance.flush())
                # let flush task initialize
                await asyncio.sleep(0)
                instance._staged_mutations = [_make_mutation()]
                flush_task2 = asyncio.create_task(instance.flush())
                # raise errors
                with pytest.raises(MutationsExceptionGroup) as exc2:
                    await flush_task2
                assert len(exc2.value.exceptions) == 1
                assert exc2.value.total_entries_attempted == 1
                assert exc2.value.exceptions[0].__cause__ == exception2

                # flushes should be finalized in order. flush_task1 should already be done
                assert flush_task1.done()
                with pytest.raises(MutationsExceptionGroup) as exc:
                    await flush_task1
                assert len(exc.value.exceptions) == 1
                assert exc2.value.total_entries_attempted == 1
                assert exc.value.exceptions[0].__cause__ == exception1

    @pytest.mark.asyncio
    async def test_schedule_flush_no_mutations(self):
        """schedule flush should return prev_flush if no new mutations"""
        async with self._make_one() as instance:
            orig_flush = instance._prev_flush
            with mock.patch.object(instance, "_flush_internal") as flush_mock:
                for i in range(3):
                    instance._schedule_flush()
                    assert flush_mock.call_count == 0
                    assert instance._prev_flush == orig_flush

    @pytest.mark.asyncio
    async def test_schedule_flush_with_mutations(self):
        """if new mutations exist, should update prev_flush to a new flush task"""
        async with self._make_one() as instance:
            orig_flush = instance._prev_flush
            with mock.patch.object(instance, "_flush_internal") as flush_mock:
                flush_mock.side_effect = lambda *args, **kwargs: setattr(
                    instance, "_next_flush_entries", []
                )
                for i in range(1, 4):
                    mutation = mock.Mock()
                    instance._staged_mutations = [mutation]
                    instance._schedule_flush()
                    assert instance._next_flush_entries == [mutation]
                    # let flush task run
                    await asyncio.sleep(0)
                    assert instance._staged_mutations == []
                    assert instance._staged_count == 0
                    assert instance._staged_bytes == 0
                    assert flush_mock.call_count == i
                    assert instance._prev_flush != orig_flush
                    orig_flush = instance._prev_flush

    @pytest.mark.asyncio
    async def test_schedule_flush_multiple_calls_single_flush(self):
        """
        If the user appends a bunch of entries above the flush limits back-to-back,
        it should still flush in a single task
        """
        from google.cloud.bigtable.mutations_batcher import MutationsBatcher

        with mock.patch.object(MutationsBatcher, "_execute_mutate_rows") as op_mock:
            async with self._make_one(flush_limit_mutation_count=1) as instance:
                # mock network calls
                async def mock_call(*args, **kwargs):
                    return []

                op_mock.side_effect = mock_call
                # append a bunch of entries back-to-back, without awaiting
                num_entries = 10
                for _ in range(num_entries):
                    instance._staged_mutations.append(_make_mutation())
                    instance._schedule_flush()
                    assert len(instance._staged_mutations) == 0
                # await to let flush run
                await asyncio.sleep(0)
                # should have batched into a single request
                assert op_mock.call_count == 1
                sent_batch = op_mock.call_args[0][0]
                assert len(sent_batch) == num_entries
                assert instance._staged_mutations == []

    @pytest.mark.asyncio
    async def test__flush_internal(self):
        """
        _flush_internal should:
          - await previous flush call
          - delegate batching to _flow_control
          - call _execute_mutate_rows on each batch
          - update self.exceptions and self._entries_processed_since_last_raise
        """
        num_entries = 10
        async with self._make_one() as instance:
            with mock.patch.object(instance, "_execute_mutate_rows") as execute_mock:
                with mock.patch.object(
                    instance._flow_control, "add_to_flow"
                ) as flow_mock:
                    # mock flow control to always return a single batch
                    async def gen(x):
                        yield x

                    flow_mock.side_effect = lambda x: gen(x)
                    prev_flush_mock = AsyncMock()
                    prev_flush = prev_flush_mock.__call__()
                    mutations = [_make_mutation(count=1, size=1)] * num_entries
                    instance._next_flush_entries = mutations
                    await instance._flush_internal(prev_flush)
                    assert prev_flush_mock.await_count == 1
                    assert instance._entries_processed_since_last_raise == num_entries
                    assert execute_mock.call_count == 1
                    assert flow_mock.call_count == 1
                    assert instance.exceptions == []

    @pytest.mark.parametrize(
        "num_starting,num_new_errors,expected_total_errors",
        [
            (0, 0, 0),
            (0, 1, 1),
            (0, 2, 2),
            (1, 0, 1),
            (1, 1, 2),
            (10, 2, 12),
        ],
    )
    @pytest.mark.asyncio
    async def test__flush_internal_with_errors(
        self, num_starting, num_new_errors, expected_total_errors
    ):
        """
        errors returned from _execute_mutate_rows should be added to self.exceptions
        """
        from google.cloud.bigtable import exceptions

        num_entries = 10
        expected_errors = [
            exceptions.FailedMutationEntryError(mock.Mock(), mock.Mock(), ValueError())
        ] * num_new_errors
        async with self._make_one() as instance:
            instance.exceptions = [mock.Mock()] * num_starting
            with mock.patch.object(instance, "_execute_mutate_rows") as execute_mock:
                execute_mock.return_value = expected_errors
                with mock.patch.object(
                    instance._flow_control, "add_to_flow"
                ) as flow_mock:
                    # mock flow control to always return a single batch
                    async def gen(x):
                        yield x

                    flow_mock.side_effect = lambda x: gen(x)
                    prev_flush_mock = AsyncMock()
                    prev_flush = prev_flush_mock.__call__()
                    mutations = [_make_mutation(count=1, size=1)] * num_entries
                    instance._next_flush_entries = mutations
                    await instance._flush_internal(prev_flush)
                    assert prev_flush_mock.await_count == 1
                    assert instance._entries_processed_since_last_raise == num_entries
                    assert execute_mock.call_count == 1
                    assert flow_mock.call_count == 1
                    assert len(instance.exceptions) == expected_total_errors
                    for i in range(num_starting, expected_total_errors):
                        assert (
                            instance.exceptions[i] == expected_errors[i - num_starting]
                        )
            instance.exceptions = []

    async def _mock_gapic_return(self, num=5):
        from google.cloud.bigtable_v2.types import MutateRowsResponse
        from google.rpc import status_pb2

        async def gen(num):
            for i in range(num):
                entry = MutateRowsResponse.Entry(
                    index=i, status=status_pb2.Status(code=0)
                )
                yield MutateRowsResponse(entries=[entry])

        return gen(num)

    @pytest.mark.asyncio
    async def test_manual_flush_end_to_end(self):
        """Test full flush process with minimal mocking"""
        num_nutations = 10
        mutations = [_make_mutation(count=2, size=2)] * num_nutations

        async with self._make_one(
            flow_control_max_count=3, flow_control_max_bytes=3
        ) as instance:
            instance._table.default_operation_timeout = 10
            instance._table.default_per_request_timeout = 9
            with mock.patch.object(
                instance._table.client._gapic_client, "mutate_rows"
            ) as gapic_mock:
                gapic_mock.side_effect = (
                    lambda *args, **kwargs: self._mock_gapic_return(num_nutations)
                )
                for m in mutations:
                    instance.append(m)
                assert instance._entries_processed_since_last_raise == 0
                await instance.flush()
                assert instance._entries_processed_since_last_raise == num_nutations

    @pytest.mark.asyncio
    async def test_timer_flush_end_to_end(self):
        """Flush should automatically trigger after flush_interval"""
        num_nutations = 10
        mutations = [_make_mutation(count=2, size=2)] * num_nutations

        async with self._make_one(flush_interval=0.05) as instance:
            instance._table.default_operation_timeout = 10
            instance._table.default_per_request_timeout = 9
            with mock.patch.object(
                instance._table.client._gapic_client, "mutate_rows"
            ) as gapic_mock:
                gapic_mock.side_effect = (
                    lambda *args, **kwargs: self._mock_gapic_return(num_nutations)
                )
                for m in mutations:
                    instance.append(m)
                assert instance._entries_processed_since_last_raise == 0
                # let flush trigger due to timer
                await asyncio.sleep(0.1)
                assert instance._entries_processed_since_last_raise == num_nutations

    @pytest.mark.asyncio
    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher._MutateRowsOperation",
    )
    async def test__execute_mutate_rows(self, mutate_rows):
        mutate_rows.return_value = AsyncMock()
        start_operation = mutate_rows().start
        table = mock.Mock()
        table.table_name = "test-table"
        table.app_profile_id = "test-app-profile"
        table.default_operation_timeout = 17
        table.default_per_request_timeout = 13
        async with self._make_one(table) as instance:
            batch = [_make_mutation()]
            result = await instance._execute_mutate_rows(batch)
            assert start_operation.call_count == 1
            args, _ = mutate_rows.call_args
            assert args[0] == table.client._gapic_client
            assert args[1] == table
            assert args[2] == batch
            assert args[3] == 17
            assert args[4] == 13
            assert result == []

    @pytest.mark.asyncio
    @unittest.mock.patch(
        "google.cloud.bigtable.mutations_batcher._MutateRowsOperation.start"
    )
    async def test__execute_mutate_rows_returns_errors(self, mutate_rows):
        """Errors from operation should be retruned as list"""
        from google.cloud.bigtable.exceptions import (
            MutationsExceptionGroup,
            FailedMutationEntryError,
        )

        err1 = FailedMutationEntryError(0, mock.Mock(), RuntimeError("test error"))
        err2 = FailedMutationEntryError(1, mock.Mock(), RuntimeError("test error"))
        mutate_rows.side_effect = MutationsExceptionGroup([err1, err2], 10)
        table = mock.Mock()
        table.default_operation_timeout = 17
        async with self._make_one(table) as instance:
            batch = [_make_mutation()]
            result = await instance._execute_mutate_rows(batch)
            assert len(result) == 2
            assert result[0] == err1
            assert result[1] == err2
            # indices should be set to None
            assert result[0].index is None
            assert result[1].index is None

    @pytest.mark.asyncio
    async def test__raise_exceptions(self):
        """Raise exceptions and reset error state"""
        from google.cloud.bigtable import exceptions

        expected_total = 1201
        expected_exceptions = [mock.Mock()] * 3
        async with self._make_one() as instance:
            instance.exceptions = expected_exceptions
            instance._entries_processed_since_last_raise = expected_total
            try:
                instance._raise_exceptions()
            except exceptions.MutationsExceptionGroup as exc:
                assert list(exc.exceptions) == expected_exceptions
                assert str(expected_total) in str(exc)
            assert instance._entries_processed_since_last_raise == 0
            assert instance.exceptions == []
            # try calling again
            instance._raise_exceptions()

    @pytest.mark.asyncio
    async def test___aenter__(self):
        """Should return self"""
        async with self._make_one() as instance:
            assert await instance.__aenter__() == instance

    @pytest.mark.asyncio
    async def test___aexit__(self):
        """aexit should call close"""
        async with self._make_one() as instance:
            with mock.patch.object(instance, "close") as close_mock:
                await instance.__aexit__(None, None, None)
                assert close_mock.call_count == 1

    @pytest.mark.asyncio
    async def test_close(self):
        """Should clean up all resources"""
        async with self._make_one() as instance:
            with mock.patch.object(instance, "_schedule_flush") as flush_mock:
                with mock.patch.object(instance, "_raise_exceptions") as raise_mock:
                    await instance.close()
                    assert instance.closed is True
                    assert instance._flush_timer_task.done() is True
                    assert instance._prev_flush.done() is True
                    assert flush_mock.call_count == 1
                    assert raise_mock.call_count == 1

    @pytest.mark.asyncio
    async def test_close_w_exceptions(self):
        """Raise exceptions on close"""
        from google.cloud.bigtable import exceptions

        expected_total = 10
        expected_exceptions = [mock.Mock()]
        async with self._make_one() as instance:
            instance.exceptions = expected_exceptions
            instance._entries_processed_since_last_raise = expected_total
            try:
                await instance.close()
            except exceptions.MutationsExceptionGroup as exc:
                assert list(exc.exceptions) == expected_exceptions
                assert str(expected_total) in str(exc)
            assert instance._entries_processed_since_last_raise == 0
            assert instance.exceptions == []

    @pytest.mark.asyncio
    async def test__on_exit(self, recwarn):
        """Should raise warnings if unflushed mutations exist"""
        async with self._make_one() as instance:
            # calling without mutations is noop
            instance._on_exit()
            assert len(recwarn) == 0
            # calling with existing mutations should raise warning
            num_left = 4
            instance._staged_mutations = [mock.Mock()] * num_left
            with pytest.warns(UserWarning) as w:
                instance._on_exit()
                assert len(w) == 1
                assert "unflushed mutations" in str(w[0].message).lower()
                assert str(num_left) in str(w[0].message)
            # calling while closed is noop
            instance.closed = True
            instance._on_exit()
            assert len(recwarn) == 0
            # reset staged mutations for cleanup
            instance._staged_mutations = []

    @pytest.mark.asyncio
    async def test_atexit_registration(self):
        """Should run _on_exit on program termination"""
        import atexit

        with mock.patch(
            "google.cloud.bigtable.mutations_batcher.MutationsBatcher._on_exit"
        ) as on_exit_mock:
            async with self._make_one():
                assert on_exit_mock.call_count == 0
                atexit._run_exitfuncs()
                assert on_exit_mock.call_count == 1
        # should not call after close
        atexit._run_exitfuncs()
        assert on_exit_mock.call_count == 1
