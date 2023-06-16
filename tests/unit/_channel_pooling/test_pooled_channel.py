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
from __future__ import annotations

import pytest

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


class TestPooledChannel:
    def _make_one_with_channel_mock(self, *args, async_mock=True, **kwargs):
        channel = AsyncMock() if async_mock else mock.Mock()
        return (
            self._make_one(*args, create_channel_fn=lambda: channel, **kwargs),
            channel,
        )

    def _get_target(self):
        from google.cloud.bigtable._channel_pooling.pooled_channel import PooledChannel

        return PooledChannel

    def _make_one(self, *args, async_mock=True, **kwargs):
        from google.cloud.bigtable._channel_pooling.pooled_channel import (
            StaticPoolOptions,
        )

        mock_type = AsyncMock if async_mock else mock.Mock
        kwargs.setdefault("create_channel_fn", lambda *args, **kwargs: mock_type())
        pool_size = kwargs.pop("pool_size", 3)
        kwargs.setdefault("pool_options", StaticPoolOptions(pool_size=pool_size))
        return self._get_target()(*args, **kwargs)

    def test_ctor(self):
        """
        test that constuctor sets starting values
        """
        channel_fn = mock.Mock()
        channel_fn.side_effect = lambda *args, **kwargs: mock.Mock()
        expected_pool_options = mock.Mock()
        expected_pool_size = 12
        expected_pool_options.pool_size = expected_pool_size
        extra_args = ["a", "b"]
        extra_kwargs = {"c": "d"}
        instance = self._make_one(
            *extra_args,
            create_channel_fn=channel_fn,
            pool_options=expected_pool_options,
            **extra_kwargs,
        )
        assert instance._create_channel.func == channel_fn
        assert instance._create_channel.args == tuple(extra_args)
        assert instance._create_channel.keywords == extra_kwargs
        assert len(instance.channels) == expected_pool_size
        assert channel_fn.call_count == expected_pool_size
        assert channel_fn.call_args == mock.call(*extra_args, **extra_kwargs)
        # ensure each channel is unique
        assert len(set(instance.channels)) == expected_pool_size

    def test_ctor_defaults(self):
        """
        test with minimal arguments
        """
        channel_fn = mock.Mock()
        channel_fn.side_effect = lambda *args, **kwargs: mock.Mock()
        instance = self._make_one(
            create_channel_fn=channel_fn,
        )
        assert instance._create_channel.func == channel_fn
        assert instance._create_channel.args == tuple()
        assert instance._create_channel.keywords == {}
        assert len(instance.channels) == 3  # default size
        assert channel_fn.call_count == 3
        assert channel_fn.call_args == mock.call()
        # ensure each channel is unique
        assert len(set(instance.channels)) == 3

    def test_ctor_no_create_fn(self):
        """
        test that constuctor raises error if no create_channel_fn is provided
        """
        with pytest.raises(ValueError) as exc:
            self._get_target()()
        assert "create_channel_fn" in str(exc.value)

    @pytest.mark.parametrize("pool_size", [1, 2, 7, 10, 100])
    @pytest.mark.asyncio
    async def test_next_channel(self, pool_size):
        """
        next_channel should rotate between channels
        """
        async with self._make_one(pool_size=pool_size) as instance:
            # ensure each channel is unique
            assert len(set(instance.channels)) == pool_size
            # make sure next_channel loops through all channels as expected
            instance._next_idx = 0
            expected_results = [
                instance.channels[i % pool_size] for i in range(pool_size * 2)
            ]
            for idx, expected_channel in enumerate(expected_results):
                expected_next_idx = idx % pool_size
                assert instance._next_idx == expected_next_idx
                assert instance.next_channel() is expected_channel
                # next_idx should be updated
                assert instance._next_idx == (expected_next_idx + 1) % pool_size

    def test___getitem__(self):
        """
        should be able to index on pool directly
        """
        instance = self._make_one(pool_size=3)
        assert instance[1] is instance.channels[1]

    @pytest.mark.asyncio
    async def test_next_channel_unexpected_next(self):
        """
        if _next_idx ends up out of bounds, it should be reset to 0
        """
        pool_size = 3
        async with self._make_one(pool_size=pool_size) as instance:
            assert instance._next_idx == 0
            instance.next_channel()
            assert instance._next_idx == 1
            # try to put it out of bounds
            instance._next_idx = 100
            assert instance._next_idx == 100
            instance.next_channel()
            assert instance._next_idx == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method_name", ["unary_unary", "stream_unary"])
    async def test_unary_call_api_passthrough(self, method_name):
        """
        rpc call methods should use underlying channel calls
        """
        mock_rpc = AsyncMock()
        call_mock = mock_rpc.call()
        callable_mock = lambda: call_mock  # noqa: E731
        instance, channel = self._make_one_with_channel_mock(
            async_mock=False, pool_size=1
        )
        with mock.patch.object(instance, "next_channel") as next_channel_mock:
            next_channel_mock.return_value = channel
            channel_method = getattr(channel, method_name)
            wrapper_method = getattr(instance, method_name)
            channel_method.return_value = callable_mock
            # call rpc to get Multicallable
            arg_mock = mock.Mock()
            found_callable = wrapper_method(arg_mock)
            # assert that response was passed through
            found_call = found_callable()
            await found_call
            # assert that wrapped channel method was called
            assert mock_rpc.call.await_count == 1
            # combine args and kwargs
            all_args = list(channel_method.call_args.args) + list(
                channel_method.call_args.kwargs.values()
            )
            assert all_args == [arg_mock]
            assert channel_method.call_count == 1
            assert next_channel_mock.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method_name", ["unary_stream", "stream_stream"])
    async def test_stream_call_api_passthrough(self, method_name):
        """
        rpc call methods should use underlying channel calls
        """
        expected_result = mock.Mock()

        async def mock_stream():
            yield expected_result

        instance, channel = self._make_one_with_channel_mock(
            async_mock=False, pool_size=1
        )
        with mock.patch.object(instance, "next_channel") as next_channel_mock:
            next_channel_mock.return_value = channel
            channel_method = getattr(channel, method_name)
            wrapper_method = getattr(instance, method_name)
            channel_method.return_value = lambda: mock_stream()
            # call rpc to get Multicallable
            arg_mock = mock.Mock()
            found_callable = wrapper_method(arg_mock)
            # assert that response was passed through
            found_call = found_callable()
            results = [item async for item in found_call]
            assert results == [expected_result]
            # combine args and kwargs
            all_args = list(channel_method.call_args.args) + list(
                channel_method.call_args.kwargs.values()
            )
            assert all_args == [arg_mock]
            assert channel_method.call_count == 1
            assert next_channel_mock.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pool_size,calls,expected_call_count",
        [
            (1, 1, [1]),
            (1, 2, [2]),
            (2, 1, [1, 0]),
            (4, 4, [1, 1, 1, 1]),
            (4, 5, [2, 1, 1, 1]),
            (4, 6, [2, 2, 1, 1]),
            (4, 7, [2, 2, 2, 1]),
            (4, 0, [0, 0, 0, 0]),
        ],
    )
    async def test_rpc_call_rotates(self, pool_size, calls, expected_call_count):
        """
        rpc call methods should rotate between channels in the pool
        """
        for method_name in [
            "unary_unary",
            "stream_unary",
            "unary_stream",
            "stream_stream",
        ]:
            instance = self._make_one(async_mock=False, pool_size=pool_size)
            wrapper_method = getattr(instance, method_name)
            for call_num in range(calls):
                found_callable = wrapper_method()
                found_callable()
            assert len(expected_call_count) == len(instance.channels)
            for channel_num, expected_channel_call_count in enumerate(
                expected_call_count
            ):
                channel_method = getattr(instance[channel_num], method_name)
                assert channel_method.call_count == expected_channel_call_count

    @pytest.mark.asyncio
    async def test_unimplented_state_methods(self):
        """
        get_state and wait_for_state should raise NotImplementedError, because
        behavior is not defined for a pool of channels
        """
        instance = self._make_one(async_mock=False, pool_size=1)
        with pytest.raises(NotImplementedError):
            instance.get_state()
        with pytest.raises(NotImplementedError):
            await instance.wait_for_state_change(mock.Mock())

    @pytest.mark.parametrize(
        "method_name,arg_num",
        [
            ("close", 1),
            ("__aenter__", 0),
            ("__aexit__", 3),
        ],
    )
    @pytest.mark.asyncio
    async def test_async_api_passthrough(self, method_name, arg_num):
        """
        Wrapper should respond to full grpc Channel API, and pass through
        resonses to all wrapped channels
        """
        pool_size = 5
        instance = self._make_one(async_mock=True, pool_size=pool_size)
        wrapper_method = getattr(instance, method_name)
        # make function call
        args = [mock.Mock() for _ in range(arg_num)]
        await wrapper_method(*args)
        # assert that wrapped channel method was called for each channel
        for channel in instance.channels:
            channel_method = getattr(channel, method_name)
            assert channel_method.call_count == 1
            # combine args and kwargs
            all_args = list(channel_method.call_args.args) + list(
                channel_method.call_args.kwargs.values()
            )
            assert all_args == args

    @pytest.mark.asyncio
    async def test_channel_ready(self):
        """
        channel ready should block until all channels are ready
        """
        pool_size = 5
        instance = self._make_one(async_mock=True, pool_size=pool_size)
        await instance.channel_ready()
        for channel in instance.channels:
            assert channel.channel_ready.call_count == 1
            assert channel.channel_ready.await_count == 1

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """
        entering and exit should call enter and exit on all channels
        """
        channel_list = []
        async with self._make_one() as instance:
            for channel in instance.channels:
                assert channel.__aenter__.call_count == 1
                assert channel.__aenter__.await_count == 1
                assert channel.__aexit__.call_count == 0
            channel_list = instance.channels
        for channel in channel_list:
            assert channel.__aexit__.call_count == 1
            assert channel.__aexit__.await_count == 1

    def test_index_of(self):
        """
        index_of should return the index for each channel, or -1 if not in pool
        """
        pool_size = 5
        instance = self._make_one(async_mock=True, pool_size=pool_size)
        for channel_num, channel in enumerate(instance.channels):
            found_idx = instance.index_of(channel)
            assert found_idx == channel_num
        # test for channels not in list
        fake_channel = mock.Mock()
        found_idx = instance.index_of(fake_channel)
        assert found_idx == -1
