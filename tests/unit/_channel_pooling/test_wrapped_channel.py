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

import asyncio
import pytest

from grpc.experimental import aio  # type: ignore

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


class TestWrappedChannel:
    """
    WrappedChannel should delegate all methods to the wrapped channel
    """

    def _make_one_with_channel_mock(self, *args, async_mock=True, **kwargs):
        channel = AsyncMock() if async_mock else mock.Mock()
        return self._make_one(channel), channel

    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable._channel_pooling.wrapped_channel import (
            _WrappedChannel,
        )

        return _WrappedChannel(*args, **kwargs)

    def test_ctor(self):
        channel = mock.Mock()
        instance = self._make_one(channel)
        assert instance._channel is channel

    def test_is_channel_instance(self):
        """
        should pass isinstance check for aio.Channel
        """
        instance = self._make_one(mock.Mock())
        assert isinstance(instance, aio.Channel)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method_name", ["unary_unary", "stream_unary"])
    async def test_unary_call_api_passthrough(self, method_name):
        """
        rpc call methods should use underlying channel calls
        """
        mock_rpc = AsyncMock()
        call_mock = mock_rpc.call()
        callable_mock = lambda: call_mock  # noqa: E731
        instance, channel = self._make_one_with_channel_mock(async_mock=False)
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method_name", ["unary_stream", "stream_stream"])
    async def test_stream_call_api_passthrough(self, method_name):
        """
        rpc call methods should use underlying channel calls
        """
        expected_result = mock.Mock()

        async def mock_stream():
            yield expected_result

        instance, channel = self._make_one_with_channel_mock(async_mock=False)
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

    @pytest.mark.parametrize("method_name", ["get_state"])
    def test_sync_api_passthrough(self, method_name):
        """
        Wrapper should respond to full grpc Channel API, and pass through
        resonses to wrapped channel
        """
        expected_response = "response"
        instance, channel = self._make_one_with_channel_mock(async_mock=False)
        channel_method = getattr(channel, method_name)
        wrapper_method = getattr(instance, method_name)
        channel_method.return_value = expected_response
        # make function call
        arg_mock = mock.Mock()
        found_response = wrapper_method(arg_mock)
        # assert that wrapped channel method was called
        assert channel_method.call_count == 1
        # combine args and kwargs
        all_args = list(channel_method.call_args.args) + list(
            channel_method.call_args.kwargs.values()
        )
        assert all_args == [arg_mock]
        # assert that response was passed through
        assert found_response == expected_response

    @pytest.mark.parametrize(
        "method_name,arg_num",
        [
            ("close", 1),
            ("channel_ready", 0),
            ("__aenter__", 0),
            ("__aexit__", 3),
            ("wait_for_state_change", 1),
        ],
    )
    @pytest.mark.asyncio
    async def test_async_api_passthrough(self, method_name, arg_num):
        """
        Wrapper should respond to full grpc Channel API, and pass through
        resonses to wrapped channel
        """
        expected_response = "response"
        instance, channel = self._make_one_with_channel_mock(async_mock=True)
        channel_method = getattr(channel, method_name)
        wrapper_method = getattr(instance, method_name)
        channel_method.return_value = expected_response
        # make function call
        args = [mock.Mock() for _ in range(arg_num)]
        found_response = await wrapper_method(*args)
        # assert that wrapped channel method was called
        channel_method.assert_called_once()
        channel_method.assert_awaited_once()
        # combine args andkwargs
        all_args = list(channel_method.call_args.args) + list(
            channel_method.call_args.kwargs.values()
        )
        assert all_args == args
        # assert that response was passed through
        if not method_name == "__aenter__":
            assert found_response == expected_response
        else:
            # __aenter__ is special case: should return self
            assert found_response is instance

    def test_wrapped_channel_property(self):
        """
        Should be able to access wrapped channel
        """
        instance, channel = self._make_one_with_channel_mock()
        assert instance.wrapped_channel is channel

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """
        entering and exit should call enter and exit on wrapped channel
        """
        instance, channel = self._make_one_with_channel_mock()
        assert channel.__aenter__.call_count == 0
        async with instance:
            assert channel.__aenter__.call_count == 1
            assert channel.__aenter__.await_count == 1
            assert channel.__aexit__.call_count == 0
        assert channel.__aexit__.call_count == 1
        assert channel.__aexit__.await_count == 1


class TestBackgroundTaskMixin:
    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable._channel_pooling.wrapped_channel import (
            _BackgroundTaskMixin,
        )

        class ConcreteBackgroundTask(_BackgroundTaskMixin):
            @property
            def _task_description(self):
                return "Fake task"

            def _background_coroutine(self):
                return self._fake_background_coroutine()

            async def _fake_background_coroutine(self):
                await asyncio.sleep(0.1)
                return "fake response"

        return ConcreteBackgroundTask(*args, **kwargs)

    def test_ctor(self):
        """all _BackgroundTaskMixin classes should a _background_task attribute"""
        instance = self._make_one()
        assert hasattr(instance, "_background_task")

    @pytest.mark.asyncio
    async def test_aenter_starts_task(self):
        """
        Context manager should start background task
        """
        instance = self._make_one()
        with mock.patch.object(instance, "start_background_task") as start_mock:
            async with instance:
                start_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_aexit_stops_task(self):
        """
        Context manager should stop background task
        """
        instance = self._make_one()
        async with instance:
            assert instance._background_task is not None
            assert instance._background_task.cancelled() is False
        assert instance._background_task.cancelled() is True

    @pytest.mark.asyncio
    async def test_close_stops_task(self):
        """Calling close directly should cancel background task"""
        instance = self._make_one()
        instance.start_background_task()
        assert instance._background_task is not None
        assert instance._background_task.cancelled() is False
        await instance.close()
        assert instance._background_task.cancelled() is True

    @pytest.mark.asyncio
    async def test_start_background_task(self):
        """test that task can be started properly"""
        instance = self._make_one()
        assert instance._background_task is None
        instance.start_background_task()
        assert instance._background_task is not None
        assert instance._background_task.done() is False
        assert instance._background_task.cancelled() is False
        assert isinstance(instance._background_task, asyncio.Task)
        await instance.close()

    @pytest.mark.asyncio
    async def test_start_background_task_idempotent(self):
        """Duplicate calls to start_background_task should be no-ops"""
        with mock.patch("asyncio.get_running_loop") as get_loop_mock:
            instance = self._make_one()
            assert get_loop_mock.call_count == 0
            instance.start_background_task()
            assert get_loop_mock.call_count == 1
            instance.start_background_task()
            assert get_loop_mock.call_count == 1
            await instance.close()

    def test_start_background_task_sync(self):
        """In sync context, should raise RuntimeWarning that routine can't be started"""
        instance = self._make_one()
        with pytest.warns(RuntimeWarning) as warnings:
            instance.start_background_task()
        assert instance._background_task is None
        assert len(warnings) == 1
        assert "No event loop detected." in str(warnings[0].message)
        assert instance._task_description + " is disabled" in str(warnings[0].message)

    def test__task_description(self):
        """all _BackgroundTaskMixin classes should a _trask_description method"""
        instance = self._make_one()
        assert isinstance(instance._task_description, str)
        # should start with a capital letter for proper formatting in start_background_task warning
        assert instance._task_description[0].isupper()

    @pytest.mark.parametrize(
        "task,is_done,expected",
        [(None, None, False), (mock.Mock(), False, True), (mock.Mock(), True, False)],
    )
    def test_is_active_w_mock(self, task, is_done, expected):
        """
        test all possible branches in background_task_is_active with mocks
        """
        instance = self._make_one()
        instance._background_task = task
        if is_done is not None:
            instance._background_task.done.return_value = is_done
        assert instance.background_task_is_active() == expected

    @pytest.mark.asyncio
    async def test_is_active(self):
        """
        test background_task_is_active with real task
        """
        instance = self._make_one()
        assert instance.background_task_is_active() is False
        instance.start_background_task()
        assert instance.background_task_is_active() is True
        await instance.close()
        assert instance.background_task_is_active() is False


class _WrappedMultiCallableBase:
    """
    Base class for testing wrapped multicallables
    """

    pass


class TestWrappedUnaryUnaryMultiCallable(_WrappedMultiCallableBase):
    pass


class TestWrappedUnaryStreamMultiCallable(_WrappedMultiCallableBase):
    pass


class TestWrappedStreamUnaryMultiCallable(_WrappedMultiCallableBase):
    pass


class TestWrappedStreamStreamMultiCallable(_WrappedMultiCallableBase):
    pass
