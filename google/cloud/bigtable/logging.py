# Copyright 2024 Google LLC
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

import uuid
import grpc
import time
import datetime

def log_usage(func, name=None):
    def wrapper(*args, **kwargs):
        call_id = uuid.uuid4()
        fn_name = name or str(func).split()[1]
        start_time = time.monotonic()
        print(f"Entering {fn_name}(args={args}, kwargs={kwargs}). (call_id={call_id}, utc_time={datetime.datetime.utcnow()})")
        try:
            result = func(*args, **kwargs)
            print(f"Exiting {fn_name} with result={result} (call_id={call_id}, elapsed_time={time.monotonic() - start_time}, utc_time={datetime.datetime.utcnow()})")
            return result
        except Exception as e:
            print(f"Exiting {fn_name} with exception={e} (call_id={call_id}, elapsed_time={time.monotonic() - start_time}, utc_time={datetime.datetime.utcnow()})")
            raise

    return wrapper


class WrappedMultiCallable:
    def __init__(self, channel, *args, **kwargs):
        self._init_args = args
        self._init_kwargs = kwargs
        self._channel = channel

    def with_call(self, *args, **kwargs):
        raise NotImplementedError()

    def future(self, *args, **kwargs):
        raise NotImplementedError()


class WrappedUnaryUnaryMultiCallable(WrappedMultiCallable, grpc.UnaryUnaryMultiCallable):
    @log_usage
    def __call__(self, *args, **kwargs):
        return self._channel.unary_unary(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class WrappedUnaryStreamMultiCallable(WrappedMultiCallable, grpc.UnaryStreamMultiCallable):
    @log_usage
    def __call__(self, *args, **kwargs):
        return self._channel.unary_stream(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class WrappedStreamUnaryMultiCallable(WrappedMultiCallable, grpc.StreamUnaryMultiCallable):
    @log_usage
    def __call__(self, *args, **kwargs):
        return self._channel.stream_unary(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class WrappedStreamStreamMultiCallable(
    WrappedMultiCallable, grpc.StreamStreamMultiCallable
):
    @log_usage
    def __call__(self, *args, **kwargs):
        return self._channel.stream_stream(
            *self._init_args, **self._init_kwargs
        )(*args, **kwargs)


class WrappedChannel(grpc.Channel):
    def __init__(
        self,
        channel,
    ):
        self._channel = channel


    @log_usage
    def unary_unary(self, *args, **kwargs) -> grpc.UnaryUnaryMultiCallable:
        return WrappedUnaryUnaryMultiCallable(self._channel, *args, **kwargs)

    @log_usage
    def unary_stream(self, *args, **kwargs) -> grpc.UnaryStreamMultiCallable:
        return WrappedUnaryStreamMultiCallable(self._channel, *args, **kwargs)

    @log_usage
    def stream_unary(self, *args, **kwargs) -> grpc.StreamUnaryMultiCallable:
        return WrappedStreamUnaryMultiCallable(self._channel, *args, **kwargs)

    @log_usage
    def stream_stream(self, *args, **kwargs) -> grpc.StreamStreamMultiCallable:
        return WrappedStreamStreamMultiCallable(self._channel, *args, **kwargs)

    @log_usage
    def close(self):
        return self._channel.close()

    @log_usage
    def __enter__(self):
        return self._channel.__enter__()

    @log_usage
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._channel.__exit__(exc_type, exc_val, exc_tb)

    @log_usage
    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        raise NotImplementedError()

    @log_usage
    def wait_for_state_change(self, last_observed_state):
        raise NotImplementedError()

    @log_usage
    def subscribe(
        self, callback, try_to_connect: bool = False
    ) -> grpc.ChannelConnectivity:
        raise NotImplementedError()

    @log_usage
    def unsubscribe(self, callback):
        raise NotImplementedError()
