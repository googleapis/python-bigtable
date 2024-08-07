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

# flake8: noqa
from .._testing import TYPE_INT, split_bytes_into_chunks, proto_rows_bytes


try:
    # async mock for python3.7-10
    from unittest.mock import Mock
    from asyncio import coroutine

    def async_mock(return_value=None):
        coro = Mock(name="CoroutineResult")
        corofunc = Mock(name="CoroutineFunction", side_effect=coroutine(coro))
        corofunc.coro = coro
        corofunc.coro.return_value = return_value
        return corofunc

except ImportError:
    # async mock for python3.11 or later
    from unittest.mock import AsyncMock

    def async_mock(return_value=None):
        return AsyncMock(return_value=return_value)
