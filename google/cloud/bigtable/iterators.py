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
#
from __future__ import annotations

from typing import (
    cast,
    AsyncIterable,
)
import asyncio
import time
import sys

from google.cloud.bigtable._read_rows import _ReadRowsOperation
from google.cloud.bigtable_v2.types import RequestStats
from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.exceptions import RetryExceptionGroup
from google.cloud.bigtable.exceptions import IdleTimeout
from google.cloud.bigtable.row import Row


class ReadRowsIterator(AsyncIterable[Row]):
    """
    Async iterator for ReadRows responses.
    """

    def __init__(self, merger: _ReadRowsOperation):
        self._merger: _ReadRowsOperation = merger
        self._error: Exception | None = None
        self.request_stats: RequestStats | None = None
        self.last_interaction_time = time.time()
        self._idle_timeout_task: asyncio.Task[None] | None = None

    async def _start_idle_timer(self, idle_timeout: float):
        """
        Start a coroutine that will cancel a stream if no interaction
        with the iterator occurs for the specified number of seconds.

        Subsequent access to the iterator will raise an IdleTimeout exception.

        Args:
          - idle_timeout: number of seconds of inactivity before cancelling the stream
        """
        self.last_interaction_time = time.time()
        if self._idle_timeout_task is not None:
            self._idle_timeout_task.cancel()
        self._idle_timeout_task = asyncio.create_task(
            self._idle_timeout_coroutine(idle_timeout)
        )
        if sys.version_info >= (3, 8):
            self._idle_timeout_task.name = "ReadRowsIterator._idle_timeout"

    @property
    def active(self):
        """
        Returns True if the iterator is still active and has not been closed
        """
        return self._error is None

    async def _idle_timeout_coroutine(self, idle_timeout: float):
        """
        Coroutine that will cancel a stream if no interaction with the iterator
        in the last `idle_timeout` seconds.
        """
        while self.active:
            next_timeout = self.last_interaction_time + idle_timeout
            await asyncio.sleep(next_timeout - time.time())
            if self.last_interaction_time + idle_timeout < time.time() and self.active:
                # idle timeout has expired
                await self._finish_with_error(
                    IdleTimeout(
                        (
                            "Timed out waiting for next Row to be consumed. "
                            f"(idle_timeout={idle_timeout:0.1f}s)"
                        )
                    )
                )

    def __aiter__(self):
        """Implement the async iterator protocol."""
        return self

    async def __anext__(self) -> Row:
        """
        Implement the async iterator potocol.

        Return the next item in the stream if active, or
        raise an exception if the stream has been closed.
        """
        if self._error is not None:
            raise self._error
        try:
            self.last_interaction_time = time.time()
            next_item = await self._merger.__anext__()
            if isinstance(next_item, RequestStats):
                self.request_stats = next_item
                return await self.__anext__()
            else:
                return next_item
        except core_exceptions.RetryError:
            # raised by AsyncRetry after operation deadline exceeded
            new_exc = core_exceptions.DeadlineExceeded(
                f"operation_timeout of {self._merger.operation_timeout:0.1f}s exceeded"
            )
            source_exc = None
            if self._merger.transient_errors:
                source_exc = RetryExceptionGroup(
                    f"{len(self._merger.transient_errors)} failed attempts",
                    self._merger.transient_errors,
                )
            new_exc.__cause__ = source_exc
            await self._finish_with_error(new_exc)
            raise new_exc from source_exc
        except Exception as e:
            await self._finish_with_error(e)
            raise e

    async def _finish_with_error(self, e: Exception):
        """
        Helper function to close the stream and clean up resources
        after an error has occurred.
        """
        if self.active:
            await self._merger.aclose()
            self._error = e
        if self._idle_timeout_task is not None:
            self._idle_timeout_task.cancel()
            self._idle_timeout_task = None

    async def aclose(self):
        """
        Support closing the stream with an explicit call to aclose()
        """
        await self._finish_with_error(
            StopAsyncIteration(f"{self.__class__.__name__} closed")
        )
