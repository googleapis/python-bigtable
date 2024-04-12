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
#
from __future__ import annotations

from typing import TYPE_CHECKING

import concurrent.futures
import atexit

from google.cloud.bigtable.data._sync._autogen import _FlowControl_SyncGen
from google.cloud.bigtable.data._sync._autogen import MutationsBatcher_SyncGen

# import required so MutationsBatcher_SyncGen can create _MutateRowsOperation
import google.cloud.bigtable.data._sync._mutate_rows  # noqa: F401

if TYPE_CHECKING:
    from google.cloud.bigtable.data.exceptions import FailedMutationEntryError


class _FlowControl(_FlowControl_SyncGen):
    pass


class MutationsBatcher(MutationsBatcher_SyncGen):

    @property
    def _executor(self):
        """
        Return a ThreadPoolExecutor for background tasks
        """
        if not hasattr(self, "_threadpool"):
            self._threadpool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        return self._threadpool

    def close(self):
        """
        Flush queue and clean up resources
        """
        self._closed.set()
        # attempt cancel timer if not started
        self._flush_timer.cancel()
        self._schedule_flush()
        self._executor.shutdown(wait=True)
        atexit.unregister(self._on_exit)
        # raise unreported exceptions
        self._raise_exceptions()

    def _create_bg_task(self, func, *args, **kwargs):
        return self._executor.submit(func, *args, **kwargs)

    @staticmethod
    def _wait_for_batch_results(
        *tasks: concurrent.futures.Future[list[FailedMutationEntryError]]
        | concurrent.futures.Future[None],
    ) -> list[Exception]:
        if not tasks:
            return []
        exceptions = []
        for task in tasks:
            try:
                exc_list = task.result()
                for exc in exc_list:
                    # strip index information
                    exc.index = None
                exceptions.extend(exc_list)
            except Exception as e:
                exceptions.append(e)
        return exceptions

    def _timer_routine(self, interval: float | None) -> None:
        """
        Triggers new flush tasks every `interval` seconds
        Ends when the batcher is closed
        """
        if not interval or interval <= 0:
            return None
        while not self._closed.is_set():
            # wait until interval has passed, or until closed
            self._closed.wait(timeout=interval)
            if not self._closed.is_set() and self._staged_entries:
                self._schedule_flush()

