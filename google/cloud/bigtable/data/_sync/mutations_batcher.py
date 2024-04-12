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

from concurrent.futures import ThreadPoolExecutor

from google.cloud.bigtable.data._sync._autogen import _FlowControl_SyncGen
from google.cloud.bigtable.data._sync._autogen import MutationsBatcher_SyncGen

# import required so MutationsBatcher_SyncGen can create _MutateRowsOperation
import google.cloud.bigtable.data._sync._mutate_rows  # noqa: F401

class _FlowControl(_FlowControl_SyncGen):
    pass


class MutationsBatcher(MutationsBatcher_SyncGen):

    def __init__(self, *args, **kwargs):
        self._executor = ThreadPoolExecutor(max_workers=8)
        super().__init__(*args, **kwargs)

    def _create_bg_task(self, func, *args, **kwargs):
        return self._executor.submit(func, *args, **kwargs)
