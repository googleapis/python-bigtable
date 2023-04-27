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

from typing import Any, TYPE_CHECKING

import asyncio

from google.api_core import exceptions as core_exceptions

if TYPE_CHECKING:
    from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
    from google.cloud.bigtable.mutations import BulkMutationsEntry


async def _mutate_rows_retryable_attempt(
    gapic_client:"BigtableAsyncClient",
    request : dict[str, Any],
    per_request_timeout : float | None,
    mutation_dict: dict[int, "BulkMutationsEntry"|None],
    error_dict: dict[int, list[Exception]],
):
    new_request = request.copy()
    while any(mutation is not None for mutation in mutation_dict.values()):
        await asyncio.sleep(0)
        # keep map between sub-request indices and global entry indices
        index_map : dict[int, int] = {}
        # continue to retry until timeout, or all mutations are complete (success or failure)
        request_entries : list[dict[str, Any]] = []
        for index, entry in mutation_dict.items():
            if entry is not None:
                index_map[len(request_entries)] = index
                request_entries.append(entry._to_dict())
        new_request["entries"] = request_entries
        async for result_list in await gapic_client.mutate_rows(
            new_request, timeout=per_request_timeout
        ):
            for result in result_list.entries:
                # convert sub-request index to global index
                idx = index_map[result.index]
                if result.status.code == 0:
                    # mutation succeeded
                    mutation_dict[idx] = None
                    error_dict[idx] = []
                if result.status.code != 0:
                    # mutation failed
                    exception = core_exceptions.from_grpc_status(
                        result.status.code,
                        result.status.message,
                        details=result.status.details,
                    )
                    error_dict[idx].append(exception)
                    # if not idempotent, remove from retry list
                    entry = mutation_dict[idx]
                    if entry is not None and not entry.is_idempotent():
                        mutation_dict[idx] = None

