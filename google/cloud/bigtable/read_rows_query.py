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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud.bigtable.row_filters import RowFilter
    from google.cloud.bigtable import RowKeySamples


class ReadRowsQuery:
    """
    Class to encapsulate details of a read row request
    """

    def __init__(
        self, row_keys: list[str | bytes] | str | bytes | None = None, limit=None
    ):
        pass

    def set_limit(self, limit: int) -> ReadRowsQuery:
        raise NotImplementedError

    def set_filter(self, filter: "RowFilter") -> ReadRowsQuery:
        raise NotImplementedError

    def add_rows(self, row_id_list: list[str]) -> ReadRowsQuery:
        raise NotImplementedError

    def add_range(
        self, start_key: str | bytes | None = None, end_key: str | bytes | None = None,
        start_is_open: bool = False, end_is_open: bool = False
    ) -> ReadRowsQuery:
        raise NotImplementedError

    def shard(self, shard_keys: "RowKeySamples" | None = None) -> list[ReadRowsQuery]:
        """
        Split this query into multiple queries that can be evenly distributed
        across nodes and be run in parallel

        Returns:
            - a list of queries that represent a sharded version of the original
              query (if possible)
        """
        raise NotImplementedError
