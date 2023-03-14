# -*- coding: utf-8 -*-
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

from typing import List, Tuple

from google.cloud.bigtable import gapic_version as package_version

from google.cloud.bigtable.client import BigtableDataClient
from google.cloud.bigtable.client import Table

from google.cloud.bigtable.read_rows_query import ReadRowsQuery
from google.cloud.bigtable.row_response import RowResponse
from google.cloud.bigtable.row_response import CellResponse

from google.cloud.bigtable.mutations_batcher import MutationsBatcher
from google.cloud.bigtable.mutations import Mutation
from google.cloud.bigtable.mutations import BulkMutationsEntry
from google.cloud.bigtable.mutations import SetCell
from google.cloud.bigtable.mutations import DeleteRangeFromColumn
from google.cloud.bigtable.mutations import DeleteAllFromFamily
from google.cloud.bigtable.mutations import DeleteAllFromRow

# Type alias for the output of sample_keys
RowKeySamples = List[Tuple[bytes, int]]

__version__: str = package_version.__version__

__all__ = (
    "BigtableDataClient",
    "Table",
    "RowKeySamples",
    "ReadRowsQuery",
    "MutationsBatcher",
    "Mutation",
    "BulkMutationsEntry",
    "SetCell",
    "DeleteRangeFromColumn",
    "DeleteAllFromFamily",
    "DeleteAllFromRow",
    "RowResponse",
    "CellResponse",
)
