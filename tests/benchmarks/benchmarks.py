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
"""
Contains set of benchmark classes for testing the performance of the
the v3 client compared to the v2 client.
"""

from _helpers import Benchmark
from google.cloud.bigtable_v2.types import ReadRowsResponse

# import test proxy handlers
import sys
sys.path.append("../../test_proxy")
import client_handler
import client_handler_legacy

class SimpleReads(Benchmark):
    """
    A large number of simple row reads.
    should test max throughput of read_rows
    """

    def __init__(self, num_rows=1e5, chunks_per_response=100, payload_size=10, simulate_latency=0):
        super().__init__(simulate_latency)
        self.num_rows = num_rows
        self.chunks_per_response = chunks_per_response
        self.payload_size = payload_size

    def server_responses(self, *args, **kwargs):
        sent_num = 0
        while sent_num < self.num_rows:
            batch_size = min(self.chunks_per_response, self.num_rows - sent_num)
            chunks = [
                ReadRowsResponse.CellChunk(
                    row_key=(sent_num + i).to_bytes(3, "big"),
                    family_name="F",
                    qualifier=b"Q",
                    value=("a" * int(self.payload_size)).encode(),
                    commit_row=True
                ) for i in range(batch_size)
            ]
            yield ReadRowsResponse(chunks=chunks)
            sent_num += batch_size

    async def client_setup(self, proxy_handler):
        request = {"table_name": "projects/project/instances/instance/tables/table"}
        return await proxy_handler.ReadRows(request)

