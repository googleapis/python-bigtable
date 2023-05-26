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


class SimpleReads(Benchmark):
    """
    A large number of simple row reads.
    should test max throughput of read_rows
    """

    def __init__(
        self, num_rows=1e5, chunks_per_response=100, payload_size=10, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
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
                    commit_row=True,
                )
                for i in range(batch_size)
            ]
            yield ReadRowsResponse(chunks=chunks)
            sent_num += batch_size

    async def client_setup(self, proxy_handler):
        request = {"table_name": "projects/project/instances/instance/tables/table"}
        return await proxy_handler.ReadRows(request)


class ComplexReads(Benchmark):
    """
    A more complex workload of rows, with multiple column families and qualifiers, and occasional dropped rows or retries
    """

    def __init__(
        self,
        num_rows=1e2,
        drop_every=None,
        cells_per_row=100,
        payload_size=10,
        continuation_num=1,
        *args,
        **kwargs,
    ):
        """
        Args:
          - num_rows: number of rows to send
          - drop_every: every nth row, send a reset_row to drop buffers
          - cells_per_row: number of cells to send per row
          - payload_size: size of each chunk payload value
          - continuation_num: number of continuation chunks to send for each cell
        """
        super().__init__(*args, **kwargs)
        self.num_rows = num_rows
        self.cells_per_row = cells_per_row
        self.payload_size = payload_size
        self.continuation_num = continuation_num
        self.drop_every = drop_every

    def server_responses(self, *args, **kwargs):
        for i in range(int(self.num_rows)):
            for j in range(int(self.cells_per_row)):
                # send initial chunk
                yield ReadRowsResponse(
                    chunks=[
                        ReadRowsResponse.CellChunk(
                            row_key=(i).to_bytes(3, "big") if j == 0 else None,
                            family_name=f"{j}",
                            qualifier=(j).to_bytes(3, "big"),
                            value=("a" * int(self.payload_size)).encode(),
                        )
                    ]
                )
                # send continuation of chunk
                for k in range(int(self.continuation_num)):
                    yield ReadRowsResponse(
                        chunks=[
                            ReadRowsResponse.CellChunk(
                                value=("a" * int(self.payload_size)).encode(),
                            )
                        ]
                    )
            if self.drop_every and i % self.drop_every == 0:
                # send reset row
                yield ReadRowsResponse(
                    chunks=[
                        ReadRowsResponse.CellChunk(
                            reset_row=True,
                        )
                    ]
                )
            else:
                # send commit row
                yield ReadRowsResponse(
                    chunks=[ReadRowsResponse.CellChunk(commit_row=True)]
                )

    async def client_setup(self, proxy_handler):
        request = {"table_name": "projects/project/instances/instance/tables/table"}
        return await proxy_handler.ReadRows(request)


class SimpleBulkMutations(Benchmark):
    """
    A large number of successful mutations
    """

    def __init__(
        self, num_mutations=1e5, num_per_response=100, payload_size=10, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.num_mutations = num_mutations
        self.payload_size = payload_size

    def server_responses(self, *args, **kwargs):
        from google.cloud.bigtable_v2.types import MutateRowsResponse
        sent_num = 0
        while sent_num < self.num_mutations:
            entries = [MutateRowsResponse.Entry(index=i) for i in range(self.num_per_response)]
            yield MutateRowsResponse(entries=entries)
            sent_num += self.num_per_response

    async def client_setup(self, proxy_handler):
        request = {"table_name": "projects/project/instances/instance/tables/table"}
        return await proxy_handler.BulkMutateRows(request)
