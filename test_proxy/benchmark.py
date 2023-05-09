from time import sleep
import mock
import asyncio
from abc import ABC, abstractmethod

import client_handler
import client_handler_legacy

from google.cloud.bigtable_v2.types import ReadRowsResponse


class Benchmark(ABC):

    def __init__(self, simulate_latency=0):
        self.simulate_latency = simulate_latency

    @abstractmethod
    def server_responses(self, *args, **kwargs):
        raise NotImplementedError

    def _server_responses_with_latency(self, *args, **kwargs):
        for response in self.server_responses(*args, **kwargs):
            yield response
            sleep(self.simulate_latency)

    async def _server_responses_with_latency_async(self, *args, **kwargs):
        async def inner():
            for response in self.server_responses(*args, **kwargs):
                yield response
                await asyncio.sleep(self.simulate_latency)
        return inner()


    def _server_mock_decorator(self, func):
        async def inner(*args, **kwargs):
            with mock.patch("google.cloud.bigtable_v2.services.bigtable.async_client.BigtableAsyncClient.read_rows") as mock_read_rows_async:
                with mock.patch("google.cloud.bigtable_v2.services.bigtable.client.BigtableClient.read_rows") as mock_read_rows:
                    mock_read_rows_async.side_effect = self._server_responses_with_latency_async
                    mock_read_rows.side_effect = self._server_responses_with_latency
                    return await func(*args, **kwargs)
        return inner

    @abstractmethod
    async def client_setup(self, proxy_handler):
        raise NotImplementedError

    async def run(self, proxy_handler):
        # reset profiler
        proxy_handler._profiler.clear_stats()
        # mock server responses
        wrapped = self._server_mock_decorator(self.client_setup)
        # run client code
        return await wrapped(proxy_handler)

class SimpleReads(Benchmark):
    """
    A large number of simple row reads
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


async def main():
    kwargs = {"enable_profiling":True, "enable_timing": True, "per_operation_timeout": 60*30, "raise_on_error": True}
    new_handler = client_handler.TestProxyClientHandler(**kwargs)
    legacy_handler = client_handler_legacy.LegacyTestProxyClientHandler(**kwargs)
    benchmark = SimpleReads(num_rows=1e3, simulate_latency=0)
    for handler in [new_handler, legacy_handler]:
        results = await benchmark.run(handler)
        print(f"Read {len(results)} rows in: {handler.total_time}s")
        handler.print_profile()
        handler._profiler.clear_stats()
        breakpoint()

if __name__ == "__main__":
    asyncio.run(main())
