from time import sleep
import mock
import asyncio

import client_handler
import client_handler_legacy

from google.cloud.bigtable_v2.types import ReadRowsResponse

def simple_reads(*, simulate_latency=0, num_rows=1e5, payload_size=10, chunks_per_response=100):
    """
    A large number of simple row reads
    should test max throughput of read_rows
    """

    def server_response_fn_sync(*args, latency_arg=None, **kwargs):
        sent_num = 0
        while sent_num < num_rows:
            batch_size = min(chunks_per_response, num_rows - sent_num)
            chunks = [
                ReadRowsResponse.CellChunk(
                    row_key=(sent_num + i).to_bytes(3, "big"),
                    family_name="F",
                    qualifier=b"Q",
                    value=("a" * int(payload_size)).encode(),
                    commit_row=True
                ) for i in range(batch_size)
            ]
            sleep(latency_arg if latency_arg is not None else simulate_latency)
            yield ReadRowsResponse(chunks=chunks)
            sent_num += batch_size

    async def server_response_fn(*args, **kwargs):
        async def inner():
            for response in server_response_fn_sync(*args, **kwargs, latency_arg=0):
                yield response
                await asyncio.sleep(simulate_latency)
        return inner()

    async def client_fn(proxy_handler):
        with mock.patch("google.cloud.bigtable_v2.services.bigtable.async_client.BigtableAsyncClient.read_rows") as mock_read_rows_async:
            with mock.patch("google.cloud.bigtable_v2.services.bigtable.client.BigtableClient.read_rows") as mock_read_rows:
                mock_read_rows_async.side_effect = server_response_fn
                mock_read_rows.side_effect = server_response_fn_sync
                request = {"table_name": "projects/project/instances/instance/tables/table"}
                results = await proxy_handler.ReadRows(request)
                if isinstance(results, dict) and results.get("error"):
                    print(results["error"])
                return results

    return client_fn

async def main():
    kwargs = {"enable_profiling":True,   "enable_timing": True, "per_operation_timeout": 60*30}
    new_handler = client_handler.TestProxyClientHandler(**kwargs)
    legacy_handler = client_handler_legacy.LegacyTestProxyClientHandler(**kwargs)
    benchmark_fn = simple_reads(simulate_latency=0.1, num_rows=1e3)
    for handler in [new_handler, legacy_handler]:
        results = await benchmark_fn(handler)
        print(f"Read {len(results)} rows in: {handler.total_time}s")
        handler.print_profile()
        handler._profiler.clear_stats()
        breakpoint()

if __name__ == "__main__":
    asyncio.run(main())
