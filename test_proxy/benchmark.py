from time import sleep
import mock
import asyncio

import client_handler
import client_handler_legacy

from google.cloud.bigtable_v2.types import ReadRowsResponse

def simple_reads(num_rows=1e5, payload_size=10, chunks_per_response=100, server_latency=0):
    """
    A large number of simple row reads
    should test max throughput of read_rows
    """
    async def server_response_fn(*args, **kwargs):
        async def inner():
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
                sleep(server_latency)
                yield ReadRowsResponse(chunks=chunks)
                sent_num += batch_size
        return inner()

    async def client_fn(proxy_handler):
        with mock.patch.object(proxy_handler.client._gapic_client, "read_rows") as mock_read_rows:
            mock_read_rows.side_effect = server_response_fn
            request = {"table_name": "projects/project/instances/instance/tables/table"}
            results = await proxy_handler.ReadRows(request)
            print(f"Read {len(results)} rows in: {proxy_handler.total_time}s")

    return client_fn

async def main():
    new_handler = client_handler.TestProxyClientHandler(enable_timing=True, per_operation_timeout=60*30)
    # legacy_handler = client_handler_legacy.LegacyTestProxyClientHandler(enable_timing=True, per_operation_timeout=60*30)
    benchmark_fn = simple_reads(num_rows=100)
    await benchmark_fn(new_handler)
    # await benchmark_fn(legacy_handler)

if __name__ == "__main__":
    asyncio.run(main())
