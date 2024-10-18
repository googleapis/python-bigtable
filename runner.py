from google.cloud.bigtable_v2 import BigtableAsyncClient
import asyncio


async def custom_callback(grpc_call):
    print(await grpc_call.trailing_metadata())


async def async_main():
    client = BigtableAsyncClient()
    response1 = await client.ping_and_warm(name="projects/sanche-testing-project/instances/sanche-instance", raw_grpc_callback=custom_callback)
    print(response1)

if __name__ == "__main__":
    asyncio.run(async_main())