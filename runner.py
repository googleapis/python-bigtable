from google.cloud.bigtable_v2 import BigtableAsyncClient
import asyncio


async def async_main():
    client = BigtableAsyncClient()
    response1 = await client.ping_and_warm(name="projects/sanche-testing-project/instances/sanche-instance")
    response2 = client.ping_and_warm_w_call(name="projects/sanche-testing-project/instances/sanche-instance")
    print(response1)
    print(await response2)
    print(await response2.trailing_metadata())

if __name__ == "__main__":
    asyncio.run(async_main())