from google.cloud.bigtable_v2 import BigtableAsyncClient
import asyncio


async def async_main():
    client = BigtableAsyncClient()
    response1 = await client.ping_and_warm(name="projects/sanche-testing-project/instances/sanche-instance")
    print(response1)

if __name__ == "__main__":
    asyncio.run(async_main())