import time
import asyncio
import aiohttp

async def point_reads(url="http://localhost:8000", duration=10):
    """call point_reads on FastAPI server in a loop, and record throughput"""
    async with aiohttp.ClientSession() as session:
        # run point_reads
        total_ops = 0
        start_time = time.monotonic()
        deadline = start_time + duration
        while time.monotonic() < deadline:
            async with session.get(url + "/point_read") as response:
                total_ops += 1
        total_time = time.monotonic() - start_time
    # output results
    print(f'Throughput: {total_ops/total_time} reads/s')

def process_main():
    asyncio.run(point_reads())

if __name__ == '__main__':
    # run point_reads in 8 processes
    import multiprocessing
    for _ in range(10):
        multiprocessing.Process(target=process_main).start()
