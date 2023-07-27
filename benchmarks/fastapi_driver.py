import time
import asyncio
import aiohttp

async def point_read(url="http://localhost:8000", endpoint="/", duration=10, result_queue=None):
    """call point_reads on FastAPI server in a loop, and record throughput"""
    async with aiohttp.ClientSession() as session:
        # run point_reads
        total_ops = 0
        start_time = time.monotonic()
        deadline = start_time + duration
        while time.monotonic() < deadline:
            async with session.get(url + endpoint) as response:
                total_ops += 1
        total_time = time.monotonic() - start_time
    # output results
    throughput = total_ops / total_time
    print(f'Throughput: {throughput} reads/s')
    if result_queue is not None:
        result_queue.put(throughput)

def process_main(result_queue, endpoint, duration):
    asyncio.run(point_read(endpoint=endpoint, duration=duration, result_queue=result_queue))

if __name__ == '__main__':
    # run point_reads in 8 processes
    import multiprocessing
    num_processes = 8
    duration = 10
    endpoint = "/point_read"
    # endpoint = "/"
    result_queue = multiprocessing.Queue()
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=process_main, args=(result_queue,endpoint,duration))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    # collect results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    assert len(results) == num_processes
    print()
    print(f'Average throughput: {sum(results)/len(results):,.2f} reads/s')
    print(f'Total throughput: {sum(results):,.2f} reads/s')
