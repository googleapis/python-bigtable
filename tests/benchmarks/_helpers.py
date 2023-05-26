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
Contains abstract base class used by other benchmarks.
"""
from __future__ import annotations

import rich
from rich.panel import Panel

from abc import ABC, abstractmethod
from time import sleep
import asyncio
import mock

import yappi


class Benchmark(ABC):
    """
    base class for benchmarks used by Bigtable client
    """

    def __init__(
        self,
        simulate_latency=0,
        max_time: float | None = None,
        purpose: str | None = None,
    ):
        """
        Args:
          - simulate_latency: time to sleep between each server response, to simulate network latency
          - max_time: If benchmark takes longer than max_time, it should count as failed, Used in assertion in test_benchmarks.py
          - purpose: optional string describing purpose of benchmark, for use in printing results
        """
        self.simulate_latency = simulate_latency
        self.max_time = max_time if max_time else float("inf")
        self.purpose = purpose

    @abstractmethod
    def server_responses(self, *args, **kwargs):
        """
        Subclasses implement server_responses generator to yield data from server
        """
        raise NotImplementedError

    @abstractmethod
    async def client_setup(self, proxy_handler):
        """
        Implemented by subclasses to customize client-side behavior
        """
        raise NotImplementedError

    def _server_responses_with_latency(self, *args, **kwargs):
        """
        Attach synchronous sleep latency to server_responses generator
        """
        profile_enabled = yappi.is_running()
        if profile_enabled:
            yappi.stop()
        generator = self.server_responses(*args, **kwargs)
        while True:
            try:
                next_item = next(generator)
            except StopIteration:
                break
            finally:
                if profile_enabled:
                    yappi.start()
            yield next_item
            if profile_enabled:
                yappi.stop()
            sleep(self.simulate_latency)

    async def _server_responses_with_latency_async(self, *args, **kwargs):
        """
        Attach asynchronous sleep latency to server_responses generator,
        and wrap in asyncio coroutine
        """

        async def inner():
            generator = self.server_responses(*args, **kwargs)
            profile_enabled = yappi.is_running()
            while True:
                try:
                    if profile_enabled:
                        yappi.stop()
                    next_item = next(generator)
                except StopIteration:
                    break
                finally:
                    if profile_enabled:
                        yappi.start()
                yield next_item
                await asyncio.sleep(self.simulate_latency)

        return inner()

    def _server_mock_decorator(self, func):
        """
        Wraps a function in mocked grpc calls
        """

        async def inner(*args, **kwargs):
            with mock.patch(
                "google.cloud.bigtable_v2.services.bigtable.async_client.BigtableAsyncClient.read_rows"
            ) as mock_read_rows_async:
                with mock.patch(
                    "google.cloud.bigtable_v2.services.bigtable.client.BigtableClient.read_rows"
                ) as mock_read_rows:
                    mock_read_rows_async.side_effect = (
                        self._server_responses_with_latency_async
                    )
                    mock_read_rows.side_effect = self._server_responses_with_latency
                    return await func(*args, **kwargs)

        return inner

    async def run(self, proxy_handler):
        """
        Run a benchmark against a proxy handler, clearing any previous results
        """
        # reset profiler
        proxy_handler.total_time = 0
        proxy_handler.reset_profile()
        # mock server responses
        wrapped = self._server_mock_decorator(self.client_setup)
        # run client code
        return await wrapped(proxy_handler)

    async def compare_execution(
        self, new_client, baseline_client, num_loops=1, print_results=True
    ) -> tuple[float, float]:
        """
        Run a benchmark against two clients, and compare their execution times
        """
        baseline_time, new_time = 0, 0
        for _ in range(num_loops):
            await self.run(baseline_client)
            baseline_time += baseline_client.total_time
            await self.run(new_client)
            new_time += new_client.total_time
        # find averages
        baseline_time /= num_loops
        new_time /= num_loops
        # print results
        if print_results:
            print()
            rich.print(Panel(f"[cyan]{self}", title="Timed Benchmark Results"))
            print(f"Baseline: {baseline_time:0.3f}s")
            print(f"New: {new_time:0.3f}s")
            comparison_color = "green" if new_time < baseline_time else "red"
            rich.print(
                f"[{comparison_color}]Change: {(new_time / (baseline_time+1e-9))*100:0.2f}%"
            )
        return new_time, baseline_time

    async def profile_execution(self, client, clock_type="cpu", save_path=None):
        """
        Run a benchmark with profiling enabled, and print results
        """
        yappi.set_clock_type(clock_type)
        yappi.clear_stats()
        # turn on profiling and off timing for this run
        old_settings = client._enabled_profiling, client._enabled_timing
        client._enabled_profiling, client._enabled_timing = True, False
        # run benchmark
        await self.run(client)
        # print and save results
        profile_str = client.print_profile(save_path=save_path)
        rich.print(Panel(f"[cyan]{self}", title="Profile Results"))
        print(f"\nProfile for New Client:\n{profile_str}")
        # yappi.get_func_stats().print_all()
        # reset settings to old values
        client._enabled_profiling, client._enabled_timing = old_settings
        return profile_str

    def __str__(self):
        if self.purpose:
            return f"{self.__class__.__name__} - {self.purpose}"
        else:
            return f"{self.__class__.__name__}({self.__dict__}"
