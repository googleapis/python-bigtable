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

import rich
from rich.panel import Panel

from abc import ABC, abstractmethod
from time import sleep
import asyncio
import mock


class Benchmark(ABC):

    def __init__(self, simulate_latency=0, max_time:float|None=None, purpose:str|None=None):
        self.simulate_latency = simulate_latency
        self.max_time = max_time
        self.purpose = purpose

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
        proxy_handler.total_time = 0
        if proxy_handler._profiler:
            proxy_handler._profiler.clear_stats()
        # mock server responses
        wrapped = self._server_mock_decorator(self.client_setup)
        # run client code
        return await wrapped(proxy_handler)

    async def compare_execution(self, new_client, baseline_client, show_profile=False) -> tuple[float, float]:
        await self.run(baseline_client)
        baseline_time = baseline_client.total_time
        await self.run(new_client)
        new_time = new_client.total_time
        # print results
        docstring = " ".join(self.__doc__.split())
        rich.print(Panel(f"[cyan]{self.__class__.__name__} benchmark results\n[/cyan]{docstring}", title="Benchmark Results"))
        print(f"Baseline: {baseline_time:0.2f}s")
        print(f"New: {new_time:0.2f}s")
        comparison_color = "green" if new_time < baseline_time else "red"
        rich.print(f"[{comparison_color}]Change: {(new_time / baseline_time)*100:0.2f}%")
        if show_profile:
            print(f"\nProfile for New Client:\n{new_client.print_profile()}")
        return new_time, baseline_time

    def __str__(self):
        if self.purpose:
            return f"{self.__class__.__name__} - {self.purpose}"
        else:
            return f"{self.__class__.__name__}({self.__dict__}"
