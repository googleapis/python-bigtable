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
Runs a series of benchmarks as a pytest test suite.

Benchmarks are always compared against the v2 client, and sometimes contain an absolute time limit.

Running benchmark with `python -m pytest test_benchmarks.py --profile` will save a profile for
all failed benchmarks in the current directory, which can be visualized with `snakeviz`.
"""
# import test proxy handlers
import re
import pytest
import sys
import os

from _helpers import Benchmark
import benchmarks

script_dir = os.path.dirname(os.path.realpath(__file__))
test_proxy_dir = os.path.join(script_dir, "../../test_proxy/handlers")
sys.path.append(test_proxy_dir)
import client_handler_data  # noqa: E402
import client_handler_legacy  # noqa: E402


benchmark_instances = [
    benchmarks.SimpleReads(
        num_rows=1e4, simulate_latency=0, purpose="test max throughput"
    ),
    benchmarks.ComplexReads(
        num_rows=100,
        drop_every=101,
        simulate_latency=0,
        purpose="test with more heterogeneous data",
    ),
    benchmarks.SimpleBulkMutations(5e4, 5e4, purpose="test throughput with a single large response"),
    benchmarks.SimpleBulkMutations(5e4, 1, purpose="test throughput with a large number of responses in stream"),
]


@pytest.fixture(scope="session")
def profile(pytestconfig):
    return pytestconfig.getoption("profile")


@pytest.mark.parametrize(
    "test_case", benchmark_instances, ids=[str(x) for x in benchmark_instances]
)
@pytest.mark.asyncio
async def test_benchmark(test_case: Benchmark, profile):
    kwargs = {
        "enable_profiling": False,
        "enable_timing": True,
        "per_operation_timeout": 60 * 30,
        "raise_on_error": True,
    }
    new_handler = client_handler_data.TestProxyClientHandler(**kwargs)
    legacy_handler = client_handler_legacy.LegacyTestProxyClientHandler(**kwargs)
    new_time, old_time = await test_case.compare_execution(new_handler, legacy_handler)
    await new_handler.client.close()
    # save profiles if needed
    if profile and (new_time > old_time or new_time > test_case.max_time):
        filename = re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F ]", "-", f"{test_case}.prof")
        await test_case.profile_execution(new_handler, save_path=filename)
    # process test results
    results_string = f"new handler was {(new_time/(old_time+1e-9))*100:0.2f}% the speed of the legacy handler: {new_time:0.2f} > {old_time:0.2f}"
    assert new_time <= old_time, results_string
    assert (
        new_time < test_case.max_time
    ), f"new handler is slower than max time: {test_case.max_time}"
    print(results_string)


if __name__ == "__main__":
    pytest.main(sys.argv + ["-s"])
