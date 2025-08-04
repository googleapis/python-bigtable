# Copyright 2025 Google LLC
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
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler
from google.cloud.bigtable.data._metrics.data_model import CompletedOperationMetric


class _StdoutMetricsHandler(MetricsHandler):
    """
    Prints a table of metric data after each operation, for debugging purposes.
    """

    def __init__(self, **kwargs):
        self._completed_ops = {}

    def on_operation_complete(self, op: CompletedOperationMetric) -> None:
        """
        After each operation, update the state and print the metrics table.
        """
        current_list = self._completed_ops.setdefault(op.op_type, [])
        current_list.append(op)
        self.print()

    def print(self):
        """
        Print the current state of the metrics table.
        """
        print("Bigtable Metrics:")
        for ops_type, ops_list in self._completed_ops.items():
            count = len(ops_list)
            total_latency = sum([op.duration_ns for op in ops_list])
            total_attempts = sum([len(op.completed_attempts) for op in ops_list])
            avg_latency = total_latency / count
            avg_attempts = total_attempts / count
            print(
                f"{ops_type}: count: {count}, avg latency: {avg_latency:.2f}, avg attempts: {avg_attempts:.1f}"
            )
        print()