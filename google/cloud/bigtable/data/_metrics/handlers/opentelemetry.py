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
from __future__ import annotations

from uuid import uuid4

from google.cloud.bigtable import __version__ as bigtable_version
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler
from google.cloud.bigtable.data._metrics.data_model import OperationType
from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.data_model import CompletedAttemptMetric
from google.cloud.bigtable.data._metrics.data_model import CompletedOperationMetric


class _OpenTelemetryInstrumentSingleton:
    """
    Singleton class that holds OpenTelelmetry instrument objects,
    so that multiple Tables can write to the same metrics.
    """

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(_OpenTelemetryInstrumentSingleton, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        from opentelemetry import metrics

        meter = metrics.get_meter(__name__)
        self.operation_latencies = meter.create_histogram(
            name="operation_latencies",
            description="A distribution of the latency of each client method call, across all of it's RPC attempts",
            unit="ms",
        )
        self.first_response_latencies = meter.create_histogram(
            name="first_response_latencies",
            description="A distribution of the latency of receiving the first row in a ReadRows operation.",
            unit="ms",
        )
        self.attempt_latencies = meter.create_histogram(
            name="attempt_latencies",
            description="A distribution of the latency of each client RPC, tagged by operation name and the attempt status. Under normal circumstances, this will be identical to operation_latencies. However, when the client receives transient errors, operation_latencies will be the sum of all attempt_latencies and the exponential delays.",
            unit="ms",
        )
        self.retry_count = meter.create_counter(
            name="retry_count",
            description="A count of additional RPCs sent after the initial attempt. Under normal circumstances, this will be 1.",
        )
        self.server_latencies = meter.create_histogram(
            name="server_latencies",
            description="A distribution of the latency measured between the time when Google's frontend receives an RPC and sending back the first byte of the response.",
            unit="ms",
        )
        self.connectivity_error_count = meter.create_counter(
            name="connectivity_error_count",
            description="A count of the number of attempts that failed to reach Google's network.",
        )
        self.application_blocking_latencies = meter.create_histogram(
            name="application_blocking_latencies",
            description="A distribution of the total latency introduced by your application when Cloud Bigtable has available response data but your application has not consumed it.",
            unit="ms",
        )


class OpenTelemetryMetricsHandler(MetricsHandler):
    """
    Maintains a set of OpenTelemetry metrics for the Bigtable client library,
    and updates them with each completed operation and attempt.

    The OpenTelemetry metrics that are tracked are as follows:
      - operation_latencies: latency of each client method call, over all of it's attempts.
      - first_response_latencies: latency of receiving the first row in a ReadRows operation.
      - attempt_latencies: latency of each client attempt RPC.
      - retry_count: Number of additional RPCs sent after the initial attempt.
      - server_latencies: latency recorded on the server side for each attempt.
      - connectivity_error_count: number of attempts that failed to reach Google's network.
      - application_blocking_latencies: the time spent waiting for the application to process the next response.
      - throttling_latencies: latency introduced by waiting when there are too many outstanding requests in a bulk operation.
    """

    def __init__(
        self,
        *,
        project_id: str,
        instance_id: str,
        table_id: str,
        app_profile_id: str | None,
        client_uid: str | None = None,
        **kwargs,
    ):
        super().__init__()
        # otel singleton holds shared instruments
        self.otel = _OpenTelemetryInstrumentSingleton()

        # fixed labels sent with each metric update
        self.shared_labels = {
            "client_name": f"python-bigtable/{bigtable_version}",
            "client_uid": client_uid or str(uuid4()),
        }
        if app_profile_id:
            self.shared_labels["app_profile"] = app_profile_id

    def on_operation_complete(self, op: CompletedOperationMetric) -> None:
        """
        Update the metrics associated with a completed operation:
          - operation_latencies
          - retry_count
        """
        labels = {
            "method": op.op_type.value,
            "status": op.final_status.value,
            "streaming": op.is_streaming,
            **self.shared_labels,
        }

        self.otel.operation_latencies.record(op.duration, labels)
        self.otel.retry_count.add(len(op.completed_attempts) - 1, labels)

    def on_attempt_complete(
        self, attempt: CompletedAttemptMetric, op: ActiveOperationMetric
    ):
        """
        Update the metrics associated with a completed attempt:
          - attempt_latencies
          - first_response_latencies
          - server_latencies
          - connectivity_error_count
        """
        labels = {
            "method": op.op_type.value,
            "status": attempt.end_status.value,
            "streaming": op.is_streaming,
            **self.shared_labels,
        }

        self.otel.attempt_latencies.record(
            attempt.duration, labels
        )
        self.otel.application_blocking_latencies.record(
            attempt.application_blocking_time + attempt.backoff_before_attempt, labels
        )
        if (
            op.op_type == OperationType.READ_ROWS
            and attempt.first_response_latency is not None
        ):
            self.otel.first_response_latencies.record(
                attempt.first_response_latency, labels
            )
        if attempt.gfe_latency is not None:
            self.otel.server_latencies.record(attempt.gfe_latency, labels)
        else:
            # gfe headers not attached. Record a connectivity error.
            # TODO: this should not be recorded as an error when direct path is enabled
            self.otel.connectivity_error_count.add(1, labels)
