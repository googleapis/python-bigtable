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

from enum import Enum
from uuid import uuid4
from uuid import UUID
import time
from dataclasses import dataclass
from dataclasses import field
from grpc import StatusCode

import google.cloud.bigtable.data.exceptions as bt_exceptions

OperationID = UUID


class _OperationType(Enum):
    """Enum for the type of operation being performed."""
    READ_ROWS = "read_rows"
    BULK_MUTATE = "bulk_mutate"


def _exc_to_status(exc:Exception) -> StatusCode:
    if isinstance(exc, bt_exceptions._BigtableExceptionGroup):
        exc = exc.exceptions[0].__cause__
    return exc.grpc_status_code if hasattr(exc, "grpc_status_code") else StatusCode.UNKNOWN


@dataclass
class _AttemptMetric:
    start_time: float
    first_response_time: float | None = None
    end_time: float | None = None
    end_status: StatusCode | None = None

    def end_with_status(self, status: StatusCode | Exception) -> None:
        if self.end_status is not None:
            raise ValueError("Attempted to end an attempt twice.")
        self.end_status = _exc_to_status(status) if isinstance(status, Exception) else status
        self.end_time = time.monotonic()


@dataclass
class _OperationMetric:
    op_type: _OperationType
    start_time: float
    on_complete: callable | None = None
    op_id: OperationID = field(default_factory=uuid4)
    attempts: list[_AttemptMetric] = field(default_factory=list)
    end_time: float | None = None
    final_status: StatusCode | None = None

    def reset(self) -> None:
        self.start_time = time.monotonic()
        self.attempts = []
        self.end_time = None
        self.final_status = None

    def start_attempt(self) -> _AttemptMetric:
        attempt = _AttemptMetric(start_time=time.monotonic())
        self.attempts.append(attempt)
        return attempt

    def end_attempt_with_status(self, _AttemptMetric, status:StatusCode) -> None:
        _AttemptMetric.end_with_status(status)

    def end_with_status(self, status: StatusCode | Exception) -> None:
        if self.final_status is not None:
            raise ValueError("Attempted to end an operation twice.")
        self.end_time = time.monotonic()
        self.final_status = _exc_to_status(status) if isinstance(status, Exception) else status
        last_attempt = self.attempts[-1]
        if last_attempt.end_status is not None:
            raise ValueError("Last attempt already ended")
        last_attempt.end_with_status(self.final_status)
        if self.on_complete:
            self.on_complete(self)

    def end_with_success(self):
        return self.end_with_status(StatusCode.OK)


class _BigtableClientSideMetrics():

    def __init__(self):
        self._active_ops: dict[OperationID, _OperationMetric] = {}

    def start_operation(self, op_type:_OperationType) -> _OperationMetric:
        start_time = time.monotonic()
        new_op = _OperationMetric(
            op_type=op_type,
            start_time=start_time,
            on_complete=self._on_operation_complete,
        )
        self._active_ops[new_op.op_id] = new_op
        return new_op

    def get_operation(self, op_id:OperationID) -> _OperationMetric:
        return self._active_ops[op_id]

    def _on_operation_complete(self, op: _OperationMetric) -> None:
        del self._active_ops[op.op_id]


class _BigtableOpenTelemetryMetrics(_BigtableClientSideMetrics):

    def __init__(self, project_id:str, instance_id:str, app_profile_id:str | None):
        super().__init__()
        from opentelemetry import metrics

        meter = metrics.get_meter(__name__)
        self.op_latency = meter.create_histogram(
            name="op_latency",
            description="A distribution of latency of each client method call, across all of it's RPC attempts. Tagged by operation name and final response status.",
            unit="ms",
        )
        self.completed_ops = meter.create_counter(
            name="completed_ops",
            description="The total count of method invocations. Tagged by operation name and final response status",
            unit="1",
        )
        self.read_rows_first_row_latency = meter.create_histogram(
            name="read_rows_first_row_latency",
            description="A distribution of the latency of receiving the first row in a ReadRows operation.",
            unit="ms",
        )
        self.attempt_latency = meter.create_histogram(
            name="attempt_latency",
            description="A distribution of latency of each client RPC, tagged by operation name and the attempt status. Under normal circumstances, this will be identical to op_latency. However, when the client receives transient errors, op_latency will be the sum of all attempt_latencies and the exponential delays.",
            unit="ms",
        )
        self.attempts_per_op = meter.create_histogram(
            name="attempts_per_op",
            description="A distribution of attempts that each operation required, tagged by operation name and final operation status. Under normal circumstances, this will be 1.",
        )
        self.shared_labels = {"bigtable_project_id": project_id, "bigtable_instance_id": instance_id}
        if app_profile_id:
            self.shared_labels["bigtable_app_profile_id"] = app_profile_id



    def _on_operation_complete(self, op: _OperationMetric) -> None:
        labels = {"op_name": op.op_type.value, "status": op.final_status.value, **self.shared_labels}

        self.completed_ops.add(1, labels)
        self.attempts_per_op.record(len(op.attempts), labels)
        self.op_latency.record(op.end_time - op.start_time, labels)
        for attempt in op.attempts:
            labels["status"] = attempt.end_status.value
            self.attempt_latency.record(attempt.end_time-attempt.start_time, labels)
