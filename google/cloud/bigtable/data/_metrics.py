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

from typing import Callable, Any

import time
import warnings
import os

from enum import Enum
from uuid import uuid4
from uuid import UUID
from dataclasses import dataclass
from dataclasses import field
from grpc import StatusCode

import google.cloud.bigtable.data.exceptions as bt_exceptions

OperationID = UUID

ALLOW_METRIC_EXCEPTIONS = os.getenv("BIGTABLE_METRICS_EXCEPTIONS", False)


class _OperationType(Enum):
    """Enum for the type of operation being performed."""
    READ_ROWS = "Bigtable.ReadRows"
    SAMPLE_ROW_KEYS = "Bigtable.SampleRowKeys"
    BULK_MUTATE_ROWS = "Bigtable.MutateRows"
    MUTATE_ROW = "Bigtable.MutateRow"
    CHECK_AND_MUTATE = "Bigtable.CheckAndMutateRow"
    READ_MODIFY_WRITE = "Bigtable.ReadModifyWriteRow"


@dataclass(frozen=True)
class _CompletedAttemptMetric:
    start_time: float
    end_time: float
    end_status: StatusCode


@dataclass(frozen=True)
class _CompletedOperationMetric:
    active_data: _ActiveOperationMetric
    end_time: float
    final_status: StatusCode


@dataclass
class _ActiveOperationMetric:
    op_type: _OperationType
    start_time: float
    _controller: _BigtableClientSideMetrics
    op_id: OperationID = field(default_factory=uuid4)
    active_attempt_start_time: float | None = None
    completed_attempts: list[_CompletedAttemptMetric] = field(default_factory=list)
    was_completed: bool = False

    def start(self) -> None:
        if self.was_completed:
            return self._handle_error("Operation cannot be reset after completion")
        if self.completed_attempts or self.active_attempt_start_time:
            return self._handle_error("Cannot restart operation with active attempts")
        self.start_time = time.monotonic()

    def start_attempt(self) -> None:
        if self.was_completed:
            return self._handle_error("Operation already completed")
        if self.active_attempt_start_time is not None:
            return self._handle_error("Incomplete attempt already exists")

        self.active_attempt_start_time = time.monotonic()

    def end_attempt_with_status(self, status:StatusCode | Exception) -> None:
        if self.was_completed:
            return self._handle_error("Operation already completed")
        if self.active_attempt_start_time is None:
            return self._handle_error("No active attempt")

        new_attempt = _CompletedAttemptMetric(
            start_time=self.active_attempt_start_time,
            end_time=time.monotonic(),
            end_status=self._exc_to_status(status) if isinstance(status, Exception) else status
        )
        self.completed_attempts.append(new_attempt)
        self.active_attempt_start_time = None

    def end_with_status(self, status: StatusCode | Exception) -> None:
        if self.was_completed:
            return self._handle_error("Operation already completed")
        self.end_attempt_with_status(status)
        self.was_completed = True
        finalized = _CompletedOperationMetric(
            active_data=self,
            end_time=time.monotonic(),
            final_status=self._exc_to_status(status) if isinstance(status, Exception) else status
        )
        self._controller._on_operation_complete(finalized)

    def end_with_success(self):
        return self.end_with_status(StatusCode.OK)

    def wrap_async_attempt_fn(
            self, fn:Callable[..., Any], predicate:Callable[..., bool] = lambda e: False
    ) -> Callable[..., Any]:
        async def wrapped_fn(*args, **kwargs):
            self.start_attempt()
            try:
                results = await fn(*args, **kwargs)
                self.end_with_success()
                return results
            except Exception as e:
                if predicate(e):
                    self.end_attempt_with_status(e)
                else:
                    self.end_with_status(e)
                raise
        return wrapped_fn

    @staticmethod
    def _exc_to_status(exc:Exception) -> StatusCode:
        if isinstance(exc, bt_exceptions._BigtableExceptionGroup):
            exc = exc.exceptions[0].__cause__
        return exc.grpc_status_code if hasattr(exc, "grpc_status_code") else StatusCode.UNKNOWN

    @staticmethod
    def _handle_error(message:str) -> None:
        full_message = f"Error in Bigtable Metrics: {message}"
        if ALLOW_METRIC_EXCEPTIONS:
            raise ValueError(full_message)
        else:
            warnings.warn(full_message, stacklevel=3)


class _BigtableClientSideMetrics():

    def create_operation(self, op_type:_OperationType) -> _ActiveOperationMetric:
        start_time = time.monotonic()
        new_op = _ActiveOperationMetric(
            op_type=op_type,
            start_time=start_time,
            _controller=self,
        )
        return new_op

    @staticmethod
    def create_metrics_instance(*args, **kwargs) -> _BigtableClientSideMetrics:
        try:
            metrics: _BigtableClientSideMetrics = _BigtableOpenTelemetryMetrics(*args, **kwargs)
        except ImportError:
            metrics = _BigtableClientSideMetrics()
        return metrics

    def _on_operation_complete(self, op: _CompletedOperationMetric) -> None:
        pass


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

    def _on_operation_complete(self, op: _CompletedOperationMetric) -> None:
        labels = {"op_name": op.active_data.op_type.value, "status": op.final_status, **self.shared_labels}

        self.completed_ops.add(1, labels)
        self.attempts_per_op.record(len(op.active_data.completed_attempts), labels)
        self.op_latency.record(op.end_time - op.active_data.start_time, labels)
        for attempt in op.active_data.completed_attempts:
            labels["status"] = attempt.end_status.value
            self.attempt_latency.record(attempt.end_time - attempt.start_time, labels)
