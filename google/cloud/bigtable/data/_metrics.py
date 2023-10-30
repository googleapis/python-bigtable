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

OperationID = UUID


class _OperationType(Enum):
    """Enum for the type of operation being performed."""
    READ_ROWS = "read_rows"


@dataclass
class _AttemptMetric:
    start_time: float
    first_response_time: float | None = None
    end_time: float | None = None
    end_status: StatusCode | None = None

    def end_with_status(self, status: StatusCode) -> None:
        if self.end_status is not None:
            raise ValueError("Attempted to end an attempt twice.")
        self.end_time = time.monotonic()
        self.end_status = status


@dataclass
class _OperationMetric:
    op_type: _OperationType
    start_time: float
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

    def end_with_status(self, status: StatusCode) -> None:
        if self.final_status is not None:
            raise ValueError("Attempted to end an operation twice.")
        self.end_time = time.monotonic()
        self.final_status = status
        last_attempt = self.attempts[-1]
        if last_attempt.end_status is not None:
            raise ValueError("Last attempt already ended")
        last_attempt.end_with_status(status)


class _BigtableClientSideMetrics():

    def __init__(self):
        self._active_ops: dict[OperationID, _OperationMetric] = {}

    def start_operation(self, op_type:_OperationType) -> _OperationMetric:
        start_time = time.monotonic()
        new_op = _OperationMetric(op_type=op_type, start_time=start_time)
        self._active_ops[new_op.op_id] = new_op
        return new_op

    def get_operation(self, op_id:OperationID) -> _OperationMetric:
        return self._active_ops[op_id]
