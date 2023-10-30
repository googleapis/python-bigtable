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
from grpc import Status

OperationID = UUID


class _OperationType(Enum):
    """Enum for the type of operation being performed."""
    READ_ROWS = "read_rows"


@dataclass
class _Attempt:
    start_time: float
    first_response_time: float | None = None
    end_time: float | None = None
    end_status: Status | None = None


@dataclass
class _MetricOperation:
    name: _OperationType
    start_time: float
    attempts: list[_Attempt] = []


class _BigtableClientSideMetrics():

    def __init__(self):
        self._active_ops: dict[OperationID, _MetricOperation] = {}

    def record_operation_start(self, op_type:_OperationType) -> OperationID:
        start_time = time.monotonic()
        op_id = uuid4()
        first_attempt = _Attempt(start_time=start_time)
        self._active_ops[op_id] = _MetricOperation(op_type, start_time, [first_attempt])
        return op_id

    def record_operation_end(self, op_id:OperationID, status:Status) -> None:
        del self._active_ops[op_id]

    def record_attempt_start(self, op_id:OperationID) -> None:
        start_time = time.monotonic()
        self._active_ops[op_id].attempts.append(_Attempt(start_time=start_time))

    def record_attempt_first_response(self, op_id:OperationID) -> None:
        self._active_ops[op_id].attempts[-1].first_response_time = time.monotonic()

    def record_attempt_end(self, op_id:OperationID, status:Status) -> None:
        op = self._active_ops[op_id]
        op.attempts[-1].end_status = status
        op.attempts[-1].end_time = time.monotonic()
