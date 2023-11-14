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

from typing import Callable, Any, TYPE_CHECKING

import time
import warnings
import os

from enum import Enum
from uuid import uuid4
from dataclasses import dataclass
from dataclasses import field
from grpc import StatusCode

import google.cloud.bigtable.data.exceptions as bt_exceptions

if TYPE_CHECKING:
    from uuid import UUID
    from google.cloud.bigtable.data._metrics.handlers._base import _MetricsHandler


ALLOW_METRIC_EXCEPTIONS = os.getenv("BIGTABLE_METRICS_EXCEPTIONS", False)


class OperationType(Enum):
    """Enum for the type of operation being performed."""
    READ_ROWS = "Bigtable.ReadRows"
    SAMPLE_ROW_KEYS = "Bigtable.SampleRowKeys"
    BULK_MUTATE_ROWS = "Bigtable.MutateRows"
    MUTATE_ROW = "Bigtable.MutateRow"
    CHECK_AND_MUTATE = "Bigtable.CheckAndMutateRow"
    READ_MODIFY_WRITE = "Bigtable.ReadModifyWriteRow"


@dataclass(frozen=True)
class CompletedAttemptMetric:
    start_time: float
    duration: float
    end_status: StatusCode
    first_response_latency: float | None = None


@dataclass(frozen=True)
class CompletedOperationMetric:
    op_type: OperationType
    start_time: float
    duration: float
    op_id: UUID
    completed_attempts: list[CompletedAttemptMetric]
    final_status: StatusCode
    cluster_id: str | None
    zone: str
    is_streaming: bool


@dataclass
class ActiveOperationMetric:
    op_type: OperationType
    start_time: float
    op_id: UUID = field(default_factory=uuid4)
    active_attempt_start_time: float | None = None
    active_attempt_first_response_time: float | None = None
    cluster_id: str | None = None
    zone: str | None = None
    completed_attempts: list[CompletedAttemptMetric] = field(default_factory=list)
    was_completed: bool = False
    _handlers: list[_MetricsHandler] = field(default_factory=list)
    is_streaming: bool = False  # only True for read_rows operations

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

    def add_call_metadata(self, metadata):
        if self.cluster_id is None or self.zone is None:
            bigtable_metadata = metadata.get('x-goog-ext-425905942-bin')
            if bigtable_metadata:
                decoded = ''.join(c if c.isprintable() else ' ' for c in bigtable_metadata.decode('utf-8'))
                cluster_id, zone = decoded.split()
                if cluster_id:
                    self.cluster_id = cluster_id
                if zone:
                    self.zone = zone

    def attempt_first_response(self) -> None:
        if self.was_completed:
            return self._handle_error("Operation already completed")
        elif self.active_attempt_start_time is None:
            return self._handle_error("No active attempt")
        elif self.active_attempt_first_response_time is not None:
            return self._handle_error("Attempt already received first response")
        self.attempt_first_response_time = time.monotonic()

    def end_attempt_with_status(self, status:StatusCode | Exception) -> None:
        if self.was_completed:
            return self._handle_error("Operation already completed")
        if self.active_attempt_start_time is None:
            return self._handle_error("No active attempt")

        first_response_latency = self.active_attempt_first_response_time - self.active_attempt_start_time if self.active_attempt_first_response_time else None

        new_attempt = CompletedAttemptMetric(
            start_time=self.active_attempt_start_time,
            first_response_latency=first_response_latency,
            duration=time.monotonic() - self.active_attempt_start_time,
            end_status=self._exc_to_status(status) if isinstance(status, Exception) else status
        )
        self.completed_attempts.append(new_attempt)
        self.active_attempt_start_time = None
        for handler in self._handlers:
            handler.on_attempt_complete(new_attempt, self)

    def end_with_status(self, status: StatusCode | Exception) -> None:
        if self.was_completed:
            return self._handle_error("Operation already completed")
        self.end_attempt_with_status(status)
        self.was_completed = True
        finalized = CompletedOperationMetric(
            op_type=self.op_type,
            start_time=self.start_time,
            op_id=self.op_id,
            completed_attempts=self.completed_attempts,
            duration=time.monotonic() - self.start_time,
            final_status=self._exc_to_status(status) if isinstance(status, Exception) else status,
            cluster_id=self.cluster_id,
            zone=self.zone or 'global',
            is_streaming=self.is_streaming,
        )
        for handler in self._handlers:
            handler.on_operation_complete(finalized)

    def end_with_success(self):
        return self.end_with_status(StatusCode.OK)

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

    async def __aenter__(self):
        return self._AsyncContextManager(self)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None:
            self.end_with_success()
        else:
            self.end_with_status(exc_val)

    class _AsyncContextManager:

        def __init__(self, operation:ActiveOperationMetric):
            self.operation = operation

        def add_call_metadata(self, metadata):
            self.operation.add_call_metadata(metadata)

        def wrap_attempt_fn(
            self,
            fn:Callable[..., Any],
            retryable_predicate:Callable[BaseException, bool] = lambda e: False,
            *,
            extract_call_metadata:bool = True,
        ) -> Callable[..., Any]:
            """
            Wraps a function call, tracing metadata along the way

            Typically, the wrapped function will be a gapic rpc call

            Args:
              - fn: The function to wrap
              - retryable_predicate: Tells us whether an exception is retryable.
                  Should be the same predicate used in the retry.Retry wrapper
              - extract_call_metadata: If True, the call will be treated as a
                  grpc function, and will automatically extract trailing_metadata
                  from the Call object on success.
            """
            async def wrapped_fn(*args, **kwargs):
                call = None
                self.operation.start_attempt()
                try:
                    call = fn(*args, **kwargs)
                    return await call
                except Exception as e:
                    if retryable_predicate(e):
                        self.operation.end_attempt_with_status(e)
                    raise

                finally:
                    # capture trailing metadata
                    if extract_call_metadata and call is not None:
                        metadata = await call.trailing_metadata()
                        self.operation.add_call_metadata(metadata)
            return wrapped_fn
