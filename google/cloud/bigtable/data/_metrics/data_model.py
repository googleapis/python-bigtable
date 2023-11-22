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
import os
import re
import logging

from enum import Enum
from uuid import uuid4
from dataclasses import dataclass
from dataclasses import field
from grpc import StatusCode

import google.cloud.bigtable.data.exceptions as bt_exceptions

if TYPE_CHECKING:
    from uuid import UUID
    from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler


ALLOW_METRIC_EXCEPTIONS = os.getenv("BIGTABLE_METRICS_EXCEPTIONS", False)
LOGGER = logging.getLogger(__name__) if os.getenv("BIGTABLE_METRICS_LOGS", False) else None

DEFAULT_ZONE = "global"

BIGTABLE_METADATA_KEY = "x-goog-ext-425905942-bin"
SERVER_TIMING_METADATA_KEY = "server-timing"

SERVER_TIMING_REGEX = re.compile(r"gfet4t7; dur=(\d+)")

INVALID_STATE_ERROR = "Invalid state for {}: {}"


class OperationType(Enum):
    """Enum for the type of operation being performed."""

    READ_ROWS = "Bigtable.ReadRows"
    SAMPLE_ROW_KEYS = "Bigtable.SampleRowKeys"
    BULK_MUTATE_ROWS = "Bigtable.MutateRows"
    MUTATE_ROW = "Bigtable.MutateRow"
    CHECK_AND_MUTATE = "Bigtable.CheckAndMutateRow"
    READ_MODIFY_WRITE = "Bigtable.ReadModifyWriteRow"


class OperationState(Enum):
    """Enum for the state of the active operation."""
    CREATED = 0
    ACTIVE_ATTEMPT = 1
    BETWEEN_ATTEMPTS = 2
    COMPLETED = 3


@dataclass(frozen=True)
class CompletedAttemptMetric:
    """
    A dataclass representing the data associated with a completed rpc attempt.
    """

    start_time: float
    end_time: float
    end_status: StatusCode
    first_response_latency: float | None = None
    gfe_latency: float | None = None


@dataclass(frozen=True)
class CompletedOperationMetric:
    """
    A dataclass representing the data associated with a completed rpc operation.
    """

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
class ActiveAttemptMetric:
    start_time: float = field(default_factory=time.monotonic)
    # the time it takes to recieve the first response from the server
    # currently only tracked for ReadRows
    first_response_latency: float | None = None
    # the time taken by the backend. Taken from response header
    gfe_latency: float | None = None


@dataclass
class ActiveOperationMetric:
    """
    A dataclass representing the data associated with an rpc operation that is
    currently in progress.
    """

    op_type: OperationType
    start_time: float = field(default_factory=time.monotonic)
    op_id: UUID = field(default_factory=uuid4)
    active_attempt: ActiveAttemptMetric | None = None
    cluster_id: str | None = None
    zone: str | None = None
    completed_attempts: list[CompletedAttemptMetric] = field(default_factory=list)
    handlers: list[MetricsHandler] = field(default_factory=list)
    is_streaming: bool = False  # only True for read_rows operations
    was_completed: bool = False

    @property
    def state(self) -> OperationState:
        if self.was_completed:
            return OperationState.COMPLETED
        elif self.active_attempt is None:
            if self.completed_attempts:
                return OperationState.BETWEEN_ATTEMPTS
            else:
                return OperationState.CREATED
        else:
            return OperationState.ACTIVE_ATTEMPT

    def start(self) -> None:
        """
        Optionally called to mark the start of the operation. If not called,
        the operation will be started at initialization.

        If the operation was completed or has active attempts, will raise an
        exception or warning based on the value of ALLOW_METRIC_EXCEPTIONS.
        """
        if self.state != OperationState.CREATED:
            return self._handle_error(INVALID_STATE_ERROR.format("start", self.state))
        self.start_time = time.monotonic()

    def start_attempt(self) -> None:
        """
        Called to initiate a new attempt for the operation.

        If the operation was completed or there is already an active attempt,
        will raise an exception or warning based on the value of ALLOW_METRIC_EXCEPTIONS.
        """
        if self.state != OperationState.BETWEEN_ATTEMPTS and self.state != OperationState.CREATED:
            return self._handle_error(INVALID_STATE_ERROR.format("start_attempt", self.state))

        self.active_attempt = ActiveAttemptMetric()

    def add_response_metadata(self, metadata: dict[str, bytes | str]) -> None:
        """
        Attach trailing metadata to the active attempt.

        If not called, default values for the metadata will be used.

        If the operation was completed or there is no active attempt,
        will raise an exception or warning based on the value of ALLOW_METRIC_EXCEPTIONS.

        Args:
          - metadata: the metadata as extracted from the grpc call
        """
        if self.state != OperationState.ACTIVE_ATTEMPT:
            return self._handle_error(INVALID_STATE_ERROR.format("add_response_metadata", self.state))

        if self.cluster_id is None or self.zone is None:
            bigtable_metadata = metadata.get(BIGTABLE_METADATA_KEY)
            if bigtable_metadata:
                try:
                    decoded = "".join(
                        c if c.isprintable() else " "
                        for c in bigtable_metadata.decode("utf-8")
                    )
                    split_data = decoded.split()
                    self.cluster_id = split_data[0]
                    self.zone = split_data[1]
                except (AttributeError, IndexError):
                    self._handle_error(f"Failed to decode {BIGTABLE_METADATA_KEY} metadata: {bigtable_metadata}")
        timing_header = metadata.get(SERVER_TIMING_METADATA_KEY)
        if timing_header:
            timing_data = SERVER_TIMING_REGEX.match(timing_header)
            if timing_data:
                # convert from milliseconds to seconds
                self.active_attempt.gfe_latency = float(timing_data.group(1)) / 1000

    def attempt_first_response(self) -> None:
        """
        Called to mark the timestamp of the first completed response for the
        active attempt.

        If the operation was completed, there is no active attempt, or the
        active attempt already has a first response time, will raise an
        exception or warning based on the value of ALLOW_METRIC_EXCEPTIONS.
        """
        if self.state != OperationState.ACTIVE_ATTEMPT:
            return self._handle_error(INVALID_STATE_ERROR.format("attempt_first_response", self.state))
        elif self.active_attempt.first_response_latency is not None:
            return self._handle_error("Attempt already received first response")
        self.active_attempt.first_response_latency = (
            time.monotonic() - self.active_attempt.start_time
        )

    def end_attempt_with_status(self, status: StatusCode | Exception) -> None:
        """
        Called to mark the end of a failed attempt for the operation.

        If the operation was completed or there is no active attempt,
        will raise an exception or warning based on the value of ALLOW_METRIC_EXCEPTIONS.

        Args:
          - status: The status of the attempt.
        """
        if self.state != OperationState.ACTIVE_ATTEMPT:
            return self._handle_error(INVALID_STATE_ERROR.format("end_attempt_with_status", self.state))

        new_attempt = CompletedAttemptMetric(
            start_time=self.active_attempt.start_time,
            first_response_latency=self.active_attempt.first_response_latency,
            end_time=time.monotonic(),
            end_status=self._exc_to_status(status)
            if isinstance(status, Exception)
            else status,
            gfe_latency=self.active_attempt.gfe_latency,
        )
        self.completed_attempts.append(new_attempt)
        self.active_attempt = None
        for handler in self.handlers:
            handler.on_attempt_complete(new_attempt, self)

    def end_with_status(self, status: StatusCode | Exception) -> None:
        """
        Called to mark the end of the operation. If there is an active attempt,
        end_attempt_with_status will be called with the same status.

        If the operation was already completed, will raise an exception or
        warning based on the value of ALLOW_METRIC_EXCEPTIONS.

        Causes on_operation_completed to be called for each registered handler.

        Args:
          - status: The status of the operation.
        """
        if self.state == OperationState.COMPLETED:
            return self._handle_error(INVALID_STATE_ERROR.format("end_with_status", self.state))
        final_status = self._exc_to_status(status) if isinstance(status, Exception) else status
        if self.state == OperationState.ACTIVE_ATTEMPT:
            self.end_attempt_with_status(final_status)
        self.was_completed = True
        finalized = CompletedOperationMetric(
            op_type=self.op_type,
            start_time=self.start_time,
            op_id=self.op_id,
            completed_attempts=self.completed_attempts,
            duration=time.monotonic() - self.start_time,
            final_status=final_status,
            cluster_id=self.cluster_id,
            zone=self.zone or DEFAULT_ZONE,
            is_streaming=self.is_streaming,
        )
        for handler in self.handlers:
            handler.on_operation_complete(finalized)

    def end_with_success(self):
        """
        Called to mark the end of the operation with a successful status.

        If the operation was already completed, will raise an exception or
        warning based on the value of ALLOW_METRIC_EXCEPTIONS.

        Causes on_operation_completed to be called for each registered handler.
        """
        return self.end_with_status(StatusCode.OK)

    def build_on_error_fn(
        self,
        predicate: Callable[[Exception], bool],
        wrapped_on_error: Callable[[Exception], None] | None = None,
    ) -> Callable[[Exception], None]:
        """
        Construct an on_error function that can be passed in to api_core Retry objects, and terminates
        operations and attempts based on the exceptions encountered.

        Args:
          - predicate: A function that takes an exception and returns True if the exception is retryable.
              If retryable, the attempt should be finalized, but the operation continues. If not retryable,
              they will both be finalized. Predicate object should be the same as the one passed to the
              api_core Retry object.
          - wapped_on_error: An optional secondary on_error function to be called after the metrics are handled.
        """

        def on_error(exc: Exception) -> None:
            if predicate(exc):
                # retryable exception: end attempt
                self.end_attempt_with_status(exc)
            else:
                # terminal exception: end the operation
                self.end_with_status(exc)
            if wrapped_on_error:
                wrapped_on_error(exc)

        return on_error

    @staticmethod
    def _exc_to_status(exc: Exception) -> StatusCode:
        """
        Extracts the grpc status code from an exception.

        Exception groups and wrappers will be parsed to find the underlying
        grpc Exception.

        If the exception is not a grpc exception, will return StatusCode.UNKNOWN.

        Args:
          - exc: The exception to extract the status code from.
        """
        if isinstance(exc, bt_exceptions._BigtableExceptionGroup):
            exc = exc.exceptions[-1]
        if hasattr(exc, "grpc_status_code"):
            return exc.grpc_status_code
        if exc.__cause__ and hasattr(exc.__cause__, "grpc_status_code"):
            return exc.__cause__.grpc_status_code
        return StatusCode.UNKNOWN

    @staticmethod
    def _handle_error(message: str) -> None:
        """
        Raises an exception or warning based on the value of ALLOW_METRIC_EXCEPTIONS.

        Args:
          - message: The message to include in the exception or warning.
        """
        full_message = f"Error in Bigtable Metrics: {message}"
        if ALLOW_METRIC_EXCEPTIONS:
            raise ValueError(full_message)
        if LOGGER:
            LOGGER.warning(full_message)

    async def __aenter__(self):
        """
        Implements the async context manager protocol for wrapping unary calls
        """
        return self._AsyncContextManager(self)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Implements the async context manager protocol for wrapping unary calls

        The operation is automatically ended on exit, with the status determined
        by the exception type and value.
        """
        if exc_val is None:
            self.end_with_success()
        else:
            self.end_with_status(exc_val)

    class _AsyncContextManager:
        """
        Inner class for async context manager protocol

        This class provides functions for wrapping unary gapic functions,
        and automatically tracking the metrics associated with the call
        """

        def __init__(self, operation: ActiveOperationMetric):
            self.operation = operation

        def add_response_metadata(self, metadata):
            """
            Pass through trailing metadata to the wrapped operation
            """
            return self.operation.add_response_metadata(metadata)

        def wrap_attempt_fn(
            self,
            fn: Callable[..., Any],
            *,
            extract_call_metadata: bool = True,
        ) -> Callable[..., Any]:
            """
            Wraps a function call, tracing metadata along the way

            Typically, the wrapped function will be a gapic rpc call

            Args:
              - fn: The function to wrap
              - extract_call_metadata: If True, the call will be treated as a
                  grpc function, and will automatically extract trailing_metadata
                  from the Call object on success.
            """
            async def wrapped_fn(*args, **kwargs):
                encountered_exc: Exception | None = None
                call = None
                self.operation.start_attempt()
                try:
                    call = fn(*args, **kwargs)
                    return await call
                except Exception as e:
                    encountered_exc = e
                    raise
                finally:
                    # capture trailing metadata
                    if extract_call_metadata and call is not None:
                        metadata = (
                            await call.trailing_metadata()
                            + await call.initial_metadata()
                        )
                        self.operation.add_response_metadata(metadata)
                    if encountered_exc is not None:
                        # end attempt. Let higher levels decide when to end operation
                        self.operation.end_attempt_with_status(encountered_exc)

            return wrapped_fn
