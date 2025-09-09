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

from typing import Tuple, cast, TYPE_CHECKING

import time
import re
import logging
import uuid

from enum import Enum
from functools import lru_cache
from dataclasses import dataclass
from dataclasses import field
from grpc import StatusCode

import google.cloud.bigtable.data.exceptions as bt_exceptions
from google.cloud.bigtable_v2.types.response_params import ResponseParams
from google.cloud.bigtable.data._helpers import TrackedBackoffGenerator
from google.protobuf.message import DecodeError

if TYPE_CHECKING:
    from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler


LOGGER = logging.getLogger(__name__)

# default values for zone and cluster data, if not captured
DEFAULT_ZONE = "global"
DEFAULT_CLUSTER_ID = "unspecified"

# keys for parsing metadata blobs
BIGTABLE_METADATA_KEY = "x-goog-ext-425905942-bin"
SERVER_TIMING_METADATA_KEY = "server-timing"
SERVER_TIMING_REGEX = re.compile(r".*gfet4t7;\s*dur=(\d+\.?\d*).*")

INVALID_STATE_ERROR = "Invalid state for {}: {}"

OPERATION_INTERCEPTOR_METADATA_KEY = "x-goog-operation-key"


class OperationType(Enum):
    """Enum for the type of operation being performed."""

    READ_ROWS = "ReadRows"
    SAMPLE_ROW_KEYS = "SampleRowKeys"
    BULK_MUTATE_ROWS = "MutateRows"
    MUTATE_ROW = "MutateRow"
    CHECK_AND_MUTATE = "CheckAndMutateRow"
    READ_MODIFY_WRITE = "ReadModifyWriteRow"


class OperationState(Enum):
    """Enum for the state of the active operation."""

    CREATED = 0
    ACTIVE_ATTEMPT = 1
    BETWEEN_ATTEMPTS = 2
    COMPLETED = 3


@dataclass(frozen=True)
class CompletedAttemptMetric:
    """
    An immutable dataclass representing the data associated with a
    completed rpc attempt.

    Operation-level fields (eg. type, cluster, zone) are stored on the
    corresponding CompletedOperationMetric or ActiveOperationMetric object.
    """

    duration_ns: int
    end_status: StatusCode
    gfe_latency_ns: int | None = None
    application_blocking_time_ns: int = 0
    backoff_before_attempt_ns: int = 0
    grpc_throttling_time_ns: int = 0


@dataclass(frozen=True)
class CompletedOperationMetric:
    """
    An immutable dataclass representing the data associated with a
    completed rpc operation.

    Attempt-level fields (eg. duration, latencies, etc) are stored on the
    corresponding CompletedAttemptMetric object.
    """

    op_type: OperationType
    uuid: str
    duration_ns: int
    completed_attempts: list[CompletedAttemptMetric]
    final_status: StatusCode
    cluster_id: str
    zone: str
    is_streaming: bool
    first_response_latency_ns: int | None = None
    flow_throttling_time_ns: int = 0


@dataclass
class ActiveAttemptMetric:
    """
    A dataclass representing the data associated with an rpc attempt that is
    currently in progress. Fields are mutable and may be optional.
    """

    # keep monotonic timestamps for active attempts
    start_time_ns: int = field(default_factory=time.monotonic_ns)
    # the time taken by the backend, in nanoseconds. Taken from response header
    gfe_latency_ns: int | None = None
    # time waiting on user to process the response, in nanoseconds
    # currently only relevant for ReadRows
    application_blocking_time_ns: int = 0
    # backoff time is added to application_blocking_time_ns
    backoff_before_attempt_ns: int = 0
    # time waiting on grpc channel, in nanoseconds
    # TODO: capture grpc_throttling_time
    grpc_throttling_time_ns: int = 0


@dataclass
class ActiveOperationMetric:
    """
    A dataclass representing the data associated with an rpc operation that is
    currently in progress. Fields are mutable and may be optional.
    """

    op_type: OperationType
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    # create a default backoff generator, initialized with standard default backoff values
    backoff_generator: TrackedBackoffGenerator = field(
        default_factory=lambda: TrackedBackoffGenerator(
            initial=0.01, maximum=60, multiplier=2
        )
    )
    # keep monotonic timestamps for active operations
    start_time_ns: int = field(default_factory=time.monotonic_ns)
    active_attempt: ActiveAttemptMetric | None = None
    cluster_id: str | None = None
    zone: str | None = None
    completed_attempts: list[CompletedAttemptMetric] = field(default_factory=list)
    is_streaming: bool = False  # only True for read_rows operations
    was_completed: bool = False
    handlers: list[MetricsHandler] = field(default_factory=list)
    # the time it takes to recieve the first response from the server, in nanoseconds
    # attached by interceptor
    # currently only tracked for ReadRows
    first_response_latency_ns: int | None = None
    # time waiting on flow control, in nanoseconds
    flow_throttling_time_ns: int = 0

    @property
    def interceptor_metadata(self) -> tuple[str, str]:
        """
        returns a tuple to attach to the grpc metadata.

        This metadata field will be read by the BigtableMetricsInterceptor to associate a request with an operation
        """
        return OPERATION_INTERCEPTOR_METADATA_KEY, self.uuid

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

        Assumes operation is in CREATED state.
        """
        if self.state != OperationState.CREATED:
            return self._handle_error(INVALID_STATE_ERROR.format("start", self.state))
        self.start_time_ns = time.monotonic_ns()

    def start_attempt(self) -> ActiveAttemptMetric | None:
        """
        Called to initiate a new attempt for the operation.

        Assumes operation is in either CREATED or BETWEEN_ATTEMPTS states
        """
        if (
            self.state != OperationState.BETWEEN_ATTEMPTS
            and self.state != OperationState.CREATED
        ):
            return self._handle_error(
                INVALID_STATE_ERROR.format("start_attempt", self.state)
            )

        try:
            # find backoff value before this attempt
            prev_attempt_idx = len(self.completed_attempts) - 1
            backoff = self.backoff_generator.get_attempt_backoff(prev_attempt_idx)
            # generator will return the backoff time in seconds, so convert to nanoseconds
            backoff_ns = int(backoff * 1e9)
        except IndexError:
            # backoff value not found
            backoff_ns = 0

        self.active_attempt = ActiveAttemptMetric(backoff_before_attempt_ns=backoff_ns)
        return self.active_attempt

    def add_response_metadata(self, metadata: dict[str, bytes | str]) -> None:
        """
        Attach trailing metadata to the active attempt.

        If not called, default values for the metadata will be used.

        Assumes operation is in ACTIVE_ATTEMPT state.

        Args:
          - metadata: the metadata as extracted from the grpc call
        """
        if self.state != OperationState.ACTIVE_ATTEMPT:
            return self._handle_error(
                INVALID_STATE_ERROR.format("add_response_metadata", self.state)
            )
        if self.cluster_id is None or self.zone is None:
            # BIGTABLE_METADATA_KEY should give a binary-encoded ResponseParams proto
            blob = cast(bytes, metadata.get(BIGTABLE_METADATA_KEY))
            if blob:
                parse_result = self._parse_response_metadata_blob(blob)
                if parse_result is not None:
                    cluster, zone = parse_result
                    if cluster:
                        self.cluster_id = cluster
                    if zone:
                        self.zone = zone
                else:
                    self._handle_error(
                        f"Failed to decode {BIGTABLE_METADATA_KEY} metadata: {blob!r}"
                    )
        # SERVER_TIMING_METADATA_KEY should give a string with the server-latency headers
        timing_header = cast(str, metadata.get(SERVER_TIMING_METADATA_KEY))
        if timing_header:
            timing_data = SERVER_TIMING_REGEX.match(timing_header)
            if timing_data and self.active_attempt:
                gfe_latency_ms = float(timing_data.group(1))
                self.active_attempt.gfe_latency_ns = int(gfe_latency_ms * 1e6)

    @staticmethod
    @lru_cache(maxsize=32)
    def _parse_response_metadata_blob(blob: bytes) -> Tuple[str, str] | None:
        """
        Parse the response metadata blob and return a tuple of cluster and zone.

        Function is cached to avoid parsing the same blob multiple times.

        Args:
          - blob: the metadata blob as extracted from the grpc call
        Returns:
          - a tuple of cluster_id and zone, or None if parsing failed
        """
        try:
            proto = ResponseParams.pb().FromString(blob)
            return proto.cluster_id, proto.zone_id
        except (DecodeError, TypeError):
            # failed to parse metadata
            return None

    def end_attempt_with_status(self, status: StatusCode | Exception) -> None:
        """
        Called to mark the end of an attempt for the operation.

        Typically, this is used to mark a retryable error. If a retry will not
        be attempted, `end_with_status` or `end_with_success` should be used
        to finalize the operation along with the attempt.

        Assumes operation is in ACTIVE_ATTEMPT state.

        Args:
          - status: The status of the attempt.
        """
        if self.state != OperationState.ACTIVE_ATTEMPT or self.active_attempt is None:
            return self._handle_error(
                INVALID_STATE_ERROR.format("end_attempt_with_status", self.state)
            )
        if isinstance(status, Exception):
            status = self._exc_to_status(status)
        complete_attempt = CompletedAttemptMetric(
            duration_ns=time.monotonic_ns() - self.active_attempt.start_time_ns,
            end_status=status,
            gfe_latency_ns=self.active_attempt.gfe_latency_ns,
            application_blocking_time_ns=self.active_attempt.application_blocking_time_ns,
            backoff_before_attempt_ns=self.active_attempt.backoff_before_attempt_ns,
            grpc_throttling_time_ns=self.active_attempt.grpc_throttling_time_ns,
        )
        self.completed_attempts.append(complete_attempt)
        self.active_attempt = None
        for handler in self.handlers:
            handler.on_attempt_complete(complete_attempt, self)

    def end_with_status(self, status: StatusCode | Exception) -> None:
        """
        Called to mark the end of the operation. If there is an active attempt,
        end_attempt_with_status will be called with the same status.

        Assumes operation is not already in COMPLETED state.

        Causes on_operation_completed to be called for each registered handler.

        Args:
          - status: The status of the operation.
        """
        if self.state == OperationState.COMPLETED:
            return self._handle_error(
                INVALID_STATE_ERROR.format("end_with_status", self.state)
            )
        final_status = (
            self._exc_to_status(status) if isinstance(status, Exception) else status
        )
        if self.state == OperationState.ACTIVE_ATTEMPT:
            self.end_attempt_with_status(final_status)
        self.was_completed = True
        finalized = CompletedOperationMetric(
            op_type=self.op_type,
            uuid=self.uuid,
            completed_attempts=self.completed_attempts,
            duration_ns=time.monotonic_ns() - self.start_time_ns,
            final_status=final_status,
            cluster_id=self.cluster_id or DEFAULT_CLUSTER_ID,
            zone=self.zone or DEFAULT_ZONE,
            is_streaming=self.is_streaming,
            first_response_latency_ns=self.first_response_latency_ns,
            flow_throttling_time_ns=self.flow_throttling_time_ns,
        )
        for handler in self.handlers:
            handler.on_operation_complete(finalized)

    def end_with_success(self):
        """
        Called to mark the end of the operation with a successful status.

        Assumes operation is not already in COMPLETED state.

        Causes on_operation_completed to be called for each registered handler.
        """
        return self.end_with_status(StatusCode.OK)

    def cancel(self):
        """
        Called to cancel an operation without processing emitting it.
        """
        for handler in self.handlers:
            handler.on_operation_cancelled(self)

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
        if hasattr(exc, "grpc_status_code") and exc.grpc_status_code is not None:
            return exc.grpc_status_code
        if (
            exc.__cause__
            and hasattr(exc.__cause__, "grpc_status_code")
            and exc.__cause__.grpc_status_code is not None
        ):
            return exc.__cause__.grpc_status_code
        return StatusCode.UNKNOWN

    @staticmethod
    def _handle_error(message: str) -> None:
        """
        log error metric system error messages

        Args:
          - message: The message to include in the exception or warning.
        """
        full_message = f"Error in Bigtable Metrics: {message}"
        LOGGER.warning(full_message)

    def __enter__(self):
        """
        Implements the async manager protocol

        Using the operation's context manager provides assurances that the operation
        is always closed when complete, with the proper status code automaticallty
        detected when an exception is raised.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Implements the context manager protocol

        The operation is automatically ended on exit, with the status determined
        by the exception type and value.
        """
        if exc_val is None:
            self.end_with_success()
        else:
            self.end_with_status(exc_val)
