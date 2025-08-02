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

import time
import pytest
import mock

from google.cloud.bigtable.data._metrics.data_model import OperationState as State
from google.cloud.bigtable_v2.types import ResponseParams


class TestActiveOperationMetric:
    def _make_one(self, *args, **kwargs):
        from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric

        return ActiveOperationMetric(*args, **kwargs)

    def test_ctor_defaults(self):
        """
        create an instance with default values
        """
        mock_type = mock.Mock()
        metric = self._make_one(mock_type)
        assert metric.op_type == mock_type
        assert abs(metric.start_time_ns - time.monotonic_ns()) < 1e6  # 1ms buffer
        assert metric.active_attempt is None
        assert metric.cluster_id is None
        assert metric.zone is None
        assert len(metric.completed_attempts) == 0
        assert metric.was_completed is False
        assert len(metric.handlers) == 0
        assert metric.is_streaming is False
        assert metric.flow_throttling_time_ns == 0

    def test_ctor_explicit(self):
        """
        test with explicit arguments
        """
        expected_type = mock.Mock()
        expected_start_time_ns = 7
        expected_active_attempt = mock.Mock()
        expected_cluster_id = "cluster"
        expected_zone = "zone"
        expected_completed_attempts = [mock.Mock()]
        expected_was_completed = True
        expected_handlers = [mock.Mock()]
        expected_is_streaming = True
        expected_flow_throttling = 12
        metric = self._make_one(
            op_type=expected_type,
            start_time_ns=expected_start_time_ns,
            active_attempt=expected_active_attempt,
            cluster_id=expected_cluster_id,
            zone=expected_zone,
            completed_attempts=expected_completed_attempts,
            was_completed=expected_was_completed,
            handlers=expected_handlers,
            is_streaming=expected_is_streaming,
            flow_throttling_time_ns=expected_flow_throttling,
        )
        assert metric.op_type == expected_type
        assert metric.start_time_ns == expected_start_time_ns
        assert metric.active_attempt == expected_active_attempt
        assert metric.cluster_id == expected_cluster_id
        assert metric.zone == expected_zone
        assert metric.completed_attempts == expected_completed_attempts
        assert metric.was_completed == expected_was_completed
        assert metric.handlers == expected_handlers
        assert metric.is_streaming == expected_is_streaming
        assert metric.flow_throttling_time_ns == expected_flow_throttling

    def test_state_machine_w_methods(self):
        """
        Exercise the state machine by calling methods to move between states
        """
        metric = self._make_one(mock.Mock())
        assert metric.state == State.CREATED
        metric.start()
        assert metric.state == State.CREATED
        metric.start_attempt()
        assert metric.state == State.ACTIVE_ATTEMPT
        metric.end_attempt_with_status(Exception())
        assert metric.state == State.BETWEEN_ATTEMPTS
        metric.start_attempt()
        assert metric.state == State.ACTIVE_ATTEMPT
        metric.end_with_success()
        assert metric.state == State.COMPLETED

    def test_state_machine_w_state(self):
        """
        Exercise state machine by directly manupulating state variables

        relevant variables are: active_attempt, completed_attempts, was_completed
        """
        metric = self._make_one(mock.Mock())
        for was_completed_value in [False, True]:
            metric.was_completed = was_completed_value
            for active_operation_value in [None, mock.Mock()]:
                metric.active_attempt = active_operation_value
                for completed_attempts_value in [[], [mock.Mock()]]:
                    metric.completed_attempts = completed_attempts_value
                    if was_completed_value:
                        assert metric.state == State.COMPLETED
                    elif active_operation_value is not None:
                        assert metric.state == State.ACTIVE_ATTEMPT
                    elif completed_attempts_value:
                        assert metric.state == State.BETWEEN_ATTEMPTS
                    else:
                        assert metric.state == State.CREATED

    @pytest.mark.parametrize(
        "method,args,valid_states,error_method_name",
        [
            ("start", (), (State.CREATED,), None),
            ("start_attempt", (), (State.CREATED, State.BETWEEN_ATTEMPTS), None),
            ("add_response_metadata", ({},), (State.ACTIVE_ATTEMPT,), None),
            ("attempt_first_response", (), (State.ACTIVE_ATTEMPT,), None),
            ("end_attempt_with_status", (mock.Mock(),), (State.ACTIVE_ATTEMPT,), None),
            (
                "end_with_status",
                (mock.Mock(),),
                (
                    State.CREATED,
                    State.ACTIVE_ATTEMPT,
                    State.BETWEEN_ATTEMPTS,
                ),
                None,
            ),
            (
                "end_with_success",
                (),
                (
                    State.CREATED,
                    State.ACTIVE_ATTEMPT,
                    State.BETWEEN_ATTEMPTS,
                ),
                "end_with_status",
            ),
        ],
        ids=lambda x: x if isinstance(x, str) else "",
    )
    def test_error_invalid_states(self, method, args, valid_states, error_method_name):
        """
        each method only works for certain states. Make sure _handle_error is called for invalid states
        """
        cls = type(self._make_one(mock.Mock()))
        invalid_states = set(State) - set(valid_states)
        error_method_name = error_method_name or method
        for state in invalid_states:
            with mock.patch.object(cls, "_handle_error") as mock_handle_error:
                mock_handle_error.return_value = None
                metric = self._make_one(mock.Mock())
                if state == State.ACTIVE_ATTEMPT:
                    metric.active_attempt = mock.Mock()
                elif state == State.BETWEEN_ATTEMPTS:
                    metric.completed_attempts.append(mock.Mock())
                elif state == State.COMPLETED:
                    metric.was_completed = True
                return_obj = getattr(metric, method)(*args)
                assert return_obj is None
                assert mock_handle_error.call_count == 1
                assert (
                    mock_handle_error.call_args[0][0]
                    == f"Invalid state for {error_method_name}: {state}"
                )

    def test_start(self):
        """
        calling start op operation should reset start_time
        """
        orig_time = 0
        metric = self._make_one(mock.Mock(), start_time_ns=orig_time)
        assert abs(metric.start_time_ns - time.monotonic_ns()) > 1e6  # 1ms buffer
        metric.start()
        assert metric.start_time_ns != orig_time
        assert abs(metric.start_time_ns - time.monotonic_ns()) < 1e6  # 1ms buffer
        # should remain in CREATED state after completing
        assert metric.state == State.CREATED

    def test_start_attempt(self):
        """
        calling start_attempt should create a new emptu atempt metric
        """
        from google.cloud.bigtable.data._metrics.data_model import ActiveAttemptMetric

        metric = self._make_one(mock.Mock())
        assert metric.active_attempt is None
        metric.start_attempt()
        assert isinstance(metric.active_attempt, ActiveAttemptMetric)
        # make sure it was initialized with the correct values
        assert (
            abs(time.monotonic_ns() - metric.active_attempt.start_time_ns) < 1e6
        )  # 1ms buffer
        assert metric.active_attempt.first_response_latency_ns is None
        assert metric.active_attempt.gfe_latency_ns is None
        assert metric.active_attempt.grpc_throttling_time_ns == 0
        # should be in ACTIVE_ATTEMPT state after completing
        assert metric.state == State.ACTIVE_ATTEMPT

    def test_start_attempt_with_backoff_generator(self):
        """
        If operation has a backoff generator, it should be used to attach backoff
        times to attempts
        """
        from google.cloud.bigtable.data._helpers import TrackedBackoffGenerator

        generator = TrackedBackoffGenerator()
        # pre-seed generator with exepcted values
        generator.history = list(range(10))
        metric = self._make_one(mock.Mock(), backoff_generator=generator)
        # initialize generator
        next(metric.backoff_generator)
        metric.start_attempt()
        assert len(metric.completed_attempts) == 0
        # first attempt should always be 0
        assert metric.active_attempt.backoff_before_attempt_ns == 0
        # later attempts should have their attempt number as backoff time
        for i in range(10):
            metric.end_attempt_with_status(mock.Mock())
            assert len(metric.completed_attempts) == i + 1
            metric.start_attempt()
            # expect the backoff to be converted froms seconds to ns
            assert metric.active_attempt.backoff_before_attempt_ns == (i * 1e9)

    @pytest.mark.parametrize(
        "start_cluster,start_zone,metadata_proto,end_cluster,end_zone",
        [
            (None, None, None, None, None),
            ("orig_cluster", "orig_zone", None, "orig_cluster", "orig_zone"),
            (None, None, ResponseParams(), None, None),
            (
                "orig_cluster",
                "orig_zone",
                ResponseParams(),
                "orig_cluster",
                "orig_zone",
            ),
            (
                None,
                None,
                ResponseParams(cluster_id="test-cluster", zone_id="us-central1-b"),
                "test-cluster",
                "us-central1-b",
            ),
            (
                None,
                "filled",
                ResponseParams(cluster_id="cluster", zone_id="zone"),
                "cluster",
                "zone",
            ),
            (None, "filled", ResponseParams(cluster_id="cluster"), "cluster", "filled"),
            (None, "filled", ResponseParams(zone_id="zone"), None, "zone"),
            (
                "filled",
                None,
                ResponseParams(cluster_id="cluster", zone_id="zone"),
                "cluster",
                "zone",
            ),
            ("filled", None, ResponseParams(cluster_id="cluster"), "cluster", None),
            ("filled", None, ResponseParams(zone_id="zone"), "filled", "zone"),
        ],
    )
    def test_add_response_metadata_cbt_header(
        self, start_cluster, start_zone, metadata_proto, end_cluster, end_zone
    ):
        """
        calling add_response_metadata should update fields based on grpc response metadata
        The x-goog-ext-425905942-bin field contains cluster and zone info
        """
        import grpc

        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            metric = self._make_one(
                mock.Mock(), cluster_id=start_cluster, zone=start_zone
            )
            metric.active_attempt = mock.Mock()
            metric.active_attempt.gfe_latency_ns = None
            metadata = grpc.aio.Metadata()
            if metadata_proto is not None:
                metadata["x-goog-ext-425905942-bin"] = ResponseParams.serialize(
                    metadata_proto
                )
            metric.add_response_metadata(metadata)
            assert metric.cluster_id == end_cluster
            assert metric.zone == end_zone
            # should remain in ACTIVE_ATTEMPT state after completing
            assert metric.state == State.ACTIVE_ATTEMPT
            # no errors encountered
            assert mock_handle_error.call_count == 0
            # gfe latency should not be touched
            assert metric.active_attempt.gfe_latency_ns is None

    @pytest.mark.parametrize(
        "metadata_field",
        [
            b"cluster",
            "cluster zone",  # expect bytes
        ],
    )
    def test_add_response_metadata_cbt_header_w_error(self, metadata_field):
        """
        If the x-goog-ext-425905942-bin field is present, but not structured properly,
        _handle_error should be called

        Extra fields should not result in parsingerror
        """
        import grpc

        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            metric = self._make_one(mock.Mock())
            metric.cluster_id = None
            metric.zone = None
            metric.active_attempt = mock.Mock()
            metadata = grpc.aio.Metadata()
            metadata["x-goog-ext-425905942-bin"] = metadata_field
            metric.add_response_metadata(metadata)
            # should remain in ACTIVE_ATTEMPT state after completing
            assert metric.state == State.ACTIVE_ATTEMPT
            # no errors encountered
            assert mock_handle_error.call_count == 1
            assert (
                "Failed to decode x-goog-ext-425905942-bin metadata:"
                in mock_handle_error.call_args[0][0]
            )
            assert str(metadata_field) in mock_handle_error.call_args[0][0]

    @pytest.mark.parametrize(
        "metadata_field,expected_latency_ns",
        [
            (None, None),
            ("gfet4t7; dur=1000", 1000e6),
            ("gfet4t7; dur=1000.0", 1000e6),
            ("gfet4t7; dur=1000.1", 1000.1e6),
            ("gcp; dur=15, gfet4t7; dur=300", 300e6),
            ("gfet4t7;dur=350,gcp;dur=12", 350e6),
            ("ignore_megfet4t7;dur=90ignore_me", 90e6),
            ("gfet4t7;dur=2000", 2000e6),
            ("gfet4t7; dur=0.001", 1000),
            ("gfet4t7; dur=0.000001", 1),
            ("gfet4t7; dur=0.0000001", 0),  # below recording resolution
            ("gfet4t7; dur=0", 0),
            ("gfet4t7; dur=empty", None),
            ("gfet4t7;", None),
            ("", None),
        ],
    )
    def test_add_response_metadata_server_timing_header(
        self, metadata_field, expected_latency_ns
    ):
        """
        calling add_response_metadata should update fields based on grpc response metadata
        The server-timing field contains gfle latency info
        """
        import grpc

        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            metric = self._make_one(mock.Mock())
            metric.active_attempt = mock.Mock()
            metric.active_attempt.gfe_latency_ns = None
            metadata = grpc.aio.Metadata()
            if metadata_field:
                metadata["server-timing"] = metadata_field
            metric.add_response_metadata(metadata)
            if metric.active_attempt.gfe_latency_ns is None:
                assert expected_latency_ns is None
            else:
                assert metric.active_attempt.gfe_latency_ns == int(expected_latency_ns)
            # should remain in ACTIVE_ATTEMPT state after completing
            assert metric.state == State.ACTIVE_ATTEMPT
            # no errors encountered
            assert mock_handle_error.call_count == 0
            # cluster and zone should not be touched
            assert metric.cluster_id is None
            assert metric.zone is None

    def test_attempt_first_response(self):
        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            metric = self._make_one(mock.Mock())
            metric.start_attempt()
            metric.active_attempt.start_time_ns = 0
            metric.attempt_first_response()
            got_latency_ns = metric.active_attempt.first_response_latency_ns
            # latency should be equal to current time
            assert abs(got_latency_ns - time.monotonic_ns()) < 1e6  # 1ms
            # should remain in ACTIVE_ATTEMPT state after completing
            assert metric.state == State.ACTIVE_ATTEMPT
            # no errors encountered
            assert mock_handle_error.call_count == 0
            # calling it again should cause an error
            metric.attempt_first_response()
            assert mock_handle_error.call_count == 1
            assert (
                mock_handle_error.call_args[0][0]
                == "Attempt already received first response"
            )
            # value should not be changed
            assert metric.active_attempt.first_response_latency_ns == got_latency_ns

    def test_end_attempt_with_status(self):
        """
        ending the attempt should:
        - add one to completed_attempts
        - reset active_attempt to None
        - update state
        """
        expected_latency_ns = 9
        expected_start_time = 1
        expected_status = object()
        expected_gfe_latency_ns = 5
        expected_app_blocking = 12
        expected_backoff = 2
        expected_grpc_throttle = 3

        metric = self._make_one(mock.Mock())
        assert metric.active_attempt is None
        assert len(metric.completed_attempts) == 0
        metric.start_attempt()
        metric.active_attempt.start_time_ns = expected_start_time
        metric.active_attempt.gfe_latency_ns = expected_gfe_latency_ns
        metric.active_attempt.first_response_latency_ns = expected_latency_ns
        metric.active_attempt.application_blocking_time_ns = expected_app_blocking
        metric.active_attempt.backoff_before_attempt_ns = expected_backoff
        metric.active_attempt.grpc_throttling_time_ns = expected_grpc_throttle
        metric.end_attempt_with_status(expected_status)
        assert len(metric.completed_attempts) == 1
        got_attempt = metric.completed_attempts[0]
        expected_duration = time.monotonic_ns() - expected_start_time
        assert abs(got_attempt.duration_ns - expected_duration) < 10e6  # within 10ms
        assert got_attempt.first_response_latency_ns == expected_latency_ns
        assert got_attempt.grpc_throttling_time_ns == expected_grpc_throttle
        assert got_attempt.end_status == expected_status
        assert got_attempt.gfe_latency_ns == expected_gfe_latency_ns
        assert got_attempt.application_blocking_time_ns == expected_app_blocking
        assert got_attempt.backoff_before_attempt_ns == expected_backoff
        # state should be changed to BETWEEN_ATTEMPTS
        assert metric.state == State.BETWEEN_ATTEMPTS

    def test_end_attempt_with_status_w_exception(self):
        """
        exception inputs should be converted to grpc status objects
        """
        input_status = ValueError("test")
        expected_status = object()

        metric = self._make_one(mock.Mock())
        metric.start_attempt()
        with mock.patch.object(
            metric, "_exc_to_status", return_value=expected_status
        ) as mock_exc_to_status:
            metric.end_attempt_with_status(input_status)
            assert mock_exc_to_status.call_count == 1
            assert mock_exc_to_status.call_args[0][0] == input_status
            assert metric.completed_attempts[0].end_status == expected_status

    def test_end_with_status(self):
        """
        ending the operation should:
        - end active attempt
        - mark operation as completed
        - update handlers
        """
        from google.cloud.bigtable.data._metrics.data_model import ActiveAttemptMetric

        expected_attempt_start_time = 0
        expected_attempt_first_response_latency_ns = 9
        expected_attempt_gfe_latency_ns = 5
        expected_flow_time = 16

        expected_status = object()
        expected_type = object()
        expected_start_time = 1
        expected_cluster = object()
        expected_zone = object()
        is_streaming = object()

        handlers = [mock.Mock(), mock.Mock()]
        metric = self._make_one(
            expected_type, handlers=handlers, start_time_ns=expected_start_time
        )
        metric.cluster_id = expected_cluster
        metric.zone = expected_zone
        metric.is_streaming = is_streaming
        metric.flow_throttling_time_ns = expected_flow_time
        attempt = ActiveAttemptMetric(
            start_time_ns=expected_attempt_start_time,
            first_response_latency_ns=expected_attempt_first_response_latency_ns,
            gfe_latency_ns=expected_attempt_gfe_latency_ns,
        )
        metric.active_attempt = attempt
        metric.end_with_status(expected_status)
        # test that ActiveOperation was updated to terminal state
        assert metric.state == State.COMPLETED
        assert metric.was_completed is True
        assert metric.active_attempt is None
        assert len(metric.completed_attempts) == 1
        # check that finalized operation was passed to handlers
        for h in handlers:
            assert h.on_operation_complete.call_count == 1
            assert len(h.on_operation_complete.call_args[0]) == 1
            called_with = h.on_operation_complete.call_args[0][0]
            assert called_with.op_type == expected_type
            expected_duration = time.monotonic_ns() - expected_start_time
            assert (
                abs(called_with.duration_ns - expected_duration) < 10e6
            )  # within 10ms
            assert called_with.final_status == expected_status
            assert called_with.cluster_id == expected_cluster
            assert called_with.zone == expected_zone
            assert called_with.is_streaming == is_streaming
            assert called_with.flow_throttling_time_ns == expected_flow_time
            # check the attempt
            assert len(called_with.completed_attempts) == 1
            final_attempt = called_with.completed_attempts[0]
            assert (
                final_attempt.first_response_latency_ns
                == expected_attempt_first_response_latency_ns
            )
            assert final_attempt.gfe_latency_ns == expected_attempt_gfe_latency_ns
            assert final_attempt.end_status == expected_status
            expected_duration = time.monotonic_ns() - expected_attempt_start_time
            assert (
                abs(final_attempt.duration_ns - expected_duration) < 10e6
            )  # within 10ms

    def test_end_with_status_w_exception(self):
        """
        exception inputs should be converted to grpc status objects
        """
        input_status = ValueError("test")
        expected_status = object()
        handlers = [mock.Mock()]

        metric = self._make_one(mock.Mock(), handlers=handlers)
        metric.start_attempt()
        with mock.patch.object(
            metric, "_exc_to_status", return_value=expected_status
        ) as mock_exc_to_status:
            metric.end_with_status(input_status)
            assert mock_exc_to_status.call_count == 1
            assert mock_exc_to_status.call_args[0][0] == input_status
            assert metric.completed_attempts[0].end_status == expected_status
            final_op = handlers[0].on_operation_complete.call_args[0][0]
            assert final_op.final_status == expected_status

    def test_end_with_success(self):
        """
        end with success should be a pass-through helper for end_with_status
        """
        from grpc import StatusCode

        inner_result = object()

        metric = self._make_one(mock.Mock())
        with mock.patch.object(metric, "end_with_status") as mock_end_with_status:
            mock_end_with_status.return_value = inner_result
            got_result = metric.end_with_success()
            assert mock_end_with_status.call_count == 1
            assert mock_end_with_status.call_args[0][0] == StatusCode.OK
            assert got_result is inner_result

    def test_end_on_empty_operation(self):
        """
        Should be able to end an operation without any attempts
        """
        from grpc import StatusCode

        handlers = [mock.Mock()]
        metric = self._make_one(mock.Mock(), handlers=handlers)
        metric.end_with_success()
        assert metric.state == State.COMPLETED
        assert metric.was_completed is True
        final_op = handlers[0].on_operation_complete.call_args[0][0]
        assert final_op.final_status == StatusCode.OK
        assert final_op.completed_attempts == []

    def test_build_wrapped_predicate(self):
        """
        predicate generated by object should terminate attempt or operation
        based on passed in predicate
        """
        input_exc = ValueError("test")
        cls = type(self._make_one(object()))
        # ensure predicate is called with the exception
        mock_predicate = mock.Mock()
        cls.build_wrapped_predicate(mock.Mock(), mock_predicate)(input_exc)
        assert mock_predicate.call_count == 1
        assert mock_predicate.call_args[0][0] == input_exc
        assert len(mock_predicate.call_args[0]) == 1
        # if predicate is true, end the attempt
        mock_instance = mock.Mock()
        cls.build_wrapped_predicate(mock_instance, lambda x: True)(input_exc)
        assert mock_instance.end_attempt_with_status.call_count == 1
        assert mock_instance.end_attempt_with_status.call_args[0][0] == input_exc
        assert len(mock_instance.end_attempt_with_status.call_args[0]) == 1
        # if predicate is false, end the operation
        mock_instance = mock.Mock()
        cls.build_wrapped_predicate(mock_instance, lambda x: False)(input_exc)
        assert mock_instance.end_with_status.call_count == 1
        assert mock_instance.end_with_status.call_args[0][0] == input_exc
        assert len(mock_instance.end_with_status.call_args[0]) == 1

    def test__exc_to_status(self):
        """
        Should return grpc_status_code if grpc error, otherwise UNKNOWN

        If BigtableExceptionGroup, use the most recent exception in the group
        """
        from grpc import StatusCode
        from google.api_core import exceptions as core_exc
        from google.cloud.bigtable.data import exceptions as bt_exc

        cls = type(self._make_one(object()))
        # unknown for non-grpc errors
        assert cls._exc_to_status(ValueError()) == StatusCode.UNKNOWN
        assert cls._exc_to_status(RuntimeError()) == StatusCode.UNKNOWN
        # grpc status code for grpc errors
        assert (
            cls._exc_to_status(core_exc.InvalidArgument("msg"))
            == StatusCode.INVALID_ARGUMENT
        )
        assert cls._exc_to_status(core_exc.NotFound("msg")) == StatusCode.NOT_FOUND
        assert (
            cls._exc_to_status(core_exc.AlreadyExists("msg"))
            == StatusCode.ALREADY_EXISTS
        )
        assert (
            cls._exc_to_status(core_exc.PermissionDenied("msg"))
            == StatusCode.PERMISSION_DENIED
        )
        cause_exc = core_exc.AlreadyExists("msg")
        w_cause = core_exc.DeadlineExceeded("msg")
        w_cause.__cause__ = cause_exc
        assert cls._exc_to_status(w_cause) == StatusCode.DEADLINE_EXCEEDED
        # use cause if available
        w_cause = ValueError("msg")
        w_cause.__cause__ = cause_exc
        cause_exc.grpc_status_code = object()
        custom_excs = [
            bt_exc.FailedMutationEntryError(1, mock.Mock(), cause=cause_exc),
            bt_exc.FailedQueryShardError(1, {}, cause=cause_exc),
            w_cause,
        ]
        for exc in custom_excs:
            assert cls._exc_to_status(exc) == cause_exc.grpc_status_code, exc
        # extract most recent exception for bigtable exception groups
        exc_groups = [
            bt_exc._BigtableExceptionGroup("", [ValueError(), cause_exc]),
            bt_exc.RetryExceptionGroup([RuntimeError(), cause_exc]),
            bt_exc.ShardedReadRowsExceptionGroup(
                [bt_exc.FailedQueryShardError(1, {}, cause=cause_exc)], [], 2
            ),
            bt_exc.MutationsExceptionGroup(
                [bt_exc.FailedMutationEntryError(1, mock.Mock(), cause=cause_exc)], 2
            ),
        ]
        for exc in exc_groups:
            assert cls._exc_to_status(exc) == cause_exc.grpc_status_code, exc

    def test__handle_error(self):
        """
        handle_error should write log
        """
        input_message = "test message"
        expected_message = f"Error in Bigtable Metrics: {input_message}"
        with mock.patch(
            "google.cloud.bigtable.data._metrics.data_model.LOGGER"
        ) as logger_mock:
            type(self._make_one(object()))._handle_error(input_message)
            assert logger_mock.warning.call_count == 1
            assert logger_mock.warning.call_args[0][0] == expected_message
            assert len(logger_mock.warning.call_args[0]) == 1

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """
        Should implement context manager protocol
        """
        metric = self._make_one(object())
        with mock.patch.object(metric, "end_with_success") as end_with_success_mock:
            end_with_success_mock.side_effect = lambda: metric.end_with_status(object())
            async with metric as context:
                assert isinstance(context, type(metric)._AsyncContextManager)
                assert context.operation == metric
                # inside context manager, still active
                assert end_with_success_mock.call_count == 0
                assert metric.state == State.CREATED
            # outside context manager, should be ended
            assert end_with_success_mock.call_count == 1
            assert metric.state == State.COMPLETED

    @pytest.mark.asyncio
    async def test_async_context_manager_exception(self):
        """
        Exception within context manager causes end_with_status to be called with error
        """
        expected_exc = ValueError("expected")
        metric = self._make_one(object())
        with mock.patch.object(metric, "end_with_status") as end_with_status_mock:
            try:
                async with metric as context:
                    assert isinstance(context, type(metric)._AsyncContextManager)
                    assert context.operation == metric
                    # inside context manager, still active
                    assert end_with_status_mock.call_count == 0
                    assert metric.state == State.CREATED
                    raise expected_exc
            except ValueError as e:
                assert e == expected_exc
            # outside context manager, should be ended
            assert end_with_status_mock.call_count == 1
            assert end_with_status_mock.call_args[0][0] == expected_exc
            assert len(end_with_status_mock.call_args[0]) == 1

    @pytest.mark.asyncio
    async def test_metadata_passthrough(self):
        """
        add_response_metadata in context manager should defer to wrapped operation
        """
        inner_result = object()
        fake_metadata = object()

        metric = self._make_one(mock.Mock())
        with mock.patch.object(metric, "add_response_metadata") as mock_add_metadata:
            mock_add_metadata.return_value = inner_result
            async with metric as context:
                result = context.add_response_metadata(fake_metadata)
                assert result == inner_result
                assert mock_add_metadata.call_count == 1
                assert mock_add_metadata.call_args[0][0] == fake_metadata
                assert len(mock_add_metadata.call_args[0]) == 1

    @pytest.mark.asyncio
    async def test_wrap_attempt_fn_success(self):
        """
        Context manager's wrap_attempt_fn should wrap an arbitrary function
        in operation instrumentation

        Test successful call
        - should return the result of the wrapped function
        - should call end_with_success
        """
        from grpc import StatusCode

        metric = self._make_one(object())
        async with metric as context:
            mock_call = mock.AsyncMock()
            mock_args = (1, 2, 3)
            mock_kwargs = {"a": 1, "b": 2}
            inner_fn = lambda *args, **kwargs: mock_call(*args, **kwargs)  # noqa
            wrapped_fn = context.wrap_attempt_fn(inner_fn, extract_call_metadata=False)
            # make the wrapped call
            result = await wrapped_fn(*mock_args, **mock_kwargs)
            assert result == mock_call.return_value
            assert mock_call.call_count == 1
            assert mock_call.call_args[0] == mock_args
            assert mock_call.call_args[1] == mock_kwargs
            assert mock_call.await_count == 1
            # operation should be still in progress after wrapped fn
            # let context manager close it, in case we need to add metadata, etc
            assert metric.state == State.ACTIVE_ATTEMPT
        # make sure the operation is complete after exiting context manager
        assert metric.state == State.COMPLETED
        assert len(metric.completed_attempts) == 1
        assert metric.completed_attempts[0].end_status == StatusCode.OK

    @pytest.mark.asyncio
    async def test_wrap_attempt_fn_failed_extract_call_metadata(self):
        """
        When extract_call_metadata is True, should call add_response_metadata
        on operation with output of wrapped function, even if failed
        """
        mock_call = mock.AsyncMock()
        mock_call.trailing_metadata.return_value = 3
        mock_call.initial_metadata.return_value = 4
        inner_fn = lambda *args, **kwargs: mock_call  # noqa
        metric = self._make_one(object())
        async with metric as context:
            wrapped_fn = context.wrap_attempt_fn(inner_fn, extract_call_metadata=True)
            with mock.patch.object(
                metric, "add_response_metadata"
            ) as mock_add_metadata:
                # make the wrapped call. expect exception when awaiting on mock_call
                with pytest.raises(TypeError):
                    await wrapped_fn()
                assert mock_add_metadata.call_count == 1
                assert mock_call.trailing_metadata.call_count == 1
                assert mock_call.initial_metadata.call_count == 1
                assert mock_add_metadata.call_args[0][0] == 3 + 4

    @pytest.mark.asyncio
    async def test_wrap_attempt_fn_failed_extract_call_metadata_no_mock(self):
        """
        Make sure the metadata is accessible after a failed attempt
        """
        import grpc

        mock_call = mock.AsyncMock()
        mock_call.trailing_metadata.return_value = grpc.aio.Metadata()
        mock_call.initial_metadata.return_value = grpc.aio.Metadata(
            ("server-timing", "gfet4t7; dur=5000")
        )
        inner_fn = lambda *args, **kwargs: mock_call  # noqa
        metric = self._make_one(object())
        async with metric as context:
            wrapped_fn = context.wrap_attempt_fn(inner_fn, extract_call_metadata=True)
            with pytest.raises(TypeError):
                await wrapped_fn()
            assert metric.active_attempt is None
            assert len(metric.completed_attempts) == 1
            assert metric.completed_attempts[0].gfe_latency_ns == 5000e6  # ms to ns

    @pytest.mark.asyncio
    async def test_wrap_attempt_fn_failed_attempt(self):
        """
        failed attempts should call operation.end_attempt with error
        """
        from grpc import StatusCode

        metric = self._make_one(object())
        async with metric as context:
            wrapped_fn = context.wrap_attempt_fn(
                mock.Mock(), extract_call_metadata=False
            )
            # make the wrapped call. expect type error when awaiting response of mock
            with pytest.raises(TypeError):
                await wrapped_fn()
            # should have one failed attempt, but operation still in progress
            assert len(metric.completed_attempts) == 1
            assert metric.state == State.BETWEEN_ATTEMPTS
            assert metric.active_attempt is None
            # unknown status from type error
            assert metric.completed_attempts[0].end_status == StatusCode.UNKNOWN
        # make sure operation is closed on end
        assert metric.state == State.COMPLETED

    @pytest.mark.asyncio
    async def test_wrap_attempt_fn_with_retry(self):
        """
        wrap_attampt_fn is meant to be used with retry object. Test using them together
        """
        from grpc import StatusCode
        from google.api_core.retry import AsyncRetry
        from google.api_core.exceptions import RetryError

        metric = self._make_one(object())
        with pytest.raises(RetryError):
            # should eventually fail due to timeout
            async with metric as context:
                always_retry = lambda x: True  # noqa
                retry_obj = AsyncRetry(
                    predicate=always_retry, timeout=0.05, maximum=0.001
                )
                # mock.Mock will fail on await
                double_wrapped_fn = retry_obj(
                    context.wrap_attempt_fn(mock.Mock(), extract_call_metadata=False)
                )
                await double_wrapped_fn()
        # make sure operation ended with expected state
        assert metric.state == State.COMPLETED
        # we expect > 30 retries in 0.05 seconds
        assert len(metric.completed_attempts) > 5
        # unknown error due to TyperError
        assert metric.completed_attempts[-1].end_status == StatusCode.UNKNOWN
