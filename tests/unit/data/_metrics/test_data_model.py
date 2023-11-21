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
from uuid import UUID

from google.cloud.bigtable.data._metrics.data_model import OperationType
from google.cloud.bigtable.data._metrics.data_model import OperationState as State


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
        assert metric.start_time - time.monotonic() < 0.1
        assert metric.op_id is not None
        assert isinstance(metric.op_id, UUID)
        assert metric.active_attempt is None
        assert metric.cluster_id is None
        assert metric.zone is None
        assert len(metric.completed_attempts) == 0
        assert metric.was_completed is False
        assert len(metric.handlers) == 0
        assert metric.is_streaming is False

    def test_ctor_explicit(self):
        """
        test with explicit arguments
        """
        expected_type = mock.Mock()
        expected_start_time = 12
        expected_op_id = 5
        expected_active_attempt = mock.Mock()
        expected_cluster_id = "cluster"
        expected_zone = "zone"
        expected_completed_attempts = [mock.Mock()]
        expected_was_completed = True
        expected_handlers = [mock.Mock()]
        expected_is_streaming = True
        metric = self._make_one(
            op_type=expected_type,
            start_time=expected_start_time,
            op_id=expected_op_id,
            active_attempt=expected_active_attempt,
            cluster_id=expected_cluster_id,
            zone=expected_zone,
            completed_attempts=expected_completed_attempts,
            was_completed=expected_was_completed,
            handlers=expected_handlers,
            is_streaming=expected_is_streaming,
        )
        assert metric.op_type == expected_type
        assert metric.start_time == expected_start_time
        assert metric.op_id == expected_op_id
        assert metric.active_attempt == expected_active_attempt
        assert metric.cluster_id == expected_cluster_id
        assert metric.zone == expected_zone
        assert metric.completed_attempts == expected_completed_attempts
        assert metric.was_completed == expected_was_completed
        assert metric.handlers == expected_handlers
        assert metric.is_streaming == expected_is_streaming

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


    @pytest.mark.parametrize("method,args,valid_states,error_method_name", [
        ("start", (), (State.CREATED,), None),
        ("start_attempt", (), (State.CREATED, State.BETWEEN_ATTEMPTS), None),
        ("add_call_metadata", ({},), (State.ACTIVE_ATTEMPT,), None),
        ("attempt_first_response", (), (State.ACTIVE_ATTEMPT,), None),
        ("end_attempt_with_status", (mock.Mock(),), (State.ACTIVE_ATTEMPT,), None),
        ("end_with_status", (mock.Mock(),), (State.CREATED, State.ACTIVE_ATTEMPT,State.BETWEEN_ATTEMPTS,), None),
        ("end_with_success", (), (State.CREATED, State.ACTIVE_ATTEMPT,State.BETWEEN_ATTEMPTS,), "end_with_status"),
    ], ids=lambda x: x if isinstance(x, str) else "")
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
                assert mock_handle_error.call_args[0][0] == f"Invalid state for {error_method_name}: {state}"

    def test_start(self):
        """
        calling start op operation should reset start_time
        """
        orig_time = 0
        metric = self._make_one(mock.Mock(), start_time=orig_time)
        assert abs(metric.start_time - time.monotonic()) > 5
        metric.start()
        assert metric.start_time != orig_time
        assert metric.start_time - time.monotonic() < 0.1
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
        assert time.monotonic() - metric.active_attempt.start_time < 0.1
        assert metric.active_attempt.first_response_latency is None
        assert metric.active_attempt.gfe_latency is None
        # should be in ACTIVE_ATTEMPT state after completing
        assert metric.state == State.ACTIVE_ATTEMPT


    @pytest.mark.parametrize("start_cluster,start_zone,metadata_field,end_cluster,end_zone", [
        (None,None, None, None, None),
        ("orig_cluster", "orig_zone", None, "orig_cluster", "orig_zone"),
        (None,None, b"cluster zone", "cluster", "zone"),
        (None,None, b'\n\rus-central1-b\x12\x0ctest-cluster', "us-central1-b", "test-cluster"),
        ("orig_cluster","orig_zone", b"new_new", "orig_cluster", "orig_zone"),
        (None,None, b"", None, None),
        (None,None, b"cluster zone future", "cluster", "zone"),
        (None, "filled", b"cluster zone", "cluster", "zone"),
        ("filled", None, b"cluster zone", "cluster", "zone"),
    ])
    def test_add_call_metadata_cbt_header(self, start_cluster, start_zone, metadata_field, end_cluster, end_zone):
        """
        calling add_call_metadata should update fields based on grpc response metadata
        The x-goog-ext-425905942-bin field contains cluster and zone info
        """
        import grpc
        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            metric = self._make_one(mock.Mock(), cluster_id=start_cluster, zone=start_zone)
            metric.active_attempt = mock.Mock()
            metric.active_attempt.gfe_latency = None
            metadata = grpc.aio.Metadata()
            if metadata_field:
                metadata['x-goog-ext-425905942-bin'] = metadata_field
            metric.add_call_metadata(metadata)
            assert metric.cluster_id == end_cluster
            assert metric.zone == end_zone
            # should remain in ACTIVE_ATTEMPT state after completing
            assert metric.state == State.ACTIVE_ATTEMPT
            # no errors encountered
            assert mock_handle_error.call_count == 0
            # gfe latency should not be touched
            assert metric.active_attempt.gfe_latency is None

    @pytest.mark.parametrize("metadata_field", [
        b"cluster",
        "cluster zone",  # expect bytes
    ])
    def test_add_call_metadata_cbt_header_w_error(self, metadata_field):
        """
        If the x-goog-ext-425905942-bin field is present, but not structured properly,
        _handle_error should be called

        Extra fields should not result in parsing error
        """
        import grpc
        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            metric = self._make_one(mock.Mock())
            metric.cluster_id = None
            metric.zone = None
            metric.active_attempt = mock.Mock()
            metadata = grpc.aio.Metadata()
            metadata['x-goog-ext-425905942-bin'] = metadata_field
            metric.add_call_metadata(metadata)
            # should remain in ACTIVE_ATTEMPT state after completing
            assert metric.state == State.ACTIVE_ATTEMPT
            # no errors encountered
            assert mock_handle_error.call_count == 1
            assert mock_handle_error.call_args[0][0] == f"Failed to decode x-goog-ext-425905942-bin metadata: {metadata_field}"

    @pytest.mark.parametrize("metadata_field,expected_latency", [
        (None, None),
        ("gfet4t7; dur=1000", 1),
        ("gfet4t7; dur=1000.0", 1),
        ("gfet4t7; dur=1000.1", 1.0001),
        ("gfet4t7; dur=1000 dur=2000", 2),
        ("gfet4t7; dur=0", 0),
        ("gfet4t7; dur=empty", None),
        ("gfet4t7;", None),
        ("", None),
    ])
    def test_add_call_metadata_server_timing_header(self, metadata_field, expected_latency):
        """
        calling add_call_metadata should update fields based on grpc response metadata
        The server-timing field contains gfle latency info
        """
        import grpc
        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            metric = self._make_one(mock.Mock())
            metric.active_attempt = mock.Mock()
            metric.active_attempt.gfe_latency = None
            metadata = grpc.aio.Metadata()
            if metadata_field:
                metadata['server-timing'] = metadata_field
            metric.add_call_metadata(metadata)
            if metric.active_attempt.gfe_latency is None:
                assert expected_latency is None
            else:
                assert (metric.active_attempt.gfe_latency - expected_latency) < 0.0001
            # should remain in ACTIVE_ATTEMPT state after completing
            assert metric.state == State.ACTIVE_ATTEMPT
            # no errors encountered
            assert mock_handle_error.call_count == 0
            # cluster and zone should not be touched
            assert metric.cluster_id is None
            assert metric.zone is None

