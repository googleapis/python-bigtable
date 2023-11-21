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

    @pytest.mark.parametrize("method,args", [
        ("start", ()),
        ("start_attempt", ()),
        ("add_call_metadata", ({},)),
        ("attempt_first_response", ()),
        ("end_attempt_with_status", (mock.Mock(),)),
        ("end_with_status", (mock.Mock(),)),
        ("end_with_success", ()),
    ])
    def test_error_completed_operation(self, method, args):
        """
        calling any method on a completed operation should call _handle_error
        to log or raise an error
        """
        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            mock_handle_error.return_value = None
            metric = self._make_one(mock.Mock(), was_completed=True)
            return_obj = getattr(metric, method)(*args)
            assert return_obj is None
            assert mock_handle_error.call_count == 1
            assert mock_handle_error.call_args[0][0] == "Operation already completed"

    @pytest.mark.parametrize("method,args", [
        ("add_call_metadata", ({},)),
        ("attempt_first_response", ()),
        ("end_attempt_with_status", (mock.Mock(),)),
    ])
    def test_error_no_active_attempt(self, method, args):
        """
        If a method on an attempt is called with no active attempt, call _handle_error
        to log or raise an error
        """
        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            mock_handle_error.return_value = None
            metric = self._make_one(mock.Mock())
            return_obj = getattr(metric, method)(*args)
            assert return_obj is None
            assert mock_handle_error.call_count == 1
            assert mock_handle_error.call_args[0][0] == "No active attempt"

    @pytest.mark.parametrize("method,args", [
        ("start", ({},)),
        ("start_attempt", ()),
    ])
    def test_error_has_active_attempt(self, method, args):
        """
        If a method starting attempt is called with an active attempt, call _handle_error
        to log or raise an error
        """
        cls = type(self._make_one(mock.Mock()))
        with mock.patch.object(cls, "_handle_error") as mock_handle_error:
            mock_handle_error.return_value = None
            metric = self._make_one(mock.Mock(), active_attempt=mock.Mock())
            return_obj = getattr(metric, method)(*args)
            assert return_obj is None
            assert mock_handle_error.call_count == 1
            assert mock_handle_error.call_args[0][0] == "Attempt already in progress"

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

