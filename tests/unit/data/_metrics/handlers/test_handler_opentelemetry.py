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

import pytest
import mock

from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.data_model import CompletedAttemptMetric
from google.cloud.bigtable.data._metrics.data_model import CompletedOperationMetric


class TestOpenTelemetryMetricsHandler:
    def _make_one(self, **kwargs):
        from google.cloud.bigtable.data._metrics import OpenTelemetryMetricsHandler

        if not kwargs:
            # create defaults
            kwargs = {
                "project_id": "p",
                "instance_id": "i",
                "table_id": "t",
                "app_profile_id": "a",
            }
        return OpenTelemetryMetricsHandler(**kwargs)

    @pytest.mark.parametrize(
        "metric_name,kind",
        [
            ("operation_latencies", "histogram"),
            ("first_response_latencies", "histogram"),
            ("attempt_latencies", "histogram"),
            ("retry_count", "count"),
            ("server_latencies", "histogram"),
            ("connectivity_error_count", "count"),
            ("application_latencies", "histogram"),
            ("throttling_latencies", "histogram"),
        ],
    )
    def test_ctor_creates_metrics(self, metric_name, kind):
        """
        Make sure each expected metric is created
        """
        from opentelemetry.metrics import Counter
        from opentelemetry.metrics import Histogram

        instance = self._make_one()
        metric = getattr(instance.otel, metric_name)
        if kind == "count":
            assert isinstance(metric, Counter)
        elif kind == "histogram":
            assert isinstance(metric, Histogram)
        else:
            raise ValueError(f"Unknown metric kind: {kind}")

    def test_ctor_labels(self):
        """
        should create dicts with with client name and uid, and shared labels
        """
        from google.cloud.bigtable import __version__

        expected_project = "p"
        expected_instance = "i"
        expected_table = "t"
        expected_app_profile = "a"
        expected_uid = "uid"

        instance = self._make_one(
            project_id=expected_project,
            instance_id=expected_instance,
            table_id=expected_table,
            app_profile_id=expected_app_profile,
            client_uid=expected_uid,
        )
        assert instance.shared_labels["client_uid"] == expected_uid
        assert instance.shared_labels["client_name"] == f"python-bigtable/{__version__}"
        assert instance.shared_labels["resource_project"] == expected_project
        assert instance.shared_labels["resource_instance"] == expected_instance
        assert instance.shared_labels["resource_table"] == expected_table
        assert instance.shared_labels["app_profile"] == expected_app_profile
        assert len(instance.shared_labels) == 6

    def test_ctor_shared_otel_instance(self):
        """
        Two instances should be writing to the same metrics
        """
        instance1 = self._make_one()
        instance2 = self._make_one()
        assert instance1 is not instance2
        assert instance1.otel is instance2.otel
        assert instance1.otel.attempt_latencies is instance2.otel.attempt_latencies

    def test_ctor_defaults(self):
        """
        Should work without explicit uid or app_profile_id
        """
        instance = self._make_one(
            project_id="p",
            instance_id="i",
            table_id="t",
        )
        assert instance.shared_labels["client_uid"] is not None
        assert isinstance(instance.shared_labels["client_uid"], str)
        assert len(instance.shared_labels["client_uid"]) > 10  # should be decently long
        assert instance.shared_labels["resource_project"] == "p"
        assert instance.shared_labels["resource_instance"] == "i"
        assert instance.shared_labels["resource_table"] == "t"
        assert "app_profile" not in instance.shared_labels
        assert len(instance.shared_labels) == 5

    @pytest.mark.parametrize(
        "metric_name,kind,optional_labels",
        [
            ("first_response_latencies", "histogram", ["status"]),
            ("attempt_latencies", "histogram", ["status", "streaming"]),
            ("server_latencies", "histogram", ["status", "streaming"]),
            ("connectivity_error_count", "count", ["status"]),
            ("application_latencies", "histogram", []),
            ("throttling_latencies", "histogram", []),
        ],
    )
    def test_attempt_update_labels(self, metric_name, kind, optional_labels):
        """
        test that each attempt metric is sending the set of expected labels

        optional_labels: status and streaming aren't used by all metrics.
        Mark which ones expect them
        """
        from google.cloud.bigtable.data._metrics.data_model import OperationType
        from grpc import StatusCode

        expected_op_type = OperationType.READ_ROWS
        expected_status = StatusCode.ABORTED
        expected_streaming = mock.Mock()
        # server_latencies only shows up if gfe_latency is set
        gfe_latency = 1 if metric_name == "server_latencies" else None
        attempt = CompletedAttemptMetric(
            start_time=0,
            duration=1,
            end_status=expected_status,
            gfe_latency=gfe_latency,
            first_response_latency=1,
        )
        op = ActiveOperationMetric(expected_op_type, is_streaming=expected_streaming)

        instance = self._make_one()
        metric = getattr(instance.otel, metric_name)
        record_fn = "record" if kind == "histogram" else "add"
        with mock.patch.object(metric, record_fn) as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            found_labels = record.call_args[0][1]
            assert found_labels["method"] == expected_op_type.value
            if "status" in optional_labels:
                assert found_labels["status"] == str(expected_status.value[0])
            else:
                assert "status" not in found_labels
            if "streaming" in optional_labels:
                assert found_labels["streaming"] == str(expected_streaming)
            else:
                assert "streaming" not in found_labels
            assert len(instance.shared_labels) == 6
            # shared labels should be copied over
            for k in instance.shared_labels:
                assert k in found_labels
                assert found_labels[k] == instance.shared_labels[k]

    @pytest.mark.parametrize(
        "metric_name,kind,optional_labels",
        [
            ("operation_latencies", "histogram", ["status", "streaming"]),
            ("retry_count", "count", ["status"]),
        ],
    )
    def test_operation_update_labels(self, metric_name, kind, optional_labels):
        """
        test that each operation metric is sending the set of expected labels

        optional_labels: status and streaming aren't used by all metrics.
        Mark which ones expect them
        """
        from google.cloud.bigtable.data._metrics.data_model import OperationType
        from grpc import StatusCode

        expected_op_type = OperationType.READ_ROWS
        expected_status = StatusCode.RESOURCE_EXHAUSTED
        expected_streaming = mock.Mock()
        op = CompletedOperationMetric(
            op_type=expected_op_type,
            start_time=0,
            completed_attempts=[],
            duration=1,
            final_status=expected_status,
            cluster_id="c",
            zone="z",
            is_streaming=expected_streaming,
        )
        instance = self._make_one()
        metric = getattr(instance.otel, metric_name)
        record_fn = "record" if kind == "histogram" else "add"
        with mock.patch.object(metric, record_fn) as record:
            instance.on_operation_complete(op)
            assert record.call_count == 1
            found_labels = record.call_args[0][1]
            assert found_labels["method"] == expected_op_type.value
            if "status" in optional_labels:
                assert found_labels["status"] == str(expected_status.value[0])
            else:
                assert "status" not in found_labels
            if "streaming" in optional_labels:
                assert found_labels["streaming"] == str(expected_streaming)
            else:
                assert "streaming" not in found_labels
            assert len(instance.shared_labels) == 6
            # shared labels should be copied over
            for k in instance.shared_labels:
                assert k in found_labels
                assert found_labels[k] == instance.shared_labels[k]

    def test_attempt_update_latency(self):
        """
        update attempt_latencies on attempt completion
        """
        expected_latency = 123
        attempt = CompletedAttemptMetric(
            start_time=0, duration=expected_latency, end_status=mock.Mock()
        )
        op = ActiveOperationMetric(mock.Mock())

        instance = self._make_one()
        with mock.patch.object(instance.otel.attempt_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_latency

    def test_attempt_update_first_response(self):
        """
        update first_response_latency on attempt completion
        """
        from google.cloud.bigtable.data._metrics.data_model import OperationType

        expected_first_response_latency = 123
        attempt = CompletedAttemptMetric(
            start_time=0,
            duration=1,
            end_status=mock.Mock(),
            first_response_latency=expected_first_response_latency,
        )
        op = ActiveOperationMetric(OperationType.READ_ROWS)

        instance = self._make_one()
        with mock.patch.object(
            instance.otel.first_response_latencies, "record"
        ) as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_first_response_latency

    def test_attempt_update_server_latency(self):
        """
        update server_latency on attempt completion
        """
        expected_latency = 456
        attempt = CompletedAttemptMetric(
            start_time=0,
            duration=expected_latency,
            end_status=mock.Mock(),
            gfe_latency=expected_latency,
        )
        op = ActiveOperationMetric(mock.Mock())

        instance = self._make_one()
        with mock.patch.object(instance.otel.server_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_latency

    def test_attempt_update_connectivity_error_count(self):
        """
        update connectivity_error_count on attempt completion
        """
        # error connectivity is logged when gfe_latency is None
        attempt = CompletedAttemptMetric(
            start_time=0, duration=1, end_status=mock.Mock(), gfe_latency=None
        )
        op = ActiveOperationMetric(mock.Mock())

        instance = self._make_one()
        with mock.patch.object(instance.otel.connectivity_error_count, "add") as add:
            instance.on_attempt_complete(attempt, op)
            assert add.call_count == 1
            assert add.call_args[0][0] == 1

    @pytest.mark.parametrize("app_blocking,backoff", [(0, 10), (10, 0), (123, 456)])
    def test_attempt_update_application_latencies(self, app_blocking, backoff):
        """
        update application_latencies on attempt completion
        """
        expected_total_latency = app_blocking + backoff
        attempt = CompletedAttemptMetric(
            start_time=0,
            duration=1,
            end_status=mock.Mock(),
            application_blocking_time=app_blocking,
            backoff_before_attempt=backoff,
        )
        op = ActiveOperationMetric(mock.Mock())

        instance = self._make_one()
        with mock.patch.object(instance.otel.application_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_total_latency

    @pytest.mark.parametrize("grpc,flow", [(0, 10), (10, 0), (123, 456)])
    def test_attempt_update_throttling_latencies(self, grpc, flow):
        """
        Update throttling_latencies on attempt completion
        """
        expected_total_latency = grpc + flow
        attempt = CompletedAttemptMetric(
            start_time=0,
            duration=1,
            end_status=mock.Mock(),
            grpc_throttling_time=grpc,
        )
        op = ActiveOperationMetric(mock.Mock(), flow_throttling_time=flow)

        instance = self._make_one()
        with mock.patch.object(instance.otel.throttling_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_total_latency

    def test_attempt_empty_cluster_zone(self):
        """
        if cluster and zone are None at attempt complete, fall back to default values
        """
        op = ActiveOperationMetric(mock.Mock())
        attempt = CompletedAttemptMetric(
            start_time=0,
            duration=1,
            end_status=mock.Mock(),
        )
        op.cluster_id = None
        op.zone = None
        instance = self._make_one()
        with mock.patch.object(instance.otel.throttling_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            labels = record.call_args[0][1]
            assert labels["resource_cluster"] == "unspecified"
            assert labels["resource_zone"] == "global"

    def tyest_operation_update_latency(self):
        """
        update op_latency on operation completion
        """
        expected_latency = 123
        op = CompletedOperationMetric(
            op_type=mock.Mock(),
            start_time=0,
            completed_attempts=[],
            duration=expected_latency,
            final_status=mock.Mock(),
            cluster_id="c",
            zone="z",
            is_streaming=True,
        )

        instance = self._make_one()
        with mock.patch.object(instance.otel.operation_latencies, "record") as record:
            instance.on_operation_complete(op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_latency

    def test_operation_update_retry_count(self):
        """
        update retry_count on operation completion
        """
        num_attempts = 9
        # we don't count the first attempt
        expected_count = num_attempts - 1
        op = CompletedOperationMetric(
            op_type=mock.Mock(),
            start_time=0,
            completed_attempts=[object()] * num_attempts,
            duration=1,
            final_status=mock.Mock(),
            cluster_id="c",
            zone="z",
            is_streaming=True,
        )

        instance = self._make_one()
        with mock.patch.object(instance.otel.retry_count, "add") as add:
            instance.on_operation_complete(op)
            assert add.call_count == 1
            assert add.call_args[0][0] == expected_count
