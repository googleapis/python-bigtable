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

    def setup_otel(self):
        """
        Create a concrete MeterProvider in the environment
        """
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        metrics.set_meter_provider(MeterProvider())

    def _make_one(self, **kwargs):
        from google.cloud.bigtable.data._metrics import OpenTelemetryMetricsHandler
        if not kwargs:
            # create defaults
            kwargs = {"project_id": "p", "instance_id": "i", "table_id": "t", "app_profile_id": "a"}
        self.setup_otel()
        return OpenTelemetryMetricsHandler(**kwargs)

    @pytest.mark.parametrize("metric_name,kind", [
        ("operation_latencies", "histogram"),
        ("first_response_latencies", "histogram"),
        ("attempt_latencies", "histogram"),
        ("retry_count", "count"),
        ("server_latencies", "histogram"),
        ("connectivity_error_count", "count"),
        # ("application_latencies", "histogram"),
        # ("throttling_latencies", "histogram"),
    ])
    def test_ctor_creates_metrics(self, metric_name, kind):
        """
        Make sure each expected metric is created
        """
        from opentelemetry.metrics import Counter
        from opentelemetry.metrics import Histogram
        instance = self._make_one()
        metric = getattr(instance, metric_name)
        assert metric.name == metric_name
        if kind == "counter":
            assert isinstance(metric, Counter)
        elif kind == "histogram":
            assert isinstance(metric, Histogram)
            assert metric.unit == "ms"

    def test_ctor_labels(self):
        """
        should create dicts with with client name and uid,
        and monitored resurce labels
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
        assert instance.shared_labels["app_profile"] == expected_app_profile
        assert len(instance.shared_labels) == 3

        assert instance.monitored_resource_labels["project"] == expected_project
        assert instance.monitored_resource_labels["instance"] == expected_instance
        assert instance.monitored_resource_labels["table"] == expected_table
        assert len(instance.monitored_resource_labels) == 3

    def ctor_defaults(self):
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
        assert "app_profile" not in instance.shared_labels
        assert len(instance.shared_labels) == 2

    @pytest.mark.parametrize("metric_name,kind", [
        ("first_response_latencies", "histogram"),
        ("attempt_latencies", "histogram"),
        ("server_latencies", "histogram"),
        ("connectivity_error_count", "count"),
        # ("application_latencies", "histogram"),
        # ("throttling_latencies", "histogram"),
    ])
    def test_attempt_update_labels(self, metric_name, kind):
        """
        test that each attempt metric is sending the set of expected labels
        """
        from google.cloud.bigtable.data._metrics.data_model import OperationType
        expected_op_type = OperationType.READ_ROWS
        expected_status = mock.Mock()
        expected_streaming = mock.Mock()
        # server_latencies only shows up if gfe_latency is set
        gfe_latency = 1 if metric_name == "server_latencies" else None
        attempt = CompletedAttemptMetric(start_time=0, end_time=1, end_status=expected_status,gfe_latency=gfe_latency,first_response_latency=1)
        op = ActiveOperationMetric(expected_op_type, is_streaming=expected_streaming)

        instance = self._make_one()
        metric = getattr(instance, metric_name)
        record_fn = "record" if kind == "histogram" else "add"
        with mock.patch.object(metric, record_fn) as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            found_labels = record.call_args[0][1]
            assert found_labels["method"] == expected_op_type.value
            assert found_labels["status"] == expected_status.value
            assert found_labels["streaming"] == expected_streaming
            assert len(instance.shared_labels) == 3
            # shared labels should be copied over
            for k in instance.shared_labels:
                assert k in found_labels
                assert found_labels[k] == instance.shared_labels[k]

    @pytest.mark.parametrize("metric_name,kind", [
        ("operation_latencies", "histogram"),
        ("retry_count", "count"),
    ])
    def test_operation_update_labels(self, metric_name, kind):
        """
        test that each operation metric is sending the set of expected labels
        """
        from google.cloud.bigtable.data._metrics.data_model import OperationType
        expected_op_type = OperationType.READ_ROWS
        expected_status = mock.Mock()
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
        metric = getattr(instance, metric_name)
        record_fn = "record" if kind == "histogram" else "add"
        with mock.patch.object(metric, record_fn) as record:
            instance.on_operation_complete(op)
            assert record.call_count == 1
            found_labels = record.call_args[0][1]
            assert found_labels["method"] == expected_op_type.value
            assert found_labels["status"] == expected_status.value
            assert found_labels["streaming"] == expected_streaming
            assert len(instance.shared_labels) == 3
            # shared labels should be copied over
            for k in instance.shared_labels:
                assert k in found_labels
                assert found_labels[k] == instance.shared_labels[k]

    def test_attempt_update_latency(self):
        """
        update attempt_latencies on attempt completion
        """
        expected_latency = 123
        attempt = CompletedAttemptMetric(start_time=0, end_time=expected_latency, end_status=mock.Mock())
        op = ActiveOperationMetric(mock.Mock())

        instance = self._make_one()
        with mock.patch.object(instance.attempt_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_latency

    def test_attempt_update_first_response(self):
        """
        update first_response_latency on attempt completion
        """
        from google.cloud.bigtable.data._metrics.data_model import OperationType
        expected_first_response_latency = 123
        attempt = CompletedAttemptMetric(start_time=0, end_time=1, end_status=mock.Mock(), first_response_latency=expected_first_response_latency)
        op = ActiveOperationMetric(OperationType.READ_ROWS)

        instance = self._make_one()
        with mock.patch.object(instance.first_response_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_first_response_latency

    def test_attempt_update_server_latency(self):
        """
        update server_latency on attempt completion
        """
        expected_latency = 456
        attempt = CompletedAttemptMetric(start_time=0, end_time=expected_latency, end_status=mock.Mock(), gfe_latency=expected_latency)
        op = ActiveOperationMetric(mock.Mock())

        instance = self._make_one()
        with mock.patch.object(instance.server_latencies, "record") as record:
            instance.on_attempt_complete(attempt, op)
            assert record.call_count == 1
            assert record.call_args[0][0] == expected_latency

    def test_attempt_update_connectivity_error_count(self):
        """
        update connectivity_error_count on attempt completion
        """
        # error connectivity is logged when gfe_latency is None
        attempt = CompletedAttemptMetric(start_time=0, end_time=1, end_status=mock.Mock(), gfe_latency=None)
        op = ActiveOperationMetric(mock.Mock())

        instance = self._make_one()
        with mock.patch.object(instance.connectivity_error_count, "add") as add:
            instance.on_attempt_complete(attempt, op)
            assert add.call_count == 1
            assert add.call_args[0][0] == 1

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
        with mock.patch.object(instance.operation_latencies, "record") as record:
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
        with mock.patch.object(instance.retry_count, "add") as add:
            instance.on_operation_complete(op)
            assert add.call_count == 1
            assert add.call_args[0][0] == expected_count
