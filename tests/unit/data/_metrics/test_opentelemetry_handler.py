# Copyright 2024 Google LLC
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


from google.cloud.bigtable.data._metrics.handlers.opentelemetry import _OpenTelemetryInstruments
from google.cloud.bigtable.data._metrics.handlers.opentelemetry import OpenTelemetryMetricsHandler

class TestOpentelemetryInstruments:

    EXPPECTED_METRICS = [
        "operation_latencies",
        "first_response_latencies",
        "attempt_latencies",
        "server_latencies",
        "application_latencies",
        "throttling_latencies",
        "retry_count",
        "connectivity_error_count"
    ]

    def _make_one(self, meter_provider=None):
        return _OpenTelemetryInstruments(meter_provider)

    def test_meter_name(self):
        expected_name = "bigtable.googleapis.com"
        mock_meter_provider = mock.Mock()
        self._make_one(mock_meter_provider)
        mock_meter_provider.get_meter.assert_called_once_with(expected_name)

    @pytest.mark.parametrize("metric_name", [
        m for m in EXPPECTED_METRICS if "latencies" in m
    ])
    def test_histogram_creation(self, metric_name):
        mock_meter_provider = mock.Mock()
        instruments = self._make_one(mock_meter_provider)
        mock_meter = mock_meter_provider.get_meter()
        assert any([call.kwargs["name"] == metric_name for call in mock_meter.create_histogram.call_args_list])
        assert all([call.kwargs["unit"] == "ms" for call in mock_meter.create_histogram.call_args_list])
        assert all([call.kwargs["description"] is not None for call in mock_meter.create_histogram.call_args_list])
        assert getattr(instruments, metric_name) is not None

    @pytest.mark.parametrize("metric_name", [
        m for m in EXPPECTED_METRICS if "count" in m
    ])
    def test_counter_creation(self, metric_name):
        mock_meter_provider = mock.Mock()
        instruments = self._make_one(mock_meter_provider)
        mock_meter = mock_meter_provider.get_meter()
        assert any([call.kwargs["name"] == metric_name for call in mock_meter.create_counter.call_args_list])
        assert all([call.kwargs["description"] is not None for call in mock_meter.create_histogram.call_args_list])
        assert getattr(instruments, metric_name) is not None

    def test_global_provider(self):
        instruments = self._make_one()
        # wait to import otel until after creating instance
        import opentelemetry
        for metric_name in self.EXPPECTED_METRICS:
            metric = getattr(instruments, metric_name)
            assert metric is not None
            if "latencies" in metric_name:
                assert isinstance(metric, opentelemetry.metrics.Histogram)
            else:
                assert isinstance(metric, opentelemetry.metrics.Counter)

class TestOpentelemetryMetricsHandler:

    def _make_one(self, **kwargs):
        return OpenTelemetryMetricsHandler(**kwargs)

    def test_ctor_defaults(self):
        from google.cloud.bigtable import __version__ as CLIENT_VERSION
        expected_instance = "my_instance"
        expected_table = "my_table"
        with mock.patch.object(OpenTelemetryMetricsHandler, "_generate_client_uid") as uid_mock:
            handler = self._make_one(
                instance_id=expected_instance,
                table_id=expected_table
            )
        assert isinstance(handler.otel, _OpenTelemetryInstruments)
        assert handler.shared_labels["resource_instance"] == expected_instance
        assert handler.shared_labels["resource_table"] == expected_table
        assert handler.shared_labels["app_profile"] == "default"
        assert handler.shared_labels["client_name"] == f"python-bigtable/{CLIENT_VERSION}"
        assert handler.shared_labels["client_uid"] == uid_mock()

    def test_ctor_explicit(self):
        expected_instance = "my_instance"
        expected_table = "my_table"
        expected_version = "my_version"
        expected_uid = "my_uid"
        expected_app_profile = "my_profile"
        expected_instruments = object()
        handler = self._make_one(
            instance_id=expected_instance,
            table_id=expected_table,
            app_profile_id=expected_app_profile,
            client_uid=expected_uid,
            client_version=expected_version,
            instruments=expected_instruments,
        )
        assert handler.otel == expected_instruments
        assert handler.shared_labels["resource_instance"] == expected_instance
        assert handler.shared_labels["resource_table"] == expected_table
        assert handler.shared_labels["app_profile"] == expected_app_profile
        assert handler.shared_labels["client_name"] == f"python-bigtable/{expected_version}"
        assert handler.shared_labels["client_uid"] == expected_uid

    @mock.patch("socket.gethostname", return_value="hostname")
    @mock.patch("os.getpid", return_value="pid")
    @mock.patch("uuid.uuid4", return_value="uid")
    def test_generate_client_uid_mock(self, socket_mock, os_mock, uuid_mock):
        uid = OpenTelemetryMetricsHandler._generate_client_uid()
        assert uid == "python-uid-pid@hostname"

    @mock.patch("socket.gethostname", side_effect=[ValueError("fail")])
    @mock.patch("os.getpid", side_effect=[ValueError("fail")])
    @mock.patch("uuid.uuid4", return_value="uid")
    def test_generate_client_uid_mock_with_exceptions(self, socket_mock, os_mock, uuid_mock):
        uid = OpenTelemetryMetricsHandler._generate_client_uid()
        assert uid == "python-uid-@localhost"

    def test_generate_client_uid(self):
        import re
        uid = OpenTelemetryMetricsHandler._generate_client_uid()
        # The expected pattern is python-<uuid>-<pid>@<hostname>
        expected_pattern = r"python-[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}-\d+@.+"
        assert re.match(expected_pattern, uid)

    def test_on_operation_complete(self):
        pass

    def test_on_attempt_complete(self):
        pass