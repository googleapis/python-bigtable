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


class TestGoogleCloudMetricsHandler:
    def _make_one(self, **kwargs):
        from google.cloud.bigtable.data._metrics import GoogleCloudMetricsHandler

        if not kwargs:
            # create defaults
            kwargs = {
                "project_id": "p",
                "instance_id": "i",
                "table_id": "t",
                "app_profile_id": "a",
            }
        return GoogleCloudMetricsHandler(**kwargs)

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

    def test_ctor_shared_otel_instance(self):
        """
        Each instance should use its own Otel instruments
        """
        instance_1 = self._make_one()
        instance_2 = self._make_one()
        assert instance_1.otel is not instance_2.otel

    @mock.patch("google.cloud.bigtable.data._metrics.handlers.gcp_exporter._OpenTelemetryInstruments", autospec=True)
    @mock.patch("google.cloud.bigtable.data._metrics.handlers.gcp_exporter.MeterProvider", autospec=True)
    def test_uses_custom_meter_provider(self, mock_meter_provider, mock_instruments):
        """
        Metrics should be set up using an instance-specific meter provider
        """
        from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import VIEW_LIST
        from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import _TestExporter
        project_id = 'test-project'
        instance = self._make_one(project_id=project_id, instance_id='i', table_id='t')
        assert instance.otel is mock_instruments.return_value
        otel_kwargs = mock_instruments.call_args[1]
        # meter provider was used to instantiate the instruments
        assert otel_kwargs["meter_provider"] is mock_meter_provider.return_value
        # meter provider was configured with gcp reader and views
        mp_kwargs = mock_meter_provider.call_args[1]
        assert mp_kwargs["views"] == VIEW_LIST
        reader_list = mp_kwargs["metric_readers"]
        assert len(reader_list) == 1
        found_reader = reader_list[0]
        # ensure exporter was set up
        assert isinstance(found_reader._exporter, _TestExporter)
        assert found_reader._exporter.project_name == f"projects/{project_id}"

    @mock.patch("google.cloud.bigtable.data._metrics.handlers.gcp_exporter.PeriodicExportingMetricReader", autospec=True)
    def test_custom_export_interval(self, mock_reader):
        """
        should be able to set a custom export interval
        """
        input_interval = 123
        try:
            self._make_one(export_interval=input_interval, project_id='p', instance_id='i', table_id='t')
        except Exception:
            pass
        reader_init_kwargs = mock_reader.call_args[1]
        found_interval = reader_init_kwargs["export_interval_millis"]
        assert found_interval == input_interval * 1000  # convert to ms

