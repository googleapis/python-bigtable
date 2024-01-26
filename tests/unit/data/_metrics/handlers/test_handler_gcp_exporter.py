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

    @mock.patch(
        "google.cloud.bigtable.data._metrics.handlers.gcp_exporter._OpenTelemetryInstruments",
        autospec=True,
    )
    @mock.patch(
        "google.cloud.bigtable.data._metrics.handlers.gcp_exporter.MeterProvider",
        autospec=True,
    )
    def test_uses_custom_meter_provider(self, mock_meter_provider, mock_instruments):
        """
        Metrics should be set up using an instance-specific meter provider
        """
        from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import VIEW_LIST
        from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import (
            _BigtableMetricsExporter,
        )

        project_id = "test-project"
        instance = self._make_one(project_id=project_id, instance_id="i", table_id="t")
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
        assert isinstance(found_reader._exporter, _BigtableMetricsExporter)
        assert found_reader._exporter.project_name == f"projects/{project_id}"

    @mock.patch(
        "google.cloud.bigtable.data._metrics.handlers.gcp_exporter.PeriodicExportingMetricReader",
        autospec=True,
    )
    def test_custom_export_interval(self, mock_reader):
        """
        should be able to set a custom export interval
        """
        input_interval = 123
        try:
            self._make_one(
                export_interval=input_interval,
                project_id="p",
                instance_id="i",
                table_id="t",
            )
        except Exception:
            pass
        reader_init_kwargs = mock_reader.call_args[1]
        found_interval = reader_init_kwargs["export_interval_millis"]
        assert found_interval == input_interval * 1000  # convert to ms


class Test_BigtableMetricsExporter:
    def _get_class(self):
        from google.cloud.bigtable.data._metrics.handlers.gcp_exporter import (
            _BigtableMetricsExporter,
        )

        return _BigtableMetricsExporter

    def _make_one(self, project_id="test-project"):
        return self._get_class()(project_id)

    def test_ctor(self):
        from google.cloud.monitoring_v3 import (
            MetricServiceClient,
        )

        project = "test-project"
        instance = self._make_one(project)
        assert instance.project_name == f"projects/{project}"
        assert isinstance(instance.client, MetricServiceClient)

    def test_export_conversion_number(self):
        """
        export function should properly convert between opentelemetry DataPoints
        and bigtable TimeSeries
        """
        from opentelemetry.sdk.metrics.export import (
            NumberDataPoint,
            HistogramDataPoint,
            MetricsData,
            ResourceMetrics,
            ScopeMetrics,
        )

        attributes = {
            "resource_project": "project",
            "resource_instance": "instance",
            "resource_table": "table",
            "resource_zone": "zone",
            "resource_cluster": "cluster",
            "some_other_attr": "value",
        }
        start_time = 1
        end_time = 2
        # test with different data points
        int_pt = NumberDataPoint(attributes, start_time, end_time, 10)
        float_pt = NumberDataPoint(attributes, start_time, end_time, 5.5)
        histogram_pt = HistogramDataPoint(
            attributes,
            start_time,
            end_time,
            count=3,
            sum=12,
            bucket_counts=[1, 2, 3],
            explicit_bounds=[1, 2, 3],
            min=1,
            max=3,
        )
        # wrap up data points in OpenTelemetry objects
        metric = mock.Mock()
        metric.name = "metric_name"
        metric.unit = "metric_unit"
        metric.data.data_points = [int_pt, float_pt, histogram_pt]
        full_data = MetricsData(
            [
                ResourceMetrics(
                    mock.Mock(), [ScopeMetrics(mock.Mock(), [metric], "")], ""
                )
            ]
        )
        # run through export to convert to TimeSeries
        instance = self._make_one()
        with mock.patch.object(instance, "_batch_write") as mock_batch_write:
            instance.export(full_data)
            resulting_list = mock_batch_write.call_args[0][0]
            # examine output
            assert len(resulting_list) == len(metric.data.data_points)
            found_int_pt, found_float_pt, found_hist_pt = resulting_list
            # ensure values were set correctly
            assert found_int_pt.points[0].value.int64_value == int_pt.value
            assert found_float_pt.points[0].value.double_value == float_pt.value
            hist_value = found_hist_pt.points[0].value.distribution_value
            assert hist_value.count == histogram_pt.count
            assert hist_value.mean == histogram_pt.sum / histogram_pt.count
            assert hist_value.bucket_counts == histogram_pt.bucket_counts
            assert (
                hist_value.bucket_options.explicit_buckets.bounds
                == histogram_pt.explicit_bounds
            )
            # check fields that should be common across all TimeSeries
            for result_series in resulting_list:
                result_series = resulting_list[0]
                assert result_series.metric_kind == 3  # CUMULATIVE
                assert result_series.unit == metric.unit
                assert len(result_series.points) == 1
                assert (
                    result_series.points[0].interval.start_time._nanosecond
                    == start_time
                )
                assert result_series.points[0].interval.end_time._nanosecond == end_time
                assert (
                    result_series.metric.type
                    == f"bigtable.googleapis.com/internal/client/{metric.name}"
                )
                assert (
                    result_series.metric.labels["some_other_attr"]
                    == attributes["some_other_attr"]
                )
                assert len(result_series.metric.labels) == 1
                # check the monitored resource
                monitored_resource = result_series.resource
                assert monitored_resource.type == "bigtable_client_raw"
                assert len(monitored_resource.labels) == 5
                assert (
                    monitored_resource.labels["project_id"]
                    == attributes["resource_project"]
                )
                assert (
                    monitored_resource.labels["instance"]
                    == attributes["resource_instance"]
                )
                assert (
                    monitored_resource.labels["table"] == attributes["resource_table"]
                )
                assert monitored_resource.labels["zone"] == attributes["resource_zone"]
                assert (
                    monitored_resource.labels["cluster"]
                    == attributes["resource_cluster"]
                )

    def test_export_timeout(self):
        """
        timeout value should be properly passed to _batch_write
        """
        timeout_ms = 123_000
        current_timestamp = 5
        instance = self._make_one()
        metric_data = mock.Mock()
        metric_data.resource_metrics = []

        with mock.patch("time.time", return_value=current_timestamp):
            with mock.patch.object(instance, "_batch_write") as mock_batch_write:
                instance.export(metric_data, timeout_millis=timeout_ms)
                found_args = mock_batch_write.call_args[0]
                found_deadline = found_args[1]
                assert found_deadline == current_timestamp + (timeout_ms / 1000)

    @pytest.mark.parametrize("should_fail", [True, False])
    def test_export_return_value(self, should_fail):
        """
        should return success or failure based on result of _batch_write
        """
        from opentelemetry.sdk.metrics.export import MetricExportResult

        instance = self._make_one()
        metric_data = mock.Mock()
        metric_data.resource_metrics = []

        with mock.patch.object(instance, "_batch_write") as mock_batch_write:
            if should_fail:
                mock_batch_write.side_effect = Exception("test exception")
            result = instance.export(metric_data)
            if should_fail:
                assert result == MetricExportResult.FAILURE
            else:
                assert result == MetricExportResult.SUCCESS

    @pytest.mark.parametrize(
        "num_series,batch_size,expected",
        [
            (1, 1, 1),
            (1, 2, 1),
            (2, 1, 2),
            (2, 2, 1),
            (3, 2, 2),
            (3, 3, 1),
            (0, 10, 0),
            (201, 200, 2),
            (500, None, 3),  # default batch size is 200
        ],
    )
    @mock.patch(
        "google.cloud.bigtable.data._metrics.handlers.gcp_exporter.MetricServiceClient",
        autospec=True,
    )
    def test__batch_write_batching(self, mock_client, num_series, batch_size, expected):
        """
        should properly batch large requests into multiple calls
        """
        from google.cloud.monitoring_v3 import TimeSeries

        instance = self._make_one()
        instance.project_name = "projects/project"
        series = [TimeSeries() for _ in range(num_series)]
        kwargs = {"max_batch_size": batch_size} if batch_size is not None else {}
        instance._batch_write(series, **kwargs)
        # ensure that the right number of batches were used
        rpc_count = mock_client.return_value.create_service_time_series.call_count
        assert rpc_count == expected
        # check actual batch sizes
        requests = [
            call[0][0]
            for call in mock_client.return_value.create_service_time_series.call_args_list
        ]
        assert len(requests) == expected
        if batch_size is None:
            batch_size = 200
        for request in requests[:-1]:
            assert len(request.time_series) == batch_size
        # last batch may be smaller
        if requests:
            last_size = len(requests[-1].time_series)
            assert last_size == batch_size or last_size == num_series % batch_size

    def test__batch_write_uses_project_name(self):
        """
        exporter should use project sent at init time
        """
        from google.cloud.monitoring_v3 import TimeSeries

        expected_project = "test-project"
        batch = [TimeSeries()] * 5
        instance = self._make_one()
        with mock.patch.object(
            instance.client, "create_service_time_series"
        ) as mock_rpc:
            instance._batch_write(batch, max_batch_size=1)
            requests = [call[0][0] for call in mock_rpc.call_args_list]
            assert len(requests) == 5
            for request in requests:
                assert request.name == f"projects/{expected_project}"

    @pytest.mark.parametrize(
        "start_time,deadline,rpc_time,expected",
        [
            (0, 5, 1, (5, 4, 3, 2, 1, 0, -1)),
            (1, 10, 2, (9, 7, 5, 3, 1, -1)),
        ],
    )
    @mock.patch("time.time")
    def test__batch_write_deadline(
        self, mock_time, start_time, deadline, rpc_time, expected
    ):
        """
        deadline should be properly calculated and passed to RPC as timeouts
        """
        from google.cloud.monitoring_v3 import TimeSeries

        batch = [TimeSeries()] * len(expected)
        instance = self._make_one()

        # increment time.time() each time it is called
        def increment_time(*args, **kwargs):
            mock_time.return_value += rpc_time

        mock_time.return_value = start_time

        with mock.patch.object(
            instance.client, "create_service_time_series"
        ) as mock_rpc:
            mock_rpc.side_effect = increment_time
            instance._batch_write(batch, max_batch_size=1, deadline=deadline)
            timeouts = [call[1]["timeout"] for call in mock_rpc.call_args_list]
            assert len(timeouts) == len(expected)
            for timeout, expected_timeout in zip(timeouts, expected):
                assert timeout == expected_timeout
