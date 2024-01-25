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
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics import view
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    MetricExporter,
    MetricExportResult,
    MetricsData,
    NumberDataPoint,
    PeriodicExportingMetricReader,
)
from google.protobuf.timestamp_pb2 import Timestamp
from google.api.distribution_pb2 import Distribution
from google.api.metric_pb2 import Metric as GMetric
from google.api.monitored_resource_pb2 import MonitoredResource
from google.api.metric_pb2 import MetricDescriptor
from google.cloud.monitoring_v3 import (
    CreateTimeSeriesRequest,
    MetricServiceClient,
    Point,
    TimeInterval,
    TimeSeries,
    TypedValue,
)

from google.cloud.bigtable.data._metrics.handlers.opentelemetry import (
    OpenTelemetryMetricsHandler,
)
from google.cloud.bigtable.data._metrics.handlers.opentelemetry import (
    _OpenTelemetryInstruments,
)


MAX_BATCH_WRITE = 200

# avoid reformatting into individual lines
# fmt: off
MILLIS_AGGREGATION = view.ExplicitBucketHistogramAggregation(
    [
        0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16,
        20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400,
        500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
    ]
)
# fmt: on
COUNT_AGGREGATION = view.SumAggregation()
INSTRUMENT_NAMES = (
    "operation_latencies",
    "first_response_latencies",
    "attempt_latencies",
    "retry_counts",
    "server_latencies",
    "connectivity_error_count",
    "application_latencies",
    "throttling_latencies",
)
VIEW_LIST = [
    view.View(
        instrument_name=n,
        name=n,
        aggregation=MILLIS_AGGREGATION
        if n.endswith("latencies")
        else COUNT_AGGREGATION,
    )
    for n in INSTRUMENT_NAMES
]


class TestExporter(MetricExporter):
    def __init__(self, project_id: str):
        super().__init__()
        self.client = MetricServiceClient()
        self.prefix = "bigtable.googleapis.com/internal/client"
        self.project_name = self.client.common_project_path(project_id)

    def export(
        self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs
    ) -> MetricExportResult:
        # cumulative used for all metrics
        metric_kind = MetricDescriptor.MetricKind.CUMULATIVE
        all_series: list[TimeSeries] = []
        for resource_metric in metrics_data.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    for data_point in [
                        pt for pt in metric.data.data_points if pt.attributes
                    ]:
                        if data_point.attributes:
                            project_id = data_point.attributes.get("resource_project")
                            if not isinstance(project_id, str):
                                # we expect string for project_id field
                                continue
                            monitored_resource = MonitoredResource(
                                type="bigtable_client_raw",
                                labels={
                                    "project_id": project_id,
                                    "instance": data_point.attributes[
                                        "resource_instance"
                                    ],
                                    "cluster": data_point.attributes[
                                        "resource_cluster"
                                    ],
                                    "table": data_point.attributes["resource_table"],
                                    "zone": data_point.attributes["resource_zone"],
                                },
                            )
                            point = self._to_point(data_point)
                            series = TimeSeries(
                                resource=monitored_resource,
                                metric_kind=metric_kind,
                                points=[point],
                                metric=GMetric(
                                    type=f"{self.prefix}/{metric.name}",
                                    labels={
                                        k: v
                                        for k, v in data_point.attributes.items()
                                        if not k.startswith("resource_")
                                    },
                                ),
                                unit=metric.unit,
                            )
                            all_series.append(series)
        try:
            self._batch_write(all_series)
            return MetricExportResult.SUCCESS
        except Exception:
            return MetricExportResult.FAILURE

    def _batch_write(self, series: list[TimeSeries]) -> None:
        """Cloud Monitoring allows writing up to 200 time series at once

        Adapted from CloudMonitoringMetricsExporter
        https://github.com/GoogleCloudPlatform/opentelemetry-operations-python/blob/3668dfe7ce3b80dd01f42af72428de957b58b316/opentelemetry-exporter-gcp-monitoring/src/opentelemetry/exporter/cloud_monitoring/__init__.py#L82
        """
        write_ind = 0
        while write_ind < len(series):
            self.client.create_service_time_series(
                CreateTimeSeriesRequest(
                    name=self.project_name,
                    time_series=series[write_ind : write_ind + MAX_BATCH_WRITE],
                ),
            )
            write_ind += MAX_BATCH_WRITE

    @staticmethod
    def _to_point(data_point: NumberDataPoint | HistogramDataPoint) -> Point:
        """
        Adapted from CloudMonitoringMetricsExporter
        https://github.com/GoogleCloudPlatform/opentelemetry-operations-python/blob/3668dfe7ce3b80dd01f42af72428de957b58b316/opentelemetry-exporter-gcp-monitoring/src/opentelemetry/exporter/cloud_monitoring/__init__.py#L82
        """
        if isinstance(data_point, HistogramDataPoint):
            mean = data_point.sum / data_point.count if data_point.count else 0.0
            point_value = TypedValue(
                distribution_value=Distribution(
                    count=data_point.count,
                    mean=mean,
                    bucket_counts=data_point.bucket_counts,
                    bucket_options=Distribution.BucketOptions(
                        explicit_buckets=Distribution.BucketOptions.Explicit(
                            bounds=data_point.explicit_bounds,
                        )
                    ),
                )
            )
        else:
            if isinstance(data_point.value, int):
                point_value = TypedValue(int64_value=data_point.value)
            else:
                point_value = TypedValue(double_value=data_point.value)
        start_time = Timestamp()
        start_time.FromNanoseconds(data_point.start_time_unix_nano)
        end_time = Timestamp()
        end_time.FromNanoseconds(data_point.time_unix_nano)
        interval = TimeInterval(start_time=start_time, end_time=end_time)
        return Point(interval=interval, value=point_value)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs):
        pass

    def force_flush(self, timeout_millis: float = 10_000):
        return True


class GoogleCloudMetricsHandler(OpenTelemetryMetricsHandler):
    def __init__(self, *args, project_id: str, **kwargs):
        # writes metrics into GCP timeseries objects
        exporter = TestExporter(project_id=project_id)
        # periodically executes exporter
        gcp_reader = PeriodicExportingMetricReader(
            exporter, export_interval_millis=60_000
        )
        # use private meter provider to store instruments and views
        meter_provider = MeterProvider(metric_readers=[gcp_reader], views=VIEW_LIST)
        otel = _OpenTelemetryInstruments(meter_provider=meter_provider)
        super().__init__(*args, instruments=otel, project_id=project_id, **kwargs)
