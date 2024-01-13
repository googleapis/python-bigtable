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
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics import view
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader, MetricExporter, MetricExportResult
)
# TODO: drop dependency?
from opentelemetry.exporter.cloud_monitoring import (
    CloudMonitoringMetricsExporter,
)
from google.api.monitored_resource_pb2 import MonitoredResource
from google.api.metric_pb2 import Metric as GMetric
from google.cloud.monitoring_v3 import (
    CreateMetricDescriptorRequest,
    CreateTimeSeriesRequest,
    MetricServiceClient,
    Point,
    TimeInterval,
    TimeSeries,
    TypedValue,
)

from google.cloud.bigtable.data._metrics.handlers.opentelemetry import OpenTelemetryMetricsHandler
from google.cloud.bigtable.data._metrics.handlers.opentelemetry import _OpenTelemetryInstrumentation


class TestExporter(CloudMonitoringMetricsExporter):
    def export(self, metric_records, timeout_millis=10_000, **kwargs):
        all_series = []
        for resource_metric in metric_records.resource_metrics:
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    descriptor = self._get_metric_descriptor(metric)
                    if not descriptor:
                        continue
                    for data_point in metric.data.data_points:
                        monitored_resource = MonitoredResource(
                            type="bigtable_client_raw",
                            labels={
                                "project": data_point.attributes.pop("resource_project"),
                                "instance": data_point.attributes.pop("resource_instance"),
                                "cluster": data_point.attributes.pop("resource_cluster"),
                                "table": data_point.attributes.pop("resource_table"),
                                "zone": data_point.attributes.pop("resource_zone"),
                            }
                        )
                        point = self._to_point(
                            descriptor.metric_kind, data_point
                        )

                        series = TimeSeries(
                            resource=monitored_resource,
                            metric_kind=descriptor.metric_kind,
                            points=[point],
                            metric=GMetric(
                                type=descriptor.type,
                                labels={k: str(v) for k,v in data_point.attributes.items()}
                            ),
                            unit=descriptor.unit,
                        )
                        all_series.append(series)
        try:
            self._batch_write(all_series)
        except Exception:
            return MetricExportResult.FAILURE
        return MetricExportResult.SUCCESS

    def shutdown(self, **kwargs):
        print("shutting down")

    def force_flush(self, timeout_millis=None):
        print("flushing")


def _create_private_meter_provider():
    """
    Creates a private MeterProvider to store instruments and views, and send them
    periodically through a custom GCP metrics exporter
    """
    millis_aggregation = view.ExplicitBucketHistogramAggregation(
        [0.0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0,
         13.0, 16.0, 20.0, 25.0, 30.0, 40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0,
         250.0, 300.0, 400.0, 500.0, 650.0, 800.0, 1000.0, 2000.0, 5000.0, 10000.0,
         20000.0, 50000.0, 100000.0]
    )
    # retry_acount_aggregation = view.ExplicitBucketHistorgramAggregation(
    #     [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 100.0]
    # )
    operation_latencies_view = view.View(instrument_name="operation_latencies", aggregation=millis_aggregation, name="bigtable.googleapis.com/internal/client/operation_latencies")
    # writes metrics into GCP timeseries objects
    exporter = TestExporter()
    # periodically executes exporter
    gcp_reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    # set up root meter provider
    meter_provider = MeterProvider(metric_readers=[gcp_reader], views=[operation_latencies_view])
    return meter_provider


class GCPOpenTelemetryExporterHandler(OpenTelemetryMetricsHandler):

    # Otel instrumentation must use custom meter provider
    otel = _OpenTelemetryInstrumentation(_create_private_meter_provider())
