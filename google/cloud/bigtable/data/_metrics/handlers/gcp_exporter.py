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
    PeriodicExportingMetricReader, MetricExporter
)
# from opentelemetry.exporter.cloud_monitoring import (
#     CloudMonitoringMetricsExporter,
# )
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from google.cloud.bigtable.data._metrics.handlers.opentelemetry import OpenTelemetryMetricsHandler
from google.cloud.bigtable.data._metrics.handlers.opentelemetry import _OpenTelemetryInstrumentSingleton


class TestExporter(MetricExporter):
    def export(self, metric_records, timeout_millis=10_000, **kwargs):
        data = metric_records.resource_metrics
        breakpoint()
        print("exporting", metric_records)

    def shutdown(self, **kwargs):
        print("shutting down")

    def force_flush(self, timeout_millis=None):
        print("flushing")


class _GCPExporterOpenTelemetryInstrumentSingleton(_OpenTelemetryInstrumentSingleton):
    """
    _OpenTelemetryInstrumentSingleton subclass for GCP exporter. Writes metrics to
    a custom custom MeterProvider, rather than the global one
    """

    def __init__(self):
        super().__init__()
        # create views
        millis_aggregation = view.ExplicitBucketHistogramAggregation(
            [0.0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0,
             13.0, 16.0, 20.0, 25.0, 30.0, 40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0,
             250.0, 300.0, 400.0, 500.0, 650.0, 800.0, 1000.0, 2000.0, 5000.0, 10000.0,
             20000.0, 50000.0, 100000.0]
        )
        # retry_acount_aggregation = view.ExplicitBucketHistorgramAggregation(
        #     [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 100.0]
        # )
        self.operation_latencies_view = view.View(instrument_name="operation_latencies", aggregation=millis_aggregation, name="bigtable.googleapis.com/internal/client/operation_latencies")

    def get_meter_provider(self):
        """
        build a private MeterProvider for the GCP exporter
        """
        # writes metrics into GCP timeseries objects
        exporter = TestExporter()
        # periodically executes exporter
        gcp_reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
        # set up root meter provider
        meter_provider = MeterProvider(metric_readers=[gcp_reader], views=[self.operation_latencies_view])
        return meter_provider


class GCPOpenTelemetryExporterHandler(OpenTelemetryMetricsHandler):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        self.otel = _GCPExporterOpenTelemetryInstrumentSingleton()
        super().__init__(*args, **kwargs)

    # def on_operation_complete(self, op: _CompletedOperationMetric) -> None:
    #     super().on_operation_complete(op)

    #     monitored_resource = MonitoredResource(type="bigtable_client_raw", labels={"zone": op.zone, **self.monitored_resource_labels})
    #     if op.cluster_id is not None:
    #         monitored_resource.labels["cluster"] = op.cluster_id

    # def on_attempt_complete(self, attempt: _CompletedAttemptMetric, op: _ActiveOperationMetric) -> None:
    #     super().on_attempt_complete(attempt, op)

    #     monitored_resource = MonitoredResource(type="bigtable_client_raw", labels={"zone": op.zone, **self.monitored_resource_labels})
    #     if op.cluster_id is not None:
    #         monitored_resource.labels["cluster"] = op.cluster_id
