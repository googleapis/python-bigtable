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
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
)
from opentelemetry.exporter.cloud_monitoring import (
    CloudMonitoringMetricsExporter,
)
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from google.cloud.bigtable.data._metrics.handlers.opentelemetry import OpenTelemetryExporterHandler


class GCPOpenTelemetryExporterHandler(OpenTelemetryExporterHandler):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        resource = Resource(attributes={
            SERVICE_NAME: "your-service-name",
        })
        exporter = CloudMonitoringMetricsExporter(prefix="bigtable.googleapis.com/internal/client/")
        gcp_reader = PeriodicExportingMetricReader(exporter)
        metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[gcp_reader]))
