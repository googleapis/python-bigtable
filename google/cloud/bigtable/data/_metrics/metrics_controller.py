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

import time
import os

from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.handlers.opentelemetry import OpenTelemetryMetricsHandler
from google.cloud.bigtable.data._metrics.handlers.stdout import StdoutMetricsHandler
from google.cloud.bigtable.data._metrics.handlers._base import MetricsHandler
from google.cloud.bigtable.data._metrics.data_model import OperationType


PRINT_METRICS = os.getenv("BIGTABLE_PRINT_METRICS", False)


class BigtableClientSideMetricsController():

    def __init__(self, **kwargs):
        self.handlers: list[MetricsHandler] = []
        if PRINT_METRICS:
            self.handlers.append(StdoutMetricsHandler(**kwargs))
        try:
            ot_handler = OpenTelemetryMetricsHandler(**kwargs)
            self.handlers.append(ot_handler)
        except ImportError:
            pass

    def add_handler(self, handler:MetricsHandler) -> None:
        self.handlers.append(handler)

    def create_operation(self, op_type:OperationType, is_streaming:bool = False) -> ActiveOperationMetric:
        start_time = time.monotonic()
        new_op = ActiveOperationMetric(
            op_type=op_type,
            start_time=start_time,
            _handlers=self.handlers,
            is_streaming=is_streaming,
        )
        return new_op
