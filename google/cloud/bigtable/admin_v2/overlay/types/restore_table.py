# Copyright 2025 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.api_core import operation
from google.protobuf import empty_pb2

from google.cloud.bigtable.admin_v2.types import OptimizeRestoredTableMetadata

class RestoreTableOperation(operation.Operation):
    def __init__(self, operations_client, restore_table_operation):
        self._operations_client = operations_client
        self._optimize_restore_table_operation = None
        super().__init__(
            restore_table_operation._operation,
            restore_table_operation._refresh,
            restore_table_operation._cancel,
            restore_table_operation._result_type,
            restore_table_operation._metadata_type,
            polling=restore_table_operation._polling,
        )

    def optimize_restore_table_operation(self,
                                         timeout=operation.Operation._DEFAULT_VALUE,
                                         retry=None,
                                         polling=None):
        
        self._blocking_poll(timeout=timeout, retry=retry, polling=polling)

        if self._exception is not None:
            # pylint: disable=raising-bad-type
            # Pylint doesn't recognize that this is valid in this case.
            raise self._exception

        return self._optimize_restore_table_operation


    def set_result(self, response):
        optimize_restore_table_operation_name = self.metadata.optimize_table_operation_name

        # When the RestoreTable operation finishes, it might not necessarily trigger
        # an optimize operation.
        if optimize_restore_table_operation_name:
            optimize_restore_table_operation = self._operations_client.get_operation(
                name=optimize_restore_table_operation_name
            )
            self._optimize_restore_table_operation = operation.from_gapic(
                optimize_restore_table_operation,
                self._operations_client,
                empty_pb2.Empty,
                metadata_type=OptimizeRestoredTableMetadata,
            )

        super().set_result(response)