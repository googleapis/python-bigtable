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

from typing import Optional

from google.api_core import operation
from google.protobuf import empty_pb2

from google.cloud.bigtable.admin_v2.types import OptimizeRestoredTableMetadata


class RestoreTableOperation(operation.Operation):
    """A Future for interacting with Bigtable Admin's RestoreTable Long-Running Operation.

    This is needed to expose a potential long-running operation that might run after this operation
    finishes, OptimizeRestoreTable. This is exposed via the the :meth:`optimize_restore_table_operation`
    method.

    **This class should not be instantiated by users** and should only be instantiated by the admin
    client's :meth:`restore_table
    <google.cloud.bigtable.admin_v2.overlay.services.bigtable_table_admin.BigtableTableAdminClient.restore_table>`
    method.

    Args:
        operations_client (google.api_core.operations_v1.AbstractOperationsClient): The operations
            client from the admin client class's transport.
        restore_table_operation (google.api_core.operation.Operation): A :class:`google.api_core.operation.Operation`
            instance resembling a RestoreTable long-running operation
    """

    def __init__(self, operations_client, restore_table_operation: operation.Operation):
        self._operations_client = operations_client
        self._optimize_restored_table_operation = None
        super().__init__(
            restore_table_operation._operation,
            restore_table_operation._refresh,
            restore_table_operation._cancel,
            restore_table_operation._result_type,
            restore_table_operation._metadata_type,
            polling=restore_table_operation._polling,
        )

    def optimize_restored_table_operation(
        self, timeout=operation.Operation._DEFAULT_VALUE, retry=None, polling=None
    ) -> Optional[operation.Operation]:
        """Gets the OptimizeRestoredTable long-running operation that runs after this operation finishes.

        This is a blocking call that will return the operation after this current long-running operation
        finishes, just like :meth:`google.api_core.operation.Operation.result`. The follow-up operation has
        :attr:`metadata <google.api_core.operation.Operation.metadata>` type
        :class:`OptimizeRestoredTableMetadata
        <google.cloud.bigtable.admin_v2.types.bigtable_table_admin.OptimizeRestoredTableMetadata>`
        and no return value, but can be waited for with `result`.

        The current operation might not trigger a follow-up OptimizeRestoredTable operation, in which case, this
        method will return `None`.

        For more documentation on the parameters for this method, see the documentation on :meth:`google.api_core.operation.Operation.result`.

        Returns:
            Optional[google.api_core.operation.Operation]:
                An object representing a long-running operation, or None if there is no OptimizeRestoredTable operation
                after this one.
        """
        self._blocking_poll(timeout=timeout, retry=retry, polling=polling)

        if self._exception is not None:
            # pylint: disable=raising-bad-type
            # Pylint doesn't recognize that this is valid in this case.
            raise self._exception

        return self._optimize_restored_table_operation

    def set_result(self, response):
        optimize_restored_table_operation_name = (
            self.metadata.optimize_table_operation_name
        )

        # When the RestoreTable operation finishes, it might not necessarily trigger
        # an optimize operation.
        if optimize_restored_table_operation_name:
            optimize_restore_table_operation = self._operations_client.get_operation(
                name=optimize_restored_table_operation_name
            )
            self._optimize_restored_table_operation = operation.from_gapic(
                optimize_restore_table_operation,
                self._operations_client,
                empty_pb2.Empty,
                metadata_type=OptimizeRestoredTableMetadata,
            )

        super().set_result(response)
