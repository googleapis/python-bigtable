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

# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
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
#
import functools

from typing import Optional, Sequence, Tuple, Union
from google.api_core import gapic_v1
from google.api_core import retry as retries

try:
    OptionalRetry = Union[retries.Retry, gapic_v1.method._MethodDefault, None]
except AttributeError:  # pragma: NO COVER
    OptionalRetry = Union[retries.Retry, object, None]  # type: ignore

from google.api_core import operation  # type: ignore
from google.cloud.bigtable.admin_v2.types import bigtable_table_admin

from google.cloud.bigtable.admin_v2.services.bigtable_table_admin import BaseBigtableTableAdminClient
from google.cloud.bigtable.admin_v2.overlay.types import consistency, restore_table

class BigtableTableAdminClient(BaseBigtableTableAdminClient):

    def restore_table(
        self,
        request: Optional[Union[bigtable_table_admin.RestoreTableRequest, dict]] = None,
        *,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> restore_table.RestoreTableOperation:
        r"""Create a new table by restoring from a completed backup. The
        returned table [long-running
        operation][google.longrunning.Operation] can be used to track
        the progress of the operation, and to cancel it. The
        [metadata][google.longrunning.Operation.metadata] field type is
        [RestoreTableMetadata][google.bigtable.admin.v2.RestoreTableMetadata].
        The [response][google.longrunning.Operation.response] type is
        [Table][google.bigtable.admin.v2.Table], if successful.

        .. code-block:: python

            # This snippet has been automatically generated and should be regarded as a
            # code template only.
            # It will require modifications to work:
            # - It may require correct/in-range values for request initialization.
            # - It may require specifying regional endpoints when creating the service
            #   client as shown in:
            #   https://googleapis.dev/python/google-api-core/latest/client_options.html
            from google.cloud.bigtable import admin_v2

            def sample_restore_table():
                # Create a client
                client = admin_v2.BigtableTableAdminClient()

                # Initialize request argument(s)
                request = admin_v2.RestoreTableRequest(
                    backup="backup_value",
                    parent="parent_value",
                    table_id="table_id_value",
                )

                # Make the request
                operation = client.restore_table(request=request)

                print("Waiting for operation to complete...")

                response = operation.result()

                # Handle the response
                print(response)

                # Handle LRO2
                optimize_operation = operation.optimize_restore_table_operation()

                if optimize_operation:
                    print("Waiting for table optimization to complete...")

                    response = optimize_operation.result()

        Args:
            request (Union[google.cloud.bigtable.admin_v2.types.RestoreTableRequest, dict]):
                The request object. The request for
                [RestoreTable][google.bigtable.admin.v2.BigtableTableAdmin.RestoreTable].
            retry (google.api_core.retry.Retry): Designation of what errors, if any,
                should be retried.
            timeout (float): The timeout for this request.
            metadata (Sequence[Tuple[str, Union[str, bytes]]]): Key/value pairs which should be
                sent along with the request as metadata. Normally, each value must be of type `str`,
                but for metadata keys ending with the suffix `-bin`, the corresponding values must
                be of type `bytes`.

        Returns:
            google.cloud.bigtable.admin_v2.overlay.types.restore_table.RestoreTableOperation:
                An object representing a long-running operation.

                The result type for the operation will be :class:`google.cloud.bigtable.admin_v2.types.Table` A collection of user data indexed by row, column, and timestamp.
                   Each table is served using the resources of its
                   parent cluster.
        """
        operation = self._restore_table(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        restore_table_operation = restore_table.RestoreTableOperation(self._transport.operations_client, operation)
        return restore_table_operation

    def await_consistency(
        self,
        request: Optional[
            Union[bigtable_table_admin.CheckConsistencyRequest, dict]
        ] = None,
        *,
        name: Optional[str] = None,
        consistency_token: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> consistency.CheckConsistencyPollingFuture:

        api_call = functools.partial(
            self.check_consistency,
            request,
            name=name,
            consistency_token=consistency_token,
            timeout=timeout,
            metadata=metadata,
        )
        return consistency.CheckConsistencyPollingFuture(api_call, default_retry=retry)

    def await_replication(
        self,
        request: Optional[
            Union[bigtable_table_admin.GenerateConsistencyTokenRequest, dict]
        ] = None,
        *,
        name: Optional[str] = None,
        retry: OptionalRetry = gapic_v1.method.DEFAULT,
        timeout: Union[float, object] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, Union[str, bytes]]] = (),
    ) -> consistency.CheckConsistencyPollingFuture:
        
        generate_consistency_response = self.generate_consistency_token(
            request,
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        # Create the CheckConsistencyRequest object.
        check_consistency_request = bigtable_table_admin.CheckConsistencyRequest()
        
        # If the generate_consistency_token request is valid, there's a name in the request object
        # or the name parameter.
        if isinstance(request, dict):
            check_consistency_request.name = request["name"]
        elif isinstance(request, bigtable_table_admin.GenerateConsistencyTokenRequest):
            check_consistency_request.name = request.name
        else:
            check_consistency_request.name = name

        check_consistency_request.consistency_token = generate_consistency_response.consistency_token
        
        return self.await_consistency(check_consistency_request, retry=retry, timeout=timeout, metadata=metadata)
