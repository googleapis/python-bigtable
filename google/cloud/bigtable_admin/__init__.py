# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
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
from google.cloud.bigtable_admin import gapic_version as package_version

__version__ = package_version.__version__


from google.cloud.bigtable_admin_v2.services.bigtable_instance_admin.client import (
    BigtableInstanceAdminClient,
)
from google.cloud.bigtable_admin_v2.services.bigtable_instance_admin.async_client import (
    BigtableInstanceAdminAsyncClient,
)
from google.cloud.bigtable_admin_v2.services.bigtable_table_admin.client import (
    BigtableTableAdminClient,
)
from google.cloud.bigtable_admin_v2.services.bigtable_table_admin.async_client import (
    BigtableTableAdminAsyncClient,
)

from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    CreateAppProfileRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    CreateClusterMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    CreateClusterRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    CreateInstanceMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    CreateInstanceRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    DeleteAppProfileRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    DeleteClusterRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    DeleteInstanceRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    GetAppProfileRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    GetClusterRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    GetInstanceRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListAppProfilesRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListAppProfilesResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListClustersRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListClustersResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListHotTabletsRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListHotTabletsResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListInstancesRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    ListInstancesResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    PartialUpdateClusterMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    PartialUpdateClusterRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    PartialUpdateInstanceRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    UpdateAppProfileMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    UpdateAppProfileRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    UpdateClusterMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_instance_admin import (
    UpdateInstanceMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    CheckConsistencyRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    CheckConsistencyResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    CreateBackupMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    CreateBackupRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    CreateTableFromSnapshotMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    CreateTableFromSnapshotRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import CreateTableRequest
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    DeleteBackupRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    DeleteSnapshotRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import DeleteTableRequest
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    DropRowRangeRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    GenerateConsistencyTokenRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    GenerateConsistencyTokenResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import GetBackupRequest
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import GetSnapshotRequest
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import GetTableRequest
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import ListBackupsRequest
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    ListBackupsResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    ListSnapshotsRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    ListSnapshotsResponse,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import ListTablesRequest
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import ListTablesResponse
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    ModifyColumnFamiliesRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    OptimizeRestoredTableMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    RestoreTableMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    RestoreTableRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    SnapshotTableMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    SnapshotTableRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    UndeleteTableMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    UndeleteTableRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    UpdateBackupRequest,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import (
    UpdateTableMetadata,
)
from google.cloud.bigtable_admin_v2.types.bigtable_table_admin import UpdateTableRequest
from google.cloud.bigtable_admin_v2.types.common import OperationProgress
from google.cloud.bigtable_admin_v2.types.common import StorageType
from google.cloud.bigtable_admin_v2.types.instance import AppProfile
from google.cloud.bigtable_admin_v2.types.instance import AutoscalingLimits
from google.cloud.bigtable_admin_v2.types.instance import AutoscalingTargets
from google.cloud.bigtable_admin_v2.types.instance import Cluster
from google.cloud.bigtable_admin_v2.types.instance import HotTablet
from google.cloud.bigtable_admin_v2.types.instance import Instance
from google.cloud.bigtable_admin_v2.types.table import Backup
from google.cloud.bigtable_admin_v2.types.table import BackupInfo
from google.cloud.bigtable_admin_v2.types.table import ColumnFamily
from google.cloud.bigtable_admin_v2.types.table import EncryptionInfo
from google.cloud.bigtable_admin_v2.types.table import GcRule
from google.cloud.bigtable_admin_v2.types.table import RestoreInfo
from google.cloud.bigtable_admin_v2.types.table import Snapshot
from google.cloud.bigtable_admin_v2.types.table import Table
from google.cloud.bigtable_admin_v2.types.table import RestoreSourceType

__all__ = (
    "BigtableInstanceAdminClient",
    "BigtableInstanceAdminAsyncClient",
    "BigtableTableAdminClient",
    "BigtableTableAdminAsyncClient",
    "CreateAppProfileRequest",
    "CreateClusterMetadata",
    "CreateClusterRequest",
    "CreateInstanceMetadata",
    "CreateInstanceRequest",
    "DeleteAppProfileRequest",
    "DeleteClusterRequest",
    "DeleteInstanceRequest",
    "GetAppProfileRequest",
    "GetClusterRequest",
    "GetInstanceRequest",
    "ListAppProfilesRequest",
    "ListAppProfilesResponse",
    "ListClustersRequest",
    "ListClustersResponse",
    "ListHotTabletsRequest",
    "ListHotTabletsResponse",
    "ListInstancesRequest",
    "ListInstancesResponse",
    "PartialUpdateClusterMetadata",
    "PartialUpdateClusterRequest",
    "PartialUpdateInstanceRequest",
    "UpdateAppProfileMetadata",
    "UpdateAppProfileRequest",
    "UpdateClusterMetadata",
    "UpdateInstanceMetadata",
    "CheckConsistencyRequest",
    "CheckConsistencyResponse",
    "CreateBackupMetadata",
    "CreateBackupRequest",
    "CreateTableFromSnapshotMetadata",
    "CreateTableFromSnapshotRequest",
    "CreateTableRequest",
    "DeleteBackupRequest",
    "DeleteSnapshotRequest",
    "DeleteTableRequest",
    "DropRowRangeRequest",
    "GenerateConsistencyTokenRequest",
    "GenerateConsistencyTokenResponse",
    "GetBackupRequest",
    "GetSnapshotRequest",
    "GetTableRequest",
    "ListBackupsRequest",
    "ListBackupsResponse",
    "ListSnapshotsRequest",
    "ListSnapshotsResponse",
    "ListTablesRequest",
    "ListTablesResponse",
    "ModifyColumnFamiliesRequest",
    "OptimizeRestoredTableMetadata",
    "RestoreTableMetadata",
    "RestoreTableRequest",
    "SnapshotTableMetadata",
    "SnapshotTableRequest",
    "UndeleteTableMetadata",
    "UndeleteTableRequest",
    "UpdateBackupRequest",
    "UpdateTableMetadata",
    "UpdateTableRequest",
    "OperationProgress",
    "StorageType",
    "AppProfile",
    "AutoscalingLimits",
    "AutoscalingTargets",
    "Cluster",
    "HotTablet",
    "Instance",
    "Backup",
    "BackupInfo",
    "ColumnFamily",
    "EncryptionInfo",
    "GcRule",
    "RestoreInfo",
    "Snapshot",
    "Table",
    "RestoreSourceType",
)
