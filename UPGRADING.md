# 3.0.0 Migration Guide

The v3.0.0 release of `google-cloud-bigtable` deprecates the previous `google.cloud.bigtable.Client` class in favor of distinct clients for the two API surfaces, supporting both sync and async calls:
- Data API: 
  - `google.cloud.bigtable.data.BigtableDataClient`
  - `google.cloud.bigtable.data.BigtableDataClientAsync`
- Admin API: 
  - `google.cloud.bigtable.admin.BigtableInstanceAdminClient`
  - `google.cloud.bigtable.admin.BigtableInstanceAdminAsyncClient`
  - `google.cloud.bigtable.admin.BigtableTableAdminClient`
  - `google.cloud.bigtable.admin.BigtableTableAdminAsyncClient`

The deprecated client will remain available as an alternative API surface, which internally delegates calls to the respective new clients. For most users, existing code will continue to work as before. But there may be some breaking changes associated with this update, which are detailed in this document.

### Admin Client Migration

We recommend that you instantiate the new admin API clients directly:

```
from google.cloud.bigtable.admin import (
  BigtableInstanceAdminClient,
  BigtableInstanceAdminAsyncClient,
  BigtableTableAdminClient,
  BigtableTableAdminAsyncClient,
)

instance_admin_client = BigtableInstanceAdminClient()
instance_admin_async_client = BigtableInstanceAdminAsyncClient()
table_admin_client = BigtableTableAdminClient()
table_admin_async_client = BigtableTableAdminAsyncClient()
```

Access to the new admin API sync clients via the `google.cloud.bigtable.Client` class is also supported for backwards compatibility:

```
from google.cloud.bigtable import Client

client = Client()

# google.cloud.bigtable.admin.BigtableInstanceAdminClient
instance_admin_client = client.instance_admin_client()

# google.cloud.bigtable.admin.BigtableTableAdminClient
table_admin_client = client.table_admin_client()
```

## Breaking Changes
- **[MutationBatcher](https://github.com/googleapis/python-bigtable/blob/main/google/cloud/bigtable/data/_sync_autogen/mutations_batcher.py#L151)and [MutationBatcherAsync](https://github.com/googleapis/python-bigtable/blob/main/google/cloud/bigtable/data/_async/mutations_batcher.py#L182)'s `table` argument was renamed to `target`**, since it also supports [Authorized View](https://github.com/googleapis/python-bigtable/pull/1034) instances. This matches the naming used in other classes (PR: https://github.com/googleapis/python-bigtable/pull/1153)
<!-- TODO: Replace Github link once the feature branch is merged -->
- **[`BigtableTableAdminClient`/`BigtableTableAdminAsyncClient`](https://github.com/googleapis/python-bigtable/blob/main/google/cloud/bigtable_admin_v2/overlay/services/bigtable_table_admin)'s location was changed from `google.cloud.bigtable_admin_v2.services.bigtable_table_admin` to `google.cloud.bigtable_admin_v2.overlay.services.bigtable_table_admin`**. This should not affect you if you are importing these clients via `import google.cloud.bigtable_admin_v2` due to import aliasing, but will affect you if you are importing these clients via `import google.cloud.bigtable_admin_v2.services.bigtable_table_admin`.
