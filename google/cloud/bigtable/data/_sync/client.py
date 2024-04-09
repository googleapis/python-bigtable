# Copyright 2024 Google LLC
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
from __future__ import annotations

from typing import Any

import google.auth.credentials

from google.cloud.bigtable.data._sync._autogen import BigtableDataClient_SyncGen
from google.cloud.bigtable.data._sync._autogen import Table_SyncGen


class BigtableDataClient(BigtableDataClient_SyncGen):
    def __init__(
        self,
        *,
        project: str | None = None,
        credentials: google.auth.credentials.Credentials | None = None,
        client_options: dict[str, Any]
        | "google.api_core.client_options.ClientOptions"
        | None = None,
    ):
        # remove pool size option in sync client
        super().__init__(
            project=project, credentials=credentials, client_options=client_options
        )


class Table(Table_SyncGen):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # register table with client
        self.client._register_instance(self.instance_id, self)
