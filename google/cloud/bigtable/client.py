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
#

from __future__ import annotations

from google.cloud.client import ClientWithProject

class BigtableDataClient(ClientWithProject):
    def __init__(
        self,
        *,
        project: str|None = None,
        credentials: google.auth.credentials.Credentials|None = None,
        client_options: dict[str, Any] | "google.api_core.client_options.ClientOptions" | None = None,
        metadata: list[tuple[str, str]]|None = None,
    ):
        """
        Create a client instance

        Args:
            metadata: a list of metadata headers to be attached to all calls with this client
        """
        pass


    def get_table(instance_id:str, table_id:str, app_profile_id:str|None=None) -> Table:
        pass

if __name__ == "__main__":
    client = BigtableDataClient()
    client.get_table("instance_id", "table_id")
