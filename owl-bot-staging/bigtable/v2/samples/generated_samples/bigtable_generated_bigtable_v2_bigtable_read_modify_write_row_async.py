# -*- coding: utf-8 -*-
# Copyright 2020 Google LLC
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
# Generated code. DO NOT EDIT!
#
# Snippet for ReadModifyWriteRow
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-bigtable


# [START bigtable_generated_bigtable_v2_Bigtable_ReadModifyWriteRow_async]
from google.cloud import bigtable_v2


async def sample_read_modify_write_row():
    # Create a client
    client = bigtable_v2.BigtableAsyncClient()

    # Initialize request argument(s)
    rules = bigtable_v2.ReadModifyWriteRule()
    rules.append_value = b'append_value_blob'

    request = bigtable_v2.ReadModifyWriteRowRequest(
        table_name="table_name_value",
        row_key=b'row_key_blob',
        rules=rules,
    )

    # Make the request
    response = await client.read_modify_write_row(request=request)

    # Handle the response
    print(response)

# [END bigtable_generated_bigtable_v2_Bigtable_ReadModifyWriteRow_async]
