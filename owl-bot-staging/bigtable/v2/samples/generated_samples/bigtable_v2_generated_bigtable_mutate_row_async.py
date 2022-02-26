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
# Generated code. DO NOT EDIT!
#
# Snippet for MutateRow
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-bigtable


# [START bigtable_v2_generated_Bigtable_MutateRow_async]
from google.cloud import bigtable_v2


async def sample_mutate_row():
    # Create a client
    client = bigtable_v2.BigtableAsyncClient()

    # Initialize request argument(s)
    request = bigtable_v2.MutateRowRequest(
        table_name="table_name_value",
        row_key=b'row_key_blob',
    )

    # Make the request
    response = await client.mutate_row(request=request)

    # Handle the response
    print(response)

# [END bigtable_v2_generated_Bigtable_MutateRow_async]
