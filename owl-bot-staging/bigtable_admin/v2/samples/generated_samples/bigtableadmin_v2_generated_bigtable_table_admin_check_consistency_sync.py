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
# Snippet for CheckConsistency
# NOTE: This snippet has been automatically generated for illustrative purposes only.
# It may require modifications to work in your environment.

# To install the latest published package dependency, execute the following:
#   python3 -m pip install google-cloud-bigtable-admin


# [START bigtableadmin_v2_generated_BigtableTableAdmin_CheckConsistency_sync]
from google.cloud import bigtable_admin_v2


def sample_check_consistency():
    # Create a client
    client = bigtable_admin_v2.BigtableTableAdminClient()

    # Initialize request argument(s)
    request = bigtable_admin_v2.CheckConsistencyRequest(
        name="name_value",
        consistency_token="consistency_token_value",
    )

    # Make the request
    response = client.check_consistency(request=request)

    # Handle the response
    print(response)

# [END bigtableadmin_v2_generated_BigtableTableAdmin_CheckConsistency_sync]
