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
"""
Contains a set of pytest fixtures for setting up and populating a
Bigtable database for testing purposes.
"""

import pytest
import os
import uuid
import datetime


@pytest.fixture(scope="session")
def admin_client():
    """
    Client for interacting with Table and Instance admin APIs
    """
    from google.cloud.bigtable.client import Client

    client = Client(admin=True)
    yield client


@pytest.fixture(scope="session")
def instance_id(admin_client, project_id, cluster_config):
    """
    Returns BIGTABLE_TEST_INSTANCE if set, otherwise creates a new temporary instance for the test session
    """
    from google.cloud.bigtable_admin_v2 import types
    from google.api_core import exceptions
    from google.cloud.environment_vars import BIGTABLE_EMULATOR

    # use user-specified instance if available
    user_specified_instance = os.getenv("BIGTABLE_TEST_INSTANCE")
    if user_specified_instance:
        print("Using user-specified instance: {}".format(user_specified_instance))
        yield user_specified_instance
        return

    # create a new temporary test instance
    instance_id = f"python-bigtable-tests-{uuid.uuid4().hex[:6]}"
    if os.getenv(BIGTABLE_EMULATOR):
        # don't create instance if in emulator mode
        yield instance_id
    else:
        try:
            operation = admin_client.instance_admin_client.create_instance(
                parent=f"projects/{project_id}",
                instance_id=instance_id,
                instance=types.Instance(
                    display_name="Test Instance",
                    # labels={"python-system-test": "true"},
                ),
                clusters=cluster_config,
            )
            operation.result(timeout=240)
        except exceptions.AlreadyExists:
            pass
        yield instance_id
        admin_client.instance_admin_client.delete_instance(
            name=f"projects/{project_id}/instances/{instance_id}"
        )


@pytest.fixture(scope="session")
def column_split_config():
    """
    specify initial splits to create when creating a new test table
    """
    return [(num * 1000).to_bytes(8, "big") for num in range(1, 10)]


@pytest.fixture(scope="session")
def project_id(client):
    """Returns the project ID from the client."""
    yield client.project
