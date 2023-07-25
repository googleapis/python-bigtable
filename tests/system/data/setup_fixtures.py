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
import pytest_asyncio
import os
import asyncio


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.stop()
    loop.close()


@pytest.fixture(scope="session")
def instance_admin_client():
    """Client for interacting with the Instance Admin API."""
    from google.cloud.bigtable_admin_v2 import BigtableInstanceAdminClient

    with BigtableInstanceAdminClient() as client:
        yield client


@pytest.fixture(scope="session")
def table_admin_client():
    """Client for interacting with the Table Admin API."""
    from google.cloud.bigtable_admin_v2 import BigtableTableAdminClient

    with BigtableTableAdminClient() as client:
        yield client


@pytest.fixture(scope="session")
def instance_id(instance_admin_client, project_id):
    """
    Returns BIGTABLE_TEST_INSTANCE if set, otherwise creates a new temporary instance for the test session
    """
    from google.cloud.bigtable_admin_v2 import types
    from google.api_core import exceptions

    # use user-specified instance if available
    user_specified_instance = os.getenv("BIGTABLE_TEST_INSTANCE")
    if user_specified_instance:
        print("Using user-specified instance: {}".format(user_specified_instance))
        yield user_specified_instance
        return

    # create a new temporary test instance
    instance_id = "test-instance"
    try:
        operation = instance_admin_client.create_instance(
            parent=f"projects/{project_id}",
            instance_id=instance_id,
            instance=types.Instance(
                display_name="Test Instance",
                labels={"python-system-test": "true"},
            ),
            clusters={
                "test-cluster": types.Cluster(
                    location=f"projects/{project_id}/locations/us-central1-b",
                    serve_nodes=3,
                )
            },
        )
        operation.result(timeout=240)
    except exceptions.AlreadyExists:
        pass
    yield instance_id
    instance_admin_client.delete_instance(
        name=f"projects/{project_id}/instances/{instance_id}"
    )


@pytest.fixture(scope="session")
def table_id(table_admin_client, project_id, instance_id, init_column_families, init_table_id):
    """
    Returns BIGTABLE_TEST_TABLE if set, otherwise creates a new temporary table for the test session

    Args:
      - table_admin_client: Client for interacting with the Table Admin API. Supplied by the table_admin_client fixture.
      - project_id: The project ID of the GCP project to test against. Supplied by the project_id fixture.
      - instance_id: The ID of the Bigtable instance to test against. Supplied by the instance_id fixture.
      - init_column_families: A list of column families to initialize the table with, if pre-initialized table is not given with BIGTABLE_TEST_TABLE.
            Supplied by the init_column_families fixture.
      - init_table_id: The table ID to give to the test table, if pre-initialized table is not given with BIGTABLE_TEST_TABLE.
            Supplied by the init_table_id fixture.
    """
    from google.cloud.bigtable_admin_v2 import types
    from google.api_core import exceptions
    from google.api_core import retry

    # use user-specified instance if available
    user_specified_table = os.getenv("BIGTABLE_TEST_TABLE")
    if user_specified_table:
        print("Using user-specified table: {}".format(user_specified_table))
        yield user_specified_table
        return

    retry = retry.Retry(
        predicate=retry.if_exception_type(exceptions.FailedPrecondition)
    )
    try:
        parent_path = f"projects/{project_id}/instances/{instance_id}"
        print(f"Creating table: {parent_path}/tables/{init_table_id}")
        table_admin_client.create_table(
            request={
                "parent": parent_path,
                "table_id": init_table_id,
                "table": {"column_families": init_column_families},
                "initial_splits": [{"key": (num * 1000).to_bytes(8, "big")} for num in range(1,10)],
            },
            retry=retry,
        )
    except exceptions.AlreadyExists:
        pass
    yield init_table_id
    print(f"Deleting table: {parent_path}/tables/{init_table_id}")
    table_admin_client.delete_table(
        name=f"projects/{project_id}/instances/{instance_id}/tables/{init_table_id}"
    )


@pytest_asyncio.fixture(scope="session")
async def client():
    from google.cloud.bigtable.data import BigtableDataClientAsync

    project = os.getenv("GOOGLE_CLOUD_PROJECT") or None
    async with BigtableDataClientAsync(project=project) as client:
        yield client


@pytest.fixture(scope="session")
def project_id(client):
    """Returns the project ID from the client."""
    yield client.project


@pytest_asyncio.fixture(scope="session")
async def table(client, table_id, instance_id):
    async with client.get_table(instance_id, table_id) as table:
        yield table
