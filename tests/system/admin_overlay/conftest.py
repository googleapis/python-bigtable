from google.cloud.client import ClientWithProject

import pytest


INSTANCE_PREFIX = "admin-overlay-instance"
BACKUP_PREFIX = "admin-overlay-backup"
ROW_PREFIX = "test-row"

DEFAULT_CLUSTER_LOCATIONS = [
    "us-east1-b"
]
REPLICATION_CLUSTER_LOCATIONS = [
    "us-east1-b",
    "us-west1-b"
]
TEST_TABLE_NAME = "system-test-table"
TEST_BACKUP_TABLE_NAME = "system-test-backup-table"
TEST_COLUMMN_FAMILY_NAME = "test-column"
TEST_COLUMN_NAME = "value"
NUM_ROWS = 500
INITIAL_CELL_VALUE = "Hello"
NEW_CELL_VALUE = "World"


@pytest.fixture(scope="session")
def admin_overlay_project_id():
    # Instantiate a meaningless client to get project name
    # from environment variables.
    client = ClientWithProject()
    return client.project
