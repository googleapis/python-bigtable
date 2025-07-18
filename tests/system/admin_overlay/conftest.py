import google.auth

import os
import pytest
import uuid


INSTANCE_PREFIX = "admin-overlay-instance"
BACKUP_PREFIX = "admin-overlay-backup"
ROW_PREFIX = "test-row"

DEFAULT_CLUSTER_LOCATIONS = ["us-east1-b"]
REPLICATION_CLUSTER_LOCATIONS = ["us-east1-b", "us-west1-b"]
TEST_TABLE_NAME = "system-test-table"
TEST_BACKUP_TABLE_NAME = "system-test-backup-table"
TEST_COLUMMN_FAMILY_NAME = "test-column"
TEST_COLUMN_NAME = "value"
NUM_ROWS = 500
INITIAL_CELL_VALUE = "Hello"
NEW_CELL_VALUE = "World"


@pytest.fixture(scope="session")
def admin_overlay_project_id():
    _, default_project = google.auth.default()
    yield os.getenv("GOOGLE_CLOUD_PROJECT") or default_project


def generate_unique_suffix(name):
    """
    Generates a unique suffix for the name.

    Uses UUID4 because using time.time doesn't guarantee
    uniqueness when the time is frozen in containers.
    """
    return f"{name}-{uuid.uuid4().hex[:7]}"
