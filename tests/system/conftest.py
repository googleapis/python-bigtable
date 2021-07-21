# Copyright 2011 Google LLC
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

import os

import pytest
from test_utils.system import unique_resource_id

from google.cloud.bigtable.client import Client
from google.cloud.environment_vars import BIGTABLE_EMULATOR

from . import _helpers


@pytest.fixture(scope="session")
def in_emulator():
    return os.getenv(BIGTABLE_EMULATOR) is not None


@pytest.fixture(scope="session")
def kms_key_name():
    return os.getenv("KMS_KEY_NAME")


@pytest.fixture(scope="session")
def with_kms_key_name(kms_key_name):
    if kms_key_name is None:
        pytest.skip("Test requires KMS_KEY_NAME environment variable")
    return kms_key_name


@pytest.fixture(scope="session")
def not_in_emulator(in_emulator):
    if in_emulator:
        pytest.skip("Emulator does not support this feature")


@pytest.fixture(scope="session")
def unique_suffix():
    return unique_resource_id("-")


@pytest.fixture(scope="session")
def location_id():
    return "us-central1-c"


@pytest.fixture(scope="session")
def serve_nodes():
    return 3


@pytest.fixture(scope="session")
def label_key():
    return "python-system"


@pytest.fixture(scope="session")
def instance_labels(label_key):
    return {label_key: _helpers.label_stamp()}


@pytest.fixture(scope="session")
def admin_client():
    return Client(admin=True)


@pytest.fixture(scope="session")
def admin_instance_id(unique_suffix):
    return f"g-c-p{unique_suffix}"


@pytest.fixture(scope="session")
def admin_cluster_id(admin_instance_id):
    return f"{admin_instance_id}-cluster"


@pytest.fixture(scope="session")
def admin_instance(admin_client, admin_instance_id, instance_labels):
    return admin_client.instance(admin_instance_id, labels=instance_labels)


@pytest.fixture(scope="session")
def admin_cluster(admin_instance, admin_cluster_id, location_id, serve_nodes):
    return admin_instance.cluster(
        admin_cluster_id, location_id=location_id, serve_nodes=serve_nodes,
    )


@pytest.fixture(scope="session")
def admin_instance_populated(admin_instance, admin_cluster, in_emulator):
    if not in_emulator:
        operation = admin_instance.create(clusters=[admin_cluster])
        operation.result(timeout=30)

    yield admin_instance

    if not in_emulator:
        _helpers.retry_429(admin_instance.delete)()
