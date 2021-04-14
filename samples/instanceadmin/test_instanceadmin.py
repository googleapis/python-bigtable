# Copyright 2018 Google Inc.
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
import random

import instanceadmin


PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
ID_FORMAT = "instanceadmin-test-{:06}"
ID_RANGE = 1000000

INSTANCE = ID_FORMAT.format(random.randrange(ID_RANGE))
CLUSTER1 = ID_FORMAT.format(random.randrange(ID_RANGE))
CLUSTER2 = ID_FORMAT.format(random.randrange(ID_RANGE))


def test_run_instance_operations(capsys):
    try:
        instanceadmin.run_instance_operations(PROJECT, INSTANCE, CLUSTER1)
        out = capsys.readouterr().out
        assert f"Instance {INSTANCE} does not exist." in out
        assert "Creating an instance" in out
        assert f"Created instance: {INSTANCE}" in out
        assert "Listing instances" in out
        assert f"\n{INSTANCE}\n" in out
        assert f"Name of instance: {INSTANCE}" in out
        assert "Labels: {'prod-label': 'prod-label'}" in out
        assert "Listing clusters..." in out
        assert f"\n{CLUSTER1}\n" in out

        instanceadmin.run_instance_operations(PROJECT, INSTANCE, CLUSTER1)
        out = capsys.readouterr().out
        assert f"Instance {INSTANCE} already exists." in out
        assert "Listing instances" in out
        assert f"\n{INSTANCE}\n" in out
        assert f"Name of instance: {INSTANCE}" in out
        assert "Labels: {'prod-label': 'prod-label'}" in out
        assert "Listing clusters..." in out
        assert f"\n{CLUSTER1}\n" in out

    finally:
        instanceadmin.delete_instance(PROJECT, INSTANCE)


def test_delete_instance(capsys):
    try:
        # Can't delete it, it doesn't exist
        instanceadmin.delete_instance(PROJECT, INSTANCE)
        out = capsys.readouterr().out
        assert "Deleting instance" in out
        assert f"Instance {INSTANCE} does not exist" in out

        # Ok, create it then
        instanceadmin.run_instance_operations(PROJECT, INSTANCE, CLUSTER1)
        capsys.readouterr()  # throw away output

        # Now delete it
        instanceadmin.delete_instance(PROJECT, INSTANCE)
        out = capsys.readouterr().out
        assert "Deleting instance" in out
        assert f"Deleted instance: {INSTANCE}" in out

    finally:
        instanceadmin.delete_instance(PROJECT, INSTANCE)


def test_create_dev_instance(capsys):
    try:
        instanceadmin.create_dev_instance(PROJECT, INSTANCE, CLUSTER1)
        out = capsys.readouterr().out
        assert "Creating a DEVELOPMENT instance" in out
        assert f"Created development instance: {INSTANCE}" in out

        instanceadmin.create_dev_instance(PROJECT, INSTANCE, CLUSTER1)
        out = capsys.readouterr().out
        assert f"Instance {INSTANCE} already exists" in out

    finally:
        instanceadmin.delete_instance(PROJECT, INSTANCE)


def test_add_and_delete_cluster(capsys):
    try:
        # This won't work, because the instance isn't created yet
        instanceadmin.add_cluster(PROJECT, INSTANCE, CLUSTER2)
        out = capsys.readouterr().out
        assert f"Instance {INSTANCE} does not exist" in out

        # Get the instance created
        instanceadmin.run_instance_operations(PROJECT, INSTANCE, CLUSTER1)
        capsys.readouterr()  # throw away output

        # Add a cluster to that instance
        instanceadmin.add_cluster(PROJECT, INSTANCE, CLUSTER2)
        out = capsys.readouterr().out
        assert f"Adding cluster to instance {INSTANCE}" in out
        assert "Listing clusters..." in out
        assert f"\n{CLUSTER1}\n" in out
        assert f"Cluster created: {CLUSTER2}" in out

        # Try to add the same cluster again, won't work
        instanceadmin.add_cluster(PROJECT, INSTANCE, CLUSTER2)
        out = capsys.readouterr().out
        assert "Listing clusters..." in out
        assert f"\n{CLUSTER1}\n" in out
        assert f"\n{CLUSTER2}\n" in out
        assert f"Cluster not created, as {CLUSTER2} already exists."

        # Now delete it
        instanceadmin.delete_cluster(PROJECT, INSTANCE, CLUSTER2)
        out = capsys.readouterr().out
        assert "Deleting cluster" in out
        assert f"Cluster deleted: {CLUSTER2}" in out

        # Verify deletion
        instanceadmin.run_instance_operations(PROJECT, INSTANCE, CLUSTER1)
        out = capsys.readouterr().out
        assert "Listing clusters..." in out
        assert f"\n{CLUSTER1}\n" in out
        assert f"\n{CLUSTER2}\n" not in out

        # Try deleting it again, for fun (and coverage)
        instanceadmin.delete_cluster(PROJECT, INSTANCE, CLUSTER2)
        out = capsys.readouterr().out
        assert "Deleting cluster" in out
        assert f"Cluster {CLUSTER2} does not exist" in out

    finally:
        instanceadmin.delete_instance(PROJECT, INSTANCE)
