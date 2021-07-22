# Copyright 2016 Google LLC
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

import datetime
import os
import unittest

from google.api_core.exceptions import TooManyRequests
from google.cloud.environment_vars import BIGTABLE_EMULATOR
from test_utils.retry import RetryErrors

# from test_utils.system import EmulatorCreds
from test_utils.system import unique_resource_id

from google.cloud._helpers import _datetime_from_microseconds
from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC
from google.cloud.bigtable.client import Client
from google.cloud.bigtable.row_data import Cell

# from google.cloud.bigtable_admin_v2.gapic import (
#     bigtable_table_admin_client_config as table_admin_config,
# )

UNIQUE_SUFFIX = unique_resource_id("-")
LOCATION_ID = "us-central1-c"
INSTANCE_ID = "g-c-p" + UNIQUE_SUFFIX
INSTANCE_ID_DATA = "g-c-p-d" + UNIQUE_SUFFIX
TABLE_ID = "google-cloud-python-test-table"
CLUSTER_ID = INSTANCE_ID + "-cluster"
CLUSTER_ID_DATA = INSTANCE_ID_DATA + "-cluster"
SERVE_NODES = 3
COLUMN_FAMILY_ID1 = "col-fam-id1"
COLUMN_FAMILY_ID2 = "col-fam-id2"
COL_NAME1 = b"col-name1"
COL_NAME2 = b"col-name2"
COL_NAME3 = b"col-name3-but-other-fam"
CELL_VAL1 = b"cell-val"
CELL_VAL2 = b"cell-val-newer"
CELL_VAL3 = b"altcol-cell-val"
CELL_VAL4 = b"foo"
ROW_KEY = b"row-key"
ROW_KEY_ALT = b"row-key-alt"
EXISTING_INSTANCES = []
LABEL_KEY = "python-system"
label_stamp = (
    datetime.datetime.utcnow()
    .replace(microsecond=0, tzinfo=UTC)
    .strftime("%Y-%m-%dt%H-%M-%S")
)
LABELS = {LABEL_KEY: str(label_stamp)}
KMS_KEY_NAME = os.environ.get("KMS_KEY_NAME", None)


class Config(object):
    """Run-time configuration to be modified at set-up.

    This is a mutable stand-in to allow test set-up to modify
    global state.
    """

    CLIENT = None
    INSTANCE = None
    INSTANCE_DATA = None
    CLUSTER = None
    CLUSTER_DATA = None
    IN_EMULATOR = False


def _retry_on_unavailable(exc):
    """Retry only errors whose status code is 'UNAVAILABLE'."""
    from grpc import StatusCode

    return exc.code() == StatusCode.UNAVAILABLE


retry_429 = RetryErrors(TooManyRequests, max_tries=9)


def setUpModule():
    from google.cloud.exceptions import GrpcRendezvous
    from google.cloud.bigtable.enums import Instance

    # See: https://github.com/googleapis/google-cloud-python/issues/5928
    # interfaces = table_admin_config.config["interfaces"]
    # iface_config = interfaces["google.bigtable.admin.v2.BigtableTableAdmin"]
    # methods = iface_config["methods"]
    # create_table = methods["CreateTable"]
    # create_table["timeout_millis"] = 90000

    Config.IN_EMULATOR = os.getenv(BIGTABLE_EMULATOR) is not None

    # Previously we created clients using a mock EmulatorCreds when targeting
    # an emulator.
    Config.CLIENT = Client(admin=True)

    Config.INSTANCE = Config.CLIENT.instance(INSTANCE_ID, labels=LABELS)
    Config.CLUSTER = Config.INSTANCE.cluster(
        CLUSTER_ID, location_id=LOCATION_ID, serve_nodes=SERVE_NODES,
    )
    Config.INSTANCE_DATA = Config.CLIENT.instance(
        INSTANCE_ID_DATA, instance_type=Instance.Type.DEVELOPMENT, labels=LABELS
    )
    Config.CLUSTER_DATA = Config.INSTANCE_DATA.cluster(
        CLUSTER_ID_DATA, location_id=LOCATION_ID,
    )

    if not Config.IN_EMULATOR:
        retry = RetryErrors(GrpcRendezvous, error_predicate=_retry_on_unavailable)
        instances, failed_locations = retry(Config.CLIENT.list_instances)()

        if len(failed_locations) != 0:
            raise ValueError("List instances failed in module set up.")

        EXISTING_INSTANCES[:] = instances

        # After listing, create the test instances.
        admin_op = Config.INSTANCE.create(clusters=[Config.CLUSTER])
        admin_op.result(timeout=30)
        data_op = Config.INSTANCE_DATA.create(clusters=[Config.CLUSTER_DATA])
        data_op.result(timeout=30)


def tearDownModule():
    if not Config.IN_EMULATOR:
        retry_429(Config.INSTANCE.delete)()
        retry_429(Config.INSTANCE_DATA.delete)()


class TestDataAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._table = table = Config.INSTANCE_DATA.table("test-data-api")
        table.create()
        table.column_family(COLUMN_FAMILY_ID1).create()
        table.column_family(COLUMN_FAMILY_ID2).create()

    @classmethod
    def tearDownClass(cls):
        # Will also delete any data contained in the table.
        cls._table.delete()

    def _maybe_emulator_skip(self, message):
        # NOTE: This method is necessary because ``Config.IN_EMULATOR``
        #       is set at runtime rather than import time, which means we
        #       can't use the @unittest.skipIf decorator.
        if Config.IN_EMULATOR:
            self.skipTest(message)

    def setUp(self):
        self.rows_to_delete = []

    def tearDown(self):
        for row in self.rows_to_delete:
            row.clear()
            row.delete()
            row.commit()

    def _write_to_row(self, row1=None, row2=None, row3=None, row4=None):
        timestamp1 = datetime.datetime.utcnow().replace(tzinfo=UTC)
        timestamp1_micros = _microseconds_from_datetime(timestamp1)
        # Truncate to millisecond granularity.
        timestamp1_micros -= timestamp1_micros % 1000
        timestamp1 = _datetime_from_microseconds(timestamp1_micros)
        # 1000 microseconds is a millisecond
        timestamp2 = timestamp1 + datetime.timedelta(microseconds=1000)
        timestamp2_micros = _microseconds_from_datetime(timestamp2)
        timestamp3 = timestamp1 + datetime.timedelta(microseconds=2000)
        timestamp3_micros = _microseconds_from_datetime(timestamp3)
        timestamp4 = timestamp1 + datetime.timedelta(microseconds=3000)
        timestamp4_micros = _microseconds_from_datetime(timestamp4)

        if row1 is not None:
            row1.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL1, timestamp=timestamp1)
        if row2 is not None:
            row2.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL2, timestamp=timestamp2)
        if row3 is not None:
            row3.set_cell(COLUMN_FAMILY_ID1, COL_NAME2, CELL_VAL3, timestamp=timestamp3)
        if row4 is not None:
            row4.set_cell(COLUMN_FAMILY_ID2, COL_NAME3, CELL_VAL4, timestamp=timestamp4)

        # Create the cells we will check.
        cell1 = Cell(CELL_VAL1, timestamp1_micros)
        cell2 = Cell(CELL_VAL2, timestamp2_micros)
        cell3 = Cell(CELL_VAL3, timestamp3_micros)
        cell4 = Cell(CELL_VAL4, timestamp4_micros)
        return cell1, cell2, cell3, cell4
