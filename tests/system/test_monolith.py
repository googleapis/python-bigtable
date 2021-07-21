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
import operator
import os
import time
import unittest

from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.api_core.exceptions import DeadlineExceeded
from google.api_core.exceptions import TooManyRequests
from google.cloud.environment_vars import BIGTABLE_EMULATOR
from test_utils.retry import RetryErrors
from test_utils.retry import RetryResult

# from test_utils.system import EmulatorCreds
from test_utils.system import unique_resource_id

from google.cloud._helpers import _datetime_from_microseconds
from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC
from google.cloud.bigtable.client import Client
from google.cloud.bigtable.column_family import MaxVersionsGCRule
from google.cloud.bigtable.policy import Policy
from google.cloud.bigtable.policy import BIGTABLE_ADMIN_ROLE
from google.cloud.bigtable.row_filters import ApplyLabelFilter
from google.cloud.bigtable.row_filters import ColumnQualifierRegexFilter
from google.cloud.bigtable.row_filters import RowFilterChain
from google.cloud.bigtable.row_filters import RowFilterUnion
from google.cloud.bigtable.row_data import Cell
from google.cloud.bigtable.row_data import PartialRowData
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange

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


class TestTableAdminAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._table = Config.INSTANCE_DATA.table(TABLE_ID)
        cls._table.create()

    @classmethod
    def tearDownClass(cls):
        cls._table.delete()

    def setUp(self):
        self.tables_to_delete = []
        self.backups_to_delete = []

    def tearDown(self):
        for table in self.tables_to_delete:
            table.delete()
        for backup in self.backups_to_delete:
            backup.delete()

    def _skip_if_emulated(self, message):
        # NOTE: This method is necessary because ``Config.IN_EMULATOR``
        #       is set at runtime rather than import time, which means we
        #       can't use the @unittest.skipIf decorator.
        if Config.IN_EMULATOR:
            self.skipTest(message)

    def test_list_tables(self):
        # Since `Config.INSTANCE_DATA` is newly created in `setUpModule`, the
        # table created in `setUpClass` here will be the only one.
        tables = Config.INSTANCE_DATA.list_tables()
        self.assertEqual(tables, [self._table])

    def test_exists(self):
        retry_until_true = RetryResult(lambda result: result)
        retry_until_false = RetryResult(lambda result: not result)
        temp_table_id = "test-table_existence"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        self.assertFalse(temp_table.exists())
        temp_table.create()
        self.assertTrue(retry_until_true(temp_table.exists)())
        temp_table.delete()
        self.assertFalse(retry_until_false(temp_table.exists)())

    def test_create_table(self):
        temp_table_id = "test-create-table"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        # First, create a sorted version of our expected result.
        name_attr = operator.attrgetter("name")
        expected_tables = sorted([temp_table, self._table], key=name_attr)

        # Then query for the tables in the instance and sort them by
        # name as well.
        tables = Config.INSTANCE_DATA.list_tables()
        sorted_tables = sorted(tables, key=name_attr)
        self.assertEqual(sorted_tables, expected_tables)

    def test_test_iam_permissions(self):
        self._skip_if_emulated("Method not implemented in bigtable emulator")
        temp_table_id = "test-test-iam-policy-table"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        permissions = ["bigtable.tables.mutateRows", "bigtable.tables.readRows"]
        permissions_allowed = temp_table.test_iam_permissions(permissions)
        self.assertEqual(permissions, permissions_allowed)

    def test_get_iam_policy(self):
        self._skip_if_emulated("Method not implemented in bigtable emulator")
        temp_table_id = "test-get-iam-policy-table"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        policy = temp_table.get_iam_policy().to_api_repr()
        self.assertEqual(policy["etag"], "ACAB")
        self.assertEqual(policy["version"], 0)

    def test_set_iam_policy(self):
        self._skip_if_emulated("Method not implemented in bigtable emulator")
        temp_table_id = "test-set-iam-policy-table"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        new_policy = Policy()
        service_account_email = Config.CLIENT._credentials.service_account_email
        new_policy[BIGTABLE_ADMIN_ROLE] = [
            Policy.service_account(service_account_email)
        ]
        policy_latest = temp_table.set_iam_policy(new_policy).to_api_repr()

        self.assertEqual(policy_latest["bindings"][0]["role"], "roles/bigtable.admin")
        self.assertIn(service_account_email, policy_latest["bindings"][0]["members"][0])

    def test_create_table_with_families(self):
        temp_table_id = "test-create-table-with-failies"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        gc_rule = MaxVersionsGCRule(1)
        temp_table.create(column_families={COLUMN_FAMILY_ID1: gc_rule})
        self.tables_to_delete.append(temp_table)

        col_fams = temp_table.list_column_families()

        self.assertEqual(len(col_fams), 1)
        retrieved_col_fam = col_fams[COLUMN_FAMILY_ID1]
        self.assertIs(retrieved_col_fam._table, temp_table)
        self.assertEqual(retrieved_col_fam.column_family_id, COLUMN_FAMILY_ID1)
        self.assertEqual(retrieved_col_fam.gc_rule, gc_rule)

    def test_create_table_with_split_keys(self):
        self._skip_if_emulated("Split keys are not supported by Bigtable emulator")
        temp_table_id = "foo-bar-baz-split-table"
        initial_split_keys = [b"split_key_1", b"split_key_10", b"split_key_20"]
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create(initial_split_keys=initial_split_keys)
        self.tables_to_delete.append(temp_table)

        # Read Sample Row Keys for created splits
        sample_row_keys = temp_table.sample_row_keys()
        actual_keys = [srk.row_key for srk in sample_row_keys]

        expected_keys = initial_split_keys
        expected_keys.append(b"")

        self.assertEqual(actual_keys, expected_keys)

    def test_create_column_family(self):
        temp_table_id = "test-create-column-family"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        self.assertEqual(temp_table.list_column_families(), {})
        gc_rule = MaxVersionsGCRule(1)
        column_family = temp_table.column_family(COLUMN_FAMILY_ID1, gc_rule=gc_rule)
        column_family.create()

        col_fams = temp_table.list_column_families()

        self.assertEqual(len(col_fams), 1)
        retrieved_col_fam = col_fams[COLUMN_FAMILY_ID1]
        self.assertIs(retrieved_col_fam._table, column_family._table)
        self.assertEqual(
            retrieved_col_fam.column_family_id, column_family.column_family_id
        )
        self.assertEqual(retrieved_col_fam.gc_rule, gc_rule)

    def test_update_column_family(self):
        temp_table_id = "test-update-column-family"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        gc_rule = MaxVersionsGCRule(1)
        column_family = temp_table.column_family(COLUMN_FAMILY_ID1, gc_rule=gc_rule)
        column_family.create()

        # Check that our created table is as expected.
        col_fams = temp_table.list_column_families()
        self.assertEqual(col_fams, {COLUMN_FAMILY_ID1: column_family})

        # Update the column family's GC rule and then try to update.
        column_family.gc_rule = None
        column_family.update()

        # Check that the update has propagated.
        col_fams = temp_table.list_column_families()
        self.assertIsNone(col_fams[COLUMN_FAMILY_ID1].gc_rule)

    def test_delete_column_family(self):
        temp_table_id = "test-delete-column-family"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        self.assertEqual(temp_table.list_column_families(), {})
        column_family = temp_table.column_family(COLUMN_FAMILY_ID1)
        column_family.create()

        # Make sure the family is there before deleting it.
        col_fams = temp_table.list_column_families()
        self.assertEqual(list(col_fams.keys()), [COLUMN_FAMILY_ID1])

        retry_504 = RetryErrors(DeadlineExceeded)
        retry_504(column_family.delete)()
        # Make sure we have successfully deleted it.
        self.assertEqual(temp_table.list_column_families(), {})

    def test_backup(self):
        if Config.IN_EMULATOR:
            self.skipTest("backups are not supported in the emulator")

        from google.cloud._helpers import _datetime_to_pb_timestamp
        from google.cloud.bigtable import enums

        temp_table_id = "test-backup-table"
        temp_table = Config.INSTANCE_DATA.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        temp_backup_id = "test-backup"

        # TODO: consider using `datetime.datetime.now().timestamp()`
        #  when support for Python 2 is fully dropped
        expire = int(time.mktime(datetime.datetime.now().timetuple())) + 604800

        # Testing `Table.backup()` factory
        temp_backup = temp_table.backup(
            temp_backup_id,
            cluster_id=CLUSTER_ID_DATA,
            expire_time=datetime.datetime.utcfromtimestamp(expire),
        )

        # Reinitialize the admin client. This is to test `_table_admin_client` returns a client object (and not NoneType)
        temp_backup._instance._client = Client(admin=True)

        # Sanity check for `Backup.exists()` method
        self.assertFalse(temp_backup.exists())

        # Testing `Backup.create()` method
        temp_backup.create().result(timeout=30)

        # Implicit testing of `Backup.delete()` method
        self.backups_to_delete.append(temp_backup)

        # Testing `Backup.exists()` method
        self.assertTrue(temp_backup.exists())

        # Testing `Table.list_backups()` method
        temp_table_backup = temp_table.list_backups()[0]
        self.assertEqual(temp_backup_id, temp_table_backup.backup_id)
        self.assertEqual(CLUSTER_ID_DATA, temp_table_backup.cluster)
        self.assertEqual(expire, temp_table_backup.expire_time.seconds)
        self.assertEqual(
            temp_table_backup.encryption_info.encryption_type,
            enums.EncryptionInfo.EncryptionType.GOOGLE_DEFAULT_ENCRYPTION,
        )

        # Testing `Backup.update_expire_time()` method
        expire += 3600  # A one-hour change in the `expire_time` parameter
        updated_time = datetime.datetime.utcfromtimestamp(expire)
        temp_backup.update_expire_time(updated_time)
        test = _datetime_to_pb_timestamp(updated_time)

        # Testing `Backup.get()` method
        temp_table_backup = temp_backup.get()
        self.assertEqual(
            test.seconds,
            DatetimeWithNanoseconds.timestamp(temp_table_backup.expire_time),
        )

        # Testing `Table.restore()` and `Backup.retore()` methods
        restored_table_id = "test-backup-table-restored"
        restored_table = Config.INSTANCE_DATA.table(restored_table_id)
        temp_table.restore(
            restored_table_id, cluster_id=CLUSTER_ID_DATA, backup_id=temp_backup_id,
        ).result(timeout=30)
        tables = Config.INSTANCE_DATA.list_tables()
        self.assertIn(restored_table, tables)
        restored_table.delete()

        # Testing `Backup.restore()` into a different instance:
        # Setting up another instance...
        alt_instance_id = "gcp-alt-" + UNIQUE_SUFFIX
        alt_cluster_id = alt_instance_id + "-cluster"
        alt_instance = Config.CLIENT.instance(alt_instance_id, labels=LABELS)
        alt_cluster = alt_instance.cluster(
            cluster_id=alt_cluster_id, location_id=LOCATION_ID, serve_nodes=SERVE_NODES,
        )
        if not Config.IN_EMULATOR:
            alt_instance.create(clusters=[alt_cluster]).result(timeout=30)

        # Testing `restore()`...
        temp_backup.restore(restored_table_id, alt_instance_id).result(timeout=30)
        restored_table = alt_instance.table(restored_table_id)
        self.assertIn(restored_table, alt_instance.list_tables())
        restored_table.delete()

        # Tearing down the resources...
        if not Config.IN_EMULATOR:
            retry_429(alt_instance.delete)()


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

    def test_timestamp_filter_millisecond_granularity(self):
        from google.cloud.bigtable import row_filters

        end = datetime.datetime.now()
        start = end - datetime.timedelta(minutes=60)
        timestamp_range = row_filters.TimestampRange(start=start, end=end)
        timefilter = row_filters.TimestampRangeFilter(timestamp_range)
        row_data = self._table.read_rows(filter_=timefilter)
        row_data.consume_all()

    def test_mutate_rows(self):
        row1 = self._table.row(ROW_KEY)
        row1.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL1)
        row1.commit()
        self.rows_to_delete.append(row1)
        row2 = self._table.row(ROW_KEY_ALT)
        row2.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL2)
        row2.commit()
        self.rows_to_delete.append(row2)

        # Change the contents
        row1.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL3)
        row2.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL4)
        rows = [row1, row2]
        statuses = self._table.mutate_rows(rows)
        result = [status.code for status in statuses]
        expected_result = [0, 0]
        self.assertEqual(result, expected_result)

        # Check the contents
        row1_data = self._table.read_row(ROW_KEY)
        self.assertEqual(
            row1_data.cells[COLUMN_FAMILY_ID1][COL_NAME1][0].value, CELL_VAL3
        )
        row2_data = self._table.read_row(ROW_KEY_ALT)
        self.assertEqual(
            row2_data.cells[COLUMN_FAMILY_ID1][COL_NAME1][0].value, CELL_VAL4
        )

    def test_truncate_table(self):
        row_keys = [
            b"row_key_1",
            b"row_key_2",
            b"row_key_3",
            b"row_key_4",
            b"row_key_5",
            b"row_key_pr_1",
            b"row_key_pr_2",
            b"row_key_pr_3",
            b"row_key_pr_4",
            b"row_key_pr_5",
        ]

        for row_key in row_keys:
            row = self._table.row(row_key)
            row.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL1)
            row.commit()
            self.rows_to_delete.append(row)

        self._table.truncate(timeout=200)

        read_rows = self._table.yield_rows()

        for row in read_rows:
            self.assertNotIn(row.row_key.decode("utf-8"), row_keys)

    def test_drop_by_prefix_table(self):
        row_keys = [
            b"row_key_1",
            b"row_key_2",
            b"row_key_3",
            b"row_key_4",
            b"row_key_5",
            b"row_key_pr_1",
            b"row_key_pr_2",
            b"row_key_pr_3",
            b"row_key_pr_4",
            b"row_key_pr_5",
        ]

        for row_key in row_keys:
            row = self._table.row(row_key)
            row.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL1)
            row.commit()
            self.rows_to_delete.append(row)

        self._table.drop_by_prefix(row_key_prefix="row_key_pr", timeout=200)

        read_rows = self._table.yield_rows()
        expected_rows_count = 5
        read_rows_count = 0

        for row in read_rows:
            if row.row_key in row_keys:
                read_rows_count += 1

        self.assertEqual(expected_rows_count, read_rows_count)

    def test_yield_rows_with_row_set(self):
        row_keys = [
            b"row_key_1",
            b"row_key_2",
            b"row_key_3",
            b"row_key_4",
            b"row_key_5",
            b"row_key_6",
            b"row_key_7",
            b"row_key_8",
            b"row_key_9",
        ]

        rows = []
        for row_key in row_keys:
            row = self._table.row(row_key)
            row.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL1)
            rows.append(row)
            self.rows_to_delete.append(row)
        self._table.mutate_rows(rows)

        row_set = RowSet()
        row_set.add_row_range(RowRange(start_key=b"row_key_3", end_key=b"row_key_7"))
        row_set.add_row_key(b"row_key_1")

        read_rows = self._table.yield_rows(row_set=row_set)

        expected_row_keys = [
            b"row_key_1",
            b"row_key_3",
            b"row_key_4",
            b"row_key_5",
            b"row_key_6",
        ]
        found_row_keys = [row.row_key for row in read_rows]
        self.assertEqual(found_row_keys, expected_row_keys)

    def test_add_row_range_by_prefix_from_keys(self):
        row_keys = [
            b"row_key_1",
            b"row_key_2",
            b"row_key_3",
            b"row_key_4",
            b"sample_row_key_1",
            b"sample_row_key_2",
        ]

        rows = []
        for row_key in row_keys:
            row = self._table.row(row_key)
            row.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL1)
            rows.append(row)
            self.rows_to_delete.append(row)
        self._table.mutate_rows(rows)

        row_set = RowSet()
        row_set.add_row_range_with_prefix("row")

        read_rows = self._table.yield_rows(row_set=row_set)

        expected_row_keys = [
            b"row_key_1",
            b"row_key_2",
            b"row_key_3",
            b"row_key_4",
        ]
        found_row_keys = [row.row_key for row in read_rows]
        self.assertEqual(found_row_keys, expected_row_keys)

    def test_read_large_cell_limit(self):
        self._maybe_emulator_skip(
            "Maximum gRPC received message size for emulator is 4194304 bytes."
        )
        row = self._table.row(ROW_KEY)
        self.rows_to_delete.append(row)

        number_of_bytes = 10 * 1024 * 1024
        data = b"1" * number_of_bytes  # 10MB of 1's.
        row.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, data)
        row.commit()

        # Read back the contents of the row.
        partial_row_data = self._table.read_row(ROW_KEY)
        self.assertEqual(partial_row_data.row_key, ROW_KEY)
        cell = partial_row_data.cells[COLUMN_FAMILY_ID1]
        column = cell[COL_NAME1]
        self.assertEqual(len(column), 1)
        self.assertEqual(column[0].value, data)

    def test_read_row(self):
        row = self._table.row(ROW_KEY)
        self.rows_to_delete.append(row)

        cell1, cell2, cell3, cell4 = self._write_to_row(row, row, row, row)
        row.commit()

        # Read back the contents of the row.
        partial_row_data = self._table.read_row(ROW_KEY)
        self.assertEqual(partial_row_data.row_key, ROW_KEY)

        # Check the cells match.
        ts_attr = operator.attrgetter("timestamp")
        expected_row_contents = {
            COLUMN_FAMILY_ID1: {
                COL_NAME1: sorted([cell1, cell2], key=ts_attr, reverse=True),
                COL_NAME2: [cell3],
            },
            COLUMN_FAMILY_ID2: {COL_NAME3: [cell4]},
        }
        self.assertEqual(partial_row_data.cells, expected_row_contents)

    def test_read_rows(self):
        row = self._table.row(ROW_KEY)
        row_alt = self._table.row(ROW_KEY_ALT)
        self.rows_to_delete.extend([row, row_alt])

        cell1, cell2, cell3, cell4 = self._write_to_row(row, row_alt, row, row_alt)
        row.commit()
        row_alt.commit()

        rows_data = self._table.read_rows()
        self.assertEqual(rows_data.rows, {})
        rows_data.consume_all()

        # NOTE: We should refrain from editing protected data on instances.
        #       Instead we should make the values public or provide factories
        #       for constructing objects with them.
        row_data = PartialRowData(ROW_KEY)
        row_data._chunks_encountered = True
        row_data._committed = True
        row_data._cells = {COLUMN_FAMILY_ID1: {COL_NAME1: [cell1], COL_NAME2: [cell3]}}

        row_alt_data = PartialRowData(ROW_KEY_ALT)
        row_alt_data._chunks_encountered = True
        row_alt_data._committed = True
        row_alt_data._cells = {
            COLUMN_FAMILY_ID1: {COL_NAME1: [cell2]},
            COLUMN_FAMILY_ID2: {COL_NAME3: [cell4]},
        }

        expected_rows = {ROW_KEY: row_data, ROW_KEY_ALT: row_alt_data}
        self.assertEqual(rows_data.rows, expected_rows)

    def test_read_with_label_applied(self):
        self._maybe_emulator_skip("Labels not supported by Bigtable emulator")
        row = self._table.row(ROW_KEY)
        self.rows_to_delete.append(row)

        cell1, _, cell3, _ = self._write_to_row(row, None, row)
        row.commit()

        # Combine a label with column 1.
        label1 = "label-red"
        label1_filter = ApplyLabelFilter(label1)
        col1_filter = ColumnQualifierRegexFilter(COL_NAME1)
        chain1 = RowFilterChain(filters=[col1_filter, label1_filter])

        # Combine a label with column 2.
        label2 = "label-blue"
        label2_filter = ApplyLabelFilter(label2)
        col2_filter = ColumnQualifierRegexFilter(COL_NAME2)
        chain2 = RowFilterChain(filters=[col2_filter, label2_filter])

        # Bring our two labeled columns together.
        row_filter = RowFilterUnion(filters=[chain1, chain2])
        partial_row_data = self._table.read_row(ROW_KEY, filter_=row_filter)
        self.assertEqual(partial_row_data.row_key, ROW_KEY)

        cells_returned = partial_row_data.cells
        col_fam1 = cells_returned.pop(COLUMN_FAMILY_ID1)
        # Make sure COLUMN_FAMILY_ID1 was the only key.
        self.assertEqual(len(cells_returned), 0)

        (cell1_new,) = col_fam1.pop(COL_NAME1)
        (cell3_new,) = col_fam1.pop(COL_NAME2)
        # Make sure COL_NAME1 and COL_NAME2 were the only keys.
        self.assertEqual(len(col_fam1), 0)

        # Check that cell1 has matching values and gained a label.
        self.assertEqual(cell1_new.value, cell1.value)
        self.assertEqual(cell1_new.timestamp, cell1.timestamp)
        self.assertEqual(cell1.labels, [])
        self.assertEqual(cell1_new.labels, [label1])

        # Check that cell3 has matching values and gained a label.
        self.assertEqual(cell3_new.value, cell3.value)
        self.assertEqual(cell3_new.timestamp, cell3.timestamp)
        self.assertEqual(cell3.labels, [])
        self.assertEqual(cell3_new.labels, [label2])

    def test_access_with_non_admin_client(self):
        client = Client(admin=False)
        instance = client.instance(INSTANCE_ID_DATA)
        table = instance.table(self._table.table_id)
        self.assertIsNone(table.read_row("nonesuch"))
