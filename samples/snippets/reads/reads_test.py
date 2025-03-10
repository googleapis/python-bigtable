# Copyright 2020, Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import os
import inspect
import uuid

import pytest

from .snapshots.snap_reads_test import snapshots
from . import read_snippets
from ...utils import create_table_cm


PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BIGTABLE_INSTANCE = os.environ["BIGTABLE_INSTANCE"]
TABLE_ID = f"mobile-time-series-reads-{str(uuid.uuid4())[:16]}"


@pytest.fixture(scope="module", autouse=True)
def table_id():
    with create_table_cm(PROJECT, BIGTABLE_INSTANCE, TABLE_ID, {"stats_summary": None}) as table:
        timestamp = datetime.datetime(2019, 5, 1)
        rows = [
            table.direct_row("phone#4c410523#20190501"),
            table.direct_row("phone#4c410523#20190502"),
            table.direct_row("phone#4c410523#20190505"),
            table.direct_row("phone#5c10102#20190501"),
            table.direct_row("phone#5c10102#20190502"),
        ]

        rows[0].set_cell("stats_summary", "connected_cell", 1, timestamp)
        rows[0].set_cell("stats_summary", "connected_wifi", 1, timestamp)
        rows[0].set_cell("stats_summary", "os_build", "PQ2A.190405.003", timestamp)
        rows[1].set_cell("stats_summary", "connected_cell", 1, timestamp)
        rows[1].set_cell("stats_summary", "connected_wifi", 1, timestamp)
        rows[1].set_cell("stats_summary", "os_build", "PQ2A.190405.004", timestamp)
        rows[2].set_cell("stats_summary", "connected_cell", 0, timestamp)
        rows[2].set_cell("stats_summary", "connected_wifi", 1, timestamp)
        rows[2].set_cell("stats_summary", "os_build", "PQ2A.190406.000", timestamp)
        rows[3].set_cell("stats_summary", "connected_cell", 1, timestamp)
        rows[3].set_cell("stats_summary", "connected_wifi", 1, timestamp)
        rows[3].set_cell("stats_summary", "os_build", "PQ2A.190401.002", timestamp)
        rows[4].set_cell("stats_summary", "connected_cell", 1, timestamp)
        rows[4].set_cell("stats_summary", "connected_wifi", 0, timestamp)
        rows[4].set_cell("stats_summary", "os_build", "PQ2A.190406.000", timestamp)

        table.mutate_rows(rows)

        yield TABLE_ID


def test_read_row(capsys, table_id):
    read_snippets.read_row(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


def test_read_row_partial(capsys, table_id):
    read_snippets.read_row_partial(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


def test_read_rows(capsys, table_id):
    read_snippets.read_rows(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


def test_read_row_range(capsys, table_id):
    read_snippets.read_row_range(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


def test_read_row_ranges(capsys, table_id):
    read_snippets.read_row_ranges(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


def test_read_prefix(capsys, table_id):
    read_snippets.read_prefix(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


def test_read_filter(capsys, table_id):
    read_snippets.read_filter(PROJECT, BIGTABLE_INSTANCE, table_id)

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected
