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
import time

import inspect
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from .snapshots.snap_filters_test import snapshots

from . import filter_snippets_async
from google.cloud._helpers import (
    _microseconds_from_datetime,
    _datetime_from_microseconds,
)

PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BIGTABLE_INSTANCE = os.environ["BIGTABLE_INSTANCE"]
TABLE_ID_PREFIX = "mobile-time-series-{}"


@pytest_asyncio.fixture
async def table_id() -> AsyncGenerator[str, None]:
    table_id = _create_table()
    await _populate_table(table_id)
    yield table_id
    _delete_table(table_id)


def _create_table():
    from google.cloud import bigtable
    import uuid

    client = bigtable.Client(project=PROJECT, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE)

    table_id = TABLE_ID_PREFIX.format(str(uuid.uuid4())[:16])
    table = instance.table(table_id)
    if table.exists():
        table.delete()

    table.create(column_families={"stats_summary": None, "cell_plan": None})
    return table_id


def _delete_table(table_id: str):
    from google.cloud import bigtable

    client = bigtable.Client(project=PROJECT, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE)
    table = instance.table(table_id)
    table.delete()


async def _populate_table(table_id):
    from google.cloud.bigtable.data import (
        BigtableDataClientAsync,
        RowMutationEntry,
        SetCell,
    )

    timestamp = datetime.datetime(2019, 5, 1)
    timestamp_minus_hr = timestamp - datetime.timedelta(hours=1)

    async with (BigtableDataClientAsync(project=PROJECT) as client):
        async with client.get_table(BIGTABLE_INSTANCE, table_id) as table:
            async with table.mutations_batcher() as batcher:
                await batcher.append(
                    RowMutationEntry(
                        "phone#4c410523#20190501",
                        [
                            SetCell(
                                "stats_summary",
                                "connected_cell",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "connected_cell",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "connected_wifi",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "os_build",
                                "PQ2A.190405.003",
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "cell_plan",
                                "data_plan_01gb",
                                "true",
                                _microseconds_from_datetime(timestamp_minus_hr),
                            ),
                            SetCell(
                                "cell_plan",
                                "data_plan_01gb",
                                "false",
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "cell_plan",
                                "data_plan_05gb",
                                "true",
                                _microseconds_from_datetime(timestamp),
                            ),
                        ],
                    )
                )
                await batcher.append(
                    RowMutationEntry(
                        "phone#4c410523#20190502",
                        [
                            SetCell(
                                "stats_summary",
                                "connected_cell",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "connected_wifi",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "os_build",
                                "PQ2A.190405.004",
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "cell_plan",
                                "data_plan_05gb",
                                "true",
                                _microseconds_from_datetime(timestamp),
                            ),
                        ],
                    )
                )
                await batcher.append(
                    RowMutationEntry(
                        "phone#4c410523#20190505",
                        [
                            SetCell(
                                "stats_summary",
                                "connected_cell",
                                0,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "connected_wifi",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "os_build",
                                "PQ2A.190406.000",
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "cell_plan",
                                "data_plan_05gb",
                                "true",
                                _microseconds_from_datetime(timestamp),
                            ),
                        ],
                    )
                )
                await batcher.append(
                    RowMutationEntry(
                        "phone#5c10102#20190501",
                        [
                            SetCell(
                                "stats_summary",
                                "connected_cell",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "connected_wifi",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "os_build",
                                "PQ2A.190401.002",
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "cell_plan",
                                "data_plan_10gb",
                                "true",
                                _microseconds_from_datetime(timestamp),
                            ),
                        ],
                    )
                )
                await batcher.append(
                    RowMutationEntry(
                        "phone#5c10102#20190502",
                        [
                            SetCell(
                                "stats_summary",
                                "connected_cell",
                                1,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "connected_wifi",
                                0,
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "stats_summary",
                                "os_build",
                                "PQ2A.190406.000",
                                _microseconds_from_datetime(timestamp),
                            ),
                            SetCell(
                                "cell_plan",
                                "data_plan_10gb",
                                "true",
                                _microseconds_from_datetime(timestamp),
                            ),
                        ],
                    )
                )


def _datetime_to_micros(value: datetime.datetime) -> int:
    """Uses the same conversion rules as the old client in"""
    if not value.tzinfo:
        value = value.replace(tzinfo=datetime.timezone.utc)
    # Regardless of what timezone is on the value, convert it to UTC.
    value = value.astimezone(datetime.timezone.utc)
    # Convert the datetime to a microsecond timestamp.
    return int(calendar.timegm(value.timetuple()) * 1e6) + value.microsecond
    return int(dt.timestamp() * 1000 * 1000)


@pytest.mark.asyncio
async def test_filter_limit_row_sample(capsys, table_id):
    await filter_snippets_async.filter_limit_row_sample(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    assert "Reading data for" in out


@pytest.mark.asyncio
async def test_filter_limit_row_regex(capsys, table_id):
    await filter_snippets_async.filter_limit_row_regex(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_cells_per_col(capsys, table_id):
    await filter_snippets_async.filter_limit_cells_per_col(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_cells_per_row(capsys, table_id):
    await filter_snippets_async.filter_limit_cells_per_row(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_cells_per_row_offset(capsys, table_id):
    await filter_snippets_async.filter_limit_cells_per_row_offset(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_col_family_regex(capsys, table_id):
    await filter_snippets_async.filter_limit_col_family_regex(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_col_qualifier_regex(capsys, table_id):
    await filter_snippets_async.filter_limit_col_qualifier_regex(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_col_range(capsys, table_id):
    await filter_snippets_async.filter_limit_col_range(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_value_range(capsys, table_id):
    await filter_snippets_async.filter_limit_value_range(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_value_regex(capsys, table_id):
    await filter_snippets_async.filter_limit_value_regex(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_timestamp_range(capsys, table_id):
    await filter_snippets_async.filter_limit_timestamp_range(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_block_all(capsys, table_id):
    await filter_snippets_async.filter_limit_block_all(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_limit_pass_all(capsys, table_id):
    await filter_snippets_async.filter_limit_pass_all(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_modify_strip_value(capsys, table_id):
    await filter_snippets_async.filter_modify_strip_value(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_modify_apply_label(capsys, table_id):
    await filter_snippets_async.filter_modify_apply_label(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_composing_chain(capsys, table_id):
    await filter_snippets_async.filter_composing_chain(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_composing_interleave(capsys, table_id):
    await filter_snippets_async.filter_composing_interleave(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected


@pytest.mark.asyncio
async def test_filter_composing_condition(capsys, table_id):
    await filter_snippets_async.filter_composing_condition(
        PROJECT, BIGTABLE_INSTANCE, table_id
    )

    out, _ = capsys.readouterr()
    expected = snapshots[inspect.currentframe().f_code.co_name]
    assert out == expected
